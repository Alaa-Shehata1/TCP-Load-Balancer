// Package proxy accepts client TCP connections and pipes them to a chosen backend.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alaa157/tcp-load-balancer/internal/balancer"
	"github.com/alaa157/tcp-load-balancer/internal/metrics"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Server proxies clients to backends chosen by Balancer.
type Server struct {
	p           *pool.Pool
	b           balancer.Balancer
	dialTimeout time.Duration
	idleTimeout time.Duration
	log         *slog.Logger
	m           *metrics.Metrics
	conns       atomic.Uint64
	lifecycleMu sync.Mutex
	draining    bool
	wg          sync.WaitGroup
}

// New builds a proxy server. m may be nil, in which case no metrics
// are recorded.
func New(p *pool.Pool, b balancer.Balancer, dialTimeout, idleTimeout time.Duration, log *slog.Logger, m *metrics.Metrics) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{p: p, b: b, dialTimeout: dialTimeout, idleTimeout: idleTimeout, log: log, m: m}
}

// Serve accepts connections until the listener closes. Closing the
// listener is the normal stop signal and returns nil.
func (s *Server) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		s.lifecycleMu.Lock()
		if s.draining {
			s.lifecycleMu.Unlock()
			_ = c.Close()
			return nil
		}
		s.wg.Add(1)
		s.lifecycleMu.Unlock()
		go s.HandleConn(c)
	}
}

// Shutdown waits for in-flight handlers to finish. The caller must close
// the listener first so no new handlers start. It returns ctx.Err() if the
// drain deadline expires first. The internal waiter goroutine always
// terminates: handlers are bounded by the idle timeout, so Wait returns.
func (s *Server) Shutdown(ctx context.Context) error {
	s.lifecycleMu.Lock()
	s.draining = true
	s.lifecycleMu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func closeWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
}

// readIdle refreshes the wrapped connection's read deadline whenever a
// read makes progress. Use it on the source side of io.Copy. It records
// the first non-EOF read error for failure attribution; each instance is
// owned by a single copy goroutine.
type readIdle struct {
	c       net.Conn
	idle    time.Duration
	readErr error
}

func (r *readIdle) Read(b []byte) (int, error) {
	n, err := r.c.Read(b)
	if n > 0 {
		_ = r.c.SetReadDeadline(time.Now().Add(r.idle))
	}
	if err != nil && err != io.EOF && r.readErr == nil {
		r.readErr = err
	}
	return n, err
}

// writeIdle refreshes the wrapped connection's write deadline whenever a
// write makes progress. Use it on the destination side of io.Copy.
type writeIdle struct {
	c        net.Conn
	idle     time.Duration
	writeErr error
}

func (w *writeIdle) Write(b []byte) (int, error) {
	n, err := w.c.Write(b)
	if n > 0 {
		_ = w.c.SetWriteDeadline(time.Now().Add(w.idle))
	}
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	return n, err
}

// backendFault reports whether err identifies a backend failure: a reset,
// refused connection, timeout, or other I/O error. Clean EOF, a nil error,
// and our own cleanup closes (net.ErrClosed) are not backend faults.
func backendFault(err error) bool {
	return err != nil && err != io.EOF && !errors.Is(err, net.ErrClosed)
}

// isIdleTimeout reports whether err is a deadline-exceeded network error.
func isIdleTimeout(err error) bool {
	var ne net.Error
	return err != nil && errors.As(err, &ne) && ne.Timeout()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// HandleConn picks a backend, dials with timeout (one retry on next backend),
// then pipes bytes both ways with inactivity deadlines. It records exactly one
// connections_total outcome (no_healthy, dial_failed, or ok) and keeps the
// backend gauge accurate via a single deferred cleanup.
//
// Stream semantics are allowHalfOpen=false: when either copy direction ends
// (EOF, idle timeout, or error), both connections are closed after
// propagating any possible CloseWrite. A pinned stream is never migrated to
// another backend.
func (s *Server) HandleConn(client net.Conn) {
	defer s.wg.Done()
	id := s.conns.Add(1)
	be := s.b.Next()
	if be == nil {
		s.log.Warn("no healthy backend", "conn", id)
		s.m.ConnResult("none", "no_healthy")
		_ = client.Close()
		return
	}
	label := metrics.BackendLabel(be.Name, be.Addr)
	d := &net.Dialer{Timeout: s.dialTimeout}
	up, err := d.DialContext(context.Background(), "tcp", be.Addr)
	if err != nil {
		s.log.Warn("dial failed, retry next", "conn", id, "backend", be.Addr, "err", err)
		s.p.MarkUnhealthy(be.Addr)
		be2 := s.b.Next()
		if be2 == nil || be2.Addr == be.Addr {
			s.m.ConnResult(label, "dial_failed")
			_ = client.Close()
			return
		}
		up, err = d.DialContext(context.Background(), "tcp", be2.Addr)
		if err != nil {
			s.log.Warn("retry dial failed", "conn", id, "backend", be2.Addr, "err", err)
			s.p.MarkUnhealthy(be2.Addr)
			s.m.ConnResult(metrics.BackendLabel(be2.Name, be2.Addr), "dial_failed")
			_ = client.Close()
			return
		}
		be = be2
		label = metrics.BackendLabel(be.Name, be.Addr)
	}
	s.p.AddConn(be.Addr)
	s.m.BackendInc(label)
	defer func() {
		s.p.DoneConn(be.Addr)
		s.m.BackendDec(label)
	}()

	s.log.Info("proxy start", "conn", id, "backend", be.Addr)
	// Inactivity deadlines: each copy direction refreshes its own
	// read/write timers on progress, so a continuously active stream
	// never expires while a fully idle one closes after idleTimeout.
	start := time.Now().Add(s.idleTimeout)
	_ = client.SetReadDeadline(start)
	_ = client.SetWriteDeadline(start)
	_ = up.SetReadDeadline(start)
	_ = up.SetWriteDeadline(start)

	var tx, rx atomic.Int64
	var errC2B, errB2C error
	c2bSrc := &readIdle{c: client, idle: s.idleTimeout}
	c2bDst := &writeIdle{c: up, idle: s.idleTimeout}
	b2cSrc := &readIdle{c: up, idle: s.idleTimeout}
	b2cDst := &writeIdle{c: client, idle: s.idleTimeout}
	done := make(chan struct{}, 2)
	go func() {
		var n int64
		n, errC2B = io.Copy(c2bDst, c2bSrc)
		tx.Add(n)
		closeWrite(up)
		done <- struct{}{}
	}()
	go func() {
		var n int64
		n, errB2C = io.Copy(b2cDst, b2cSrc)
		rx.Add(n)
		closeWrite(client)
		done <- struct{}{}
	}()
	<-done
	_ = client.Close()
	_ = up.Close()
	<-done // both copies always signal; closing unblocks the other side
	// Passive failure marking: backend-side I/O errors (reset, refused,
	// timeout) eject the backend for new connections. Clean EOF (normal
	// close by either side), our own cleanup closes, and pure
	// client-side disconnects never mark. Timeouts count: a backend that
	// cannot produce or accept bytes within the idle window is suspect;
	// the active checker readmits it on the next successful dial.
	if backendFault(b2cSrc.readErr) || backendFault(c2bDst.writeErr) {
		s.p.MarkUnhealthy(be.Addr)
		s.log.Warn("proxy ejecting backend after stream failure",
			"conn", id, "backend", be.Addr,
			"backendReadErr", errString(b2cSrc.readErr),
			"backendWriteErr", errString(c2bDst.writeErr))
	}
	s.m.AddTx(label, float64(tx.Load()))
	s.m.AddRx(label, float64(rx.Load()))
	s.m.ConnResult(label, "ok")
	doneAttrs := []any{"conn", id, "backend", be.Addr, "tx", tx.Load(), "rx", rx.Load()}
	switch {
	case isIdleTimeout(errC2B) || isIdleTimeout(errB2C):
		s.log.Info("proxy done: idle timeout", append(doneAttrs,
			"c2bErr", errString(errC2B), "b2cErr", errString(errB2C))...)
	case errC2B != nil || errB2C != nil:
		s.log.Warn("proxy done: stream error", append(doneAttrs,
			"c2bErr", errString(errC2B), "b2cErr", errString(errB2C))...)
	default:
		s.log.Info("proxy done", doneAttrs...)
	}
}
