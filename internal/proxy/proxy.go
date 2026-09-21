// Package proxy accepts client TCP connections and pipes them to a chosen backend.
package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
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
}

// New builds a proxy server. m may be nil, in which case no metrics
// are recorded.
func New(p *pool.Pool, b balancer.Balancer, dialTimeout, idleTimeout time.Duration, log *slog.Logger, m *metrics.Metrics) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{p: p, b: b, dialTimeout: dialTimeout, idleTimeout: idleTimeout, log: log, m: m}
}

// Serve accepts connections until the listener closes.
func (s *Server) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		go s.HandleConn(c)
	}
}

func closeWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
}

// HandleConn picks a backend, dials with timeout (one retry on next backend),
// then pipes bytes both ways with idle deadlines. It records exactly one
// connections_total outcome (no_healthy, dial_failed, or ok) and keeps the
// backend gauge accurate via a single deferred cleanup.
func (s *Server) HandleConn(client net.Conn) {
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
	deadline := time.Now().Add(s.idleTimeout)
	_ = client.SetDeadline(deadline)
	_ = up.SetDeadline(deadline)

	var tx, rx atomic.Int64
	done := make(chan struct{}, 2)
	go func() {
		n, _ := io.Copy(up, client)
		tx.Add(n)
		closeWrite(up)
		done <- struct{}{}
	}()
	go func() {
		n, _ := io.Copy(client, up)
		rx.Add(n)
		closeWrite(client)
		done <- struct{}{}
	}()
	<-done
	_ = client.Close()
	_ = up.Close()
	<-done // both copies always signal; closing unblocks the other side
	s.m.AddTx(label, float64(tx.Load()))
	s.m.AddRx(label, float64(rx.Load()))
	s.m.ConnResult(label, "ok")
	s.log.Info("proxy done", "conn", id, "backend", be.Addr)
}
