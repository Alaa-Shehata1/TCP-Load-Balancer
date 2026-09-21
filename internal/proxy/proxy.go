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
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Server proxies clients to backends chosen by Balancer.
type Server struct {
	p           *pool.Pool
	b           balancer.Balancer
	dialTimeout time.Duration
	idleTimeout time.Duration
	log         *slog.Logger
	conns       atomic.Uint64
}

// New builds a proxy server.
func New(p *pool.Pool, b balancer.Balancer, dialTimeout, idleTimeout time.Duration, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{p: p, b: b, dialTimeout: dialTimeout, idleTimeout: idleTimeout, log: log}
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
// then pipes bytes both ways with idle deadlines.
func (s *Server) HandleConn(client net.Conn) {
	id := s.conns.Add(1)
	be := s.b.Next()
	if be == nil {
		s.log.Warn("no healthy backend", "conn", id)
		client.Close()
		return
	}
	d := &net.Dialer{Timeout: s.dialTimeout}
	up, err := d.DialContext(context.Background(), "tcp", be.Addr)
	if err != nil {
		s.log.Warn("dial failed, retry next", "conn", id, "backend", be.Addr, "err", err)
		s.p.MarkUnhealthy(be.Addr)
		be2 := s.b.Next()
		if be2 == nil || be2.Addr == be.Addr {
			client.Close()
			return
		}
		up, err = d.DialContext(context.Background(), "tcp", be2.Addr)
		if err != nil {
			s.log.Warn("retry dial failed", "conn", id, "backend", be2.Addr, "err", err)
			s.p.MarkUnhealthy(be2.Addr)
			client.Close()
			return
		}
		be = be2
	}
	s.p.AddConn(be.Addr)
	defer s.p.DoneConn(be.Addr)

	s.log.Info("proxy start", "conn", id, "backend", be.Addr)
	deadline := time.Now().Add(s.idleTimeout)
	_ = client.SetDeadline(deadline)
	_ = up.SetDeadline(deadline)

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(up, client)
		closeWrite(up)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, up)
		closeWrite(client)
		done <- struct{}{}
	}()
	<-done
	client.Close()
	up.Close()
	s.log.Info("proxy done", "conn", id, "backend", be.Addr)
}
