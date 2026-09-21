// Package balancer picks a healthy backend per connection.
package balancer

import (
	"fmt"
	"sync/atomic"

	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Balancer chooses the backend for each new client connection.
type Balancer interface {
	Next() *pool.Backend
}

type roundRobin struct {
	p *pool.Pool
	n atomic.Uint64
}

func (r *roundRobin) Next() *pool.Backend {
	h := r.p.Healthy()
	if len(h) == 0 {
		return nil
	}
	i := r.n.Add(1) - 1
	return h[int(i%uint64(len(h)))]
}

type leastConn struct {
	p *pool.Pool
}

func (l *leastConn) Next() *pool.Backend {
	h := l.p.Healthy()
	if len(h) == 0 {
		return nil
	}
	best := h[0]
	for _, b := range h[1:] {
		if b.Active() < best.Active() {
			best = b
		}
	}
	return best
}

// New builds a balancer by name: "round-robin" or "least-conn".
func New(algo string, p *pool.Pool) (Balancer, error) {
	switch algo {
	case "round-robin":
		return &roundRobin{p: p}, nil
	case "least-conn":
		return &leastConn{p: p}, nil
	default:
		return nil, fmt.Errorf("unknown algorithm %q", algo)
	}
}
