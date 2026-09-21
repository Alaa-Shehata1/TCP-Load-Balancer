// Package pool tracks backend health and active connection counts.
package pool

import (
	"sync"
	"sync/atomic"

	"github.com/alaa157/tcp-load-balancer/internal/config"
)

// Backend is one upstream server.
type Backend struct {
	Name string
	Addr string

	mu      sync.RWMutex
	healthy bool
	active  atomic.Int64
}

// IsHealthy reports whether the backend should receive traffic.
func (b *Backend) IsHealthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.healthy
}

// Active returns current proxied connection count.
func (b *Backend) Active() int64 {
	return b.active.Load()
}

// Pool is the set of backends in config order.
type Pool struct {
	mu     sync.RWMutex
	byAddr map[string]*Backend
	order  []string
}

// New builds a pool with all backends healthy, preserving config order.
// config.Load rejects duplicate addresses; as a defensive rule for direct
// callers, later entries with an already-seen address are skipped so one
// backend can never appear twice.
func New(cfgs []config.BackendConfig) *Pool {
	p := &Pool{byAddr: make(map[string]*Backend, len(cfgs))}
	for _, c := range cfgs {
		if _, ok := p.byAddr[c.Addr]; ok {
			continue
		}
		p.byAddr[c.Addr] = &Backend{Name: c.Name, Addr: c.Addr, healthy: true}
		p.order = append(p.order, c.Addr)
	}
	return p
}

// Healthy returns healthy backends in config order.
func (p *Pool) Healthy() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []*Backend
	for _, addr := range p.order {
		b := p.byAddr[addr]
		if b.IsHealthy() {
			out = append(out, b)
		}
	}
	return out
}

// All returns all backends in config order (healthy or not).
func (p *Pool) All() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Backend, 0, len(p.order))
	for _, addr := range p.order {
		out = append(out, p.byAddr[addr])
	}
	return out
}

// MarkUnhealthy excludes a backend from picking.
func (p *Pool) MarkUnhealthy(addr string) {
	p.mu.RLock()
	b, ok := p.byAddr[addr]
	p.mu.RUnlock()
	if !ok {
		return
	}
	b.mu.Lock()
	b.healthy = false
	b.mu.Unlock()
}

// MarkHealthy readmits a backend.
func (p *Pool) MarkHealthy(addr string) {
	p.mu.RLock()
	b, ok := p.byAddr[addr]
	p.mu.RUnlock()
	if !ok {
		return
	}
	b.mu.Lock()
	b.healthy = true
	b.mu.Unlock()
}

// AddConn increments the active count.
func (p *Pool) AddConn(addr string) {
	p.mu.RLock()
	b, ok := p.byAddr[addr]
	p.mu.RUnlock()
	if ok {
		b.active.Add(1)
	}
}

// DoneConn decrements the active count.
func (p *Pool) DoneConn(addr string) {
	p.mu.RLock()
	b, ok := p.byAddr[addr]
	p.mu.RUnlock()
	if ok {
		b.active.Add(-1)
	}
}
