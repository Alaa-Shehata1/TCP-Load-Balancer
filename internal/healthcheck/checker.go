// Package healthcheck actively dials backends and ejects/readmits them.
package healthcheck

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/alaa157/tcp-load-balancer/internal/metrics"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Start launches a checker goroutine per backend. It marks a backend
// unhealthy after threshold consecutive dial failures and readmits it
// on the first successful dial. Stops when ctx is done.
func Start(ctx context.Context, p *pool.Pool, interval, timeout time.Duration, threshold int, m *metrics.Metrics) {
	if threshold <= 0 {
		threshold = 2
	}
	for _, b := range p.All() {
		go checkLoop(ctx, p, b, interval, timeout, threshold, m)
	}
}

func checkLoop(ctx context.Context, p *pool.Pool, b *pool.Backend, interval, timeout time.Duration, threshold int, m *metrics.Metrics) {
	fails := 0
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c, err := net.DialTimeout("tcp", b.Addr, timeout)
			if err != nil {
				fails++
				m.HealthFailed(metrics.BackendLabel(b.Name, b.Addr))
				if fails >= threshold {
					p.MarkUnhealthy(b.Addr)
					slog.Warn("healthcheck failed", "backend", b.Addr, "fails", fails, "err", err)
				}
				continue
			}
			_ = c.Close()
			if fails >= threshold {
				slog.Info("healthcheck recovered", "backend", b.Addr)
			}
			fails = 0
			p.MarkHealthy(b.Addr)
		}
	}
}
