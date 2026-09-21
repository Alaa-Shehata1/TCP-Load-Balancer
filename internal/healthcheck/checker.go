// Package healthcheck actively dials backends and ejects/readmits them.
package healthcheck

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Start launches a checker goroutine per backend. It marks a backend
// unhealthy after threshold consecutive dial failures and readmits it
// on the first successful dial. Stops when ctx is done.
func Start(ctx context.Context, p *pool.Pool, interval, timeout time.Duration, threshold int) {
	if threshold <= 0 {
		threshold = 2
	}
	for _, b := range p.All() {
		go checkLoop(ctx, p, b.Addr, interval, timeout, threshold)
	}
}

func checkLoop(ctx context.Context, p *pool.Pool, addr string, interval, timeout time.Duration, threshold int) {
	fails := 0
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c, err := net.DialTimeout("tcp", addr, timeout)
			if err != nil {
				fails++
				if fails >= threshold {
					p.MarkUnhealthy(addr)
					slog.Warn("healthcheck failed", "backend", addr, "fails", fails, "err", err)
				}
				continue
			}
			_ = c.Close()
			if fails >= threshold {
				slog.Info("healthcheck recovered", "backend", addr)
			}
			fails = 0
			p.MarkHealthy(addr)
		}
	}
}
