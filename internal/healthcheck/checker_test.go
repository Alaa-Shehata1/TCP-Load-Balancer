package healthcheck

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alaa157/tcp-load-balancer/internal/config"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

func TestChecker_MarksDownAfterThreshold(t *testing.T) {
	p := pool.New([]config.BackendConfig{
		{Name: "dead", Addr: "127.0.0.1:1"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	Start(ctx, p, 10*time.Millisecond, 10*time.Millisecond, 2)
	time.Sleep(150 * time.Millisecond)
	if got := len(p.Healthy()); got != 0 {
		t.Fatalf("want 0 healthy got %d", got)
	}
}

func TestChecker_HealthyStaysHealthy(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	addr := ln.Addr().String()
	p := pool.New([]config.BackendConfig{{Name: "live", Addr: addr}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	Start(ctx, p, 10*time.Millisecond, 20*time.Millisecond, 2)
	time.Sleep(80 * time.Millisecond)
	if got := len(p.Healthy()); got != 1 {
		t.Fatalf("want 1 healthy got %d", got)
	}
}

func TestChecker_RejoinsWhenBack(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	defer ln.Close()

	p := pool.New([]config.BackendConfig{{Name: "flap", Addr: addr}})
	p.MarkUnhealthy(addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	Start(ctx, p, 10*time.Millisecond, 20*time.Millisecond, 2)
	time.Sleep(80 * time.Millisecond)
	if got := len(p.Healthy()); got != 1 {
		t.Fatalf("want rejoined 1 healthy got %d", got)
	}
}
