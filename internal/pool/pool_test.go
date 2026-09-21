package pool

import (
	"testing"

	"github.com/alaa157/tcp-load-balancer/internal/config"
)

func testBackends() []config.BackendConfig {
	return []config.BackendConfig{
		{Name: "a", Host: "127.0.0.1", Port: 9001, Addr: "127.0.0.1:9001"},
		{Name: "b", Host: "127.0.0.1", Port: 9002, Addr: "127.0.0.1:9002"},
	}
}

func TestHealthy_AllByDefault(t *testing.T) {
	p := New(testBackends())
	if got := len(p.Healthy()); got != 2 {
		t.Fatalf("want 2 healthy got %d", got)
	}
}

func TestMarkUnhealthy_Excluded(t *testing.T) {
	p := New(testBackends())
	p.MarkUnhealthy("127.0.0.1:9001")
	h := p.Healthy()
	if len(h) != 1 || h[0].Name != "b" {
		t.Fatalf("want only b, got %+v", h)
	}
	p.MarkHealthy("127.0.0.1:9001")
	if got := len(p.Healthy()); got != 2 {
		t.Fatalf("want 2 after rejoin got %d", got)
	}
}

func TestConnCounting(t *testing.T) {
	p := New(testBackends())
	p.AddConn("127.0.0.1:9002")
	p.AddConn("127.0.0.1:9002")
	h := p.Healthy()
	var b *Backend
	for _, x := range h {
		if x.Addr == "127.0.0.1:9002" {
			b = x
		}
	}
	if b == nil || b.Active() != 2 {
		t.Fatalf("want active=2 got %+v", b)
	}
	p.DoneConn("127.0.0.1:9002")
	if b.Active() != 1 {
		t.Fatalf("want active=1 got %d", b.Active())
	}
}
