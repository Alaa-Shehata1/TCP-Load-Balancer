package balancer

import (
	"testing"

	"github.com/alaa157/tcp-load-balancer/internal/config"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

func testPool() *pool.Pool {
	return pool.New([]config.BackendConfig{
		{Name: "a", Host: "127.0.0.1", Port: 9001, Addr: "a"},
		{Name: "b", Host: "127.0.0.1", Port: 9002, Addr: "b"},
	})
}

func TestRoundRobin_Cycles(t *testing.T) {
	p := testPool()
	b, err := New("round-robin", p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := b.Next().Name; got != "a" {
		t.Fatalf("got %s", got)
	}
	if got := b.Next().Name; got != "b" {
		t.Fatalf("got %s", got)
	}
	if got := b.Next().Name; got != "a" {
		t.Fatalf("got %s", got)
	}
}

func TestLeastConn_PicksMin(t *testing.T) {
	p := testPool()
	p.AddConn("b")
	p.AddConn("b")
	b, err := New("least-conn", p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := b.Next().Name; got != "a" {
		t.Fatalf("want a got %s", got)
	}
}

func TestSkipsUnhealthy(t *testing.T) {
	p := testPool()
	p.MarkUnhealthy("a")
	b, err := New("round-robin", p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 0; i < 4; i++ {
		if got := b.Next().Name; got != "b" {
			t.Fatalf("got %s", got)
		}
	}
}

func TestNoHealthy_ReturnsNil(t *testing.T) {
	p := testPool()
	p.MarkUnhealthy("a")
	p.MarkUnhealthy("b")
	b, err := New("round-robin", p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := b.Next(); got != nil {
		t.Fatalf("want nil got %+v", got)
	}
}

func TestUnknownAlgorithm_Errors(t *testing.T) {
	if _, err := New("bogus", testPool()); err == nil {
		t.Fatal("want error")
	}
}
