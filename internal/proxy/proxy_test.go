package proxy

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alaa157/tcp-load-balancer/internal/balancer"
	"github.com/alaa157/tcp-load-balancer/internal/config"
	"github.com/alaa157/tcp-load-balancer/internal/metrics"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

func startEcho(t *testing.T, id string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				_, _ = fmt.Fprintf(conn, "served-by:%s\n", id)
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()
	return ln.Addr().String()
}

func startProxy(t *testing.T, p *pool.Pool, b balancer.Balancer) (string, *metrics.Metrics) {
	t.Helper()
	_, m := metrics.New(p)
	s := New(p, b, 2*time.Second, 10*time.Second, slog.Default(), m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = s.Serve(ln) }()
	return ln.Addr().String(), m
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestProxy_ForwardsToBackend(t *testing.T) {
	a1 := startEcho(t, "A")
	a2 := startEcho(t, "B")
	p := pool.New([]config.BackendConfig{
		{Name: "a", Addr: a1},
		{Name: "b", Addr: a2},
	})
	b, _ := balancer.New("round-robin", p)
	addr, _ := startProxy(t, p, b)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)
	line, err := rd.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "served-by:") {
		t.Fatalf("banner=%q err=%v", line, err)
	}
	if _, err := fmt.Fprintf(conn, "hello\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	echo, err := rd.ReadString('\n')
	if err != nil || strings.TrimSpace(echo) != "hello" {
		t.Fatalf("echo=%q err=%v", echo, err)
	}
}

func TestProxy_NoHealthy_ClosesFast(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	p.MarkUnhealthy(a1)
	b, _ := balancer.New("round-robin", p)
	addr, _ := startProxy(t, p, b)

	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("want close/error when no healthy backend")
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Fatalf("took too long: %v", time.Since(start))
	}
}

func TestProxy_SkipsDeadBackend(t *testing.T) {
	good := startEcho(t, "GOOD")
	// dead port: listen then close to get a free-but-closed addr
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	_ = ln.Close()

	p := pool.New([]config.BackendConfig{
		{Name: "dead", Addr: dead},
		{Name: "good", Addr: good},
	})
	// Force RR to hit dead first: fresh RR starts at index 0 = dead
	b, _ := balancer.New("round-robin", p)
	addr, _ := startProxy(t, p, b)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)
	line, err := rd.ReadString('\n')
	if err != nil || !strings.Contains(line, "GOOD") {
		t.Fatalf("want GOOD backend, got %q err=%v", line, err)
	}
}

func TestProxy_RecordsOkMetrics(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	b, _ := balancer.New("round-robin", p)
	addr, m := startProxy(t, p, b)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)
	banner, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("banner: %v", err)
	}
	if _, err := fmt.Fprintf(conn, "hello\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if echo, err := rd.ReadString('\n'); err != nil || strings.TrimSpace(echo) != "hello" {
		t.Fatalf("echo=%q err=%v", echo, err)
	}
	_ = conn.Close()

	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(m.Connections.WithLabelValues("a", "ok")) == 1
	}, "connections_total{backend=a,result=ok}==1")
	if got := testutil.ToFloat64(m.BytesTx.WithLabelValues("a")); got != 6 {
		t.Fatalf("bytes_tx=%v want 6", got)
	}
	wantRx := float64(len(banner) + len("hello\n"))
	if got := testutil.ToFloat64(m.BytesRx.WithLabelValues("a")); got != wantRx {
		t.Fatalf("bytes_rx=%v want %v", got, wantRx)
	}
	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(m.BackendConnections.WithLabelValues("a")) == 0
	}, "backend_connections==0")
}

func TestProxy_RecordsNoHealthy(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	p.MarkUnhealthy(a1)
	b, _ := balancer.New("round-robin", p)
	addr, m := startProxy(t, p, b)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = io.Copy(io.Discard, conn)
	_ = conn.Close()

	waitFor(t, 2*time.Second, func() bool {
		return testutil.ToFloat64(m.Connections.WithLabelValues("none", "no_healthy")) == 1
	}, "connections_total{result=no_healthy}==1")
}
