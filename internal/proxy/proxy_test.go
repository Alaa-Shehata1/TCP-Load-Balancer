package proxy

import (
	"bufio"
	"context"
	"errors"
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
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	_ = ln.Close()

	p := pool.New([]config.BackendConfig{
		{Name: "dead", Addr: dead},
		{Name: "good", Addr: good},
	})
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

// startDrainProxy starts a proxy whose listener is closed only by the caller.
// The returned stop function unblocks Serve; call it to begin the drain.
func startDrainProxy(t *testing.T, p *pool.Pool, b balancer.Balancer) (*Server, func(), net.Listener) {
	t.Helper()
	_, m := metrics.New(p)
	s := New(p, b, 2*time.Second, 30*time.Second, slog.Default(), m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(ln) }()
	stopServe := func() { _ = ln.Close() }
	// Wait for listener to be accepting.
	for i := 0; i < 50; i++ {
		if c, err := net.DialTimeout("tcp", ln.Addr().String(), 10*time.Millisecond); err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return s, stopServe, ln
}

func openProxiedConn(t *testing.T, proxyAddr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	rd := bufio.NewReader(conn)
	if _, err := rd.ReadString('\n'); err != nil {
		_ = conn.Close()
		t.Fatalf("banner: %v", err)
	}
	return conn, rd
}

// TestProxy_DrainWaitsForHandlers verifies: no new accepts after listener close,
// existing handler stays usable, and Shutdown blocks until the handler exits.
func TestProxy_DrainWaitsForHandlers(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	b, _ := balancer.New("round-robin", p)
	s, stopServe, ln := startDrainProxy(t, p, b)
	proxyAddr := ln.Addr().String()

	conn, rd := openProxiedConn(t, proxyAddr)

	// Begin drain: stop accepting new connections.
	stopServe()
	if c2, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond); err == nil {
		_ = c2.Close()
		t.Fatal("accepted a connection after the listener closed")
	}

	// Existing connection must stay usable during the drain window.
	if _, err := fmt.Fprintf(conn, "ping\n"); err != nil {
		t.Fatalf("write during drain: %v", err)
	}
	if line, err := rd.ReadString('\n'); err != nil || strings.TrimSpace(line) != "ping" {
		t.Fatalf("echo during drain=%q err=%v", line, err)
	}

	// Shutdown must wait for the open handler.
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown(context.Background()) }()
	select {
	case <-time.After(100 * time.Millisecond):
		// still draining — correct so far
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned while a handler was still open: %v", err)
	}

	_ = conn.Close()
	done := make(chan struct{})
	go func() {
		_ = s.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after the handler closed")
	}
}

// TestProxy_DrainTimesOut verifies Shutdown returns ctx.DeadlineExceeded
// when a handler never exits (simulated by an unclosable backend).
func TestProxy_DrainTimesOut(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	b, _ := balancer.New("round-robin", p)
	s, stopServe, ln := startDrainProxy(t, p, b)
	proxyAddr := ln.Addr().String()

	conn, _ := openProxiedConn(t, proxyAddr)
	defer func() { _ = conn.Close() }()

	// Close listener first so no new handlers start.
	stopServe()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown=%v want DeadlineExceeded", err)
	}
}

func TestProxy_ShutdownRejectsNewConnections(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	b, _ := balancer.New("round-robin", p)
	s, _, ln := startDrainProxy(t, p, b)

	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 500*time.Millisecond)
	if err != nil {
		return // The listener may already have been closed by the test cleanup.
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := bufio.NewReader(conn).ReadByte(); err == nil {
		t.Fatal("connection accepted after shutdown")
	}
}

func startIdleProxy(t *testing.T, p *pool.Pool, b balancer.Balancer, idle time.Duration) string {
	t.Helper()
	_, m := metrics.New(p)
	s := New(p, b, 2*time.Second, idle, slog.Default(), m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = s.Serve(ln) }()
	return ln.Addr().String()
}

// TestProxy_IdleKeepsActiveStreamAlive sends traffic periodically for much
// longer than the idle timeout: an inactivity (not absolute) timeout must
// keep the stream open. Afterwards it stops and expects closure after
// approximately one idle interval.
func TestProxy_IdleKeepsActiveStreamAlive(t *testing.T) {
	a1 := startEcho(t, "A")
	p := pool.New([]config.BackendConfig{{Name: "a", Addr: a1}})
	b, _ := balancer.New("round-robin", p)
	addr := startIdleProxy(t, p, b, 300*time.Millisecond)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	rd := bufio.NewReader(conn)
	if _, err := rd.ReadString('\n'); err != nil {
		t.Fatalf("banner: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := fmt.Fprintf(conn, "tick\n"); err != nil {
			t.Fatalf("tick %d write: %v", i, err)
		}
		line, err := rd.ReadString('\n')
		if err != nil || strings.TrimSpace(line) != "tick" {
			t.Fatalf("tick %d echo=%q err=%v", i, line, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Idle now: both sides must close after ~one interval.
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := rd.ReadString('\n'); err == nil {
		t.Fatal("expected the idle connection to close")
	}
}

// startClosingBackend accepts connections, sends one line, then closes
// (backend EOF).
func startClosingBackend(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_, _ = fmt.Fprintln(c, "bye")
			_ = c.Close()
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func backendActive(p *pool.Pool, addr string) int64 {
	for _, b := range p.All() {
		if b.Addr == addr {
			return b.Active()
		}
	}
	return -1
}

// TestProxy_BackendEOFClosesClient verifies a backend EOF reaches the
// client and the handler cleans up (active count back to zero).
func TestProxy_BackendEOFClosesClient(t *testing.T) {
	beAddr, kill := startClosingBackend(t)
	defer kill()
	p := pool.New([]config.BackendConfig{{Name: "eof", Addr: beAddr}})
	b, _ := balancer.New("round-robin", p)
	addr := startIdleProxy(t, p, b, 5*time.Second)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)
	if line, err := rd.ReadString('\n'); err != nil || strings.TrimSpace(line) != "bye" {
		t.Fatalf("banner=%q err=%v", line, err)
	}
	// Backend closed: client must observe EOF promptly.
	if _, err := rd.ReadString('\n'); err == nil {
		t.Fatal("expected EOF after backend close")
	}
	waitFor(t, 2*time.Second, func() bool {
		return backendActive(p, beAddr) == 0
	}, "backend active count back to zero")
}

// startResetBackend accepts connections and immediately resets them
// (zero linger), simulating a backend crash mid-stream.
func startResetBackend(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			_ = c.Close()
		}
	}()
	return ln.Addr().String()
}

// TestProxy_MarksBackendUnhealthyOnReset verifies passive failure marking:
// a backend reset mid-stream ejects the backend for new connections and
// the handler still cleans up.
func TestProxy_MarksBackendUnhealthyOnReset(t *testing.T) {
	rst := startResetBackend(t)
	p := pool.New([]config.BackendConfig{{Name: "rst", Addr: rst}})
	b, _ := balancer.New("round-robin", p)
	addr, _ := startProxy(t, p, b)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = io.Copy(io.Discard, conn) // expect prompt EOF

	waitFor(t, 2*time.Second, func() bool {
		return len(p.Healthy()) == 0
	}, "reset backend ejected")
	waitFor(t, 2*time.Second, func() bool {
		return backendActive(p, rst) == 0
	}, "backend active count back to zero")
}
