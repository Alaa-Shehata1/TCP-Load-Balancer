package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alaa157/tcp-load-balancer/internal/balancer"
	"github.com/alaa157/tcp-load-balancer/internal/config"
	"github.com/alaa157/tcp-load-balancer/internal/healthcheck"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
	"github.com/alaa157/tcp-load-balancer/internal/proxy"
)

// echoServer starts an identifying echo server, returning its addr and a kill func.
func echoServer(t *testing.T, id string) (string, func()) {
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
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				_, _ = fmt.Fprintf(conn, "served-by:%s\n", id)
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func dialBanner(t *testing.T, addr string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read banner: %v", err)
	}
	return strings.TrimSpace(line)
}

// TestFailover_KillOneBackendStillServes wires the real stack (pool +
// balancer + proxy + healthcheck), kills a backend, and asserts traffic
// keeps flowing to the survivor.
func TestFailover_KillOneBackendStillServes(t *testing.T) {
	addrA, _ := echoServer(t, "A")
	addrB, killB := echoServer(t, "B")

	p := pool.New([]config.BackendConfig{
		{Name: "a", Addr: addrA},
		{Name: "b", Addr: addrB},
	})
	b, err := balancer.New("round-robin", p)
	if err != nil {
		t.Fatalf("balancer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	healthcheck.Start(ctx, p, 20*time.Millisecond, 10*time.Millisecond, 2, nil)

	srv := proxy.New(p, b, 2*time.Second, 5*time.Second, slog.Default(), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() { _ = srv.Serve(ln) }()
	proxyAddr := ln.Addr().String()

	// Baseline: round-robin across both.
	if got := dialBanner(t, proxyAddr); got != "served-by:A" {
		t.Fatalf("want A got %q", got)
	}
	if got := dialBanner(t, proxyAddr); got != "served-by:B" {
		t.Fatalf("want B got %q", got)
	}

	// Kill B (like `docker stop server-b`).
	killB()
	time.Sleep(200 * time.Millisecond) // let healthcheck eject B

	if got := len(p.Healthy()); got != 1 {
		t.Fatalf("want 1 healthy got %d", got)
	}

	// All further traffic must reach A.
	for i := 0; i < 4; i++ {
		if got := dialBanner(t, proxyAddr); got != "served-by:A" {
			t.Fatalf("conn %d: want A got %q", i, got)
		}
	}
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

func backendActive(p *pool.Pool, addr string) int64 {
	for _, b := range p.All() {
		if b.Addr == addr {
			return b.Active()
		}
	}
	return -1
}

// TestFailover_MidStreamBackendDeath establishes a stream, kills the
// backend mid-flow (connection + listener, like a crashing server), and
// asserts the client observes EOF, the handler exits (active count zero),
// and the backend is ejected for new connections.
func TestFailover_MidStreamBackendDeath(t *testing.T) {
	kln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	kaddr := kln.Addr().String()
	go func() {
		c, err := kln.Accept()
		if err != nil {
			return
		}
		_, _ = fmt.Fprintln(c, "served-by:K")
		br := bufio.NewReader(c)
		if line, err := br.ReadString('\n'); err == nil {
			_, _ = fmt.Fprint(c, line)
		}
		_ = c.Close()
		_ = kln.Close()
	}()

	p := pool.New([]config.BackendConfig{{Name: "k", Addr: kaddr}})
	b, err := balancer.New("round-robin", p)
	if err != nil {
		t.Fatalf("balancer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	healthcheck.Start(ctx, p, 20*time.Millisecond, 10*time.Millisecond, 2, nil)

	srv := proxy.New(p, b, 2*time.Second, 5*time.Second, slog.Default(), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() { _ = srv.Serve(ln) }()
	proxyAddr := ln.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	rd := bufio.NewReader(conn)
	if line, err := rd.ReadString('\n'); err != nil || strings.TrimSpace(line) != "served-by:K" {
		t.Fatalf("banner=%q err=%v", line, err)
	}
	if _, err := fmt.Fprintf(conn, "hello\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if echo, err := rd.ReadString('\n'); err != nil || strings.TrimSpace(echo) != "hello" {
		t.Fatalf("echo=%q err=%v", echo, err)
	}
	// Backend died mid-stream: client must observe EOF promptly.
	if _, err := rd.ReadString('\n'); err == nil {
		t.Fatal("expected EOF after backend death")
	}
	waitFor(t, 2*time.Second, func() bool {
		return backendActive(p, kaddr) == 0
	}, "handler exit (active count zero)")
	waitFor(t, 2*time.Second, func() bool {
		return len(p.Healthy()) == 0
	}, "dead backend ejected")
}

// dialBannerOnce opens one short-lived connection and returns its banner.
func dialBannerOnce(proxyAddr string) (string, error) {
	conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// TestFailover_ConcurrentSurvivorOnlyTraffic kills a backend, then hammers
// the proxy with concurrent short-lived connections: every banner must
// come from a survivor and nothing may panic or deadlock.
func TestFailover_ConcurrentSurvivorOnlyTraffic(t *testing.T) {
	addrA, _ := echoServer(t, "A")
	addrB, killB := echoServer(t, "B")

	p := pool.New([]config.BackendConfig{
		{Name: "a", Addr: addrA},
		{Name: "b", Addr: addrB},
	})
	b, err := balancer.New("round-robin", p)
	if err != nil {
		t.Fatalf("balancer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	healthcheck.Start(ctx, p, 20*time.Millisecond, 10*time.Millisecond, 2, nil)

	srv := proxy.New(p, b, 2*time.Second, 10*time.Second, slog.Default(), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() { _ = srv.Serve(ln) }()
	proxyAddr := ln.Addr().String()

	killB()
	time.Sleep(300 * time.Millisecond) // let the health checker eject B

	const workers = 8
	const perWorker = 15
	type result struct {
		banner string
		err    error
	}
	results := make(chan result, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				banner, err := dialBannerOnce(proxyAddr)
				results <- result{banner, err}
			}
		}()
	}
	wg.Wait()
	close(results)

	count := 0
	for r := range results {
		count++
		if r.err != nil {
			t.Fatalf("conn failed: %v", r.err)
		}
		if r.banner != "served-by:A" {
			t.Fatalf("got %q after B died, want survivor A only", r.banner)
		}
	}
	if count != workers*perWorker {
		t.Fatalf("got %d results, want %d", count, workers*perWorker)
	}
}

// TestMalformedConfig_ExitsNonZero builds the real binary and proves a
// malformed listen address fails startup with a non-zero exit and the
// field name in the output.
func TestMalformedConfig_ExitsNonZero(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	doc := "listen_addr: \":99999\"\nadmin_addr: \":8080\"\n" +
		"algorithm: \"round-robin\"\nbackends:\n" +
		"  - {name: \"a\", host: \"127.0.0.1\", port: 9001}\n"
	if err := os.WriteFile(cfgPath, []byte(doc), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	bin := filepath.Join(dir, "lb")
	build := exec.Command("go", "build", "-o", bin, "./cmd/lb")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build lb: %v\n%s", err, out)
	}
	out, err := exec.Command(bin, "--config", cfgPath).CombinedOutput()
	if err == nil {
		t.Fatalf("want non-zero exit, output:\n%s", out)
	}
	if !strings.Contains(string(out), "listen_addr") {
		t.Fatalf("output %q does not mention listen_addr", out)
	}
}
