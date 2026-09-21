package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alaa157/tcp-load-balancer/internal/config"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

func testPool2() *pool.Pool {
	return pool.New([]config.BackendConfig{
		{Name: "a", Addr: "127.0.0.1:9001"},
		{Name: "b", Addr: "127.0.0.1:9002"},
	})
}

func TestHealth_ReturnsHealthyCount(t *testing.T) {
	p := testPool2()
	p.MarkUnhealthy("127.0.0.1:9001")
	h, _ := New(p)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("code=%d", w.Code)
	}
	var body struct {
		Healthy int `json:"healthy"`
		Total   int `json:"total"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Healthy != 1 || body.Total != 2 {
		t.Fatalf("got %+v", body)
	}
}

func TestMetrics_ExposesProm(t *testing.T) {
	p := testPool2()
	h, _ := New(p)
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "connections_total") {
		t.Fatalf("missing connections_total in:\n%s", w.Body.String())
	}
}

func TestMetrics_MethodsRecordExactValues(t *testing.T) {
	_, m := New(testPool2())

	m.ConnResult("a", "ok")
	m.ConnResult("a", "ok")
	if got := testutil.ToFloat64(m.Connections.WithLabelValues("a", "ok")); got != 2 {
		t.Fatalf("connections_total=%v want 2", got)
	}

	m.BackendInc("a")
	m.BackendInc("a")
	m.BackendDec("a")
	if got := testutil.ToFloat64(m.BackendConnections.WithLabelValues("a")); got != 1 {
		t.Fatalf("backend_connections=%v want 1", got)
	}

	m.AddTx("a", 6)
	m.AddRx("a", 17)
	if got := testutil.ToFloat64(m.BytesTx.WithLabelValues("a")); got != 6 {
		t.Fatalf("bytes_tx=%v want 6", got)
	}
	if got := testutil.ToFloat64(m.BytesRx.WithLabelValues("a")); got != 17 {
		t.Fatalf("bytes_rx=%v want 17", got)
	}

	m.HealthFailed("b")
	m.HealthFailed("b")
	m.HealthFailed("b")
	if got := testutil.ToFloat64(m.HealthFailures.WithLabelValues("b")); got != 3 {
		t.Fatalf("health_check_failures_total=%v want 3", got)
	}
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *Metrics
	m.ConnResult("a", "ok")
	m.BackendInc("a")
	m.BackendDec("a")
	m.AddTx("a", 1)
	m.AddRx("a", 1)
	m.HealthFailed("a")
}
