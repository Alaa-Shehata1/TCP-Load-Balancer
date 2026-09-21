package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	h := New(p)

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
	h := New(p)
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
