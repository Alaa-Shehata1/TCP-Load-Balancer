// Package metrics exposes /health and Prometheus /metrics for the LB.
package metrics

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Metrics owns the registry and all load balancer collectors.
// Labels stay bounded: backend is the configured backend name
// (never client addresses); result is one of ok, no_healthy, dial_failed.
// All methods are safe to call on a nil *Metrics and from any goroutine.
type Metrics struct {
	Registry           *prometheus.Registry
	Connections        *prometheus.CounterVec
	BackendConnections *prometheus.GaugeVec
	BytesTx            *prometheus.CounterVec
	BytesRx            *prometheus.CounterVec
	HealthFailures     *prometheus.CounterVec
}

// BackendLabel returns the bounded label for a backend: its configured
// name, falling back to the address when no name is set.
func BackendLabel(name, addr string) string {
	if name != "" {
		return name
	}
	return addr
}

// NewCounters builds and registers collectors on reg.
func NewCounters(reg *prometheus.Registry) *Metrics {
	c := &Metrics{
		Registry: reg,
		Connections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connections_total",
			Help: "Total proxied TCP connections.",
		}, []string{"backend", "result"}),
		BackendConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "backend_connections",
			Help: "Current connections per backend.",
		}, []string{"backend"}),
		BytesTx: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bytes_tx",
			Help: "Bytes client -> backend.",
		}, []string{"backend"}),
		BytesRx: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bytes_rx",
			Help: "Bytes backend -> client.",
		}, []string{"backend"}),
		HealthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "health_check_failures_total",
			Help: "Health check failures per backend.",
		}, []string{"backend"}),
	}
	reg.MustRegister(c.Connections, c.BackendConnections, c.BytesTx, c.BytesRx, c.HealthFailures)
	return c
}

// ConnResult records one finished connection attempt.
func (m *Metrics) ConnResult(backend, result string) {
	if m == nil {
		return
	}
	m.Connections.WithLabelValues(backend, result).Inc()
}

// BackendInc marks a backend connection as active.
func (m *Metrics) BackendInc(backend string) {
	if m == nil {
		return
	}
	m.BackendConnections.WithLabelValues(backend).Inc()
}

// BackendDec marks a backend connection as finished.
func (m *Metrics) BackendDec(backend string) {
	if m == nil {
		return
	}
	m.BackendConnections.WithLabelValues(backend).Dec()
}

// AddTx adds n client->backend bytes.
func (m *Metrics) AddTx(backend string, n float64) {
	if m == nil {
		return
	}
	m.BytesTx.WithLabelValues(backend).Add(n)
}

// AddRx adds n backend->client bytes.
func (m *Metrics) AddRx(backend string, n float64) {
	if m == nil {
		return
	}
	m.BytesRx.WithLabelValues(backend).Add(n)
}

// HealthFailed records one failed health-check dial.
func (m *Metrics) HealthFailed(backend string) {
	if m == nil {
		return
	}
	m.HealthFailures.WithLabelValues(backend).Inc()
}

// New builds the shared registry, registers all collectors once, and
// returns an http.Handler serving /health and /metrics on that same
// registry alongside the *Metrics to inject into proxy and health checker.
func New(p *pool.Pool) (http.Handler, *Metrics) {
	reg := prometheus.NewRegistry()
	m := NewCounters(reg)
	// Ensure series exist so /metrics exposes names even before traffic.
	m.Connections.WithLabelValues("none", "ok").Add(0)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{
			"healthy": len(p.Healthy()),
			"total":   len(p.All()),
		})
	})
	return mux, m
}
