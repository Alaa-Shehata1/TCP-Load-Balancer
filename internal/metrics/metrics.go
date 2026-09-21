// Package metrics exposes /health and Prometheus /metrics for the LB.
package metrics

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alaa157/tcp-load-balancer/internal/pool"
)

// Counters is the LB's Prometheus counters.
type Counters struct {
	Connections *prometheus.CounterVec
	BackendConn *prometheus.GaugeVec
	BytesTx     *prometheus.CounterVec
	BytesRx     *prometheus.CounterVec
	HealthFails *prometheus.CounterVec
}

// NewCounters builds and registers counters on reg.
func NewCounters(reg *prometheus.Registry) *Counters {
	c := &Counters{
		Connections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connections_total",
			Help: "Total proxied TCP connections.",
		}, []string{"backend", "result"}),
		BackendConn: prometheus.NewGaugeVec(prometheus.GaugeOpts{
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
		HealthFails: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "health_check_failures_total",
			Help: "Health check failures per backend.",
		}, []string{"backend"}),
	}
	reg.MustRegister(c.Connections, c.BackendConn, c.BytesTx, c.BytesRx, c.HealthFails)
	return c
}

// Handler bundles /health + /metrics.
type Handler struct {
	p   *pool.Pool
	mux *http.ServeMux
}

// New builds an http.Handler serving /health and /metrics.
func New(p *pool.Pool) http.Handler {
	reg := prometheus.NewRegistry()
	c := NewCounters(reg)
	// Ensure series exist so /metrics exposes names even before traffic.
	c.Connections.WithLabelValues("none", "ok").Add(0)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{
			"healthy": len(p.Healthy()),
			"total":   len(p.All()),
		})
	})
	return mux
}
