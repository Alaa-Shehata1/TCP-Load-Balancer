# TCP Load Balancer

[![CI](https://github.com/alaa157/TCP-Load-Balancer/actions/workflows/ci.yml/badge.svg)](https://github.com/alaa157/TCP-Load-Balancer/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.25%2B-00ADD8?logo=go)

A Layer-4 TCP load balancer in Go: raw byte proxying, pluggable balancing
algorithms, active + passive health checks, automatic failover, and
Prometheus observability — with a one-command Docker failover demo.

Built to learn networking + systems + concurrency, and to show it.

```
                 Client
                   │
                   ▼
             ┌───────────┐
             │    LB     │  :9000 (TCP)  +  :8080 (/health, /metrics)
             └─────┬─────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
     Server A   Server B   Server C
     :9001      :9002      :9003
```

## Features

- **TCP proxying** — `net.Listen` → `DialTimeout` → bidirectional `io.Copy`;
  backend pinned per connection, backpressure via pipes (no buffering).
- **Algorithms** — `round-robin` (lock-free `atomic.Uint64`) and
  `least-conn` (atomic active-connection counters) behind a `Balancer`
  interface.
- **Health checks** — active TCP dial every N seconds (eject after M
  failures, rejoin on success) **plus** passive eject on dial error/RST.
- **Failover** — single retry on the next healthy backend for failed dials.
- **Timeouts** — configurable dial + idle deadlines.
- **Observability** — JSON `slog` per connection, Prometheus
  `connections_total / backend_connections / bytes_tx / bytes_rx /
  health_check_failures_total`, `GET /health → {"healthy":n,"total":m}`.
- **Graceful drain** — `SIGTERM` stops accepting, shuts down admin, exits.

## Quickstart

```bash
# Local (needs Go 1.25+)
go run ./cmd/echo --id A --port 9001 &
go run ./cmd/echo --id B --port 9002 &
go run ./cmd/echo --id C --port 9003 &
go run ./cmd/lb --config config.yaml

# Talk to it (each conn prints its backend, then echoes)
python3 scripts/demo.py 6
curl -s localhost:8080/health   # {"healthy":3,"total":3}
curl -s localhost:8080/metrics | grep connections_total
```

## Failover demo (Docker)

```bash
make demo            # or: docker compose up -d --build
python3 scripts/demo.py 6
# conn0: served-by:A
# conn1: served-by:B
# conn2: served-by:C
# ...

docker stop tcp-load-balancer-server-b-1
python3 scripts/demo.py 6
# conn0: served-by:A
# conn1: served-by:C   <- B gone, traffic keeps flowing
# ...
curl -s localhost:8080/health   # {"healthy":2,"total":3}

docker start tcp-load-balancer-server-b-1  # auto-rejoins in seconds
```

> Restricted environments (some sandboxes block container-to-container
> traffic): run the backends in Docker and the LB on the host —
> `make demo-host` (`docker compose up server-a/b/c` + `go run ./cmd/lb`).
> The `docker stop server-b` failover works the same way.

## Configuration (`config.yaml`)

```yaml
listen_addr: ":9000"
admin_addr: ":8080"
algorithm: "round-robin"   # or "least-conn"
backends:
  - {name: "server-a", host: "127.0.0.1", port: 9001}
  - {name: "server-b", host: "127.0.0.1", port: 9002}
  - {name: "server-c", host: "127.0.0.1", port: 9003}
healthcheck: {interval_secs: 5, timeout_secs: 2, fail_threshold: 2}
timeouts: {dial_secs: 3, idle_secs: 60}
```

Invalid files fail fast with an explicit error (bad `host:port`, unknown
algorithm, empty pool).

## Project structure

```
cmd/lb            — wiring: config → pool → balancer → proxy + healthcheck + admin
cmd/echo          — tiny identifying echo backend for demos/tests
internal/config   — YAML load + validate + defaults
internal/pool     — healthy set (RWMutex) + atomic conn counters
internal/balancer — Balancer interface: roundRobin, leastConn
internal/proxy    — accept → pick → dial → io.Copy both ways
internal/healthcheck — per-backend dial loop, eject + rejoin
internal/metrics  — /health + Prometheus /metrics
tests/integration — full-stack kill-a-backend failover test
```

## Testing

```bash
go test -race ./...   # unit + integration (incl. failover)
go vet ./... && gofmt -l .
```

Highlights: table-driven balancer tests, `NoHealthy_ClosesFast`,
`SkipsDeadBackend`, and `TestFailover_KillOneBackendStillServes`
(in-process LB, kill a backend, assert all traffic reaches the survivor).

## What I learned

- `net` package fundamentals: listeners, dial timeouts, deadlines.
- Concurrency: goroutine-per-connection, `sync.RWMutex` vs `atomic`
  counters, `context` cancellation for health loops and shutdown.
- TCP realities: half-close (`CloseWrite`), RST/timeout handling,
  why you never migrate a connection mid-stream.
- Why `-race` matters: the pool/balancer/proxy share state on every
  connection — the race detector is the test suite's best feature.

## Roadmap (v2)

Weighted round-robin · consistent hashing (sticky sessions) · `SIGHUP`
config hot-reload · TLS termination / PROXY protocol · per-backend byte
counters wired into Prometheus · Grafana dashboard · k6/tcpkali load test.

## Design docs

- `docs/specs/2026-09-21-tcp-lb-design.md` — architecture + decisions
- `docs/superpowers/plans/2026-09-21-tcp-lb.md` — step-by-step build plan
