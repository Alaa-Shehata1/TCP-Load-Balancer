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
  failures, rejoin on success) **plus** passive eject on dial errors and
  mid-stream backend resets/timeouts (clean EOFs and pure client
  disconnects never eject).
- **Failover** — single retry on the next healthy backend for failed dials.
- **Timeouts** — inactivity (not absolute) deadlines: every read refreshes
  its connection's read deadline and every write refreshes the write
  deadline, so an active stream never expires while a fully idle one
  closes after `idle_secs`.
- **Observability** — JSON `slog` per connection (idle-timeout vs
  stream-error classified), Prometheus metrics (below),
  `GET /health → {"healthy":n,"total":m}`.
- **Graceful drain** — `SIGTERM`/`SIGINT` stops accepting, cancels health
  checks, shuts down the admin server, then waits up to 5s for open
  streams (`drain complete` vs `drain timed out` in the log).

### Metrics

Labels stay bounded: `backend` is the configured backend name, `result`
is `ok`, `no_healthy`, or `dial_failed`. No client addresses are labeled.

| Metric | Labels | Meaning |
|---|---|---|
| `connections_total` | `backend`, `result` | One increment per finished attempt |
| `backend_connections` | `backend` | Gauge: open streams (0 after drain) |
| `bytes_tx` / `bytes_rx` | `backend` | Exact `io.Copy` byte counts |
| `health_check_failures_total` | `backend` | Failed active-check dials |

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
python3 scripts/demo.py 20 | sort | uniq -c
# 10 conn*: served-by:A
# 10 conn*: served-by:C   <- B gone, 20/20 to survivors
curl -s localhost:8080/health   # {"healthy":2,"total":3}
curl -s localhost:8080/metrics | grep health_check_failures_total
# health_check_failures_total{backend="server-b"} 1

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

Invalid files fail fast with an explicit error naming the field and
value (bad port, missing/duplicate host, unknown algorithm, bad
`listen_addr`/`admin_addr`, negative timeouts, empty pool). Backend DNS
names are never resolved at load time; only the local listen/admin
addresses are parsed.

## Toolchain

Go 1.25 everywhere (`go.mod`, CI, both Dockerfiles): 1.25 is the minimum
required by `prometheus/client_golang`, so it is the floor, not a
preference. Newer local toolchains still build it fine.

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
go test -count=1 -race ./...   # unit + integration, no cache
go vet ./... && test -z "$(gofmt -l .)"
golangci-lint run ./...        # v2, 0 issues (or: make build test vet lint)
```

Highlights: `DrainWaitsForHandlers`/`DrainTimesOut` (bounded shutdown),
`IdleKeepsActiveStreamAlive` (inactivity vs absolute timeout),
`StalledBackendClosesBothSides`, `MarksBackendUnhealthyOnReset`,
`ConcurrentSurvivorOnlyTraffic` (120 conns, 8 goroutines, `-race`),
`MalformedConfig_ExitsNonZero` (real binary, exit code + field).

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
config hot-reload · TLS termination / PROXY protocol · Grafana dashboard ·
k6/tcpkali load test · connection migration is explicitly out of scope
(streams stay pinned by design).

## Design docs

- `docs/specs/2026-09-21-tcp-lb-design.md` — architecture + decisions
- `docs/superpowers/plans/2026-09-21-tcp-lb.md` — step-by-step build plan
