# TCP Load Balancer — Design (2026-09-21)

## 1. Goal
Learn networking + systems + concurrency in Go, and ship a recruiter-friendly portfolio repo.
Success = `docker compose up` runs LB + 3 echo backends, `docker stop server-b` triggers automatic failover, README shows diagram + demo + metrics.

## 2. Decisions (locked)
- Scope: Portfolio-max + Learning-deep combined, v1 = Core L4 + observability
- Stack: Go 1.21+, `gopkg.in/yaml.v3`, `prometheus/client_golang`, Docker Compose
- No browser visuals, text-only diagrams

## 3. Architecture
```
Client :9000
  |
  v
[LB :9000 listener -> picker -> DialTimeout -> io.Copy bidir]
  |-- pool (healthy list) <- healthchecker (dial/5s, 2 fails=out, backoff)
  |-> A :9001 / B :9002 / C :9003 (tiny Go echo + server ID)
  +-- admin :8080 (/health, /metrics)
```

## 4. Components
- `internal/config`: YAML `listenAddr, backends[], algorithm, healthcheck{interval,timeout,failThreshold}, timeouts{dial,idle}`. Validate at startup, fail fast on bad host:port.
- `internal/pool`: guarded healthy set (mutex + atomics). Active + passive health (dial error, timeout, RST marks unhealthy).
- `internal/balancer`: `type Balancer interface { Next() *Backend }`. `RoundRobin` via `atomic.Uint64`, `LeastConn` via active-conn counters. Pluggable, unit-testable. Pin at conn start.
- `internal/proxy`: `net.Listen` -> `net.DialTimeout` -> `go io.Copy(c->u)`, `go io.Copy(u->c)`. `SetDeadline` idle timeout, `allowHalfOpen=false`, destroy both sides on error, backpressure via pipes (no manual buffering).
- `internal/healthcheck`: per-backend goroutine with `context.Context`, TCP dial + optional ping check, `health_check_failures_total`.
- `internal/metrics`: prom `connections_total, backend_connections, bytes_tx/rx`. `slog` JSON per-conn `conn_id client backend algorithm latency`.
- `cmd/lb/main.go`: wire config+pool+balancer+proxy+admin, `SIGTERM` drain (stop accept, wait conns).
- `cmd/echo/main.go`: echo + ID banner for demo attribution.
- `docker-compose.yml`: `lb + server-a/b/c` on shared net. `config.yaml`, `Makefile`, CI.

## 5. Data flow / Failover
1. Client connects :9000. 2. Picker chooses healthy backend. 3. Dial with `connectTimeout`, on fail retry once on next backend. 4. Pipe bytes, track counters/deadlines. 5. HC marks B out in <10s after `docker stop server-b`; picker skips; restart rejoins. No mid-connection migration.

## 6. Error handling / Resilience
- Timeouts: dial (e.g. 3s), idle (e.g. 60s). Retry once only on initial dial.
- Leak prevention: `defer Close`, `destroy()` on error, decrement LeastConn, context cancellation ends HC goroutines.
- Shutdown: SIGTERM/SIGINT drain.

## 7. Testing
- Unit table-driven + `-race`: RR order, LeastConn min-pick, pool exclusion, config validation.
- Integration: 3 echos, kill one, assert traffic only to healthy; `go vet`, `golangci-lint`.
- Load smoke: concurrent TCP clients script (stdlib) + optional k6/tcpkali in v2.

## 8. Portfolio artifacts
README (diagram, quickstart, `docker stop server-b` demo, metrics screenshot, roadmap v2: weighted, consistent-hash, hot-reload SIGHUP, TLS/PROXY-protocol), Makefile `build/test/demo/lint`, badges, ADR-001 (why Go + pluggable balancer).

## 9. v2 Roadmap (not v1)
Weighted, consistent-hashing for sticky sessions, SIGHUP hot-reload, TLS termination, chaos test (`chaos-engineer`), Grafana dashboard.

## 10. Alternatives considered
- A (chosen): single-binary pluggable — best learn/portfolio balance.
- B (rejected): stdlib-only minimal — too tutorial-like.
- C (deferred): production-style day-1 — too big for v1.
