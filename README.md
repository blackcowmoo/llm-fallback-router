# llm-fallback-router

An active-request-aware HTTP router for OpenAI-compatible LLM backends. It
routes each request to the least-loaded healthy upstream, shares in-flight
counts between router replicas through Redis, and fails over when an upstream
is slow, overloaded, or unavailable.

The router is a transparent reverse proxy: `/v1/chat/completions`,
`/v1/completions`, `/v1/models`, and any other path are forwarded unchanged.
Only a small set of local endpoints is reserved for health, metrics, and
administration.

## Why this exists

LLM inference backends are long-running and unevenly loaded. A simple
round-robin proxy keeps sending requests to a backend that is already
generating at capacity. This router combines two independent load signals:

1. **Reported load** — the active request count scraped from each backend's
   Prometheus endpoint or a dedicated JSON metrics endpoint. Readings are
   cached, can be shared through Redis, and may be briefly stale.
2. **Observed load** — requests currently in flight through the router,
   counted locally and aggregated across router replicas in Redis.

By default the effective load is `max(reported, observed)`. The two sources
cover each other: reported metrics capture work started outside this router,
while observed counts stay accurate during scrape gaps and across replicas.

## Features

- Transparent reverse proxy for any HTTP API, including streaming SSE.
- `least_active`, `weighted_least_active`, and `round_robin` strategies.
- Prometheus text-format and JSON metric scrapers with label selection and
  `sum`/`max`/`min`/`avg` aggregation.
- TTL cache with stale-while-unavailable behavior.
- Redis-shared active counts with per-instance keys, heartbeats, and TTL
  cleanup for crashed replicas.
- Optional Redis-shared metric readings so new replicas can route immediately.
- Bounded retries/failover with replayable request bodies.
- Passive health checks with recovery windows and optional fail-open.
- Prometheus metrics for requests, retries, scrapes, upstream state, and Redis.
- Admin state endpoint, readiness probe, and structured logs.
- Multi-stage Docker images, Docker Compose demo, and GitHub Actions CI/release.

## Architecture

```
                         ┌────────────────────────────┐
  OpenAI client ────────▶│  llm-router replica A      │
                         │                            │
                         │  balancer ──▶ proxy ───────┼──▶ upstream A
                         │     ▲          │           │       (Prometheus)
                         │     │       tracker        │
                         │  metrics cache  │          │──▶ upstream B
                         │     ▲           │          │       (JSON metrics)
                         └─────┼───────────┼──────────┘
                               │           │
                         scrape│           │ active counts
                               │           ▼
                         ┌─────┴──────────────────────┐
                         │ Redis                      │
                         │  routers (liveness ZSET)   │
                         │  active:<instance> (hash)  │
                         │  reading:<upstream> (JSON) │
                         └─────▲──────────────────────┘
                               │
                         ┌─────┴──────────────────────┐
                         │  llm-router replica B      │
                         └────────────────────────────┘
```

Each replica scrapes upstream metrics independently, but can hydrate missing
readings from Redis. In-flight counts are always published as **absolute
values**, not increments, which makes reconnects and retries idempotent.

### Request flow

1. The proxy buffers a replayable copy of the request body (up to
   `routing.retry.buffer_limit_bytes`).
2. The balancer loads local + remote active counts and the cached reported
   reading for every upstream.
3. Unhealthy and saturated upstreams are filtered out.
4. The least-loaded eligible upstream is selected. Weighted mode divides load
   by `capacity`.
5. The request is proxied. The local tracker increments the upstream count,
   and a throttled background sync publishes the new absolute count to Redis.
6. On a retryable transport error or status, the upstream is excluded for this
   request, a failure is recorded, and the next-best upstream is tried.
7. The response is streamed back. Streaming responses are flushed as they
   arrive; they are never buffered for a retry after headers are written.

## Quick start with Docker Compose

Requirements: Docker with the Compose plugin.

```bash
docker compose up --build
```

This starts Redis, two mock LLM backends, and the router:

| Service | Address | Metrics source |
| --- | --- | --- |
| `mock-a` | http://localhost:8001 | Prometheus `/metrics` |
| `mock-b` | http://localhost:8002 | JSON `/dedicated-metrics` |
| `router` | http://localhost:8080 | — |
| `redis` | localhost:6379 | — |

`mock-b` reports a constant load of 3, so requests should go to `mock-a`
while it is healthy.

```bash
curl -sS http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-mock-a","messages":[{"role":"user","content":"hello"}]}'

curl -sS http://localhost:8080/admin/state | jq .
curl -sS http://localhost:8080/metrics | grep llm_router
```

Run the automated end-to-end checks (starts and stops its own stack):

```bash
scripts/e2e.sh
```

The script verifies mixed Prometheus/JSON scraping, metric-driven routing,
streaming, router metrics, Redis state, and failover after stopping `mock-a`.

## Running locally without Compose

Start Redis, then the mocks:

```bash
docker run --rm -p 6379:6379 redis:7-alpine

go run ./cmd/mock-llm -name mock-a -addr :8001 -model mock-mock-a
go run ./cmd/mock-llm -name mock-b -addr :8002 -model mock-mock-b -base-active 3

go run ./cmd/llm-router -config examples/router.yaml
```

The example config uses environment overrides:

```bash
REDIS_ADDR=127.0.0.1:6379 \
INSTANCE_ID=router-local \
MOCK_A_URL=http://127.0.0.1:8001 \
MOCK_B_URL=http://127.0.0.1:8002 \
go run ./cmd/llm-router -config examples/router.yaml
```

`${VAR}` and `${VAR:-default}` are expanded before YAML parsing. An unset
variable without a default is a startup error.

## Configuration

See [`examples/router.yaml`](examples/router.yaml) for a complete annotated
file. The parser is strict: unknown fields, duplicate upstream names, invalid
durations, and invalid URLs are rejected.

### `server`

| Key | Default | Description |
| --- | --- | --- |
| `listen` | `:8080` | HTTP listen address (must include a port). |
| `instance_id` | hostname + random suffix | Unique Redis identity. Set explicitly for stable deployments. |
| `read_header_timeout` | `10s` | Header read timeout. |
| `idle_timeout` | `120s` | Keep-alive idle timeout. |
| `shutdown_timeout` | `20s` | Graceful shutdown budget. |
| `max_header_bytes` | `1048576` | Maximum request header size. |
| `access_log` | `true` | Emit one structured log line per proxied request. |
| `admin_token` | empty | Bearer token required by `/admin/state` when set. |

### `routing`

| Key | Default | Description |
| --- | --- | --- |
| `strategy` | `least_active` | `least_active`, `weighted_least_active`, or `round_robin`. |
| `combine` | `max` | How reported and observed counts are combined. |
| `failure_threshold` | `3` | Consecutive failures before an upstream enters recovery. |
| `recovery_interval` | `15s` | How long a failed upstream stays out of rotation. |
| `fail_open` | `true` | Use unhealthy upstreams when no healthy candidate remains. |
| `reject_when_saturated` | `false` | Return 503 instead of using a full upstream. |
| `response_header_timeout` | `300s` | Upstream response-header timeout. |
| `idle_conn_timeout` | `90s` | Upstream keep-alive idle timeout. |
| `max_idle_conns_per_host` | `100` | Upstream connection pool size. |

Combine modes:

| Mode | Effective load |
| --- | --- |
| `max` | `max(reported, observed)` — default; conservative. |
| `sum` | `reported + observed` — useful when the reported metric excludes router traffic. |
| `reported` | Reported only, falling back to observed when no reading is available. |
| `observed` | Router-observed only; useful when upstream metrics are unreliable. |

`round_robin` still respects health and saturation filters, but ignores load
when choosing among eligible upstreams. Weighted mode divides effective load
by `capacity`, so a backend with `capacity: 2` can carry twice the load of one
with `capacity: 1` at the same score.

### `routing.retry`

| Key | Default | Description |
| --- | --- | --- |
| `max_attempts` | `2` | Total attempts, including the first. |
| `buffer_limit_bytes` | `33554432` | Maximum body size that can be replayed. Larger bodies are streamed once with no retry. |
| `on_status` | `[429, 502, 503, 504]` | Status codes that trigger failover before response headers are sent. |

A `429` is treated as overload, not as a health failure: the router retries
elsewhere but does not count it toward `failure_threshold`. Transport errors,
`502`, `503`, and `504` do count. Once any response bytes are written, the
router cannot fail over.

### `cache`

| Key | Default | Description |
| --- | --- | --- |
| `metrics_ttl` | `1s` | Age at which a reading is considered non-fresh. |
| `metrics_stale_ttl` | `30s` | Maximum age at which a reading may still be used. |
| `shared` | `true` | Publish readings to Redis and hydrate from them. Requires `redis.address`. |
| `snapshot_ttl` | `100ms` | How long a replica reuses its aggregated remote-count snapshot. |

### `redis`

| Key | Default | Description |
| --- | --- | --- |
| `address` | `127.0.0.1:6379` | Redis address. Empty disables distributed tracking. |
| `username` / `password` | empty | Redis ACL credentials. |
| `db` | `0` | Redis database. |
| `key_prefix` | `llm-router:` | Prefix for all keys. A trailing colon is added if missing. |
| `instance_ttl` | `30s` | Active-counter key and liveness TTL. |
| `heartbeat_interval` | `10s` | Periodic publish interval. |
| `sync_interval` | `100ms` | Minimum interval between event-driven publishes. |
| `operation_timeout` | `250ms` | Per-operation Redis timeout. |
| `dial_timeout` | `2s` | Connection dial timeout. |
| `pool_size` | `20` | Redis connection pool size. |
| `required` | `false` | Fail startup and readiness when Redis is unreachable. |
| `tls.enabled` | `false` | Enable TLS (TLS 1.2+). |
| `tls.ca_file` | empty | Custom CA bundle. |
| `tls.insecure_skip_verify` | `false` | Disable certificate verification; explicit opt-in only. |

`instance_ttl` must be greater than twice `heartbeat_interval`, and
`sync_interval` must be less than `instance_ttl`.

When Redis is optional and unavailable at startup, the router logs a warning,
continues with process-local counts, and reconnects automatically. When
`required: true`, startup fails and `/readyz` returns 503 while Redis is down.

### `upstreams[]`

| Key | Description |
| --- | --- |
| `name` | Unique name used in headers, logs, metrics, and Redis hashes. |
| `url` | Base URL of the upstream. Paths and query strings are preserved. |
| `capacity` | Relative capacity for weighted routing; default `1`. |
| `max_active` | Optional saturation limit; `0` means unlimited. |
| `headers` | Extra headers set on every proxied request. |
| `metrics.type` | `none`, `prometheus`, or `json`. |
| `metrics.url` | Metrics endpoint URL. |
| `metrics.interval` | Scrape interval; default `1s`. |
| `metrics.timeout` | Scrape timeout; must be `<= interval` and at most `30s`. |
| `metrics.headers` | Extra headers for scrapes (for example, an API key). |

Prometheus metrics:

```yaml
metrics:
  type: prometheus
  url: http://vllm-a:8000/metrics
  interval: 1s
  timeout: 500ms
  active_requests:
    metric: vllm:num_requests_running
    labels:
      model_name: llama-3
    aggregation: sum
  waiting_requests:
    metric: vllm:num_requests_waiting
    labels:
      model_name: llama-3
    aggregation: sum
```

The metric must be a gauge, counter, or untyped sample. All labels in `labels`
must match exactly; remaining labels are ignored. Aggregation applies across
matching samples.

JSON metrics:

```yaml
metrics:
  type: json
  url: http://my-backend:9000/load
  interval: 1s
  timeout: 500ms
  active_requests_field: active_requests
  waiting_requests_field: waiting_requests
```

Fields may be dot paths (`load.active`) and values may be numbers or numeric
strings. A missing active field is a scrape error; a missing waiting field is
ignored.

Set `metrics.type: none` to rely only on router-observed counts. This is the
default when no metrics URL or type is provided.

## Redis data model

All keys use `redis.key_prefix`.

| Key | Type | Purpose |
| --- | --- | --- |
| `<prefix>routers` | sorted set | Instance ID → last heartbeat (Unix milliseconds). Entries older than `instance_ttl` are removed. |
| `<prefix>active:<instance>` | hash | Upstream name → absolute in-flight count for that replica. Expires after `instance_ttl`. |
| `<prefix>reading:<upstream>` | string | JSON `metrics.Reading`, including timestamp and source. Expires after `cache.metrics_stale_ttl`. |

A replica excludes its own hash from remote aggregation and adds its local
counts directly, avoiding double counting. A crashed replica simply stops
heartbeating; its liveness entry and active hash expire. Graceful shutdown
publishes zeros and deregisters explicitly.

## Local endpoints

| Path | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness; always `200` while the HTTP server runs. |
| `GET /readyz` | Ready when at least one upstream is healthy and required Redis is reachable. |
| `GET /metrics` | Prometheus exposition. |
| `GET /admin/state` | JSON state: upstream load, health, effective load, Redis status. |
| Everything else | Proxied to the selected upstream. |

`/admin/state` is protected by `server.admin_token` when configured. The other
local endpoints are unauthenticated; put them behind your platform's network
policy or ingress controls if needed.

Example admin response (abridged):

```json
{
  "instance_id": "router-compose",
  "strategy": "weighted_least_active",
  "upstreams": [
    {
      "name": "mock-a",
      "healthy": true,
      "reported_active": 0,
      "observed_active": 0,
      "local_active": 0,
      "effective_load": 0,
      "reported_fresh": true
    }
  ],
  "redis": {
    "enabled": true,
    "remote_counts": {"mock-a": 0, "mock-b": 0}
  }
}
```

Response headers on proxied requests:

- `X-LLM-Router-Upstream` — selected upstream name.
- `X-LLM-Router-Attempts` — number of attempts used.
- `X-LLM-Router-Instance` — set on requests sent to the upstream.

## Observability

Router metrics use the `llm_router_` namespace:

| Metric | Labels | Description |
| --- | --- | --- |
| `requests_total` | `upstream`, `status` | Completed client requests. |
| `request_duration_seconds` | `upstream` | End-to-end duration histogram. |
| `request_retries_total` | `upstream`, `reason` | Failed attempts that triggered failover. |
| `upstream_selections_total` | `upstream`, `strategy` | Selections by strategy. |
| `upstream_scrapes_total` | `upstream`, `result` | Scrape outcomes. |
| `upstream_scrape_duration_seconds` | `upstream` | Scrape latency histogram. |
| `redis_up` | — | `1` if Redis is reachable. |
| `redis_errors_total` | `operation` | Redis errors by operation. |
| `shared_cache_operations_total` | `operation`, `result` | Shared-reading get/put outcomes. |
| `instance_info` | `instance_id`, `strategy` | Build/runtime identity. |
| `upstream_reported_active_requests` | `upstream` | Cached reported active count. |
| `upstream_metrics_available` | `upstream` | Whether any usable reading exists. |
| `upstream_metrics_fresh` | `upstream` | Whether the reading is within `metrics_ttl`. |
| `upstream_metrics_age_seconds` | `upstream` | Reading age. |
| `upstream_observed_active_requests` | `upstream` | Local + remote observed count. |
| `upstream_local_active_requests` | `upstream` | This replica's in-flight count. |
| `upstream_healthy` | `upstream` | Outside the recovery window. |
| `upstream_saturated` | `upstream` | At `max_active`. |
| `upstream_capacity` | `upstream` | Configured relative capacity. |
| `upstream_max_active` | `upstream` | Configured saturation limit. |
| `build_info` | `version`, `revision`, `build_date` | Build metadata. |

Useful alerts:

- `llm_router_upstream_healthy == 0` for an extended period.
- `llm_router_upstream_metrics_available == 0`.
- `rate(llm_router_redis_errors_total[5m]) > 0` with `redis.required: true`.
- High `llm_router_request_retries_total` relative to `requests_total`.
- `llm_router_upstream_saturated == 1` when `reject_when_saturated` is on.

## Production deployment

- Run at least two router replicas behind a load balancer.
- Give each replica a **unique, stable** `server.instance_id`. If omitted, the
  router generates one from the hostname plus a random suffix; that is fine for
  ephemeral replicas but less useful in dashboards.
- Use Redis with persistence/HA appropriate to your environment. The router
  treats counters as ephemeral state, but losing Redis briefly degrades
  cross-replica load awareness.
- Keep scrape intervals short enough to reflect real load, but longer than
  typical scrape latency. `1s` is a reasonable starting point.
- Size `routing.retry.buffer_limit_bytes` for your largest prompt. Requests
  above the limit are streamed once and cannot fail over.
- Set `redis.required: true` only if cross-replica accounting is a hard
  requirement; otherwise the router stays available during Redis outages.
- Protect `/admin/state` with `admin_token` and restrict `/metrics` at the
  network layer.
- Use a distinct `key_prefix` per environment sharing one Redis instance.
- The router does not terminate TLS and does not authenticate clients; place it
  behind your existing ingress/API gateway if those are needed.

### Container image

```bash
docker build -t llm-fallback-router:local .
docker run --rm -p 8080:8080 \
  -v "$PWD/examples:/etc/llm-router:ro" \
  -e REDIS_ADDR=redis:6379 \
  llm-fallback-router:local
```

The runtime image is distroless and runs as a non-root user. Its health check
uses the binary's `healthcheck` subcommand:

```bash
llm-router healthcheck --url http://127.0.0.1:8080/healthz
```

## Development

Requirements: Go 1.24+, Docker (for image and end-to-end tests), and
golangci-lint (optional).

```bash
make test          # go test ./...
make race          # go test -race -shuffle=on ./...
make vet
make fmt-check
make lint
make cover
make build         # bin/llm-router
make mock          # bin/mock-llm
make docker-build
make e2e
```

The test suite uses `miniredis` for Redis behavior, `httptest` upstreams, and
race detection in CI. It covers metric parsing and caching, distributed
counters, routing strategies, retries, health transitions, telemetry, HTTP
handlers, and full app wiring.

## GitHub Actions

- `.github/workflows/ci.yml` — formatting, vet, race tests, lint, image
  builds, and Docker Compose end-to-end checks on pushes to `main` and pull
  requests.
- `.github/workflows/release.yml` — on `v*` tags, runs GoReleaser for
  cross-platform binaries and publishes a multi-arch image to
  `ghcr.io/<owner>/<repo>`.
- `.github/dependabot.yml` — weekly Go module, GitHub Actions, Docker, and
  Compose dependency updates.

## License

[MIT](LICENSE)
