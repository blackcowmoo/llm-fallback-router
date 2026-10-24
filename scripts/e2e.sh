#!/usr/bin/env bash
# End-to-end checks against the Docker Compose stack.
#
# Usage:
#   scripts/e2e.sh
#   COMPOSE="docker-compose" scripts/e2e.sh
#
# The script builds and starts the stack, waits for metrics to be scraped,
# exercises routing, streaming, metrics, Redis state, and failover, then tears
# the stack down.
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

read -r -a COMPOSE_CMD <<< "${COMPOSE:-docker compose}"
ROUTER_URL="${ROUTER_URL:-http://127.0.0.1:8080}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/llm-router-e2e.XXXXXX")"

compose() {
	"${COMPOSE_CMD[@]}" "$@"
}

fail() {
	echo "e2e: $*" >&2
	exit 1
}

cleanup() {
	local status=$?
	trap - EXIT
	if [[ $status -ne 0 ]]; then
		echo "e2e: collecting diagnostics" >&2
		compose ps >&2 || true
		compose logs --no-color --tail=200 >&2 || true
	fi
	compose down -v --remove-orphans >/dev/null 2>&1 || true
	rm -rf "$WORK_DIR"
	exit "$status"
}
trap cleanup EXIT

command -v curl >/dev/null || fail "curl is required"
command -v jq >/dev/null || fail "jq is required"

echo "e2e: starting compose stack"
compose up -d --build

echo "e2e: waiting for router readiness"
ready=""
for _ in $(seq 1 90); do
	if ready="$(curl -fsS "$ROUTER_URL/readyz" 2>/dev/null)"; then
		break
	fi
	sleep 1
done
[[ -n "$ready" ]] || fail "router did not become ready"
jq -e '.status == "ok" and .upstreams.healthy == 2' <<<"$ready" >/dev/null ||
	fail "unexpected readiness response: $ready"

echo "e2e: waiting for both metric sources to be scraped"
reported_a=""
reported_b=""
for _ in $(seq 1 60); do
	state="$(curl -fsS "$ROUTER_URL/admin/state")"
	reported_a="$(jq -r '.upstreams[] | select(.name == "mock-a") | .reported_active' <<<"$state")"
	reported_b="$(jq -r '.upstreams[] | select(.name == "mock-b") | .reported_active' <<<"$state")"
	if [[ "$reported_a" =~ ^[0-9]+$ ]] && [[ "$reported_b" =~ ^[0-9]+$ ]] &&
		((reported_a == 0 && reported_b >= 3)); then
		break
	fi
	sleep 1
done
[[ "$reported_a" == "0" ]] || fail "mock-a reported_active=$reported_a, want 0"
[[ "$reported_b" =~ ^[0-9]+$ ]] && ((reported_b >= 3)) ||
	fail "mock-b reported_active=$reported_b, want >= 3"

echo "e2e: checking metric-driven routing and request headers"
for i in $(seq 1 5); do
	code="$(curl -sS -o "$WORK_DIR/body" -D "$WORK_DIR/headers" -w '%{http_code}' \
		-H 'Content-Type: application/json' \
		-d '{"model":"mock-mock-a","messages":[{"role":"user","content":"ping"}]}' \
		"$ROUTER_URL/v1/chat/completions")"
	[[ "$code" == "200" ]] || fail "request $i returned HTTP $code: $(cat "$WORK_DIR/body")"
	grep -q 'hello from mock-a' "$WORK_DIR/body" ||
		fail "request $i did not go to mock-a: $(cat "$WORK_DIR/body")"
	upstream="$(awk 'tolower($1) == "x-llm-router-upstream:" {gsub("\r", "", $2); print $2}' "$WORK_DIR/headers")"
	[[ "$upstream" == "mock-a" ]] || fail "request $i upstream header=$upstream, want mock-a"
done

echo "e2e: checking streaming responses"
curl -fsS -N -o "$WORK_DIR/stream" -D "$WORK_DIR/stream-headers" \
	-H 'Content-Type: application/json' \
	-H 'X-Mock-Delay: 10ms' \
	-d '{"model":"mock-mock-a","stream":true,"messages":[{"role":"user","content":"stream"}]}' \
	"$ROUTER_URL/v1/chat/completions"
grep -q '^data: \[DONE\]' "$WORK_DIR/stream" || fail "stream did not finish with [DONE]"
grep -q 'from mock-a' "$WORK_DIR/stream" || fail "stream did not come from mock-a"
grep -qi '^content-type: text/event-stream' "$WORK_DIR/stream-headers" ||
	fail "stream content type was not text/event-stream"

echo "e2e: checking router Prometheus metrics"
metrics="$(curl -fsS "$ROUTER_URL/metrics")"
grep -q 'llm_router_requests_total{' <<<"$metrics" || fail "request counter missing"
grep -q 'llm_router_upstream_reported_active_requests{upstream="mock-b"}' <<<"$metrics" ||
	fail "reported active gauge missing"
grep -q 'llm_router_upstream_scrapes_total{' <<<"$metrics" || fail "scrape counter missing"

echo "e2e: checking shared Redis counters"
routers="$(compose exec -T redis redis-cli ZRANGE llm-router:routers 0 -1)"
grep -qx 'router-compose' <<<"$routers" || fail "router instance not registered in Redis: $routers"
active="$(compose exec -T redis redis-cli HGETALL llm-router:active:router-compose)"
grep -q '^mock-a$' <<<"$active" || fail "mock-a active counter missing from Redis"
grep -q '^mock-b$' <<<"$active" || fail "mock-b active counter missing from Redis"

echo "e2e: checking failover when mock-a is stopped"
compose stop mock-a >/dev/null
failed_over=0
unhealthy=0
for _ in $(seq 1 15); do
	code="$(curl -sS -o "$WORK_DIR/body" -D "$WORK_DIR/headers" -w '%{http_code}' \
		-H 'Content-Type: application/json' \
		-d '{"model":"mock-mock-a","messages":[{"role":"user","content":"failover"}]}' \
		"$ROUTER_URL/v1/chat/completions" || true)"
	upstream="$(awk 'tolower($1) == "x-llm-router-upstream:" {gsub("\r", "", $2); print $2}' "$WORK_DIR/headers" 2>/dev/null || true)"
	if [[ "$code" == "200" && "$upstream" == "mock-b" ]]; then
		failed_over=1
	fi
	state="$(curl -fsS "$ROUTER_URL/admin/state")"
	if jq -e '.upstreams[] | select(.name == "mock-a") | .healthy == false' <<<"$state" >/dev/null; then
		unhealthy=1
		break
	fi
	sleep 0.2
done
[[ "$failed_over" == "1" ]] || fail "router did not fail over from mock-a to mock-b"
[[ "$unhealthy" == "1" ]] || fail "mock-a was not marked unhealthy after repeated failures"

echo "e2e: all checks passed"
