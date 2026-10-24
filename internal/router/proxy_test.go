package router_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/balancer"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/metrics"
	"github.com/blackcowmoo/llm-fallback-router/internal/router"
	"github.com/blackcowmoo/llm-fallback-router/internal/telemetry"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

type fakeMetrics struct {
	snapshots map[string]metrics.ReadingSnapshot
}

func (f *fakeMetrics) Snapshot(upstream string) (metrics.ReadingSnapshot, bool) {
	snapshot, ok := f.snapshots[upstream]
	return snapshot, ok
}

func reading(active float64) metrics.ReadingSnapshot {
	return metrics.ReadingSnapshot{
		Reading: metrics.Reading{Active: active, Timestamp: time.Now(), Source: "test"},
		Fresh:   true,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func upstreamFromServer(t *testing.T, name string, server *httptest.Server, maxActive int) balancer.Upstream {
	t.Helper()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	return balancer.Upstream{Name: name, URL: parsed, Capacity: 1, MaxActive: maxActive}
}

type proxyEnv struct {
	proxy     *router.Proxy
	balancer  *balancer.Balancer
	tracker   *tracker.Local
	telemetry *telemetry.Metrics
	server    *httptest.Server
	client    *http.Client
}

func newProxyEnv(t *testing.T, upstreams []balancer.Upstream, source balancer.MetricsSource, mutate func(*balancer.Options, *router.ProxyOptions)) *proxyEnv {
	t.Helper()
	names := make([]string, 0, len(upstreams))
	for _, upstream := range upstreams {
		names = append(names, upstream.Name)
	}
	tr := tracker.NewLocal(names)
	tel, err := telemetry.New(names)
	if err != nil {
		t.Fatalf("telemetry.New: %v", err)
	}
	balancerOpts := balancer.Options{
		Strategy:         config.StrategyLeastActive,
		Combine:          config.CombineMax,
		Metrics:          source,
		Tracker:          tr,
		FailureThreshold: 3,
		RecoveryInterval: time.Minute,
		FailOpen:         false,
	}
	proxyOpts := router.ProxyOptions{
		Logger:     discardLogger(),
		InstanceID: "router-test",
		Strategy:   config.StrategyLeastActive,
		Retry: config.Retry{
			MaxAttempts:      2,
			BufferLimitBytes: 1 << 20,
			OnStatus:         []int{429, 502, 503, 504},
		},
	}
	if mutate != nil {
		mutate(&balancerOpts, &proxyOpts)
	}
	bal, err := balancer.New(upstreams, balancerOpts)
	if err != nil {
		t.Fatalf("balancer.New: %v", err)
	}
	proxyOpts.Balancer = bal
	proxyOpts.Tracker = tr
	proxyOpts.Telemetry = tel
	proxy, err := router.NewProxy(proxyOpts)
	if err != nil {
		t.Fatalf("NewProxy: %v", err)
	}
	server := httptest.NewServer(proxy)
	t.Cleanup(func() {
		server.Close()
		proxy.CloseIdleConnections()
	})
	return &proxyEnv{
		proxy:     proxy,
		balancer:  bal,
		tracker:   tr,
		telemetry: tel,
		server:    server,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (e *proxyEnv) do(t *testing.T, method, path string, body io.Reader) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, e.server.URL+path, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := e.client.Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response
}

func TestForwardsRequestAndInjectsHeaders(t *testing.T) {
	var called atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("stream") != "true" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer upstream-key" {
			t.Errorf("authorization = %q", got)
		}
		if got := r.Header.Get("X-LLM-Router-Instance"); got != "router-test" {
			t.Errorf("instance header = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"prompt":"hi"}` {
			t.Errorf("body = %q", body)
		}
		w.Header().Set("X-Upstream", "a")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	env := newProxyEnv(t, []balancer.Upstream{{
		Name: "a", URL: mustParse(t, upstream.URL), Capacity: 1,
		Headers: map[string]string{"Authorization": "Bearer upstream-key"},
	}}, nil, nil)

	response := env.do(t, http.MethodPost, "/v1/chat/completions?stream=true", strings.NewReader(`{"prompt":"hi"}`))
	body, _ := io.ReadAll(response.Body)

	if response.StatusCode != http.StatusCreated {
		t.Errorf("status = %d", response.StatusCode)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %q", body)
	}
	if got := response.Header.Get("X-LLM-Router-Upstream"); got != "a" {
		t.Errorf("upstream header = %q", got)
	}
	if got := response.Header.Get("X-LLM-Router-Attempts"); got != "1" {
		t.Errorf("attempts header = %q", got)
	}
	if called.Load() != 1 {
		t.Errorf("upstream calls = %d", called.Load())
	}
	if got := env.tracker.Local()["a"]; got != 0 {
		t.Errorf("active count after response = %d", got)
	}
}

func TestRoutesToLeastLoadedUpstream(t *testing.T) {
	var callsA, callsB atomic.Int64
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsA.Add(1)
		_, _ = w.Write([]byte("a"))
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsB.Add(1)
		_, _ = w.Write([]byte("b"))
	}))
	defer serverB.Close()

	source := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reading(5),
		"b": reading(0),
	}}
	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, source, nil)

	response := env.do(t, http.MethodGet, "/", nil)
	body, _ := io.ReadAll(response.Body)
	if string(body) != "b" {
		t.Errorf("body = %q, want b", body)
	}
	if got := response.Header.Get("X-LLM-Router-Upstream"); got != "b" {
		t.Errorf("upstream header = %q", got)
	}
	if callsA.Load() != 0 || callsB.Load() != 1 {
		t.Errorf("calls a=%d b=%d", callsA.Load(), callsB.Load())
	}
}

func TestFailoverOnRetryableStatus(t *testing.T) {
	var callsA, callsB atomic.Int64
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsA.Add(1)
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsB.Add(1)
		_, _ = w.Write([]byte("b"))
	}))
	defer serverB.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, nil)

	response := env.do(t, http.MethodGet, "/", nil)
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || string(body) != "b" {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	if got := response.Header.Get("X-LLM-Router-Upstream"); got != "b" {
		t.Errorf("upstream header = %q", got)
	}
	if got := response.Header.Get("X-LLM-Router-Attempts"); got != "2" {
		t.Errorf("attempts header = %q", got)
	}
	if callsA.Load() != 1 || callsB.Load() != 1 {
		t.Errorf("calls a=%d b=%d", callsA.Load(), callsB.Load())
	}
}

func TestRetryOnTooManyRequestsDoesNotOpenCircuit(t *testing.T) {
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("b"))
	}))
	defer serverB.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, nil)

	response := env.do(t, http.MethodGet, "/", nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if got := env.balancer.HealthyCount(); got != 2 {
		t.Errorf("healthy count = %d, want 2 (429 is not an outage)", got)
	}
}

func TestRetriedRequestBodyIsReplayed(t *testing.T) {
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}))
	defer serverB.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, nil)

	response := env.do(t, http.MethodPost, "/v1/chat", strings.NewReader("hello"))
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || string(body) != "hello" {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
}

func TestLargeRequestBodyIsNotRetried(t *testing.T) {
	var callsB atomic.Int64
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsB.Add(1)
		_, _ = w.Write([]byte("b"))
	}))
	defer serverB.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, func(_ *balancer.Options, opts *router.ProxyOptions) {
		opts.Retry.BufferLimitBytes = 4
	})

	response := env.do(t, http.MethodPost, "/", strings.NewReader("0123456789"))
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want upstream 503 (no retry)", response.StatusCode)
	}
	if callsB.Load() != 0 {
		t.Error("oversized request body was retried")
	}
}

func TestNonRetryableStatusIsForwarded(t *testing.T) {
	var callsB atomic.Int64
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsB.Add(1)
		_, _ = w.Write([]byte("b"))
	}))
	defer serverB.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, nil)

	response := env.do(t, http.MethodPost, "/", strings.NewReader("{}"))
	if response.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", response.StatusCode)
	}
	if callsB.Load() != 0 {
		t.Error("non-retryable response was retried")
	}
}

func TestRetriesExhaustedForwardsLastStatus(t *testing.T) {
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
	}))
	defer serverB.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, nil)

	response := env.do(t, http.MethodGet, "/", nil)
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", response.StatusCode)
	}
	if got := response.Header.Get("X-LLM-Router-Attempts"); got != "2" {
		t.Errorf("attempts header = %q", got)
	}
}

func TestTransportFailureReturnsBadGateway(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	env := newProxyEnv(t, []balancer.Upstream{{
		Name: "dead", URL: mustParse(t, deadURL), Capacity: 1,
	}}, nil, nil)

	response := env.do(t, http.MethodGet, "/", nil)
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", response.StatusCode)
	}
	if !strings.Contains(string(body), "upstream_failure") {
		t.Errorf("body = %q", body)
	}
	if got := env.tracker.Local()["dead"]; got != 0 {
		t.Errorf("active count after failure = %d", got)
	}
}

func TestSaturatedReturnsServiceUnavailable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	source := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{"a": reading(2)}}
	env := newProxyEnv(t, []balancer.Upstream{upstreamFromServer(t, "a", upstream, 2)}, source,
		func(balancerOpts *balancer.Options, _ *router.ProxyOptions) {
			balancerOpts.RejectWhenSaturated = true
		})

	response := env.do(t, http.MethodGet, "/", nil)
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", response.StatusCode)
	}
	if got := response.Header.Get("Retry-After"); got != "1" {
		t.Errorf("retry-after = %q", got)
	}
	if !strings.Contains(string(body), "upstreams_saturated") {
		t.Errorf("body = %q", body)
	}
}

func TestActiveRequestCountedWhileInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = w.Write([]byte("done"))
	}))
	defer upstream.Close()

	env := newProxyEnv(t, []balancer.Upstream{upstreamFromServer(t, "a", upstream, 0)}, nil, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		response, err := env.client.Get(env.server.URL + "/")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach upstream")
	}
	deadline := time.Now().Add(2 * time.Second)
	for env.tracker.Local()["a"] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := env.tracker.Local()["a"]; got != 1 {
		t.Errorf("in-flight count = %d, want 1", got)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish")
	}
	deadline = time.Now().Add(2 * time.Second)
	for env.tracker.Local()["a"] != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := env.tracker.Local()["a"]; got != 0 {
		t.Errorf("count after completion = %d, want 0", got)
	}
}

func TestStreamingResponseIsFlushed(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte("data: one\n\n"))
		flusher.Flush()
		<-release
		_, _ = w.Write([]byte("data: two\n\n"))
		flusher.Flush()
	}))
	defer upstream.Close()

	env := newProxyEnv(t, []balancer.Upstream{upstreamFromServer(t, "a", upstream, 0)}, nil, nil)

	response, err := env.client.Get(env.server.URL + "/v1/stream")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()

	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read first chunk: %v", err)
	}
	if line != "data: one\n" {
		t.Errorf("first chunk = %q", line)
	}
	if blank, _ := reader.ReadString('\n'); blank != "\n" {
		t.Errorf("first event terminator = %q", blank)
	}
	if response.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("content type = %q", response.Header.Get("Content-Type"))
	}

	close(release)
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read second chunk: %v", err)
	}
	if line != "data: two\n" {
		t.Errorf("second chunk = %q", line)
	}
}

func TestNoHealthyUpstream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()

	env := newProxyEnv(t, []balancer.Upstream{upstreamFromServer(t, "a", upstream, 0)}, nil,
		func(opts *balancer.Options, _ *router.ProxyOptions) {
			opts.FailureThreshold = 1
			opts.RecoveryInterval = time.Minute
		})
	env.balancer.ReportFailure("a", "boom")

	response := env.do(t, http.MethodGet, "/", nil)
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", response.StatusCode)
	}
	if !strings.Contains(string(body), "no_upstream_available") {
		t.Errorf("body = %q", body)
	}
	if got := response.Header.Get("X-LLM-Router-Upstream"); got != "none" {
		t.Errorf("upstream header = %q", got)
	}
}

func TestClientCancellationReleasesCount(t *testing.T) {
	entered := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}))
	defer upstream.Close()

	env := newProxyEnv(t, []balancer.Upstream{upstreamFromServer(t, "a", upstream, 0)}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.server.URL+"/", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, err := env.client.Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach upstream")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request did not return")
	}
	deadline := time.Now().Add(2 * time.Second)
	for env.tracker.Local()["a"] != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := env.tracker.Local()["a"]; got != 0 {
		t.Errorf("count after cancellation = %d, want 0", got)
	}
}

func TestDistributedCountsInfluenceRouting(t *testing.T) {
	// Observed remote counts should be used even when upstream metrics are absent.
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("a"))
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("b"))
	}))
	defer serverB.Close()

	distributed := &fakeTracker{counts: map[string]int64{"a": 4, "b": 0}, local: map[string]int64{}}
	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", serverB, 0),
	}, nil, func(opts *balancer.Options, _ *router.ProxyOptions) {
		opts.Tracker = distributed
	})
	// newProxyEnv wires a local tracker into the proxy; distributed tracker is
	// only used for selection in this test.
	_ = env

	response := env.do(t, http.MethodGet, "/", nil)
	body, _ := io.ReadAll(response.Body)
	if string(body) != "b" {
		t.Errorf("body = %q, want b", body)
	}
}

type fakeTracker struct {
	counts map[string]int64
	local  map[string]int64
}

func (f *fakeTracker) Begin(string) func()                       { return func() {} }
func (f *fakeTracker) Snapshot(context.Context) map[string]int64 { return copyCounts(f.counts) }
func (f *fakeTracker) Local() map[string]int64                   { return copyCounts(f.local) }
func (f *fakeTracker) Status() tracker.Status                    { return tracker.Status{} }
func (f *fakeTracker) Close() error                              { return nil }

func copyCounts(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	return parsed
}

func TestBodyBufferLimit(t *testing.T) {
	// Regression guard for the Content-Length fast path: a body exactly at the
	// limit is buffered and retried; one above is not.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	var mu sync.Mutex
	bodies := []string{}
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		if len(bodies) == 1 {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer serverA.Close()

	env := newProxyEnv(t, []balancer.Upstream{
		upstreamFromServer(t, "a", serverA, 0),
		upstreamFromServer(t, "b", upstream, 0),
	}, nil, func(_ *balancer.Options, opts *router.ProxyOptions) {
		opts.Retry.BufferLimitBytes = 5
	})

	response := env.do(t, http.MethodPost, "/", bytes.NewBufferString("12345"))
	if response.StatusCode != http.StatusOK {
		t.Errorf("status = %d", response.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 || bodies[0] != "12345" {
		t.Errorf("bodies = %v", bodies)
	}
}
