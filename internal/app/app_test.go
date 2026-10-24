package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackcowmoo/llm-fallback-router/internal/app"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
)

type fakeUpstream struct {
	name     string
	server   *httptest.Server
	reported atomic.Int64
	active   atomic.Int64
	calls    atomic.Int64
	release  chan struct{}
	entered  chan struct{}
}

func newFakeUpstream(t *testing.T, name string) *fakeUpstream {
	t.Helper()
	up := &fakeUpstream{name: name}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		up.calls.Add(1)
		up.active.Add(1)
		defer up.active.Add(-1)
		if up.release != nil {
			select {
			case up.entered <- struct{}{}:
			default:
			}
			w.Header().Set("X-Fake-Upstream", up.name)
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-up.release
		}
		fmt.Fprintf(w, "served-by-%s", up.name)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "# TYPE test_active_requests gauge\ntest_active_requests{instance=%q} %d\n", up.name, up.reported.Load())
	})
	mux.HandleFunc("/dedicated-metrics", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"active_requests":  up.reported.Load(),
			"waiting_requests": 0,
			"name":             up.name,
		})
	})
	up.server = httptest.NewServer(mux)
	t.Cleanup(up.server.Close)
	return up
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func baseConfig() *config.Config {
	cfg := config.Default()
	cfg.Server.AccessLog = false
	cfg.Redis.KeyPrefix = "it:"
	cfg.Redis.InstanceTTL = config.Duration(2 * time.Second)
	cfg.Redis.HeartbeatInterval = config.Duration(500 * time.Millisecond)
	cfg.Redis.SyncInterval = config.Duration(10 * time.Millisecond)
	cfg.Redis.OperationTimeout = config.Duration(200 * time.Millisecond)
	cfg.Redis.DialTimeout = config.Duration(500 * time.Millisecond)
	cfg.Redis.Required = false
	cfg.Cache.MetricsTTL = config.Duration(100 * time.Millisecond)
	cfg.Cache.MetricsStaleTTL = config.Duration(2 * time.Second)
	cfg.Cache.SnapshotTTL = config.Duration(10 * time.Millisecond)
	cfg.Cache.Shared = true
	cfg.Routing.FailureThreshold = 2
	cfg.Routing.RecoveryInterval = config.Duration(time.Second)
	cfg.Routing.ResponseHeaderTimeout = config.Duration(5 * time.Second)
	cfg.Routing.Retry.MaxAttempts = 2
	cfg.Routing.Retry.BufferLimitBytes = 1 << 20
	cfg.Routing.Retry.OnStatus = []int{429, 502, 503, 504}
	return cfg
}

type upstreamSpec struct {
	name        string
	url         string
	metricsURL  string
	metricsType string
	jsonURL     string
}

func configureUpstreams(cfg *config.Config, specs ...upstreamSpec) {
	cfg.Upstreams = nil
	for _, spec := range specs {
		url := spec.url
		if url == "" {
			url = "http://" + spec.name + ".invalid"
		}
		up := config.Upstream{
			Name:     spec.name,
			URL:      url,
			Capacity: 1,
			Metrics: config.Metrics{
				Type:     spec.metricsType,
				URL:      spec.metricsURL,
				Interval: config.Duration(20 * time.Millisecond),
				Timeout:  config.Duration(20 * time.Millisecond),
				Active: config.ActiveMetric{
					Metric:      "test_active_requests",
					Aggregation: config.AggregationSum,
				},
				ActiveField:  "active_requests",
				WaitingField: "waiting_requests",
			},
		}
		if spec.metricsType == config.MetricsTypeJSON && spec.jsonURL != "" {
			up.Metrics.URL = spec.jsonURL
		}
		cfg.Upstreams = append(cfg.Upstreams, up)
	}
}

func startApp(t *testing.T, cfg *config.Config) (*app.App, *httptest.Server) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	instance, err := app.New(context.Background(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	server := httptest.NewServer(instance.Handler())
	t.Cleanup(server.Close)
	return instance, server
}

type adminState struct {
	InstanceID string `json:"instance_id"`
	Upstreams  []struct {
		Name           string   `json:"name"`
		Healthy        bool     `json:"healthy"`
		ReportedActive *float64 `json:"reported_active"`
		ObservedActive int64    `json:"observed_active"`
		LocalActive    int64    `json:"local_active"`
	} `json:"upstreams"`
	Redis struct {
		Enabled bool `json:"enabled"`
	} `json:"redis"`
}

func fetchAdmin(t *testing.T, serverURL string) adminState {
	t.Helper()
	response, err := http.Get(serverURL + "/admin/state")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("admin status = %d: %s", response.StatusCode, body)
	}
	var state adminState
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatalf("decode admin: %v", err)
	}
	return state
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", message)
}

func postChat(t *testing.T, serverURL string) (*http.Response, string) {
	t.Helper()
	response, err := http.Post(serverURL+"/v1/chat/completions", "application/json", strings.NewReader(`{"stream":false}`))
	if err != nil {
		t.Fatalf("chat request: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}

func upstreamByName(state adminState, name string) (int, bool) {
	for index, upstream := range state.Upstreams {
		if upstream.Name == name {
			return index, true
		}
	}
	return 0, false
}

func adminObserved(state adminState) int64 {
	var total int64
	for _, upstream := range state.Upstreams {
		total += upstream.ObservedActive
	}
	return total
}

func TestAppRoutesByMixedMetricsSources(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newFakeUpstream(t, "a")
	b := newFakeUpstream(t, "b")
	a.reported.Store(10)
	b.reported.Store(0)

	cfg := baseConfig()
	cfg.Redis.Address = mr.Addr()
	cfg.Server.InstanceID = "router-1"
	configureUpstreams(cfg,
		upstreamSpec{name: "a", url: a.server.URL, metricsType: config.MetricsTypePrometheus, metricsURL: a.server.URL + "/metrics"},
		upstreamSpec{name: "b", url: b.server.URL, metricsType: config.MetricsTypeJSON, jsonURL: b.server.URL + "/dedicated-metrics"},
	)
	instance, server := startApp(t, cfg)

	waitFor(t, 3*time.Second, func() bool {
		state := fetchAdmin(t, server.URL)
		indexA, okA := upstreamByName(state, "a")
		indexB, okB := upstreamByName(state, "b")
		return okA && okB && state.Upstreams[indexA].ReportedActive != nil &&
			*state.Upstreams[indexA].ReportedActive == 10 &&
			state.Upstreams[indexB].ReportedActive != nil &&
			*state.Upstreams[indexB].ReportedActive == 0
	}, "both metric sources should be scraped")

	if !fetchAdmin(t, server.URL).Redis.Enabled {
		t.Error("admin state should report distributed tracking")
	}

	response, body := postChat(t, server.URL)
	if response.StatusCode != 200 {
		t.Fatalf("status = %d body = %s", response.StatusCode, body)
	}
	if body != "served-by-b" {
		t.Errorf("body = %q, want served-by-b", body)
	}
	if got := response.Header.Get("X-LLM-Router-Upstream"); got != "b" {
		t.Errorf("upstream header = %q", got)
	}
	if a.calls.Load() != 0 || b.calls.Load() != 1 {
		t.Errorf("calls a=%d b=%d", a.calls.Load(), b.calls.Load())
	}
	if !strings.Contains(instance.InstanceID(), "router-1") {
		t.Errorf("instance ID = %q", instance.InstanceID())
	}
}

func TestCrossInstanceActiveCountsDriveRouting(t *testing.T) {
	mr := miniredis.RunT(t)
	a := newFakeUpstream(t, "a")
	b := newFakeUpstream(t, "b")
	a.release = make(chan struct{})
	a.entered = make(chan struct{}, 4)

	// Disable upstream-reported metrics: routing must use distributed observed
	// counts only.
	cfg1 := baseConfig()
	cfg1.Redis.Address = mr.Addr()
	cfg1.Server.InstanceID = "router-1"
	configureUpstreams(cfg1,
		upstreamSpec{name: "a", url: a.server.URL, metricsType: config.MetricsTypeNone},
		upstreamSpec{name: "b", url: b.server.URL, metricsType: config.MetricsTypeNone},
	)
	cfg2 := baseConfig()
	cfg2.Redis.Address = mr.Addr()
	cfg2.Server.InstanceID = "router-2"
	configureUpstreams(cfg2,
		upstreamSpec{name: "a", url: a.server.URL, metricsType: config.MetricsTypeNone},
		upstreamSpec{name: "b", url: b.server.URL, metricsType: config.MetricsTypeNone},
	)

	_, server1 := startApp(t, cfg1)
	_, server2 := startApp(t, cfg2)

	// Occupy upstream "a" through router-1. The proxy sends response headers
	// early, so the request stays in flight while we query router-2.
	longRequest := make(chan *http.Response, 1)
	longErr := make(chan error, 1)
	go func() {
		request, _ := http.NewRequest(http.MethodPost, server1.URL+"/v1/chat/completions", strings.NewReader("{}"))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			longErr <- err
			return
		}
		longRequest <- response
	}()

	select {
	case <-a.entered:
	case err := <-longErr:
		t.Fatalf("long request failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("long request did not reach upstream a")
	}
	var longResponse *http.Response
	select {
	case longResponse = <-longRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("long request headers did not arrive")
	}
	defer longResponse.Body.Close()
	if got := longResponse.Header.Get("X-LLM-Router-Upstream"); got != "a" {
		t.Fatalf("long request went to %q, want a", got)
	}

	// Router-2 has no local request but must see router-1's count in Redis.
	waitFor(t, 3*time.Second, func() bool {
		state := fetchAdmin(t, server2.URL)
		index, ok := upstreamByName(state, "a")
		return ok && state.Upstreams[index].ObservedActive >= 1 && state.Upstreams[index].LocalActive == 0
	}, "router-2 should observe router-1's in-flight request")

	// The next request on router-2 must avoid the busy upstream.
	response, body := postChat(t, server2.URL)
	if response.StatusCode != 200 || body != "served-by-b" {
		t.Fatalf("second request status=%d body=%q", response.StatusCode, body)
	}
	if got := response.Header.Get("X-LLM-Router-Upstream"); got != "b" {
		t.Errorf("second request went to %q, want b", got)
	}

	close(a.release)
	waitFor(t, 3*time.Second, func() bool {
		return adminObserved(fetchAdmin(t, server1.URL)) == 0 &&
			adminObserved(fetchAdmin(t, server2.URL)) == 0
	}, "active counts should drain across instances")
}

func TestAppFallsBackWhenRedisIsOptionalAndUnavailable(t *testing.T) {
	cfg := baseConfig()
	cfg.Redis.Address = "127.0.0.1:1" // nothing listens here
	cfg.Redis.DialTimeout = config.Duration(100 * time.Millisecond)
	cfg.Redis.OperationTimeout = config.Duration(100 * time.Millisecond)
	cfg.Redis.Required = false
	cfg.Cache.Shared = false
	cfg.Server.InstanceID = "router-local"
	a := newFakeUpstream(t, "a")
	b := newFakeUpstream(t, "b")
	configureUpstreams(cfg,
		upstreamSpec{name: "a", url: a.server.URL, metricsType: config.MetricsTypeNone},
		upstreamSpec{name: "b", url: b.server.URL, metricsType: config.MetricsTypeNone},
	)
	_, server := startApp(t, cfg)

	response, body := postChat(t, server.URL)
	if response.StatusCode != 200 || !strings.HasPrefix(body, "served-by-") {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	state := fetchAdmin(t, server.URL)
	if !state.Redis.Enabled {
		t.Error("redis tracking should stay enabled while it recovers from an outage")
	}
	if observed := adminObserved(state); observed != 0 {
		t.Errorf("observed counts after request = %d, want 0", observed)
	}
}

func TestAppRunsWithLocalTrackerWhenRedisIsNotConfigured(t *testing.T) {
	cfg := baseConfig()
	cfg.Redis.Address = ""
	cfg.Cache.Shared = false
	cfg.Server.InstanceID = "router-local"
	a := newFakeUpstream(t, "a")
	b := newFakeUpstream(t, "b")
	configureUpstreams(cfg,
		upstreamSpec{name: "a", url: a.server.URL, metricsType: config.MetricsTypeNone},
		upstreamSpec{name: "b", url: b.server.URL, metricsType: config.MetricsTypeNone},
	)
	_, server := startApp(t, cfg)

	response, body := postChat(t, server.URL)
	if response.StatusCode != 200 || !strings.HasPrefix(body, "served-by-") {
		t.Fatalf("status=%d body=%q", response.StatusCode, body)
	}
	state := fetchAdmin(t, server.URL)
	if state.Redis.Enabled {
		t.Error("redis should not be enabled without a configured address")
	}
}

func TestAppRequiresRedisWhenConfigured(t *testing.T) {
	cfg := baseConfig()
	cfg.Redis.Address = "127.0.0.1:1"
	cfg.Redis.DialTimeout = config.Duration(100 * time.Millisecond)
	cfg.Redis.OperationTimeout = config.Duration(100 * time.Millisecond)
	cfg.Redis.Required = true
	cfg.Server.InstanceID = "router-required"
	a := newFakeUpstream(t, "a")
	configureUpstreams(cfg, upstreamSpec{name: "a", url: a.server.URL, metricsType: config.MetricsTypeNone})

	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	if _, err := app.New(context.Background(), cfg, discardLogger()); err == nil {
		t.Fatal("expected startup failure when required Redis is unavailable")
	}
}
