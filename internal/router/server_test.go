package router_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/balancer"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/router"
	"github.com/blackcowmoo/llm-fallback-router/internal/telemetry"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

type serverEnv struct {
	balancer *balancer.Balancer
	server   *httptest.Server
}

func newServerEnv(t *testing.T, mutate func(*router.ServerOptions, *balancer.Options)) *serverEnv {
	t.Helper()
	parsed, _ := url.Parse("http://upstream-a:8000")
	tr := tracker.NewLocal([]string{"a"})
	tel, err := telemetry.New([]string{"a"})
	if err != nil {
		t.Fatalf("telemetry.New: %v", err)
	}
	balancerOpts := balancer.Options{
		Strategy:         config.StrategyLeastActive,
		Tracker:          tr,
		FailureThreshold: 3,
		RecoveryInterval: time.Minute,
		FailOpen:         false,
	}
	opts := router.ServerOptions{
		Proxy: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Proxied-Path", r.URL.Path)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("proxied"))
		}),
		Tracker:    tr,
		Telemetry:  tel,
		InstanceID: "router-test",
		Strategy:   config.StrategyLeastActive,
		Version:    "test",
		Started:    time.Now(),
	}
	if mutate != nil {
		mutate(&opts, &balancerOpts)
	}
	bal, err := balancer.New([]balancer.Upstream{{
		Name: "a", URL: parsed, Capacity: 1,
	}}, balancerOpts)
	if err != nil {
		t.Fatalf("balancer.New: %v", err)
	}
	opts.Balancer = bal
	opts.Tracker = tr
	opts.Telemetry = tel
	server := httptest.NewServer(router.NewServer(opts))
	t.Cleanup(server.Close)
	return &serverEnv{balancer: bal, server: server}
}

func (e *serverEnv) get(t *testing.T, path string) (*http.Response, string) {
	t.Helper()
	response, err := http.Get(e.server.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}

func TestServerEndpoints(t *testing.T) {
	env := newServerEnv(t, func(opts *router.ServerOptions, _ *balancer.Options) {
		opts.AdminToken = "secret"
	})

	response, body := env.get(t, "/healthz")
	if response.StatusCode != 200 || body != "ok\n" {
		t.Errorf("healthz = %d %q", response.StatusCode, body)
	}

	response, body = env.get(t, "/readyz")
	if response.StatusCode != 200 {
		t.Errorf("readyz = %d %q", response.StatusCode, body)
	}
	var ready map[string]any
	if err := json.Unmarshal([]byte(body), &ready); err != nil {
		t.Fatalf("readyz JSON: %v", err)
	}
	if ready["status"] != "ok" {
		t.Errorf("readyz status = %v", ready["status"])
	}

	response, body = env.get(t, "/metrics")
	if response.StatusCode != 200 || !strings.Contains(body, "llm_router_build_info") {
		t.Errorf("metrics = %d, missing build info", response.StatusCode)
	}

	response, body = env.get(t, "/admin/state")
	if response.StatusCode != http.StatusUnauthorized {
		t.Errorf("admin without token = %d %q", response.StatusCode, body)
	}
	response, _ = env.get(t, "/admin/state?token=secret")
	if response.StatusCode != http.StatusUnauthorized {
		t.Errorf("admin with query token = %d", response.StatusCode)
	}

	request, _ := http.NewRequest(http.MethodGet, env.server.URL+"/admin/state", nil)
	request.Header.Set("Authorization", "Bearer wrong")
	resp, _ := http.DefaultClient.Do(request)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("admin with wrong token = %d", resp.StatusCode)
	}

	request, _ = http.NewRequest(http.MethodGet, env.server.URL+"/admin/state", nil)
	request.Header.Set("Authorization", "Bearer secret")
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("admin request: %v", err)
	}
	defer resp.Body.Close()
	adminBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("admin = %d %s", resp.StatusCode, adminBody)
	}
	var state map[string]any
	if err := json.Unmarshal(adminBody, &state); err != nil {
		t.Fatalf("admin JSON: %v", err)
	}
	if state["instance_id"] != "router-test" || state["strategy"] != config.StrategyLeastActive {
		t.Errorf("admin state = %v", state)
	}
	upstreams, ok := state["upstreams"].([]any)
	if !ok || len(upstreams) != 1 {
		t.Errorf("admin upstreams = %v", state["upstreams"])
	}
	if _, ok := state["redis"]; !ok {
		t.Error("admin state missing redis")
	}

	request, _ = http.NewRequest(http.MethodGet, env.server.URL+"/admin/unknown", nil)
	request.Header.Set("Authorization", "Bearer secret")
	resp, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("unknown admin request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown admin path = %d", resp.StatusCode)
	}

	response, body = env.get(t, "/v1/chat/completions")
	if response.StatusCode != 200 || body != "proxied" {
		t.Errorf("proxy = %d %q", response.StatusCode, body)
	}
	if got := response.Header.Get("X-Proxied-Path"); got != "/v1/chat/completions" {
		t.Errorf("proxied path = %q", got)
	}
}

func TestServerMethodNotAllowed(t *testing.T) {
	env := newServerEnv(t, nil)
	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/admin/state"} {
		request, _ := http.NewRequest(http.MethodPost, env.server.URL+path, nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("POST %s = %d, want 405", path, response.StatusCode)
		}
		if got := response.Header.Get("Allow"); !strings.Contains(got, "GET") {
			t.Errorf("Allow for %s = %q", path, got)
		}
	}
}

func TestReadyzWhenUpstreamUnhealthy(t *testing.T) {
	env := newServerEnv(t, func(_ *router.ServerOptions, opts *balancer.Options) {
		opts.FailureThreshold = 1
	})
	env.balancer.ReportFailure("a", "down")

	response, body := env.get(t, "/readyz")
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d %q", response.StatusCode, body)
	}
	if !strings.Contains(body, "no healthy upstreams") {
		t.Errorf("readyz body = %q", body)
	}
	// Liveness must remain independent of upstream health.
	response, _ = env.get(t, "/healthz")
	if response.StatusCode != 200 {
		t.Errorf("healthz = %d", response.StatusCode)
	}
}

func TestReadyzRequiresRedis(t *testing.T) {
	env := newServerEnv(t, func(opts *router.ServerOptions, _ *balancer.Options) {
		opts.RedisRequired = true
		opts.RedisPing = func(context.Context) error { return errors.New("redis down") }
	})
	response, body := env.get(t, "/readyz")
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d %q", response.StatusCode, body)
	}
	if !strings.Contains(body, "redis is required") {
		t.Errorf("readyz body = %q", body)
	}

	optsEnv := newServerEnv(t, func(opts *router.ServerOptions, _ *balancer.Options) {
		opts.RedisRequired = false
		opts.RedisPing = func(context.Context) error { return errors.New("redis down") }
	})
	response, body = optsEnv.get(t, "/readyz")
	if response.StatusCode != http.StatusOK {
		t.Errorf("optional redis readyz = %d %q", response.StatusCode, body)
	}
}

func TestAdminWithoutToken(t *testing.T) {
	env := newServerEnv(t, nil)
	response, body := env.get(t, "/admin/state")
	if response.StatusCode != 200 || !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("admin = %d %q", response.StatusCode, body)
	}
}
