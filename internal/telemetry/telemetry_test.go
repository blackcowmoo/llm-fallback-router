package telemetry

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestUpstreamGauges(t *testing.T) {
	m, err := New([]string{"a", "b"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.SetInstance("router-1", "least_active")
	m.SetUpstreamConfig("a", 2, 10)

	m.UpdateUpstream("a", UpstreamState{
		Reported:          5,
		ReportedAvailable: true,
		ReportedFresh:     true,
		ReportedAge:       250 * time.Millisecond,
		Observed:          7,
		Local:             3,
		Healthy:           true,
		Saturated:         true,
	})

	if got := testutil.ToFloat64(m.reportedActive.WithLabelValues("a")); got != 5 {
		t.Errorf("reported = %v", got)
	}
	if got := testutil.ToFloat64(m.metricsAvailable.WithLabelValues("a")); got != 1 {
		t.Errorf("available = %v", got)
	}
	if got := testutil.ToFloat64(m.observedActive.WithLabelValues("a")); got != 7 {
		t.Errorf("observed = %v", got)
	}
	if got := testutil.ToFloat64(m.localActive.WithLabelValues("a")); got != 3 {
		t.Errorf("local = %v", got)
	}
	if got := testutil.ToFloat64(m.fresh.WithLabelValues("a")); got != 1 {
		t.Errorf("fresh = %v", got)
	}
	if got := testutil.ToFloat64(m.metricAge.WithLabelValues("a")); got != 0.25 {
		t.Errorf("age = %v", got)
	}
	if got := testutil.ToFloat64(m.healthy.WithLabelValues("a")); got != 1 {
		t.Errorf("healthy = %v", got)
	}
	if got := testutil.ToFloat64(m.saturated.WithLabelValues("a")); got != 1 {
		t.Errorf("saturated = %v", got)
	}
	if got := testutil.ToFloat64(m.capacity.WithLabelValues("a")); got != 2 {
		t.Errorf("capacity = %v", got)
	}
	if got := testutil.ToFloat64(m.maxActive.WithLabelValues("a")); got != 10 {
		t.Errorf("max active = %v", got)
	}
	if got := testutil.ToFloat64(m.instanceInfo.WithLabelValues("router-1", "least_active")); got != 1 {
		t.Errorf("instance info = %v", got)
	}

	m.UpdateUpstream("a", UpstreamState{Observed: 2, Healthy: false})
	if got := testutil.ToFloat64(m.reportedActive.WithLabelValues("a")); got != 0 {
		t.Errorf("reported after metric loss = %v", got)
	}
	if got := testutil.ToFloat64(m.metricsAvailable.WithLabelValues("a")); got != 0 {
		t.Errorf("available after metric loss = %v", got)
	}
	if got := testutil.ToFloat64(m.healthy.WithLabelValues("a")); got != 0 {
		t.Errorf("healthy after update = %v", got)
	}
}

func TestCounters(t *testing.T) {
	m, err := New([]string{"a"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.ObserveRequest("a", 200, 2*time.Second)
	m.ObserveRequest("a", 502, time.Second)
	if got := testutil.ToFloat64(m.requests.WithLabelValues("a", "200")); got != 1 {
		t.Errorf("200 counter = %v", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("a", "502")); got != 1 {
		t.Errorf("502 counter = %v", got)
	}
	m.ObserveRetry("a", "status_503")
	if got := testutil.ToFloat64(m.retries.WithLabelValues("a", "status_503")); got != 1 {
		t.Errorf("retry counter = %v", got)
	}
	m.ObserveSelection("a", "least_active")
	if got := testutil.ToFloat64(m.selections.WithLabelValues("a", "least_active")); got != 1 {
		t.Errorf("selection counter = %v", got)
	}
	m.ObserveScrape("a", true, 10*time.Millisecond)
	m.ObserveScrape("a", false, 20*time.Millisecond)
	if got := testutil.ToFloat64(m.scrapes.WithLabelValues("a", "success")); got != 1 {
		t.Errorf("successful scrapes = %v", got)
	}
	if got := testutil.ToFloat64(m.scrapes.WithLabelValues("a", "error")); got != 1 {
		t.Errorf("failed scrapes = %v", got)
	}
	m.SetRedisUp(true)
	if got := testutil.ToFloat64(m.redisUp); got != 1 {
		t.Errorf("redis up = %v", got)
	}
	m.SetRedisUp(false)
	if got := testutil.ToFloat64(m.redisUp); got != 0 {
		t.Errorf("redis down = %v", got)
	}
	m.RecordRedisError("sync")
	if got := testutil.ToFloat64(m.redisErrors.WithLabelValues("sync")); got != 1 {
		t.Errorf("redis error = %v", got)
	}
	m.ObserveSharedCache("put", nil)
	m.ObserveSharedCache("get", errors.New("boom"))
	if got := testutil.ToFloat64(m.sharedCacheOps.WithLabelValues("put", "success")); got != 1 {
		t.Errorf("shared put = %v", got)
	}
	if got := testutil.ToFloat64(m.sharedCacheOps.WithLabelValues("get", "error")); got != 1 {
		t.Errorf("shared get error = %v", got)
	}
}

func TestHandler(t *testing.T) {
	m, err := New([]string{"a"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m.ObserveRequest("a", 200, time.Second)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(recorder, request)

	if recorder.Code != 200 {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, expected := range []string{
		"llm_router_requests_total",
		"llm_router_upstream_observed_active_requests",
		"llm_router_build_info",
		"go_goroutines",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("metrics body missing %q", expected)
		}
	}
}
