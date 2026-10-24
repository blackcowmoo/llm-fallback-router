package balancer_test

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/balancer"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/metrics"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

type fakeMetrics struct {
	snapshots map[string]metrics.ReadingSnapshot
}

func (f *fakeMetrics) Snapshot(upstream string) (metrics.ReadingSnapshot, bool) {
	snapshot, ok := f.snapshots[upstream]
	return snapshot, ok
}

type fakeTracker struct {
	counts map[string]int64
	local  map[string]int64
}

func (f *fakeTracker) Begin(string) func() { return func() {} }
func (f *fakeTracker) Snapshot(context.Context) map[string]int64 {
	return copyMap(f.counts)
}
func (f *fakeTracker) Local() map[string]int64 { return copyMap(f.local) }
func (f *fakeTracker) Status() tracker.Status  { return tracker.Status{} }
func (f *fakeTracker) Close() error            { return nil }

func copyMap(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func upstream(name, rawURL string, capacity float64, maxActive int) balancer.Upstream {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return balancer.Upstream{Name: name, URL: parsed, Capacity: capacity, MaxActive: maxActive}
}

func reported(active float64) metrics.ReadingSnapshot {
	return metrics.ReadingSnapshot{
		Reading: metrics.Reading{Active: active, Timestamp: time.Now(), Source: "test"},
		Fresh:   true,
	}
}

func newBalancer(t *testing.T, upstreams []balancer.Upstream, opts balancer.Options) *balancer.Balancer {
	t.Helper()
	if opts.Tracker == nil {
		opts.Tracker = &fakeTracker{counts: map[string]int64{}, local: map[string]int64{}}
	}
	b, err := balancer.New(upstreams, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

func selectName(t *testing.T, b *balancer.Balancer, excluded map[string]bool) string {
	t.Helper()
	target, err := b.Select(context.Background(), excluded)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	return target.Name
}

func TestLeastActive(t *testing.T) {
	metricsSource := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reported(5),
		"b": reported(1),
	}}
	b := newBalancer(t, []balancer.Upstream{
		upstream("a", "http://a:8000", 1, 0),
		upstream("b", "http://b:8000", 1, 0),
	}, balancer.Options{Strategy: config.StrategyLeastActive, Metrics: metricsSource})

	if got := selectName(t, b, nil); got != "b" {
		t.Errorf("selected %q, want b", got)
	}
}

func TestObservedCountsRouteWhenMetricsMissing(t *testing.T) {
	tr := &fakeTracker{counts: map[string]int64{"a": 3, "b": 1}, local: map[string]int64{"a": 1}}
	b := newBalancer(t, []balancer.Upstream{
		upstream("a", "http://a:8000", 1, 0),
		upstream("b", "http://b:8000", 1, 0),
	}, balancer.Options{Strategy: config.StrategyLeastActive, Tracker: tr})

	if got := selectName(t, b, nil); got != "b" {
		t.Errorf("selected %q, want b", got)
	}
}

func TestCombineModes(t *testing.T) {
	metricsSource := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reported(1),
		"b": reported(5),
	}}
	tr := &fakeTracker{counts: map[string]int64{"a": 10, "b": 0}, local: map[string]int64{}}

	tests := []struct {
		combine string
		want    string
	}{
		{combine: config.CombineMax, want: "b"},
		{combine: config.CombineSum, want: "b"},
		{combine: config.CombineObserved, want: "b"},
		{combine: config.CombineReported, want: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.combine, func(t *testing.T) {
			b := newBalancer(t, []balancer.Upstream{
				upstream("a", "http://a:8000", 1, 0),
				upstream("b", "http://b:8000", 1, 0),
			}, balancer.Options{
				Strategy: config.StrategyLeastActive,
				Combine:  tt.combine,
				Metrics:  metricsSource,
				Tracker:  tr,
			})
			if got := selectName(t, b, nil); got != tt.want {
				t.Errorf("selected %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWeightedLeastActive(t *testing.T) {
	metricsSource := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reported(6),
		"b": reported(3),
	}}
	upstreams := []balancer.Upstream{
		upstream("a", "http://a:8000", 3, 0),
		upstream("b", "http://b:8000", 1, 0),
	}

	least := newBalancer(t, upstreams, balancer.Options{
		Strategy: config.StrategyLeastActive, Metrics: metricsSource,
	})
	if got := selectName(t, least, nil); got != "b" {
		t.Errorf("least_active selected %q, want b", got)
	}

	weighted := newBalancer(t, upstreams, balancer.Options{
		Strategy: config.StrategyWeightedLeastActive, Metrics: metricsSource,
	})
	if got := selectName(t, weighted, nil); got != "a" {
		t.Errorf("weighted_least_active selected %q, want a", got)
	}
}

func TestSaturation(t *testing.T) {
	metricsSource := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reported(2),
		"b": reported(3),
	}}
	upstreams := []balancer.Upstream{
		upstream("a", "http://a:8000", 1, 2),
		upstream("b", "http://b:8000", 1, 10),
	}

	b := newBalancer(t, upstreams, balancer.Options{
		Strategy: config.StrategyLeastActive, Metrics: metricsSource,
	})
	if got := selectName(t, b, nil); got != "b" {
		t.Errorf("selected %q, want unsaturated b", got)
	}

	saturatedMetrics := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reported(2), "b": reported(10),
	}}
	rejecting := newBalancer(t, upstreams, balancer.Options{
		Strategy:            config.StrategyLeastActive,
		Metrics:             saturatedMetrics,
		RejectWhenSaturated: true,
	})
	if _, err := rejecting.Select(context.Background(), nil); !errors.Is(err, balancer.ErrSaturated) {
		t.Fatalf("error = %v, want ErrSaturated", err)
	}

	queueing := newBalancer(t, upstreams, balancer.Options{
		Strategy: config.StrategyLeastActive, Metrics: saturatedMetrics,
	})
	if got := selectName(t, queueing, nil); got != "a" {
		t.Errorf("selected %q, want least-loaded a", got)
	}
}

func TestWeightedSaturationUsesUnweightedLoad(t *testing.T) {
	metricsSource := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": reported(5),
		"b": reported(2),
	}}
	upstreams := []balancer.Upstream{
		// a has a low weighted score but has reached its absolute limit.
		upstream("a", "http://a:8000", 10, 5),
		upstream("b", "http://b:8000", 1, 10),
	}
	b := newBalancer(t, upstreams, balancer.Options{
		Strategy:            config.StrategyWeightedLeastActive,
		Metrics:             metricsSource,
		RejectWhenSaturated: true,
	})

	if got := selectName(t, b, nil); got != "b" {
		t.Errorf("selected %q, want unsaturated b", got)
	}
	for _, state := range b.States(context.Background()) {
		if state.Name == "a" && !state.Saturated {
			t.Error("a should be saturated based on its raw active count")
		}
	}
}

func TestHealthRecoveryAndFailOpen(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	upstreams := []balancer.Upstream{
		upstream("a", "http://a:8000", 1, 0),
		upstream("b", "http://b:8000", 1, 0),
	}

	b := newBalancer(t, upstreams, balancer.Options{
		Strategy:         config.StrategyRoundRobin,
		FailureThreshold: 2,
		RecoveryInterval: 10 * time.Second,
		FailOpen:         true,
		Now:              clock,
	})
	// Round-robin starts at a; make b fail twice.
	b.ReportFailure("b", "boom")
	b.ReportFailure("b", "boom")
	for i := 0; i < 3; i++ {
		if got := selectName(t, b, nil); got != "a" {
			t.Fatalf("selected %q while b is unhealthy", got)
		}
	}
	if got := b.HealthyCount(); got != 1 {
		t.Errorf("healthy count = %d, want 1", got)
	}

	now = now.Add(11 * time.Second)
	if got := b.HealthyCount(); got != 2 {
		t.Errorf("healthy count after recovery = %d, want 2", got)
	}

	// A failure after the recovery window re-opens the circuit immediately.
	b.ReportFailure("b", "still bad")
	if got := b.HealthyCount(); got != 1 {
		t.Errorf("healthy count = %d, want 1", got)
	}
	b.ReportSuccess("b")
	if got := b.HealthyCount(); got != 2 {
		t.Errorf("healthy count after success = %d, want 2", got)
	}

	b.ReportFailure("a", "boom")
	b.ReportFailure("a", "boom")
	b.ReportFailure("b", "boom")
	b.ReportFailure("b", "boom")
	if _, err := b.Select(context.Background(), nil); err != nil {
		t.Fatalf("fail-open Select: %v", err)
	}

	strict := newBalancer(t, upstreams, balancer.Options{
		Strategy:         config.StrategyRoundRobin,
		FailureThreshold: 1,
		RecoveryInterval: time.Minute,
		FailOpen:         false,
		Now:              clock,
	})
	strict.ReportFailure("a", "boom")
	strict.ReportFailure("b", "boom")
	if _, err := strict.Select(context.Background(), nil); !errors.Is(err, balancer.ErrNoUpstream) {
		t.Fatalf("error = %v, want ErrNoUpstream", err)
	}
}

func TestExclusions(t *testing.T) {
	b := newBalancer(t, []balancer.Upstream{
		upstream("a", "http://a:8000", 1, 0),
		upstream("b", "http://b:8000", 1, 0),
	}, balancer.Options{Strategy: config.StrategyRoundRobin})

	if got := selectName(t, b, map[string]bool{"a": true}); got != "b" {
		t.Errorf("selected %q, want b", got)
	}
	if _, err := b.Select(context.Background(), map[string]bool{"a": true, "b": true}); !errors.Is(err, balancer.ErrNoUpstream) {
		t.Fatalf("error = %v, want ErrNoUpstream", err)
	}
}

func TestRoundRobinAndTieRotation(t *testing.T) {
	upstreams := []balancer.Upstream{
		upstream("a", "http://a:8000", 1, 0),
		upstream("b", "http://b:8000", 1, 0),
	}
	rr := newBalancer(t, upstreams, balancer.Options{Strategy: config.StrategyRoundRobin})
	want := []string{"a", "b", "a", "b"}
	for i, expected := range want {
		if got := selectName(t, rr, nil); got != expected {
			t.Errorf("call %d selected %q, want %q", i, got, expected)
		}
	}

	least := newBalancer(t, upstreams, balancer.Options{Strategy: config.StrategyLeastActive})
	if got := selectName(t, least, nil); got != "a" {
		t.Errorf("first tie selection = %q, want a", got)
	}
	if got := selectName(t, least, nil); got != "b" {
		t.Errorf("second tie selection = %q, want rotating b", got)
	}
}

func TestStates(t *testing.T) {
	metricsSource := &fakeMetrics{snapshots: map[string]metrics.ReadingSnapshot{
		"a": {Reading: metrics.Reading{Active: 4, Timestamp: time.Now(), Source: "prometheus"}, Age: 150 * time.Millisecond, Fresh: true},
	}}
	tr := &fakeTracker{counts: map[string]int64{"a": 7, "b": 2}, local: map[string]int64{"a": 3, "b": 2}}
	b := newBalancer(t, []balancer.Upstream{
		upstream("a", "http://a:8000", 2, 10),
		upstream("b", "http://b:8000", 1, 0),
	}, balancer.Options{
		Strategy: config.StrategyLeastActive,
		Metrics:  metricsSource,
		Tracker:  tr,
	})

	states := b.States(context.Background())
	if len(states) != 2 {
		t.Fatalf("states = %d", len(states))
	}
	a := states[0]
	if a.Name != "a" || !a.Healthy {
		t.Errorf("state a = %+v", a)
	}
	if a.ReportedActive == nil || *a.ReportedActive != 4 {
		t.Errorf("reported active = %v", a.ReportedActive)
	}
	if a.ReportedAgeMS != 150 || !a.ReportedFresh {
		t.Errorf("reported age/freshness = %d/%v", a.ReportedAgeMS, a.ReportedFresh)
	}
	if a.ObservedActive != 7 || a.LocalActive != 3 {
		t.Errorf("observed/local = %d/%d", a.ObservedActive, a.LocalActive)
	}
	if a.EffectiveLoad != 7 {
		t.Errorf("effective load = %v, want max(4,7)=7", a.EffectiveLoad)
	}
	if a.ReportedSource != "prometheus" {
		t.Errorf("source = %q", a.ReportedSource)
	}
	if states[1].ReportedActive != nil {
		t.Errorf("b should have no reported active: %v", states[1].ReportedActive)
	}
}

func TestNewValidation(t *testing.T) {
	validURL, _ := url.Parse("http://a:8000")
	if _, err := balancer.New(nil, balancer.Options{}); err == nil {
		t.Error("expected error for no upstreams")
	}
	if _, err := balancer.New([]balancer.Upstream{{Name: "a", URL: validURL}}, balancer.Options{}); err == nil {
		t.Error("expected error for missing tracker")
	}
	if _, err := balancer.New([]balancer.Upstream{{Name: "a", URL: validURL}}, balancer.Options{
		Tracker: &fakeTracker{counts: map[string]int64{}, local: map[string]int64{}},
	}); err != nil {
		t.Errorf("New: %v", err)
	}
	validURL2, _ := url.Parse("http://b:8000")
	if _, err := balancer.New([]balancer.Upstream{
		{Name: "a", URL: validURL},
		{Name: "a", URL: validURL2},
	}, balancer.Options{Tracker: &fakeTracker{}}); err == nil {
		t.Error("expected duplicate error")
	}
	if _, err := balancer.New([]balancer.Upstream{{Name: "a"}}, balancer.Options{
		Tracker: &fakeTracker{},
	}); err == nil {
		t.Error("expected missing URL error")
	}
}
