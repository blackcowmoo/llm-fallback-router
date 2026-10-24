// Package balancer selects an upstream using reported and observed active
// request counts.
package balancer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/metrics"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

var (
	// ErrNoUpstream is returned when every candidate is excluded or unhealthy.
	ErrNoUpstream = errors.New("no upstream available")
	// ErrSaturated is returned when all upstreams reached max_active and
	// reject_when_saturated is enabled.
	ErrSaturated = errors.New("all upstreams are saturated")
)

// MetricsSource supplies cached upstream metrics.
type MetricsSource interface {
	Snapshot(upstream string) (metrics.ReadingSnapshot, bool)
}

// Upstream is the static configuration of one backend.
type Upstream struct {
	Name      string
	URL       *url.URL
	Capacity  float64
	MaxActive int
	Headers   map[string]string
}

// Target is a selected backend for one request attempt.
type Target struct {
	Name    string
	URL     *url.URL
	Headers map[string]string
}

// Options configures selection behavior.
type Options struct {
	Strategy            string
	Combine             string
	Metrics             MetricsSource
	Tracker             tracker.Tracker
	FailureThreshold    int
	RecoveryInterval    time.Duration
	FailOpen            bool
	RejectWhenSaturated bool
	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
}

// Balancer holds per-upstream runtime state.
type Balancer struct {
	opts      Options
	upstreams []*upstreamState

	mu         sync.Mutex
	roundRobin uint64
}

type upstreamState struct {
	Upstream
	failures       int
	unhealthyUntil time.Time
	lastError      string
}

// New creates a balancer. URLs must be valid and upstream names unique; the
// configuration loader already enforces this, but New validates defensively.
func New(upstreams []Upstream, opts Options) (*Balancer, error) {
	if opts.Tracker == nil {
		return nil, errors.New("balancer: tracker is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.FailureThreshold < 1 {
		opts.FailureThreshold = 1
	}
	if opts.RecoveryInterval <= 0 {
		opts.RecoveryInterval = 15 * time.Second
	}
	if opts.Combine == "" {
		opts.Combine = config.CombineMax
	}
	if opts.Strategy == "" {
		opts.Strategy = config.StrategyLeastActive
	}

	seen := make(map[string]bool, len(upstreams))
	states := make([]*upstreamState, 0, len(upstreams))
	for _, upstream := range upstreams {
		if upstream.Name == "" {
			return nil, errors.New("balancer: upstream name is empty")
		}
		if seen[upstream.Name] {
			return nil, fmt.Errorf("balancer: duplicate upstream %q", upstream.Name)
		}
		seen[upstream.Name] = true
		if upstream.URL == nil {
			return nil, fmt.Errorf("balancer: upstream %q has no URL", upstream.Name)
		}
		if upstream.Capacity <= 0 {
			upstream.Capacity = 1
		}
		headers := make(map[string]string, len(upstream.Headers))
		for key, value := range upstream.Headers {
			headers[key] = value
		}
		upstream.Headers = headers
		states = append(states, &upstreamState{Upstream: upstream})
	}
	if len(states) == 0 {
		return nil, errors.New("balancer: at least one upstream is required")
	}
	return &Balancer{opts: opts, upstreams: states}, nil
}

// Select chooses an upstream and returns it. The excluded set is used for
// per-request failover after an attempt failed.
func (b *Balancer) Select(ctx context.Context, excluded map[string]bool) (*Target, error) {
	observed := b.opts.Tracker.Snapshot(ctx)
	now := b.opts.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	candidates := make([]candidate, 0, len(b.upstreams))
	for _, upstream := range b.upstreams {
		if excluded[upstream.Name] {
			continue
		}
		healthy := upstream.unhealthyUntil.IsZero() || !now.Before(upstream.unhealthyUntil)
		load, rawLoad, _, _, _ := b.load(upstream.Name, upstream.Capacity, observed)
		candidates = append(candidates, candidate{
			upstream:  upstream,
			healthy:   healthy,
			load:      load,
			saturated: upstream.MaxActive > 0 && rawLoad >= float64(upstream.MaxActive),
		})
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: all upstreams excluded", ErrNoUpstream)
	}

	eligible := filterHealthy(candidates)
	if len(eligible) == 0 {
		if !b.opts.FailOpen {
			return nil, fmt.Errorf("%w: all upstreams are unhealthy", ErrNoUpstream)
		}
		eligible = candidates
	}
	if len(eligible) == 0 {
		return nil, ErrNoUpstream
	}

	if unsaturated := filterUnsaturated(eligible); len(unsaturated) > 0 {
		eligible = unsaturated
	} else if b.opts.RejectWhenSaturated {
		return nil, fmt.Errorf("%w: %d upstream(s) at max_active", ErrSaturated, len(eligible))
	}

	var chosen *candidate
	if b.opts.Strategy == config.StrategyRoundRobin {
		index := int(b.roundRobin % uint64(len(eligible))) //nolint:gosec // modulo by len(eligible) fits in int
		b.roundRobin++
		chosen = &eligible[index]
	} else {
		start := int(b.roundRobin % uint64(len(eligible))) //nolint:gosec // modulo by len(eligible) fits in int
		b.roundRobin++
		best := -1
		for i := 0; i < len(eligible); i++ {
			index := (start + i) % len(eligible)
			if best == -1 || eligible[index].load < eligible[best].load-1e-9 {
				best = index
			}
		}
		chosen = &eligible[best]
	}

	return &Target{
		Name:    chosen.upstream.Name,
		URL:     chosen.upstream.URL,
		Headers: chosen.upstream.Headers,
	}, nil
}

// ReportSuccess clears the failure state for an upstream.
func (b *Balancer) ReportSuccess(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if upstream := b.find(name); upstream != nil {
		upstream.failures = 0
		upstream.unhealthyUntil = time.Time{}
		upstream.lastError = ""
	}
}

// ReportFailure records an upstream failure and opens the recovery window once
// the configured threshold is reached.
func (b *Balancer) ReportFailure(name, reason string) {
	now := b.opts.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	upstream := b.find(name)
	if upstream == nil {
		return
	}
	upstream.failures++
	upstream.lastError = reason
	if upstream.failures >= b.opts.FailureThreshold {
		upstream.unhealthyUntil = now.Add(b.opts.RecoveryInterval)
	}
}

// UpstreamState is an inspection snapshot for admin endpoints and metrics.
type UpstreamState struct {
	Name           string        `json:"name"`
	URL            string        `json:"url"`
	Healthy        bool          `json:"healthy"`
	Failures       int           `json:"failures"`
	UnhealthyUntil string        `json:"unhealthy_until,omitempty"`
	LastError      string        `json:"last_error,omitempty"`
	Capacity       float64       `json:"capacity"`
	MaxActive      int           `json:"max_active"`
	Saturated      bool          `json:"saturated"`
	ReportedActive *float64      `json:"reported_active,omitempty"`
	ReportedAge    time.Duration `json:"-"`
	ReportedAgeMS  int64         `json:"reported_age_ms,omitempty"`
	ReportedFresh  bool          `json:"reported_fresh"`
	ReportedSource string        `json:"reported_source,omitempty"`
	ObservedActive int64         `json:"observed_active"`
	LocalActive    int64         `json:"local_active"`
	EffectiveLoad  float64       `json:"effective_load"`
}

// States returns an inspection snapshot for every upstream.
func (b *Balancer) States(ctx context.Context) []UpstreamState {
	observed := b.opts.Tracker.Snapshot(ctx)
	local := b.opts.Tracker.Local()
	now := b.opts.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	states := make([]UpstreamState, 0, len(b.upstreams))
	for _, upstream := range b.upstreams {
		load, rawLoad, reported, reportedOK, fresh := b.load(upstream.Name, upstream.Capacity, observed)
		healthy := upstream.unhealthyUntil.IsZero() || !now.Before(upstream.unhealthyUntil)
		state := UpstreamState{
			Name:           upstream.Name,
			URL:            upstream.URL.String(),
			Healthy:        healthy,
			Failures:       upstream.failures,
			LastError:      upstream.lastError,
			Capacity:       upstream.Capacity,
			MaxActive:      upstream.MaxActive,
			Saturated:      upstream.MaxActive > 0 && rawLoad >= float64(upstream.MaxActive),
			ObservedActive: observed[upstream.Name],
			LocalActive:    local[upstream.Name],
			EffectiveLoad:  load,
			ReportedFresh:  fresh,
		}
		if !upstream.unhealthyUntil.IsZero() && now.Before(upstream.unhealthyUntil) {
			state.UnhealthyUntil = upstream.unhealthyUntil.UTC().Format(time.RFC3339Nano)
		}
		if reportedOK {
			value := reported
			state.ReportedActive = &value
			if b.opts.Metrics != nil {
				if snapshot, ok := b.opts.Metrics.Snapshot(upstream.Name); ok {
					state.ReportedAge = snapshot.Age
					state.ReportedAgeMS = snapshot.Age.Milliseconds()
					state.ReportedFresh = snapshot.Fresh
					state.ReportedSource = snapshot.Reading.Source
				}
			}
		}
		states = append(states, state)
	}
	return states
}

// HealthyCount returns how many upstreams are currently outside their
// recovery window. It is used by readiness checks.
func (b *Balancer) HealthyCount() int {
	now := b.opts.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
	for _, upstream := range b.upstreams {
		if upstream.unhealthyUntil.IsZero() || !now.Before(upstream.unhealthyUntil) {
			count++
		}
	}
	return count
}

// UpstreamCount returns the number of configured upstreams.
func (b *Balancer) UpstreamCount() int { return len(b.upstreams) }

// Strategy returns the configured strategy name.
func (b *Balancer) Strategy() string { return b.opts.Strategy }

// load returns the routing score, the unweighted effective load used for
// saturation checks, the reported load, whether a reported reading exists, and
// whether that reading is fresh.
func (b *Balancer) load(name string, capacity float64, observed map[string]int64) (float64, float64, float64, bool, bool) {
	observedCount := observed[name]
	if observedCount < 0 {
		observedCount = 0
	}
	observedLoad := float64(observedCount)

	reportedLoad := 0.0
	reportedOK := false
	fresh := false
	if b.opts.Metrics != nil {
		if snapshot, ok := b.opts.Metrics.Snapshot(name); ok {
			reportedLoad = math.Max(0, snapshot.Reading.Active)
			reportedOK = true
			fresh = snapshot.Fresh
		}
	}

	var load float64
	switch b.opts.Combine {
	case config.CombineObserved:
		load = observedLoad
	case config.CombineReported:
		if reportedOK {
			load = reportedLoad
		} else {
			load = observedLoad
		}
	case config.CombineSum:
		load = observedLoad + reportedLoad
		if !reportedOK {
			load = observedLoad
		}
	default: // max
		if reportedOK {
			load = math.Max(observedLoad, reportedLoad)
		} else {
			load = observedLoad
		}
	}
	rawLoad := load
	if b.opts.Strategy == config.StrategyWeightedLeastActive {
		if capacity <= 0 {
			capacity = 1
		}
		load /= capacity
	}
	return load, rawLoad, reportedLoad, reportedOK, fresh
}

func (b *Balancer) find(name string) *upstreamState {
	for _, upstream := range b.upstreams {
		if upstream.Name == name {
			return upstream
		}
	}
	return nil
}

type candidate struct {
	upstream  *upstreamState
	healthy   bool
	load      float64
	saturated bool
}

func filterHealthy(candidates []candidate) []candidate {
	out := make([]candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.healthy {
			out = append(out, candidate)
		}
	}
	return out
}

func filterUnsaturated(candidates []candidate) []candidate {
	out := make([]candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !candidate.saturated {
			out = append(out, candidate)
		}
	}
	return out
}
