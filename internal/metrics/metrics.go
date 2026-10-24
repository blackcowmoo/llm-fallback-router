// Package metrics collects and caches active-request metrics from LLM backends.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"
)

// Reading is one observation of an upstream's load metrics.
type Reading struct {
	Active    float64   `json:"active"`
	Waiting   *float64  `json:"waiting,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"`
}

// ReadingSnapshot is a cached reading together with its age and freshness.
type ReadingSnapshot struct {
	Reading Reading
	Age     time.Duration
	Fresh   bool
}

// Cache is a small in-memory TTL cache keyed by upstream name.
type Cache struct {
	ttl      time.Duration
	staleTTL time.Duration

	mu      sync.RWMutex
	entries map[string]Reading
}

// NewCache returns a cache that considers entries fresh for ttl and usable for
// staleTTL. Entries older than staleTTL are not returned by Snapshot.
func NewCache(ttl, staleTTL time.Duration) *Cache {
	return &Cache{
		ttl:      ttl,
		staleTTL: staleTTL,
		entries:  make(map[string]Reading),
	}
}

// Put stores a reading. A zero timestamp is replaced with the current time.
func (c *Cache) Put(upstream string, reading Reading) {
	if reading.Timestamp.IsZero() {
		reading.Timestamp = time.Now().UTC()
	}
	c.mu.Lock()
	c.entries[upstream] = reading
	c.mu.Unlock()
}

// Lookup returns the most recent reading regardless of age.
func (c *Cache) Lookup(upstream string) (Reading, bool) {
	c.mu.RLock()
	reading, ok := c.entries[upstream]
	c.mu.RUnlock()
	return reading, ok
}

// Snapshot returns a reading if it is no older than the stale TTL.
func (c *Cache) Snapshot(upstream string, now time.Time) (ReadingSnapshot, bool) {
	reading, ok := c.Lookup(upstream)
	if !ok {
		return ReadingSnapshot{}, false
	}
	age := now.Sub(reading.Timestamp)
	if age < 0 {
		age = 0
	}
	if c.staleTTL > 0 && age > c.staleTTL {
		return ReadingSnapshot{}, false
	}
	return ReadingSnapshot{
		Reading: reading,
		Age:     age,
		Fresh:   c.ttl <= 0 || age <= c.ttl,
	}, true
}

// StaleTTL exposes the configured maximum usable age.
func (c *Cache) StaleTTL() time.Duration { return c.staleTTL }

// FreshTTL exposes the configured freshness window.
func (c *Cache) FreshTTL() time.Duration { return c.ttl }

// Shared is implemented by a Redis-backed shared metric cache.
type Shared interface {
	GetReading(ctx context.Context, upstream string) (Reading, bool, error)
	PutReading(ctx context.Context, upstream string, reading Reading) error
}

// Hooks are optional instrumentation callbacks used by the manager.
type Hooks struct {
	Scrape func(upstream string, ok bool, duration time.Duration)
	Shared func(operation string, err error)
}

// Scraper reads one upstream's metrics endpoint.
type Scraper interface {
	// Interval is how often the scraper should run.
	Interval() time.Duration
	// Scrape performs one scrape. Implementations must honour the context.
	Scrape(ctx context.Context) (Reading, error)
}

// Job pairs an upstream name with its scraper.
type Job struct {
	Name    string
	Scraper Scraper
}

// Manager runs one scrape loop per upstream and keeps the cache up to date.
type Manager struct {
	cache  *Cache
	shared Shared
	hooks  Hooks
	logger *slog.Logger
	jobs   []Job
}

// NewManager creates a scrape manager. shared may be nil.
func NewManager(cache *Cache, shared Shared, hooks Hooks, logger *slog.Logger, jobs []Job) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cache:  cache,
		shared: shared,
		hooks:  hooks,
		logger: logger,
		jobs:   jobs,
	}
}

// Run scrapes until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, job := range m.jobs {
		job := job
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runJob(ctx, job)
		}()
	}
	wg.Wait()
}

func (m *Manager) runJob(ctx context.Context, job Job) {
	m.hydrate(ctx, job.Name)

	failures := 0
	var lastFailureLog time.Time
	for {
		start := time.Now()
		reading, err := job.Scraper.Scrape(ctx)
		duration := time.Since(start)
		if m.hooks.Scrape != nil {
			m.hooks.Scrape(job.Name, err == nil, duration)
		}
		if err == nil {
			if reading.Timestamp.IsZero() {
				reading.Timestamp = time.Now().UTC()
			}
			m.cache.Put(job.Name, reading)
			m.share(ctx, job.Name, reading)
			if failures > 0 {
				m.logger.Info("upstream metrics recovered",
					"upstream", job.Name, "failures", failures, "active", reading.Active)
			}
			failures = 0
		} else {
			failures++
			now := time.Now()
			if failures == 1 || now.Sub(lastFailureLog) >= 30*time.Second {
				m.logger.Warn("upstream metrics scrape failed",
					"upstream", job.Name, "error", err, "failures", failures)
				lastFailureLog = now
			}
			// A peer router may still have a recent value for this upstream.
			m.hydrate(ctx, job.Name)
		}

		timer := time.NewTimer(job.Scraper.Interval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (m *Manager) share(ctx context.Context, upstream string, reading Reading) {
	if m.shared == nil {
		return
	}
	if err := m.shared.PutReading(ctx, upstream, reading); err != nil {
		if m.hooks.Shared != nil {
			m.hooks.Shared("put", err)
		}
		m.logger.Debug("could not share metrics reading", "upstream", upstream, "error", err)
	}
}

func (m *Manager) hydrate(ctx context.Context, upstream string) {
	if m.shared == nil {
		return
	}
	reading, ok, err := m.shared.GetReading(ctx, upstream)
	if err != nil {
		if m.hooks.Shared != nil {
			m.hooks.Shared("get", err)
		}
		m.logger.Debug("could not read shared metrics reading", "upstream", upstream, "error", err)
		return
	}
	if !ok || reading.Timestamp.IsZero() {
		return
	}
	if m.cache.staleTTL > 0 && time.Since(reading.Timestamp) > m.cache.staleTTL {
		return
	}
	current, ok := m.cache.Lookup(upstream)
	if !ok || reading.Timestamp.After(current.Timestamp) {
		m.cache.Put(upstream, reading)
	}
}

// Snapshot returns the cached reading for an upstream if it is still usable.
func (m *Manager) Snapshot(upstream string) (ReadingSnapshot, bool) {
	return m.cache.Snapshot(upstream, time.Now())
}

// Lookup returns the cached reading for an upstream regardless of age.
func (m *Manager) Lookup(upstream string) (Reading, bool) {
	return m.cache.Lookup(upstream)
}

// Cache exposes the underlying cache (used by tests and diagnostics).
func (m *Manager) Cache() *Cache { return m.cache }

var errBodyTooLarge = errors.New("response body too large")

const maxMetricsBody = 8 << 20

func fetch(ctx context.Context, client *http.Client, url string, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain, application/json;q=0.9, */*;q=0.1")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var snippet [512]byte
		n, _ := resp.Body.Read(snippet[:])
		return nil, fmt.Errorf("metrics endpoint returned HTTP %d: %s", resp.StatusCode, string(snippet[:n]))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetricsBody+1))
	if err != nil {
		return nil, fmt.Errorf("read metrics response: %w", err)
	}
	if len(body) > maxMetricsBody {
		return nil, errBodyTooLarge
	}
	return body, nil
}

// selection helpers ---------------------------------------------------------

func sampleLabels(metricLabels map[string]string, required map[string]string) bool {
	for key, want := range required {
		if got, ok := metricLabels[key]; !ok || got != want {
			return false
		}
	}
	return true
}

func aggregate(values []float64, method string) (float64, error) {
	if len(values) == 0 {
		return 0, errors.New("no matching samples")
	}
	switch method {
	case "sum", "":
		var total float64
		for _, v := range values {
			total += v
		}
		return total, nil
	case "max":
		m := values[0]
		for _, v := range values[1:] {
			m = math.Max(m, v)
		}
		return m, nil
	case "min":
		m := values[0]
		for _, v := range values[1:] {
			m = math.Min(m, v)
		}
		return m, nil
	case "avg":
		var total float64
		for _, v := range values {
			total += v
		}
		return total / float64(len(values)), nil
	default:
		return 0, fmt.Errorf("unsupported aggregation %q", method)
	}
}

func sanitizeValue(name string, value float64) (float64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("metric %s has non-finite value %v", name, value)
	}
	if value < 0 {
		return 0, nil
	}
	return value, nil
}
