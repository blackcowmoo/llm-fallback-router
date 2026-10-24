package metrics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCacheFreshness(t *testing.T) {
	cache := NewCache(20*time.Millisecond, 100*time.Millisecond)
	now := time.Now()

	cache.Put("a", Reading{Active: 1, Timestamp: now})
	snapshot, ok := cache.Snapshot("a", now)
	if !ok || !snapshot.Fresh || snapshot.Reading.Active != 1 {
		t.Fatalf("snapshot = %+v, ok = %v", snapshot, ok)
	}

	stale := now.Add(-50 * time.Millisecond)
	cache.Put("a", Reading{Active: 2, Timestamp: stale})
	snapshot, ok = cache.Snapshot("a", now)
	if !ok || snapshot.Fresh {
		t.Fatalf("expected usable but stale snapshot, got %+v ok=%v", snapshot, ok)
	}
	if snapshot.Age != 50*time.Millisecond {
		t.Errorf("age = %s", snapshot.Age)
	}

	if _, ok := cache.Snapshot("a", now.Add(200*time.Millisecond)); ok {
		t.Error("expired reading should not be usable")
	}
	if _, ok := cache.Snapshot("missing", now); ok {
		t.Error("missing reading should not be usable")
	}
}

type fakeScraper struct {
	interval time.Duration
	err      error

	mu    sync.Mutex
	calls int
}

func (f *fakeScraper) Interval() time.Duration { return f.interval }

func (f *fakeScraper) Scrape(ctx context.Context) (Reading, error) {
	if err := ctx.Err(); err != nil {
		return Reading{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return Reading{}, f.err
	}
	return Reading{Active: float64(f.calls), Source: "fake", Timestamp: time.Now()}, nil
}

type fakeShared struct {
	mu       sync.Mutex
	readings map[string]Reading
	getErr   error
	putErr   error
	gets     int
	puts     int
}

func (f *fakeShared) GetReading(ctx context.Context, upstream string) (Reading, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.getErr != nil {
		return Reading{}, false, f.getErr
	}
	reading, ok := f.readings[upstream]
	return reading, ok, nil
}

func (f *fakeShared) PutReading(ctx context.Context, upstream string, reading Reading) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.putErr != nil {
		return f.putErr
	}
	f.readings[upstream] = reading
	return nil
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", message)
}

func TestManagerScrapesAndShares(t *testing.T) {
	cache := NewCache(time.Second, 10*time.Second)
	shared := &fakeShared{readings: map[string]Reading{}}
	scraper := &fakeScraper{interval: 5 * time.Millisecond}

	var scrapeCalls int
	var mu sync.Mutex
	manager := NewManager(cache, shared, Hooks{
		Scrape: func(upstream string, ok bool, d time.Duration) {
			mu.Lock()
			scrapeCalls++
			mu.Unlock()
			if upstream != "a" || !ok {
				t.Errorf("unexpected scrape hook: upstream=%q ok=%v", upstream, ok)
			}
		},
	}, nil, []Job{{Name: "a", Scraper: scraper}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.Run(ctx)
		close(done)
	}()

	waitFor(t, 2*time.Second, func() bool {
		reading, ok := cache.Lookup("a")
		return ok && reading.Active >= 1
	}, "cache should receive a reading")

	waitFor(t, 2*time.Second, func() bool {
		shared.mu.Lock()
		defer shared.mu.Unlock()
		return shared.puts > 0 && shared.readings["a"].Active >= 1
	}, "reading should be shared")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if scrapeCalls == 0 {
		t.Error("scrape hook was never called")
	}
}

func TestManagerHydratesFromSharedWhenScrapeFails(t *testing.T) {
	cache := NewCache(time.Second, 10*time.Second)
	shared := &fakeShared{readings: map[string]Reading{
		"a": {Active: 4, Source: "peer", Timestamp: time.Now()},
	}}
	scraper := &fakeScraper{interval: 5 * time.Millisecond, err: errors.New("upstream down")}

	manager := NewManager(cache, shared, Hooks{}, nil, []Job{{Name: "a", Scraper: scraper}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		reading, ok := cache.Lookup("a")
		return ok && reading.Active == 4 && reading.Source == "peer"
	}, "shared reading should hydrate the local cache")
}

func TestManagerIgnoresExpiredSharedReading(t *testing.T) {
	cache := NewCache(time.Second, 10*time.Second)
	shared := &fakeShared{readings: map[string]Reading{
		"a": {Active: 9, Source: "peer", Timestamp: time.Now().Add(-time.Minute)},
	}}
	scraper := &fakeScraper{interval: 5 * time.Millisecond, err: errors.New("upstream down")}

	manager := NewManager(cache, shared, Hooks{}, nil, []Job{{Name: "a", Scraper: scraper}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Run(ctx)

	waitFor(t, time.Second, func() bool {
		scraper.mu.Lock()
		defer scraper.mu.Unlock()
		return scraper.calls > 2
	}, "scraper should run")
	if reading, ok := cache.Lookup("a"); ok {
		t.Fatalf("expired reading was hydrated: %+v", reading)
	}
}

func TestManagerStopsWithContext(t *testing.T) {
	manager := NewManager(NewCache(time.Second, time.Second), nil, Hooks{}, nil, []Job{
		{Name: "a", Scraper: &fakeScraper{interval: time.Millisecond}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
