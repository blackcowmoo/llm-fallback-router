package tracker_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackcowmoo/llm-fallback-router/internal/store"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

var testUpstreams = []string{"u1", "u2"}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newDistributed(t *testing.T, mr *miniredis.Miniredis, instanceID string, opts tracker.DistributedOptions) *tracker.Distributed {
	t.Helper()
	if opts.InstanceTTL == 0 {
		opts.InstanceTTL = 5 * time.Second
	}
	st, err := store.New(store.Options{
		Address:          mr.Addr(),
		KeyPrefix:        "test",
		InstanceID:       instanceID,
		InstanceTTL:      opts.InstanceTTL,
		ReadingTTL:       time.Minute,
		OperationTimeout: time.Second,
		DialTimeout:      time.Second,
		PoolSize:         4,
	})
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	opts.Logger = discardLogger()
	if opts.SyncInterval == 0 {
		opts.SyncInterval = 5 * time.Millisecond
	}
	if opts.SnapshotTTL == 0 {
		opts.SnapshotTTL = 0
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = time.Second
	}
	tr := tracker.NewDistributed(context.Background(), st, testUpstreams, opts)
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", message)
}

func TestLocalTracker(t *testing.T) {
	local := tracker.NewLocal(testUpstreams)
	release1 := local.Begin("u1")
	release2 := local.Begin("u1")
	if got := local.Snapshot(context.Background())["u1"]; got != 2 {
		t.Errorf("u1 = %d, want 2", got)
	}
	release1()
	release1() // idempotent
	if got := local.Local()["u1"]; got != 1 {
		t.Errorf("u1 = %d, want 1", got)
	}
	release2()
	if got := local.Snapshot(context.Background())["u1"]; got != 0 {
		t.Errorf("u1 = %d, want 0", got)
	}
	if local.Begin("unknown") == nil {
		t.Fatal("Begin should always return a release function")
	}
	if err := local.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if local.Status().Enabled {
		t.Error("local tracker should not report distributed state")
	}
}

func TestLocalTrackerConcurrent(t *testing.T) {
	local := tracker.NewLocal(testUpstreams)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				release := local.Begin("u1")
				release()
			}
		}()
	}
	wg.Wait()
	if got := local.Local()["u1"]; got != 0 {
		t.Errorf("counter = %d, want 0", got)
	}
}

func TestDistributedAggregatesAcrossInstances(t *testing.T) {
	mr := miniredis.RunT(t)
	instanceA := newDistributed(t, mr, "router-a", tracker.DistributedOptions{})
	instanceB := newDistributed(t, mr, "router-b", tracker.DistributedOptions{})

	releaseA := instanceA.Begin("u1")
	waitFor(t, 2*time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 1
	}, "instance B should observe A's request")

	// The instance's own count must not be double-counted via Redis.
	if got := instanceA.Snapshot(context.Background())["u1"]; got != 1 {
		t.Errorf("instance A total = %d, want 1", got)
	}

	releaseB := instanceB.Begin("u2")
	waitFor(t, 2*time.Second, func() bool {
		return instanceA.Snapshot(context.Background())["u2"] == 1
	}, "instance A should observe B's request")

	releaseA()
	waitFor(t, 2*time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 0
	}, "instance B should observe A's request finish")

	if got := instanceA.Snapshot(context.Background())["u2"]; got != 1 {
		t.Errorf("A total u2 = %d, want 1", got)
	}
	releaseB()
}

func TestDistributedCloseDeregisters(t *testing.T) {
	mr := miniredis.RunT(t)
	instanceA := newDistributed(t, mr, "router-a", tracker.DistributedOptions{})
	instanceB := newDistributed(t, mr, "router-b", tracker.DistributedOptions{})

	_ = instanceA.Begin("u1")
	waitFor(t, 2*time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 1
	}, "B should see A")

	if err := instanceA.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitFor(t, time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 0
	}, "B should stop seeing A after deregistration")
}

func TestDistributedCountsExpireAfterTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	instanceA := newDistributed(t, mr, "router-a", tracker.DistributedOptions{
		InstanceTTL:       2 * time.Second,
		HeartbeatInterval: 10 * time.Second,
	})
	instanceB := newDistributed(t, mr, "router-b", tracker.DistributedOptions{})

	_ = instanceA.Begin("u1")
	waitFor(t, 2*time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 1
	}, "B should see A")

	mr.FastForward(3 * time.Second)
	waitFor(t, 2*time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 0
	}, "expired counts should disappear")
}

func TestDistributedFallsBackToLocalWhenRedisIsDown(t *testing.T) {
	mr := miniredis.RunT(t)
	instanceA := newDistributed(t, mr, "router-a", tracker.DistributedOptions{})
	instanceB := newDistributed(t, mr, "router-b", tracker.DistributedOptions{})

	_ = instanceA.Begin("u1")
	waitFor(t, 2*time.Second, func() bool {
		return instanceB.Snapshot(context.Background())["u1"] == 1
	}, "B should see A")

	mr.Close()

	// Requests must keep working against local counters.
	release := instanceA.Begin("u1")
	if got := instanceA.Local()["u1"]; got != 2 {
		t.Errorf("local count = %d, want 2", got)
	}
	if got := instanceA.Snapshot(context.Background())["u1"]; got < 2 {
		t.Errorf("snapshot = %d, want at least local count 2", got)
	}
	release()

	waitFor(t, 2*time.Second, func() bool {
		status := instanceA.Status()
		return status.LastSyncError != "" || status.RemoteError != ""
	}, "Redis errors should be surfaced in status")
}

func TestDistributedFlush(t *testing.T) {
	mr := miniredis.RunT(t)
	// Long sync interval: only Flush can publish within the test window.
	instanceA := newDistributed(t, mr, "router-a", tracker.DistributedOptions{
		SyncInterval: time.Hour,
	})
	instanceB := newDistributed(t, mr, "router-b", tracker.DistributedOptions{})

	_ = instanceA.Begin("u1")
	if err := instanceA.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := instanceB.Snapshot(context.Background())["u1"]; got != 1 {
		t.Errorf("B snapshot = %d, want 1", got)
	}
	if status := instanceA.Status(); !status.Enabled || status.LastSync.IsZero() {
		t.Errorf("status = %+v", status)
	}
}
