package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackcowmoo/llm-fallback-router/internal/metrics"
	"github.com/blackcowmoo/llm-fallback-router/internal/store"
)

func newStore(t *testing.T, mr *miniredis.Miniredis, instanceID string, ttl time.Duration) *store.Store {
	t.Helper()
	st, err := store.New(store.Options{
		Address:          mr.Addr(),
		KeyPrefix:        "test",
		InstanceID:       instanceID,
		InstanceTTL:      ttl,
		ReadingTTL:       5 * time.Second,
		OperationTimeout: time.Second,
		DialTimeout:      time.Second,
		PoolSize:         4,
	})
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return st
}

func TestHeartbeatAndRemoteCounts(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	instanceA := newStore(t, mr, "router-a", 30*time.Second)
	instanceB := newStore(t, mr, "router-b", 30*time.Second)

	if err := instanceA.Heartbeat(ctx, map[string]int64{"a": 2, "b": 0}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	// An instance never sees its own published counts in the remote snapshot;
	// callers add local counts separately.
	counts, err := instanceA.ActiveCounts(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatalf("ActiveCounts: %v", err)
	}
	if counts["a"] != 0 || counts["b"] != 0 {
		t.Errorf("own counts leaked into remote snapshot: %v", counts)
	}

	counts, err = instanceB.ActiveCounts(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatalf("ActiveCounts: %v", err)
	}
	if counts["a"] != 2 || counts["b"] != 0 {
		t.Errorf("remote counts = %v, want a=2 b=0", counts)
	}

	if err := instanceB.Heartbeat(ctx, map[string]int64{"a": 1, "b": 3}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	counts, err = instanceA.ActiveCounts(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatalf("ActiveCounts: %v", err)
	}
	if counts["a"] != 1 || counts["b"] != 3 {
		t.Errorf("remote counts = %v, want a=1 b=3", counts)
	}
}

func TestHeartbeatTTLExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	instanceA := newStore(t, mr, "router-a", 2*time.Second)
	instanceB := newStore(t, mr, "router-b", 30*time.Second)

	if err := instanceA.Heartbeat(ctx, map[string]int64{"a": 4}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	mr.FastForward(3 * time.Second)

	counts, err := instanceB.ActiveCounts(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("ActiveCounts: %v", err)
	}
	if counts["a"] != 0 {
		t.Errorf("expired instance counts = %d, want 0", counts["a"])
	}
}

func TestDeregister(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	instanceA := newStore(t, mr, "router-a", 30*time.Second)
	instanceB := newStore(t, mr, "router-b", 30*time.Second)

	if err := instanceA.Heartbeat(ctx, map[string]int64{"a": 4}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := instanceA.Deregister(ctx); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	counts, err := instanceB.ActiveCounts(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("ActiveCounts: %v", err)
	}
	if counts["a"] != 0 {
		t.Errorf("counts after deregister = %d, want 0", counts["a"])
	}
	for _, key := range mr.Keys() {
		if strings.Contains(key, "active:router-a") {
			t.Errorf("active key still present: %s", key)
		}
	}
}

func TestReadingRoundTrip(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	st := newStore(t, mr, "router-a", 30*time.Second)

	waiting := 2.0
	reading := metrics.Reading{
		Active:    3,
		Waiting:   &waiting,
		Timestamp: time.Now().UTC().Truncate(time.Microsecond),
		Source:    "prometheus",
	}
	if err := st.PutReading(ctx, "upstream-a", reading); err != nil {
		t.Fatalf("PutReading: %v", err)
	}
	got, ok, err := st.GetReading(ctx, "upstream-a")
	if err != nil {
		t.Fatalf("GetReading: %v", err)
	}
	if !ok {
		t.Fatal("reading not found")
	}
	if got.Active != reading.Active || got.Source != reading.Source {
		t.Errorf("reading = %+v", got)
	}
	if got.Waiting == nil || *got.Waiting != waiting {
		t.Errorf("waiting = %v", got.Waiting)
	}
	if !got.Timestamp.Equal(reading.Timestamp) {
		t.Errorf("timestamp = %s, want %s", got.Timestamp, reading.Timestamp)
	}

	mr.FastForward(6 * time.Second)
	if _, ok, err := st.GetReading(ctx, "upstream-a"); err != nil || ok {
		t.Errorf("expired reading: ok=%v err=%v", ok, err)
	}
}

func TestPingFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	st := newStore(t, mr, "router-a", 30*time.Second)
	mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := st.Ping(ctx); err == nil {
		t.Error("expected ping error after server shutdown")
	}
	if err := st.Heartbeat(ctx, map[string]int64{"a": 1}); err == nil {
		t.Error("expected heartbeat error after server shutdown")
	}
	if _, err := st.ActiveCounts(ctx, []string{"a"}); err == nil {
		t.Error("expected ActiveCounts error after server shutdown")
	}
}

func TestKeyPrefixNormalization(t *testing.T) {
	mr := miniredis.RunT(t)
	st := newStore(t, mr, "router-a", 30*time.Second)
	if err := st.Heartbeat(context.Background(), map[string]int64{"a": 1}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	found := false
	for _, key := range mr.Keys() {
		if key == "test:routers" {
			found = true
		}
		if strings.HasPrefix(key, "testrouters") {
			t.Errorf("prefix was not normalized: %s", key)
		}
	}
	if !found {
		t.Errorf("routers key not found in %v", mr.Keys())
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := store.New(store.Options{InstanceID: "a", InstanceTTL: time.Second, OperationTimeout: time.Second}); err == nil {
		t.Error("expected error for empty address")
	}
	if _, err := store.New(store.Options{Address: "localhost:6379", InstanceTTL: time.Second, OperationTimeout: time.Second}); err == nil {
		t.Error("expected error for empty instance ID")
	}
	if _, err := store.New(store.Options{Address: "localhost:6379", InstanceID: "a", OperationTimeout: time.Second}); err == nil {
		t.Error("expected error for zero instance TTL")
	}
}
