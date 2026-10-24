// Package tracker counts in-flight requests locally and, when Redis is
// configured, aggregates counts published by other router instances.
package tracker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/store"
)

// Status describes the state of the distributed counter.
type Status struct {
	Enabled         bool             `json:"enabled"`
	LastSync        time.Time        `json:"last_sync,omitempty"`
	LastSyncError   string           `json:"last_sync_error,omitempty"`
	RemoteTimestamp time.Time        `json:"remote_timestamp,omitempty"`
	RemoteError     string           `json:"remote_error,omitempty"`
	RemoteCounts    map[string]int64 `json:"remote_counts,omitempty"`
}

// Tracker counts active requests. Begin returns a release function that must be
// called exactly once when the request finishes.
type Tracker interface {
	Begin(upstream string) func()
	Snapshot(ctx context.Context) map[string]int64
	Local() map[string]int64
	Status() Status
	Close() error
}

var _ Tracker = (*Local)(nil)

// Local counts requests in this process only.
type Local struct {
	upstreams []string
	counters  map[string]*atomic.Int64
}

// NewLocal creates a process-local tracker for the given upstreams.
func NewLocal(upstreams []string) *Local {
	counters := make(map[string]*atomic.Int64, len(upstreams))
	for _, upstream := range upstreams {
		counters[upstream] = &atomic.Int64{}
	}
	return &Local{
		upstreams: append([]string(nil), upstreams...),
		counters:  counters,
	}
}

// Begin increments the counter for upstream.
func (l *Local) Begin(upstream string) func() {
	counter, ok := l.counters[upstream]
	if !ok {
		return func() {}
	}
	counter.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() { counter.Add(-1) })
	}
}

// Snapshot returns current local counts.
func (l *Local) Snapshot(context.Context) map[string]int64 { return l.Local() }

// Local returns current local counts.
func (l *Local) Local() map[string]int64 {
	counts := make(map[string]int64, len(l.upstreams))
	for _, upstream := range l.upstreams {
		counts[upstream] = l.counters[upstream].Load()
	}
	return counts
}

// Status reports that distributed tracking is disabled.
func (l *Local) Status() Status { return Status{Enabled: false} }

// Close is a no-op.
func (l *Local) Close() error { return nil }

// DistributedOptions configures the Redis-backed tracker.
type DistributedOptions struct {
	SyncInterval      time.Duration
	SnapshotTTL       time.Duration
	HeartbeatInterval time.Duration
	InstanceTTL       time.Duration
	Logger            *slog.Logger
	Hooks             Hooks
}

// Hooks receive asynchronous Redis errors.
type Hooks struct {
	Sync   func(err error)
	Remote func(err error)
}

// Distributed publishes absolute local counts to Redis at a bounded rate and
// adds live remote counts to Snapshot results.
type Distributed struct {
	local     *Local
	store     *store.Store
	upstreams []string
	opts      DistributedOptions
	logger    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	dirty  chan struct{}

	syncMu   sync.Mutex
	lastSync time.Time
	syncErr  error

	refreshMu sync.Mutex
	remoteMu  sync.Mutex
	remote    map[string]int64
	remoteAt  time.Time
	remoteErr error

	closeOnce sync.Once
}

// NewDistributed starts the distributed tracker. Closing the tracker publishes
// zero counts, removes the instance from Redis, and closes the store.
func NewDistributed(parent context.Context, st *store.Store, upstreams []string, opts DistributedOptions) *Distributed {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 100 * time.Millisecond
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 10 * time.Second
	}
	if opts.InstanceTTL <= 0 {
		opts.InstanceTTL = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	d := &Distributed{
		local:     NewLocal(upstreams),
		store:     st,
		upstreams: append([]string(nil), upstreams...),
		opts:      opts,
		logger:    opts.Logger,
		ctx:       ctx,
		cancel:    cancel,
		dirty:     make(chan struct{}, 1),
		remote:    make(map[string]int64, len(upstreams)),
	}
	d.wg.Add(1)
	go d.run()
	return d
}

// Begin increments the local count and schedules a shared-counter sync.
func (d *Distributed) Begin(upstream string) func() {
	release := d.local.Begin(upstream)
	d.notify()
	var once sync.Once
	return func() {
		once.Do(func() {
			release()
			d.notify()
		})
	}
}

// Snapshot returns local counts plus live counts from other instances.
func (d *Distributed) Snapshot(ctx context.Context) map[string]int64 {
	counts := d.local.Local()
	for upstream, count := range d.remoteCounts(ctx) {
		counts[upstream] += count
	}
	return counts
}

// Local returns this process's counts only.
func (d *Distributed) Local() map[string]int64 { return d.local.Local() }

// Flush synchronously publishes current local counts.
func (d *Distributed) Flush(ctx context.Context) error {
	return d.sync(ctx)
}

// Close stops the worker, publishes zeros, deregisters the instance, and
// closes the Redis store.
func (d *Distributed) Close() error {
	var err error
	d.closeOnce.Do(func() {
		d.cancel()
		d.wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		zeros := make(map[string]int64, len(d.upstreams))
		for _, upstream := range d.upstreams {
			zeros[upstream] = 0
		}
		if heartbeatErr := d.store.Heartbeat(ctx, zeros); heartbeatErr != nil {
			err = heartbeatErr
		}
		if deregisterErr := d.store.Deregister(ctx); deregisterErr != nil && err == nil {
			err = deregisterErr
		}
		if closeErr := d.store.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	})
	return err
}

// Status returns the last sync/remote-read state.
func (d *Distributed) Status() Status {
	d.syncMu.Lock()
	status := Status{
		Enabled:       true,
		LastSync:      d.lastSync,
		LastSyncError: errorString(d.syncErr),
	}
	d.syncMu.Unlock()

	d.remoteMu.Lock()
	status.RemoteTimestamp = d.remoteAt
	status.RemoteError = errorString(d.remoteErr)
	status.RemoteCounts = copyCounts(d.remote)
	d.remoteMu.Unlock()
	return status
}

func (d *Distributed) run() {
	defer d.wg.Done()

	_ = d.sync(d.ctx)

	heartbeat := time.NewTicker(d.opts.HeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-d.dirty:
			if wait := d.opts.SyncInterval - time.Since(d.lastSyncTime()); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-d.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			_ = d.sync(d.ctx)
		case <-heartbeat.C:
			_ = d.sync(d.ctx)
		}
	}
}

func (d *Distributed) sync(ctx context.Context) error {
	d.syncMu.Lock()
	defer d.syncMu.Unlock()

	err := d.store.Heartbeat(ctx, d.local.Local())
	d.lastSync = time.Now()
	d.syncErr = err
	if err != nil {
		if d.opts.Hooks.Sync != nil {
			d.opts.Hooks.Sync(err)
		}
		d.logger.Warn("failed to publish active request counts", "error", err)
	}
	return err
}

func (d *Distributed) lastSyncTime() time.Time {
	d.syncMu.Lock()
	defer d.syncMu.Unlock()
	return d.lastSync
}

func (d *Distributed) notify() {
	select {
	case d.dirty <- struct{}{}:
	default:
	}
}

func (d *Distributed) remoteCounts(ctx context.Context) map[string]int64 {
	if d.freshRemote() {
		return d.remoteCopy()
	}

	// Serialize refreshes so a burst of requests does not stampede Redis.
	d.refreshMu.Lock()
	defer d.refreshMu.Unlock()
	if d.freshRemote() {
		return d.remoteCopy()
	}

	counts, err := d.store.ActiveCounts(ctx, d.upstreams)
	now := time.Now()
	if err != nil {
		if d.opts.Hooks.Remote != nil {
			d.opts.Hooks.Remote(err)
		}
		d.remoteMu.Lock()
		d.remoteErr = err
		stale := copyCounts(d.remote)
		at := d.remoteAt
		d.remoteMu.Unlock()
		// Keep using the last good remote snapshot briefly while Redis is down,
		// but do not let it linger forever.
		if !at.IsZero() && now.Sub(at) <= d.opts.InstanceTTL {
			return stale
		}
		return zeroCounts(d.upstreams)
	}

	d.remoteMu.Lock()
	d.remote = counts
	d.remoteAt = now
	d.remoteErr = nil
	d.remoteMu.Unlock()
	return copyCounts(counts)
}

func (d *Distributed) freshRemote() bool {
	d.remoteMu.Lock()
	defer d.remoteMu.Unlock()
	return !d.remoteAt.IsZero() && time.Since(d.remoteAt) < d.opts.SnapshotTTL
}

func (d *Distributed) remoteCopy() map[string]int64 {
	d.remoteMu.Lock()
	defer d.remoteMu.Unlock()
	return copyCounts(d.remote)
}

func copyCounts(counts map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(counts))
	for key, value := range counts {
		out[key] = value
	}
	return out
}

func zeroCounts(upstreams []string) map[string]int64 {
	out := make(map[string]int64, len(upstreams))
	for _, upstream := range upstreams {
		out[upstream] = 0
	}
	return out
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
