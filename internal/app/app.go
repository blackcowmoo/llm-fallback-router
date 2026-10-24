// Package app wires the router components together.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/balancer"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/metrics"
	"github.com/blackcowmoo/llm-fallback-router/internal/router"
	"github.com/blackcowmoo/llm-fallback-router/internal/store"
	"github.com/blackcowmoo/llm-fallback-router/internal/telemetry"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
	"github.com/blackcowmoo/llm-fallback-router/internal/version"
)

// App owns the router's long-lived components.
type App struct {
	cfg        *config.Config
	logger     *slog.Logger
	instanceID string
	started    time.Time

	telemetry *telemetry.Metrics
	metrics   *metrics.Manager
	tracker   tracker.Tracker
	store     *store.Store
	balancer  *balancer.Balancer
	proxy     *router.Proxy
	handler   http.Handler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

// New creates the application, starts background workers, and returns a
// handler. The caller must Close the app on shutdown.
func New(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*App, error) {
	if cfg == nil {
		return nil, errors.New("app: config is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	instanceID := cfg.Server.InstanceID
	if instanceID == "" {
		instanceID = generateInstanceID()
	}
	names := make([]string, 0, len(cfg.Upstreams))
	for _, upstream := range cfg.Upstreams {
		names = append(names, upstream.Name)
	}

	tel, err := telemetry.New(names)
	if err != nil {
		return nil, fmt.Errorf("init telemetry: %w", err)
	}
	tel.SetInstance(instanceID, cfg.Routing.Strategy)
	for _, upstream := range cfg.Upstreams {
		tel.SetUpstreamConfig(upstream.Name, upstream.Capacity, upstream.MaxActive)
	}

	appCtx, cancel := context.WithCancel(context.Background())
	a := &App{
		cfg:        cfg,
		logger:     logger,
		instanceID: instanceID,
		started:    time.Now(),
		telemetry:  tel,
		ctx:        appCtx,
		cancel:     cancel,
	}
	cleanup := func(err error) (*App, error) {
		cancel()
		if a.tracker != nil {
			_ = a.tracker.Close()
		}
		return nil, err
	}

	cache := metrics.NewCache(cfg.Cache.MetricsTTL.D(), cfg.Cache.MetricsStaleTTL.D())
	jobs := make([]metrics.Job, 0, len(cfg.Upstreams))
	for _, upstream := range cfg.Upstreams {
		if upstream.Metrics.Type == config.MetricsTypeNone {
			continue
		}
		scraper, err := newScraper(upstream)
		if err != nil {
			return cleanup(fmt.Errorf("upstream %s metrics: %w", upstream.Name, err))
		}
		jobs = append(jobs, metrics.Job{Name: upstream.Name, Scraper: scraper})
	}

	if cfg.Redis.Address != "" {
		st, err := store.New(store.Options{
			Address:          cfg.Redis.Address,
			Username:         cfg.Redis.Username,
			Password:         cfg.Redis.Password,
			DB:               cfg.Redis.DB,
			KeyPrefix:        cfg.Redis.KeyPrefix,
			InstanceID:       instanceID,
			InstanceTTL:      cfg.Redis.InstanceTTL.D(),
			ReadingTTL:       cfg.Cache.MetricsStaleTTL.D(),
			OperationTimeout: cfg.Redis.OperationTimeout.D(),
			DialTimeout:      cfg.Redis.DialTimeout.D(),
			PoolSize:         cfg.Redis.PoolSize,
			TLSEnabled:       cfg.Redis.TLS.Enabled,
			TLSCAFile:        cfg.Redis.TLS.CAFile,
			TLSSkipVerify:    cfg.Redis.TLS.InsecureSkipVerify,
		})
		if err != nil {
			return cleanup(fmt.Errorf("init redis store: %w", err))
		}
		a.store = st

		pingCtx, pingCancel := context.WithTimeout(ctx, cfg.Redis.DialTimeout.D()+cfg.Redis.OperationTimeout.D()+time.Second)
		pingErr := st.Ping(pingCtx)
		pingCancel()
		if pingErr != nil {
			if cfg.Redis.Required {
				_ = st.Close()
				return cleanup(fmt.Errorf("redis is required but unavailable: %w", pingErr))
			}
			logger.Warn("redis is unavailable; continuing with local active counts", "error", pingErr)
			tel.SetRedisUp(false)
		} else {
			tel.SetRedisUp(true)
		}

		a.tracker = tracker.NewDistributed(appCtx, st, names, tracker.DistributedOptions{
			SyncInterval:      cfg.Redis.SyncInterval.D(),
			SnapshotTTL:       cfg.Cache.SnapshotTTL.D(),
			HeartbeatInterval: cfg.Redis.HeartbeatInterval.D(),
			InstanceTTL:       cfg.Redis.InstanceTTL.D(),
			Logger:            logger,
			Hooks: tracker.Hooks{
				Sync:   func(error) { tel.RecordRedisError("sync") },
				Remote: func(error) { tel.RecordRedisError("remote_read") },
			},
		})
	} else {
		a.tracker = tracker.NewLocal(names)
	}

	var shared metrics.Shared
	if cfg.Cache.Shared && a.store != nil {
		shared = a.store
	}
	a.metrics = metrics.NewManager(cache, shared, metrics.Hooks{
		Scrape: func(upstream string, ok bool, duration time.Duration) {
			tel.ObserveScrape(upstream, ok, duration)
		},
		Shared: func(operation string, err error) {
			tel.ObserveSharedCache(operation, err)
		},
	}, logger, jobs)

	upstreams := make([]balancer.Upstream, 0, len(cfg.Upstreams))
	for _, up := range cfg.Upstreams {
		parsed, err := url.Parse(up.URL)
		if err != nil {
			return cleanup(fmt.Errorf("upstream %s URL: %w", up.Name, err))
		}
		upstreams = append(upstreams, balancer.Upstream{
			Name:      up.Name,
			URL:       parsed,
			Capacity:  up.Capacity,
			MaxActive: up.MaxActive,
			Headers:   up.Headers,
		})
	}
	bal, err := balancer.New(upstreams, balancer.Options{
		Strategy:            cfg.Routing.Strategy,
		Combine:             cfg.Routing.Combine,
		Metrics:             a.metrics,
		Tracker:             a.tracker,
		FailureThreshold:    cfg.Routing.FailureThreshold,
		RecoveryInterval:    cfg.Routing.RecoveryInterval.D(),
		FailOpen:            cfg.Routing.FailOpen,
		RejectWhenSaturated: cfg.Routing.RejectWhenSaturated,
	})
	if err != nil {
		return cleanup(fmt.Errorf("init balancer: %w", err))
	}
	a.balancer = bal

	a.proxy, err = router.NewProxy(router.ProxyOptions{
		Balancer:              bal,
		Tracker:               a.tracker,
		Telemetry:             tel,
		Logger:                logger,
		InstanceID:            instanceID,
		Strategy:              cfg.Routing.Strategy,
		Retry:                 cfg.Routing.Retry,
		AccessLog:             cfg.Server.AccessLog,
		ResponseHeaderTimeout: cfg.Routing.ResponseHeaderTimeout.D(),
		IdleConnTimeout:       cfg.Routing.IdleConnTimeout.D(),
		MaxIdleConnsPerHost:   cfg.Routing.MaxIdleConnsPerHost,
	})
	if err != nil {
		return cleanup(fmt.Errorf("init proxy: %w", err))
	}

	var redisPing func(context.Context) error
	if a.store != nil {
		redisPing = a.store.Ping
	}
	a.handler = router.NewServer(router.ServerOptions{
		Proxy:         a.proxy,
		Balancer:      bal,
		Tracker:       a.tracker,
		Telemetry:     tel,
		RedisPing:     redisPing,
		RedisRequired: cfg.Redis.Required && a.store != nil,
		InstanceID:    instanceID,
		Strategy:      cfg.Routing.Strategy,
		AdminToken:    cfg.Server.AdminToken,
		Version:       version.String(),
		Started:       a.started,
	})

	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		a.metrics.Run(appCtx)
	}()
	go func() {
		defer a.wg.Done()
		a.runTelemetry(appCtx)
	}()

	return a, nil
}

// Handler returns the HTTP handler for the router.
func (a *App) Handler() http.Handler { return a.handler }

// InstanceID returns the unique ID used for distributed counters.
func (a *App) InstanceID() string { return a.instanceID }

// Balancer exposes the balancer for diagnostics and tests.
func (a *App) Balancer() *balancer.Balancer { return a.balancer }

// Close stops background workers, publishes zero active counts, and removes the
// instance from Redis.
func (a *App) Close() error {
	a.closeOnce.Do(func() {
		a.cancel()
		a.wg.Wait()
		a.proxy.CloseIdleConnections()
		if err := a.tracker.Close(); err != nil {
			a.closeErr = err
		}
	})
	return a.closeErr
}

func (a *App) runTelemetry(ctx context.Context) {
	gaugeTicker := time.NewTicker(time.Second)
	defer gaugeTicker.Stop()
	redisTicker := time.NewTicker(5 * time.Second)
	defer redisTicker.Stop()

	a.publishGauges(ctx)
	a.checkRedis(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-gaugeTicker.C:
			a.publishGauges(ctx)
		case <-redisTicker.C:
			a.checkRedis(ctx)
		}
	}
}

func (a *App) publishGauges(ctx context.Context) {
	for _, upstream := range a.balancer.States(ctx) {
		state := telemetry.UpstreamState{
			ReportedAvailable: upstream.ReportedActive != nil,
			ReportedFresh:     upstream.ReportedFresh,
			ReportedAge:       upstream.ReportedAge,
			Observed:          upstream.ObservedActive,
			Local:             upstream.LocalActive,
			Healthy:           upstream.Healthy,
			Saturated:         upstream.Saturated,
		}
		if upstream.ReportedActive != nil {
			state.Reported = *upstream.ReportedActive
		}
		a.telemetry.UpdateUpstream(upstream.Name, state)
	}
}

func (a *App) checkRedis(ctx context.Context) {
	if a.store == nil {
		return
	}
	pingCtx, cancel := context.WithTimeout(ctx, a.cfg.Redis.OperationTimeout.D())
	defer cancel()
	err := a.store.Ping(pingCtx)
	a.telemetry.SetRedisUp(err == nil)
	if err != nil {
		a.telemetry.RecordRedisError("ping")
	}
}

func newScraper(upstream config.Upstream) (metrics.Scraper, error) {
	m := upstream.Metrics
	switch m.Type {
	case config.MetricsTypePrometheus:
		active := metrics.Selection{
			Metric:      m.Active.Metric,
			Labels:      m.Active.Labels,
			Aggregation: m.Active.Aggregation,
		}
		var waiting *metrics.Selection
		if m.Waiting != nil {
			waiting = &metrics.Selection{
				Metric:      m.Waiting.Metric,
				Labels:      m.Waiting.Labels,
				Aggregation: m.Waiting.Aggregation,
			}
		}
		return metrics.NewPrometheusScraper(metrics.PrometheusOptions{
			URL:      m.URL,
			Headers:  m.Headers,
			Interval: m.Interval.D(),
			Timeout:  m.Timeout.D(),
			Active:   active,
			Waiting:  waiting,
		}), nil
	case config.MetricsTypeJSON:
		return metrics.NewJSONScraper(metrics.JSONOptions{
			URL:          m.URL,
			Headers:      m.Headers,
			Interval:     m.Interval.D(),
			Timeout:      m.Timeout.D(),
			ActiveField:  m.ActiveField,
			WaitingField: m.WaitingField,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported metrics type %q", m.Type)
	}
}

func generateInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "router"
	}
	host = sanitizeInstanceID(host)
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return fmt.Sprintf("%s-%d", host, time.Now().UnixNano())
	}
	id := host + "-" + hex.EncodeToString(suffix[:])
	if len(id) > 128 {
		id = id[:128]
	}
	return id
}

func sanitizeInstanceID(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-', r == ':':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	out := strings.Trim(builder.String(), "-")
	if out == "" {
		return "router"
	}
	return out
}
