// Package telemetry exposes router metrics for Prometheus.
package telemetry

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/blackcowmoo/llm-fallback-router/internal/version"
)

const namespace = "llm_router"

// UpstreamState is the gauge state published for one upstream.
type UpstreamState struct {
	Reported          float64
	ReportedAvailable bool
	ReportedFresh     bool
	ReportedAge       time.Duration
	Observed          int64
	Local             int64
	Healthy           bool
	Saturated         bool
}

// Metrics owns the router's Prometheus registry and collectors.
type Metrics struct {
	registry *prometheus.Registry

	requests       *prometheus.CounterVec
	requestLatency *prometheus.HistogramVec
	retries        *prometheus.CounterVec
	selections     *prometheus.CounterVec

	scrapes        *prometheus.CounterVec
	scrapeLatency  *prometheus.HistogramVec
	redisUp        prometheus.Gauge
	redisErrors    *prometheus.CounterVec
	sharedCacheOps *prometheus.CounterVec
	instanceInfo   *prometheus.GaugeVec

	reportedActive   *prometheus.GaugeVec
	metricsAvailable *prometheus.GaugeVec
	observedActive   *prometheus.GaugeVec
	localActive      *prometheus.GaugeVec
	healthy          *prometheus.GaugeVec
	fresh            *prometheus.GaugeVec
	metricAge        *prometheus.GaugeVec
	saturated        *prometheus.GaugeVec
	capacity         *prometheus.GaugeVec
	maxActive        *prometheus.GaugeVec
}

// New creates and registers all router collectors.
func New(upstreams []string) (*Metrics, error) {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_total",
			Help:      "HTTP requests forwarded by the router.",
		}, []string{"upstream", "status"}),
		requestLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "request_duration_seconds",
			Help:      "End-to-end request duration observed by the router.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600},
		}, []string{"upstream"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "request_retries_total",
			Help:      "Failed attempts that triggered failover to another upstream.",
		}, []string{"upstream", "reason"}),
		selections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "upstream_selections_total",
			Help:      "Upstream selections by routing strategy.",
		}, []string{"upstream", "strategy"}),
		scrapes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "upstream_scrapes_total",
			Help:      "Metric endpoint scrapes by result.",
		}, []string{"upstream", "result"}),
		scrapeLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "upstream_scrape_duration_seconds",
			Help:      "Duration of upstream metric scrapes.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"upstream"}),
		redisUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "redis_up",
			Help:      "Whether the Redis backend is reachable (1) or not (0).",
		}),
		redisErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "redis_errors_total",
			Help:      "Redis operation errors.",
		}, []string{"operation"}),
		sharedCacheOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "shared_cache_operations_total",
			Help:      "Shared metric cache operations by result.",
		}, []string{"operation", "result"}),
		instanceInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "instance_info",
			Help:      "Router instance information.",
		}, []string{"instance_id", "strategy"}),
		reportedActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_reported_active_requests",
			Help:      "Active requests reported by the upstream itself (cached).",
		}, []string{"upstream"}),
		metricsAvailable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_metrics_available",
			Help:      "Whether a usable cached reading exists for the upstream.",
		}, []string{"upstream"}),
		observedActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_observed_active_requests",
			Help:      "Active requests observed by all router instances (local + Redis).",
		}, []string{"upstream"}),
		localActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_local_active_requests",
			Help:      "Active requests observed by this router instance.",
		}, []string{"upstream"}),
		healthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_healthy",
			Help:      "Whether an upstream is outside its failure recovery window.",
		}, []string{"upstream"}),
		fresh: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_metrics_fresh",
			Help:      "Whether the upstream's cached metrics are within cache.metrics_ttl.",
		}, []string{"upstream"}),
		metricAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_metrics_age_seconds",
			Help:      "Age of the cached upstream metrics reading.",
		}, []string{"upstream"}),
		saturated: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_saturated",
			Help:      "Whether the upstream reached its configured max_active.",
		}, []string{"upstream"}),
		capacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_capacity",
			Help:      "Configured relative capacity of the upstream.",
		}, []string{"upstream"}),
		maxActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "upstream_max_active",
			Help:      "Configured max_active limit (0 means unlimited).",
		}, []string{"upstream"}),
	}

	collectorsList := []prometheus.Collector{
		m.requests,
		m.requestLatency,
		m.retries,
		m.selections,
		m.scrapes,
		m.scrapeLatency,
		m.redisUp,
		m.redisErrors,
		m.sharedCacheOps,
		m.instanceInfo,
		m.reportedActive,
		m.metricsAvailable,
		m.observedActive,
		m.localActive,
		m.healthy,
		m.fresh,
		m.metricAge,
		m.saturated,
		m.capacity,
		m.maxActive,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	}
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help:      "Build information.",
	}, []string{"version", "revision", "build_date"})
	buildInfo.WithLabelValues(version.Version, version.Commit, version.BuildDate).Set(1)
	collectorsList = append(collectorsList, buildInfo)

	for _, collector := range collectorsList {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	for _, upstream := range upstreams {
		m.reportedActive.WithLabelValues(upstream).Set(0)
		m.observedActive.WithLabelValues(upstream).Set(0)
		m.localActive.WithLabelValues(upstream).Set(0)
		m.healthy.WithLabelValues(upstream).Set(1)
	}
	return m, nil
}

// Handler serves the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// SetInstance publishes instance identity.
func (m *Metrics) SetInstance(instanceID, strategy string) {
	m.instanceInfo.WithLabelValues(instanceID, strategy).Set(1)
}

// SetUpstreamConfig publishes static upstream configuration gauges.
func (m *Metrics) SetUpstreamConfig(upstream string, capacity float64, maxActive int) {
	m.capacity.WithLabelValues(upstream).Set(capacity)
	m.maxActive.WithLabelValues(upstream).Set(float64(maxActive))
}

// UpdateUpstream publishes dynamic per-upstream gauges.
func (m *Metrics) UpdateUpstream(upstream string, state UpstreamState) {
	available := 0.0
	if state.ReportedAvailable {
		available = 1
	}
	fresh := 0.0
	if state.ReportedFresh {
		fresh = 1
	}
	healthy := 0.0
	if state.Healthy {
		healthy = 1
	}
	saturated := 0.0
	if state.Saturated {
		saturated = 1
	}
	if !state.ReportedAvailable {
		state.Reported = 0
	}
	m.reportedActive.WithLabelValues(upstream).Set(state.Reported)
	m.metricsAvailable.WithLabelValues(upstream).Set(available)
	m.observedActive.WithLabelValues(upstream).Set(float64(state.Observed))
	m.localActive.WithLabelValues(upstream).Set(float64(state.Local))
	m.healthy.WithLabelValues(upstream).Set(healthy)
	m.fresh.WithLabelValues(upstream).Set(fresh)
	m.metricAge.WithLabelValues(upstream).Set(state.ReportedAge.Seconds())
	m.saturated.WithLabelValues(upstream).Set(saturated)
}

// ObserveRequest records one completed client request.
func (m *Metrics) ObserveRequest(upstream string, status int, duration time.Duration) {
	m.requests.WithLabelValues(upstream, strconv.Itoa(status)).Inc()
	m.requestLatency.WithLabelValues(upstream).Observe(duration.Seconds())
}

// ObserveRetry records a failed attempt that led to failover.
func (m *Metrics) ObserveRetry(upstream, reason string) {
	m.retries.WithLabelValues(upstream, reason).Inc()
}

// ObserveSelection records which upstream was chosen.
func (m *Metrics) ObserveSelection(upstream, strategy string) {
	m.selections.WithLabelValues(upstream, strategy).Inc()
}

// ObserveScrape records the result and duration of a metrics scrape.
func (m *Metrics) ObserveScrape(upstream string, ok bool, duration time.Duration) {
	result := "error"
	if ok {
		result = "success"
	}
	m.scrapes.WithLabelValues(upstream, result).Inc()
	m.scrapeLatency.WithLabelValues(upstream).Observe(duration.Seconds())
}

// SetRedisUp publishes Redis connectivity.
func (m *Metrics) SetRedisUp(up bool) {
	if up {
		m.redisUp.Set(1)
		return
	}
	m.redisUp.Set(0)
}

// RecordRedisError records a Redis operation failure.
func (m *Metrics) RecordRedisError(operation string) {
	m.redisErrors.WithLabelValues(operation).Inc()
}

// ObserveSharedCache records a shared metric cache operation.
func (m *Metrics) ObserveSharedCache(operation string, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	m.sharedCacheOps.WithLabelValues(operation, result).Inc()
}
