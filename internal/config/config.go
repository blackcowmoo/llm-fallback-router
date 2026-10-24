// Package config defines the router configuration, its defaults, and validation.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that (de)serializes as a Go duration string
// (for example "1.5s") in YAML and JSON.
type Duration time.Duration

// D converts the configuration duration to a standard time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// String implements fmt.Stringer.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
		return fmt.Errorf("duration %q must be a string such as \"5s\"", value.Value)
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"5s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Config is the complete router configuration.
type Config struct {
	Server    Server     `yaml:"server"`
	Routing   Routing    `yaml:"routing"`
	Cache     Cache      `yaml:"cache"`
	Redis     Redis      `yaml:"redis"`
	Upstreams []Upstream `yaml:"upstreams"`
}

// Server contains HTTP server settings.
type Server struct {
	Listen            string   `yaml:"listen"`
	InstanceID        string   `yaml:"instance_id"`
	ReadHeaderTimeout Duration `yaml:"read_header_timeout"`
	IdleTimeout       Duration `yaml:"idle_timeout"`
	ShutdownTimeout   Duration `yaml:"shutdown_timeout"`
	MaxHeaderBytes    int      `yaml:"max_header_bytes"`
	AccessLog         bool     `yaml:"access_log"`
	AdminToken        string   `yaml:"admin_token"`
}

// Routing contains load-balancing and upstream health settings.
type Routing struct {
	Strategy              string   `yaml:"strategy"`
	Combine               string   `yaml:"combine"`
	FailureThreshold      int      `yaml:"failure_threshold"`
	RecoveryInterval      Duration `yaml:"recovery_interval"`
	FailOpen              bool     `yaml:"fail_open"`
	RejectWhenSaturated   bool     `yaml:"reject_when_saturated"`
	ResponseHeaderTimeout Duration `yaml:"response_header_timeout"`
	IdleConnTimeout       Duration `yaml:"idle_conn_timeout"`
	MaxIdleConnsPerHost   int      `yaml:"max_idle_conns_per_host"`
	Retry                 Retry    `yaml:"retry"`
}

// Retry controls bounded failover when an upstream cannot serve a request.
type Retry struct {
	MaxAttempts      int   `yaml:"max_attempts"`
	BufferLimitBytes int64 `yaml:"buffer_limit_bytes"`
	OnStatus         []int `yaml:"on_status"`
}

// Cache controls how scraped upstream metrics are cached.
type Cache struct {
	MetricsTTL      Duration `yaml:"metrics_ttl"`
	MetricsStaleTTL Duration `yaml:"metrics_stale_ttl"`
	Shared          bool     `yaml:"shared"`
	SnapshotTTL     Duration `yaml:"snapshot_ttl"`
}

// Redis configures the shared active-request counter and metric cache.
type Redis struct {
	Address           string   `yaml:"address"`
	Username          string   `yaml:"username"`
	Password          string   `yaml:"password"`
	DB                int      `yaml:"db"`
	KeyPrefix         string   `yaml:"key_prefix"`
	InstanceTTL       Duration `yaml:"instance_ttl"`
	HeartbeatInterval Duration `yaml:"heartbeat_interval"`
	SyncInterval      Duration `yaml:"sync_interval"`
	OperationTimeout  Duration `yaml:"operation_timeout"`
	DialTimeout       Duration `yaml:"dial_timeout"`
	PoolSize          int      `yaml:"pool_size"`
	Required          bool     `yaml:"required"`
	TLS               TLS      `yaml:"tls"`
}

// TLS configures TLS for Redis connections.
type TLS struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// Upstream describes one LLM backend.
type Upstream struct {
	Name      string            `yaml:"name"`
	URL       string            `yaml:"url"`
	Capacity  float64           `yaml:"capacity"`
	MaxActive int               `yaml:"max_active"`
	Headers   map[string]string `yaml:"headers"`
	Metrics   Metrics           `yaml:"metrics"`
}

// Metrics describes how to read an upstream's active-request count.
type Metrics struct {
	Type         string            `yaml:"type"`
	URL          string            `yaml:"url"`
	Interval     Duration          `yaml:"interval"`
	Timeout      Duration          `yaml:"timeout"`
	Headers      map[string]string `yaml:"headers"`
	Active       ActiveMetric      `yaml:"active_requests"`
	Waiting      *ActiveMetric     `yaml:"waiting_requests"`
	ActiveField  string            `yaml:"active_requests_field"`
	WaitingField string            `yaml:"waiting_requests_field"`
}

// ActiveMetric selects and aggregates samples for a Prometheus metric.
type ActiveMetric struct {
	Metric      string            `yaml:"metric"`
	Labels      map[string]string `yaml:"labels"`
	Aggregation string            `yaml:"aggregation"`
}

// Strategies, combine modes, metrics source types, and aggregations accepted by the configuration.
const (
	StrategyLeastActive         = "least_active"
	StrategyWeightedLeastActive = "weighted_least_active"
	StrategyRoundRobin          = "round_robin"

	CombineMax      = "max"
	CombineSum      = "sum"
	CombineReported = "reported"
	CombineObserved = "observed"

	MetricsTypeNone       = "none"
	MetricsTypePrometheus = "prometheus"
	MetricsTypeJSON       = "json"

	AggregationSum = "sum"
	AggregationMax = "max"
	AggregationMin = "min"
	AggregationAvg = "avg"
)

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// Default returns a configuration with production-friendly defaults.
func Default() *Config {
	return &Config{
		Server: Server{
			Listen:            ":8080",
			ReadHeaderTimeout: Duration(10 * time.Second),
			IdleTimeout:       Duration(120 * time.Second),
			ShutdownTimeout:   Duration(20 * time.Second),
			MaxHeaderBytes:    1 << 20,
			AccessLog:         true,
		},
		Routing: Routing{
			Strategy:              StrategyLeastActive,
			Combine:               CombineMax,
			FailureThreshold:      3,
			RecoveryInterval:      Duration(15 * time.Second),
			FailOpen:              true,
			ResponseHeaderTimeout: Duration(300 * time.Second),
			IdleConnTimeout:       Duration(90 * time.Second),
			MaxIdleConnsPerHost:   100,
			Retry: Retry{
				MaxAttempts:      2,
				BufferLimitBytes: 32 << 20,
				OnStatus:         []int{429, 502, 503, 504},
			},
		},
		Cache: Cache{
			MetricsTTL:      Duration(time.Second),
			MetricsStaleTTL: Duration(30 * time.Second),
			Shared:          true,
			SnapshotTTL:     Duration(100 * time.Millisecond),
		},
		Redis: Redis{
			Address:           "127.0.0.1:6379",
			DB:                0,
			KeyPrefix:         "llm-router:",
			InstanceTTL:       Duration(30 * time.Second),
			HeartbeatInterval: Duration(10 * time.Second),
			SyncInterval:      Duration(100 * time.Millisecond),
			OperationTimeout:  Duration(250 * time.Millisecond),
			DialTimeout:       Duration(2 * time.Second),
			PoolSize:          20,
		},
	}
}

// Load reads, expands, parses, defaults, and validates a YAML configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the config path is provided by the operator
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// Parse parses YAML configuration from data. ${VAR} and ${VAR:-default}
// references in the document are expanded from the environment.
func Parse(data []byte) (*Config, error) {
	expanded, err := ExpandEnv(string(data))
	if err != nil {
		return nil, err
	}

	cfg := Default()
	decoder := yaml.NewDecoder(strings.NewReader(expanded))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	applyDefaults(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ExpandEnv replaces ${VAR} and ${VAR:-default} references. Unset variables
// without a default are an error so that missing secrets never go unnoticed.
func ExpandEnv(input string) (string, error) {
	var missing []string
	out := envPattern.ReplaceAllStringFunc(input, func(match string) string {
		parts := envPattern.FindStringSubmatch(match)
		name, fallback := parts[1], parts[2]
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		if strings.Contains(match, ":-") {
			return fallback
		}
		missing = append(missing, name)
		return match
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("environment variable(s) not set: %s", strings.Join(unique(missing), ", "))
	}
	return out, nil
}

func applyDefaults(cfg *Config) {
	for i := range cfg.Upstreams {
		up := &cfg.Upstreams[i]
		if up.Capacity == 0 {
			up.Capacity = 1
		}
		m := &up.Metrics
		if m.Type == "" && m.URL == "" {
			m.Type = MetricsTypeNone
		}
		m.Type = strings.ToLower(strings.TrimSpace(m.Type))
		if m.Interval == 0 {
			m.Interval = Duration(time.Second)
		}
		if m.Timeout == 0 {
			m.Timeout = Duration(time.Second)
		}
		if m.Active.Metric != "" && m.Active.Aggregation == "" {
			m.Active.Aggregation = AggregationSum
		}
		if m.Active.Aggregation != "" {
			m.Active.Aggregation = strings.ToLower(m.Active.Aggregation)
		}
		if m.Waiting != nil {
			if m.Waiting.Aggregation == "" {
				m.Waiting.Aggregation = AggregationSum
			}
			m.Waiting.Aggregation = strings.ToLower(m.Waiting.Aggregation)
		}
		if m.Type == MetricsTypeJSON {
			if m.ActiveField == "" {
				m.ActiveField = "active_requests"
			}
			if m.WaitingField == "" {
				m.WaitingField = "waiting_requests"
			}
		}
	}
	if cfg.Routing.Retry.OnStatus == nil {
		cfg.Routing.Retry.OnStatus = []int{429, 502, 503, 504}
	}
}

// Validate checks the configuration and returns all problems at once.
func (c *Config) Validate() error {
	var problems []error
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf(format, args...))
	}

	if c.Server.Listen == "" {
		add("server.listen must not be empty")
	}
	if _, _, err := net.SplitHostPort(c.Server.Listen); err != nil {
		add("server.listen %q is not a valid host:port address", c.Server.Listen)
	}
	if c.Server.MaxHeaderBytes < 1024 {
		add("server.max_header_bytes must be at least 1024")
	}
	if c.Server.ReadHeaderTimeout <= 0 {
		add("server.read_header_timeout must be positive")
	}
	if c.Server.IdleTimeout <= 0 {
		add("server.idle_timeout must be positive")
	}
	if c.Server.ShutdownTimeout <= 0 {
		add("server.shutdown_timeout must be positive")
	}
	if c.Server.InstanceID != "" && !instanceIDPattern.MatchString(c.Server.InstanceID) {
		add("server.instance_id %q may only contain letters, digits, '.', '_', '-', and ':' (max 128 chars)", c.Server.InstanceID)
	}

	switch c.Routing.Strategy {
	case StrategyLeastActive, StrategyWeightedLeastActive, StrategyRoundRobin:
	default:
		add("routing.strategy must be one of %q, %q, %q", StrategyLeastActive, StrategyWeightedLeastActive, StrategyRoundRobin)
	}
	switch c.Routing.Combine {
	case CombineMax, CombineSum, CombineReported, CombineObserved:
	default:
		add("routing.combine must be one of %q, %q, %q, %q", CombineMax, CombineSum, CombineReported, CombineObserved)
	}
	if c.Routing.FailureThreshold < 1 {
		add("routing.failure_threshold must be at least 1")
	}
	if c.Routing.RecoveryInterval <= 0 {
		add("routing.recovery_interval must be positive")
	}
	if c.Routing.ResponseHeaderTimeout <= 0 {
		add("routing.response_header_timeout must be positive")
	}
	if c.Routing.IdleConnTimeout <= 0 {
		add("routing.idle_conn_timeout must be positive")
	}
	if c.Routing.MaxIdleConnsPerHost < 1 {
		add("routing.max_idle_conns_per_host must be at least 1")
	}
	if c.Routing.Retry.MaxAttempts < 1 {
		add("routing.retry.max_attempts must be at least 1")
	}
	if c.Routing.Retry.BufferLimitBytes < 0 {
		add("routing.retry.buffer_limit_bytes must not be negative")
	}
	for _, status := range c.Routing.Retry.OnStatus {
		if status < 100 || status > 599 {
			add("routing.retry.on_status contains invalid HTTP status %d", status)
		}
	}

	if c.Cache.MetricsTTL <= 0 {
		add("cache.metrics_ttl must be positive")
	}
	if c.Cache.MetricsStaleTTL <= 0 {
		add("cache.metrics_stale_ttl must be positive")
	}
	if c.Cache.MetricsStaleTTL < c.Cache.MetricsTTL {
		add("cache.metrics_stale_ttl must be greater than or equal to cache.metrics_ttl")
	}
	if c.Cache.SnapshotTTL < 0 {
		add("cache.snapshot_ttl must not be negative")
	}

	if c.Redis.Address != "" {
		if c.Redis.KeyPrefix == "" {
			add("redis.key_prefix must not be empty")
		}
		if c.Redis.DB < 0 {
			add("redis.db must not be negative")
		}
		if c.Redis.InstanceTTL <= 0 {
			add("redis.instance_ttl must be positive")
		}
		if c.Redis.HeartbeatInterval <= 0 {
			add("redis.heartbeat_interval must be positive")
		}
		if c.Redis.InstanceTTL <= 2*c.Redis.HeartbeatInterval {
			add("redis.instance_ttl must be greater than twice redis.heartbeat_interval")
		}
		if c.Redis.SyncInterval <= 0 {
			add("redis.sync_interval must be positive")
		}
		if c.Redis.SyncInterval >= c.Redis.InstanceTTL {
			add("redis.sync_interval must be less than redis.instance_ttl")
		}
		if c.Redis.OperationTimeout <= 0 {
			add("redis.operation_timeout must be positive")
		}
		if c.Redis.DialTimeout <= 0 {
			add("redis.dial_timeout must be positive")
		}
		if c.Redis.PoolSize < 1 {
			add("redis.pool_size must be at least 1")
		}
		if !c.Redis.TLS.Enabled && c.Redis.TLS.InsecureSkipVerify {
			add("redis.tls.insecure_skip_verify requires redis.tls.enabled")
		}
	}
	if c.Cache.Shared && c.Redis.Address == "" {
		add("cache.shared requires redis.address to be set")
	}

	if len(c.Upstreams) == 0 {
		add("at least one upstream is required")
	}
	names := map[string]bool{}
	for i, up := range c.Upstreams {
		prefix := fmt.Sprintf("upstreams[%d]", i)
		if up.Name == "" {
			add("%s.name must not be empty", prefix)
		} else {
			if !upstreamNamePattern.MatchString(up.Name) {
				add("%s.name %q may only contain letters, digits, '.', '_', '-', and ':'", prefix, up.Name)
			}
			if names[up.Name] {
				add("%s.name %q is duplicated", prefix, up.Name)
			}
			names[up.Name] = true
		}
		if err := validateHTTPURL(up.URL); err != nil {
			add("%s.url: %v", prefix, err)
		}
		if up.Capacity <= 0 {
			add("%s.capacity must be positive", prefix)
		}
		if up.MaxActive < 0 {
			add("%s.max_active must not be negative", prefix)
		}
		for header := range up.Headers {
			if strings.TrimSpace(header) == "" {
				add("%s.headers contains an empty header name", prefix)
			}
		}

		m := up.Metrics
		switch m.Type {
		case MetricsTypeNone:
			if m.URL != "" {
				add("%s.metrics.type is required when metrics.url is set (use %q to disable)", prefix, MetricsTypeNone)
			}
		case "":
			add("%s.metrics.type is required (use %q to rely only on router-observed counts)", prefix, MetricsTypeNone)
		case MetricsTypePrometheus:
			if err := validateHTTPURL(m.URL); err != nil {
				add("%s.metrics.url: %v", prefix, err)
			}
			if m.Active.Metric == "" {
				add("%s.metrics.active_requests.metric is required for prometheus metrics", prefix)
			}
			if err := validateAggregation(m.Active.Aggregation); err != nil {
				add("%s.metrics.active_requests: %v", prefix, err)
			}
			if m.Waiting != nil {
				if m.Waiting.Metric == "" {
					add("%s.metrics.waiting_requests.metric must not be empty", prefix)
				}
				if err := validateAggregation(m.Waiting.Aggregation); err != nil {
					add("%s.metrics.waiting_requests: %v", prefix, err)
				}
			}
		case MetricsTypeJSON:
			if err := validateHTTPURL(m.URL); err != nil {
				add("%s.metrics.url: %v", prefix, err)
			}
			if m.ActiveField == "" {
				add("%s.metrics.active_requests_field must not be empty", prefix)
			}
		default:
			add("%s.metrics.type must be one of %q, %q, %q", prefix, MetricsTypeNone, MetricsTypePrometheus, MetricsTypeJSON)
		}
		if m.Interval <= 0 {
			add("%s.metrics.interval must be positive", prefix)
		}
		if m.Timeout <= 0 {
			add("%s.metrics.timeout must be positive", prefix)
		}
		if m.Timeout.D() > 30*time.Second {
			add("%s.metrics.timeout must not exceed 30s", prefix)
		}
		if m.Type != MetricsTypeNone && m.Timeout > m.Interval {
			add("%s.metrics.timeout (%s) must not exceed metrics.interval (%s)", prefix, m.Timeout, m.Interval)
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration:\n%w", errors.Join(problems...))
	}
	return nil
}

func validateHTTPURL(raw string) error {
	if raw == "" {
		return errors.New("must not be empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("URL %q must use http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("URL %q must include a host", raw)
	}
	return nil
}

func validateAggregation(a string) error {
	switch a {
	case AggregationSum, AggregationMax, AggregationMin, AggregationAvg:
		return nil
	default:
		return fmt.Errorf("aggregation must be one of %q, %q, %q, %q", AggregationSum, AggregationMax, AggregationMin, AggregationAvg)
	}
}

func unique(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := values[:0]
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

var (
	upstreamNamePattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	instanceIDPattern   = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)
