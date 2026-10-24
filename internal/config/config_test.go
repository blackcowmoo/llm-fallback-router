package config

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestExampleConfigLoads(t *testing.T) {
	cfg, err := Load("../../examples/router.yaml")
	if err != nil {
		t.Fatalf("Load example: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("upstreams = %d, want 2", len(cfg.Upstreams))
	}
	if cfg.Upstreams[0].Metrics.Type != MetricsTypePrometheus {
		t.Errorf("first metrics type = %q", cfg.Upstreams[0].Metrics.Type)
	}
	if cfg.Upstreams[1].Metrics.Type != MetricsTypeJSON {
		t.Errorf("second metrics type = %q", cfg.Upstreams[1].Metrics.Type)
	}
	if cfg.Routing.Strategy != StrategyWeightedLeastActive {
		t.Errorf("strategy = %q", cfg.Routing.Strategy)
	}
}

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse([]byte(`
upstreams:
  - name: a
    url: http://127.0.0.1:8001
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Routing.Strategy != StrategyLeastActive {
		t.Errorf("strategy = %q", cfg.Routing.Strategy)
	}
	if cfg.Routing.Combine != CombineMax {
		t.Errorf("combine = %q", cfg.Routing.Combine)
	}
	if got := cfg.Cache.MetricsTTL.D(); got != time.Second {
		t.Errorf("metrics ttl = %s", got)
	}
	if !cfg.Cache.Shared {
		t.Error("cache.shared default should be true")
	}
	if cfg.Redis.Address == "" {
		t.Error("redis address default should be set")
	}
	if len(cfg.Upstreams) != 1 {
		t.Fatalf("upstreams = %d", len(cfg.Upstreams))
	}
	up := cfg.Upstreams[0]
	if up.Capacity != 1 {
		t.Errorf("capacity = %v", up.Capacity)
	}
	if up.Metrics.Type != MetricsTypeNone {
		t.Errorf("metrics type = %q", up.Metrics.Type)
	}
	if up.Metrics.Interval.D() != time.Second || up.Metrics.Timeout.D() != time.Second {
		t.Errorf("metrics durations = %s/%s", up.Metrics.Interval, up.Metrics.Timeout)
	}
	if cfg.Routing.Retry.MaxAttempts != 2 {
		t.Errorf("retry attempts = %d", cfg.Routing.Retry.MaxAttempts)
	}
	if len(cfg.Routing.Retry.OnStatus) != 4 {
		t.Errorf("retry statuses = %v", cfg.Routing.Retry.OnStatus)
	}
}

func TestParseFullConfigAndEnvironment(t *testing.T) {
	t.Setenv("TEST_REDIS_PASSWORD", "s3cret")
	t.Setenv("TEST_UPSTREAM_URL", "http://vllm:8000")

	cfg, err := Parse([]byte(`
server:
  listen: "127.0.0.1:9090"
  instance_id: "router-a"
  shutdown_timeout: 5s
routing:
  strategy: weighted_least_active
  combine: sum
  failure_threshold: 5
  recovery_interval: 20s
  fail_open: false
  reject_when_saturated: true
  retry:
    max_attempts: 3
    buffer_limit_bytes: 1024
    on_status: [500, 503]
cache:
  metrics_ttl: 2s
  metrics_stale_ttl: 10s
  shared: true
  snapshot_ttl: 50ms
redis:
  address: "redis:6379"
  password: "${TEST_REDIS_PASSWORD}"
  db: 2
  key_prefix: "test:"
  instance_ttl: 12s
  heartbeat_interval: 3s
  sync_interval: 50ms
  operation_timeout: 100ms
  dial_timeout: 1s
  pool_size: 5
  required: true
upstreams:
  - name: vllm
    url: "${TEST_UPSTREAM_URL}"
    capacity: 2.5
    max_active: 10
    headers:
      Authorization: "Bearer test"
    metrics:
      type: prometheus
      url: http://vllm:8000/metrics
      interval: 500ms
      timeout: 400ms
      active_requests:
        metric: "vllm:num_requests_running"
        labels:
          model_name: llama
        aggregation: max
      waiting_requests:
        metric: "vllm:num_requests_waiting"
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Server.Listen != "127.0.0.1:9090" || cfg.Server.InstanceID != "router-a" {
		t.Errorf("server = %+v", cfg.Server)
	}
	if cfg.Server.ShutdownTimeout.D() != 5*time.Second {
		t.Errorf("shutdown timeout = %s", cfg.Server.ShutdownTimeout)
	}
	if cfg.Redis.Password != "s3cret" {
		t.Errorf("password = %q", cfg.Redis.Password)
	}
	if cfg.Upstreams[0].URL != "http://vllm:8000" {
		t.Errorf("url = %q", cfg.Upstreams[0].URL)
	}
	if cfg.Upstreams[0].Metrics.Active.Aggregation != AggregationMax {
		t.Errorf("aggregation = %q", cfg.Upstreams[0].Metrics.Active.Aggregation)
	}
	if cfg.Upstreams[0].Metrics.Waiting == nil || cfg.Upstreams[0].Metrics.Waiting.Aggregation != AggregationSum {
		t.Errorf("waiting defaults not applied: %+v", cfg.Upstreams[0].Metrics.Waiting)
	}
	if cfg.Routing.Retry.MaxAttempts != 3 || len(cfg.Routing.Retry.OnStatus) != 2 {
		t.Errorf("retry = %+v", cfg.Routing.Retry)
	}
}

func TestExpandEnv(t *testing.T) {
	t.Setenv("PRESENT", "yes")
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr string
	}{
		{name: "present", input: "a=${PRESENT}", want: "a=yes"},
		{name: "default", input: "a=${MISSING:-fallback}", want: "a=fallback"},
		{name: "empty default", input: "a=${MISSING:-}", want: "a="},
		{name: "present wins", input: "a=${PRESENT:-fallback}", want: "a=yes"},
		{name: "missing", input: "a=${MISSING}", wantErr: "MISSING"},
		{name: "untouched", input: "a=$PRESENT ${PRESENT}", want: "a=$PRESENT yes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExpandEnv(tt.input)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ExpandEnv: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidationErrors(t *testing.T) {
	base := func(upstreams string, extra string) string {
		return extra + "\nupstreams:\n" + upstreams
	}
	validUpstream := `
  - name: a
    url: http://127.0.0.1:8001
    metrics:
      type: prometheus
      url: http://127.0.0.1:8001/metrics
      active_requests:
        metric: active_requests
`
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{name: "no upstreams", yaml: "server:\n  listen: \":8080\"\n", wantErr: "at least one upstream"},
		{name: "bad strategy", yaml: base(validUpstream, "routing:\n  strategy: nope"), wantErr: "routing.strategy"},
		{name: "bad combine", yaml: base(validUpstream, "routing:\n  combine: nope"), wantErr: "routing.combine"},
		{name: "duplicate names", yaml: base(validUpstream+validUpstream, ""), wantErr: "duplicated"},
		{name: "bad upstream name", yaml: base(strings.Replace(validUpstream, "name: a", "name: bad name", 1), ""), wantErr: "upstreams[0].name"},
		{name: "bad url", yaml: base(strings.Replace(validUpstream, "url: http://127.0.0.1:8001", "url: ftp://127.0.0.1", 1), ""), wantErr: "must use http or https"},
		{name: "url without metrics type", yaml: base(strings.Replace(validUpstream, "type: prometheus", "type: \"\"", 1), ""), wantErr: "metrics.type is required"},
		{name: "prometheus without metric", yaml: base(strings.Replace(validUpstream, "metric: active_requests", "metric: \"\"", 1), ""), wantErr: "active_requests.metric is required"},
		{name: "bad aggregation", yaml: base(strings.Replace(validUpstream, "metric: active_requests", "metric: active_requests\n        aggregation: median", 1), ""), wantErr: "aggregation must be"},
		{name: "metrics timeout too long", yaml: base(strings.Replace(validUpstream, "active_requests:\n        metric: active_requests", "interval: 1s\n      timeout: 2s\n      active_requests:\n        metric: active_requests", 1), ""), wantErr: "must not exceed"},
		{name: "shared cache without redis", yaml: base(validUpstream, "redis:\n  address: \"\""), wantErr: "cache.shared requires redis.address"},
		{name: "bad redis ttl", yaml: base(validUpstream, "redis:\n  address: redis:6379\n  instance_ttl: 10s\n  heartbeat_interval: 10s"), wantErr: "greater than twice"},
		{name: "zero capacity", yaml: base(strings.Replace(validUpstream, "url: http://127.0.0.1:8001", "url: http://127.0.0.1:8001\n    capacity: -1", 1), ""), wantErr: "capacity must be positive"},
		{name: "invalid retry status", yaml: base(validUpstream, "routing:\n  retry:\n    on_status: [999]"), wantErr: "invalid HTTP status"},
		{name: "unknown field", yaml: base(validUpstream, "routing:\n  stratgy: least_active"), wantErr: "field stratgy not found"},
		{name: "bad duration", yaml: base(validUpstream, "server:\n  shutdown_timeout: later"), wantErr: "invalid duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestDurationJSONAndYAML(t *testing.T) {
	type holder struct {
		D Duration `yaml:"d" json:"d"`
	}
	var h holder
	if err := yaml.Unmarshal([]byte("d: 1500ms\n"), &h); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	if h.D.D() != 1500*time.Millisecond {
		t.Errorf("duration = %s", h.D)
	}
	data, err := h.D.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"1.5s"` {
		t.Errorf("json = %s", data)
	}
}
