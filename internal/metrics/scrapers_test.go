package metrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const prometheusBody = `# HELP vllm:num_requests_running Number of requests currently running.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="llama"} 2
vllm:num_requests_running{model_name="mistral"} 3
vllm:num_requests_running{model_name="other"} 10
# HELP vllm:num_requests_waiting Waiting requests.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="llama"} 1
vllm:num_requests_waiting{model_name="mistral"} 2
# HELP vllm:gpu_cache_usage_perc GPU cache usage.
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc 0.75
`

func TestPrometheusScraper(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Errorf("authorization = %q", got)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, prometheusBody)
	}))
	defer server.Close()

	scraper := NewPrometheusScraper(PrometheusOptions{
		URL:      server.URL,
		Headers:  map[string]string{"Authorization": "Bearer token"},
		Interval: time.Second,
		Timeout:  time.Second,
		Active: Selection{
			Metric:      "vllm:num_requests_running",
			Labels:      map[string]string{"model_name": "llama"},
			Aggregation: "sum",
		},
		Waiting: &Selection{
			Metric:      "vllm:num_requests_waiting",
			Aggregation: "sum",
		},
	})

	reading, err := scraper.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if reading.Active != 2 {
		t.Errorf("active = %v, want 2", reading.Active)
	}
	if reading.Waiting == nil || *reading.Waiting != 3 {
		t.Errorf("waiting = %v, want 3", reading.Waiting)
	}
	if reading.Source != "prometheus" {
		t.Errorf("source = %q", reading.Source)
	}
	if reading.Timestamp.IsZero() {
		t.Error("timestamp is zero")
	}
}

func TestPrometheusAggregations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, prometheusBody)
	}))
	defer server.Close()

	tests := []struct {
		aggregation string
		want        float64
	}{
		{aggregation: "sum", want: 15},
		{aggregation: "max", want: 10},
		{aggregation: "min", want: 2},
		{aggregation: "avg", want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.aggregation, func(t *testing.T) {
			scraper := NewPrometheusScraper(PrometheusOptions{
				URL: server.URL, Interval: time.Second, Timeout: time.Second,
				Active: Selection{Metric: "vllm:num_requests_running", Aggregation: tt.aggregation},
			})
			reading, err := scraper.Scrape(context.Background())
			if err != nil {
				t.Fatalf("Scrape: %v", err)
			}
			if reading.Active != tt.want {
				t.Errorf("active = %v, want %v", reading.Active, tt.want)
			}
		})
	}
}

func TestPrometheusScraperErrors(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
		active Selection
		want   string
	}{
		{
			name:   "missing metric",
			body:   prometheusBody,
			status: 200,
			active: Selection{Metric: "does_not_exist"},
			want:   "metric not found",
		},
		{
			name:   "no matching labels",
			body:   prometheusBody,
			status: 200,
			active: Selection{Metric: "vllm:num_requests_running", Labels: map[string]string{"model_name": "missing"}},
			want:   "no samples matched",
		},
		{
			name:   "non-finite value",
			body:   "# TYPE active_requests gauge\nactive_requests NaN\n",
			status: 200,
			active: Selection{Metric: "active_requests"},
			want:   "non-finite",
		},
		{
			name: "unsupported type",
			body: `# TYPE active_requests histogram
active_requests_bucket{le="1"} 1
active_requests_sum 1
active_requests_count 1
`,
			status: 200,
			active: Selection{Metric: "active_requests"},
			want:   "unsupported metric type",
		},
		{
			name:   "http error",
			body:   "nope",
			status: 503,
			active: Selection{Metric: "active_requests"},
			want:   "HTTP 503",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				fmt.Fprint(w, tt.body)
			}))
			defer server.Close()

			scraper := NewPrometheusScraper(PrometheusOptions{
				URL: server.URL, Interval: time.Second, Timeout: time.Second, Active: tt.active,
			})
			_, err := scraper.Scrape(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestPrometheusNegativeValueClamped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "# TYPE active_requests gauge\nactive_requests -3\n")
	}))
	defer server.Close()

	scraper := NewPrometheusScraper(PrometheusOptions{
		URL: server.URL, Interval: time.Second, Timeout: time.Second,
		Active: Selection{Metric: "active_requests"},
	})
	reading, err := scraper.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if reading.Active != 0 {
		t.Errorf("active = %v, want 0", reading.Active)
	}
}

func TestJSONScraper(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"load":{"active":5.5,"waiting":"2"},"model":"llama"}`)
	}))
	defer server.Close()

	scraper := NewJSONScraper(JSONOptions{
		URL: server.URL, Interval: time.Second, Timeout: time.Second,
		ActiveField: "load.active", WaitingField: "load.waiting",
	})
	reading, err := scraper.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if reading.Active != 5.5 {
		t.Errorf("active = %v", reading.Active)
	}
	if reading.Waiting == nil || *reading.Waiting != 2 {
		t.Errorf("waiting = %v", reading.Waiting)
	}
	if reading.Source != "json" {
		t.Errorf("source = %q", reading.Source)
	}
}

func TestJSONScraperErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "invalid json", body: `{`, want: "decode JSON"},
		{name: "missing field", body: `{"other":1}`, want: "not found"},
		{name: "not a number", body: `{"active_requests":true}`, want: "want number"},
		{name: "invalid numeric string", body: `{"active_requests":"many"}`, want: "invalid numeric string"},
		{name: "negative", body: `{"active_requests":-1}`, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, tt.body)
			}))
			defer server.Close()

			scraper := NewJSONScraper(JSONOptions{
				URL: server.URL, Interval: time.Second, Timeout: time.Second,
				ActiveField: "active_requests",
			})
			reading, err := scraper.Scrape(context.Background())
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Scrape: %v", err)
				}
				if reading.Active != 0 {
					t.Errorf("active = %v, want clamped 0", reading.Active)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
