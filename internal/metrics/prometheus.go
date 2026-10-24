package metrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Selection describes a Prometheus metric and how to aggregate its samples.
type Selection struct {
	Metric      string
	Labels      map[string]string
	Aggregation string
}

// PrometheusOptions configures a Prometheus text-format scraper.
type PrometheusOptions struct {
	URL      string
	Headers  map[string]string
	Interval time.Duration
	Timeout  time.Duration
	Active   Selection
	Waiting  *Selection
}

// PrometheusScraper scrapes an upstream's Prometheus /metrics endpoint.
type PrometheusScraper struct {
	opts   PrometheusOptions
	client *http.Client
}

// NewPrometheusScraper creates a scraper.
func NewPrometheusScraper(opts PrometheusOptions) *PrometheusScraper {
	return &PrometheusScraper{
		opts:   opts,
		client: &http.Client{Timeout: opts.Timeout},
	}
}

// Interval implements Scraper.
func (p *PrometheusScraper) Interval() time.Duration { return p.opts.Interval }

// Scrape implements Scraper.
func (p *PrometheusScraper) Scrape(ctx context.Context) (Reading, error) {
	body, err := fetch(ctx, p.client, p.opts.URL, p.opts.Headers)
	if err != nil {
		return Reading{}, fmt.Errorf("scrape %s: %w", p.opts.URL, err)
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return Reading{}, fmt.Errorf("parse prometheus metrics from %s: %w", p.opts.URL, err)
	}

	active, err := extractValue(families, p.opts.Active)
	if err != nil {
		return Reading{}, fmt.Errorf("active requests metric %q: %w", p.opts.Active.Metric, err)
	}

	reading := Reading{
		Active:    active,
		Timestamp: time.Now().UTC(),
		Source:    "prometheus",
	}
	if p.opts.Waiting != nil {
		if waiting, err := extractValue(families, *p.opts.Waiting); err == nil {
			reading.Waiting = &waiting
		}
	}
	return reading, nil
}

func extractValue(families map[string]*dto.MetricFamily, sel Selection) (float64, error) {
	family, ok := families[sel.Metric]
	if !ok {
		return 0, fmt.Errorf("metric not found in response")
	}

	values := make([]float64, 0, len(family.GetMetric()))
	for _, metric := range family.GetMetric() {
		labels := make(map[string]string, len(metric.GetLabel()))
		for _, pair := range metric.GetLabel() {
			labels[pair.GetName()] = pair.GetValue()
		}
		if !sampleLabels(labels, sel.Labels) {
			continue
		}
		value, err := metricValue(metric, family.GetType())
		if err != nil {
			return 0, err
		}
		value, err = sanitizeValue(sel.Metric, value)
		if err != nil {
			return 0, err
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return 0, fmt.Errorf("no samples matched labels %v", sel.Labels)
	}

	value, err := aggregate(values, sel.Aggregation)
	if err != nil {
		return 0, err
	}
	return sanitizeValue(sel.Metric, value)
}

func metricValue(metric *dto.Metric, familyType dto.MetricType) (float64, error) {
	switch familyType {
	case dto.MetricType_GAUGE:
		return metric.GetGauge().GetValue(), nil
	case dto.MetricType_COUNTER:
		return metric.GetCounter().GetValue(), nil
	case dto.MetricType_UNTYPED:
		return metric.GetUntyped().GetValue(), nil
	default:
		return 0, fmt.Errorf("unsupported metric type %s (active request metrics must be gauges)", familyType)
	}
}
