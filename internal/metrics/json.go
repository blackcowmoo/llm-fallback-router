package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// JSONOptions configures a scraper for a dedicated JSON metrics server.
type JSONOptions struct {
	URL          string
	Headers      map[string]string
	Interval     time.Duration
	Timeout      time.Duration
	ActiveField  string
	WaitingField string
}

// JSONScraper reads metrics from a JSON endpoint such as:
//
//	{"active_requests": 3, "waiting_requests": 0}
//
// Nested fields can be selected with a dot path ("load.active").
type JSONScraper struct {
	opts   JSONOptions
	client *http.Client
}

// NewJSONScraper creates a scraper.
func NewJSONScraper(opts JSONOptions) *JSONScraper {
	return &JSONScraper{
		opts:   opts,
		client: &http.Client{Timeout: opts.Timeout},
	}
}

// Interval implements Scraper.
func (j *JSONScraper) Interval() time.Duration { return j.opts.Interval }

// Scrape implements Scraper.
func (j *JSONScraper) Scrape(ctx context.Context) (Reading, error) {
	body, err := fetch(ctx, j.client, j.opts.URL, j.opts.Headers)
	if err != nil {
		return Reading{}, fmt.Errorf("scrape %s: %w", j.opts.URL, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return Reading{}, fmt.Errorf("decode JSON metrics from %s: %w", j.opts.URL, err)
	}

	active, ok, err := numericField(document, j.opts.ActiveField)
	if err != nil {
		return Reading{}, fmt.Errorf("active requests field %q: %w", j.opts.ActiveField, err)
	}
	if !ok {
		return Reading{}, fmt.Errorf("active requests field %q not found", j.opts.ActiveField)
	}
	active, err = sanitizeValue(j.opts.ActiveField, active)
	if err != nil {
		return Reading{}, err
	}

	reading := Reading{
		Active:    active,
		Timestamp: time.Now().UTC(),
		Source:    "json",
	}
	if j.opts.WaitingField != "" {
		if waiting, ok, err := numericField(document, j.opts.WaitingField); err == nil && ok {
			if waiting, err = sanitizeValue(j.opts.WaitingField, waiting); err == nil {
				reading.Waiting = &waiting
			}
		}
	}
	return reading, nil
}

func numericField(document any, path string) (float64, bool, error) {
	value, ok := lookupPath(document, path)
	if !ok {
		return 0, false, nil
	}
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, true, fmt.Errorf("invalid number %q: %w", v.String(), err)
		}
		return f, true, nil
	case float64:
		return v, true, nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, true, fmt.Errorf("invalid numeric string %q: %w", v, err)
		}
		return f, true, nil
	case nil:
		return 0, false, nil
	default:
		return 0, true, fmt.Errorf("value has type %T, want number or numeric string", value)
	}
}

func lookupPath(document any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	current := document
	for _, part := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}
