// Package router implements the HTTP proxy and its local endpoints.
package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/balancer"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/telemetry"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

// ProxyOptions configures the reverse proxy handler.
type ProxyOptions struct {
	Balancer   *balancer.Balancer
	Tracker    tracker.Tracker
	Telemetry  *telemetry.Metrics
	Logger     *slog.Logger
	InstanceID string
	Strategy   string
	Retry      config.Retry
	AccessLog  bool
	Transport  http.RoundTripper

	ResponseHeaderTimeout time.Duration
	IdleConnTimeout       time.Duration
	MaxIdleConnsPerHost   int
}

// Proxy is the active-request-aware reverse proxy.
type Proxy struct {
	opts        ProxyOptions
	transport   http.RoundTripper
	retryStatus map[int]bool
}

// NewProxy creates the proxy handler.
func NewProxy(opts ProxyOptions) (*Proxy, error) {
	if opts.Balancer == nil {
		return nil, errors.New("router: balancer is required")
	}
	if opts.Tracker == nil {
		return nil, errors.New("router: tracker is required")
	}
	if opts.Telemetry == nil {
		return nil, errors.New("router: telemetry is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Retry.MaxAttempts < 1 {
		opts.Retry.MaxAttempts = 1
	}
	if opts.ResponseHeaderTimeout <= 0 {
		opts.ResponseHeaderTimeout = 300 * time.Second
	}
	if opts.IdleConnTimeout <= 0 {
		opts.IdleConnTimeout = 90 * time.Second
	}
	if opts.MaxIdleConnsPerHost < 1 {
		opts.MaxIdleConnsPerHost = 100
	}
	if opts.Transport == nil {
		dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		opts.Transport = &http.Transport{
			Proxy:                 nil,
			DialContext:           dialer.DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          opts.MaxIdleConnsPerHost * 2,
			MaxIdleConnsPerHost:   opts.MaxIdleConnsPerHost,
			IdleConnTimeout:       opts.IdleConnTimeout,
			ResponseHeaderTimeout: opts.ResponseHeaderTimeout,
			ExpectContinueTimeout: time.Second,
			// Let the client negotiate compression; auto-decompression buffers
			// streaming responses such as SSE.
			DisableCompression: true,
		}
	}
	retryStatus := make(map[int]bool, len(opts.Retry.OnStatus))
	for _, status := range opts.Retry.OnStatus {
		retryStatus[status] = true
	}
	return &Proxy{opts: opts, transport: opts.Transport, retryStatus: retryStatus}, nil
}

// CloseIdleConnections closes upstream keep-alive connections.
func (p *Proxy) CloseIdleConnections() {
	if closer, ok := p.transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// ServeHTTP routes the request, retrying on eligible failures before any
// response bytes are written.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	recorder := newResponseRecorder(w)

	body, err := p.prepareBody(r)
	if err != nil {
		writeJSONError(recorder, http.StatusBadRequest, "invalid_request_body", "could not read the request body", "", 0, nil)
		p.logRequest(r, "", recorder.Status(), 0, start, recorder)
		return
	}

	maxAttempts := p.opts.Retry.MaxAttempts
	if !body.reusable {
		// The body could not be buffered, so a retry would send an empty or
		// truncated request. Fail over is disabled for this request.
		maxAttempts = 1
	}

	excluded := make(map[string]bool)
	var lastErr error
	lastUpstream := ""
	attempts := 0

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		target, selectErr := p.opts.Balancer.Select(r.Context(), excluded)
		if selectErr != nil {
			if attempts == 0 {
				lastErr = selectErr
			}
			break
		}
		lastUpstream = target.Name
		attempts = attempt
		p.opts.Telemetry.ObserveSelection(target.Name, p.opts.Strategy)

		body.apply(r)
		var attemptErr error
		var status int
		func() {
			release := p.opts.Tracker.Begin(target.Name)
			defer release()
			status, attemptErr = p.proxyOnce(recorder, r, target, attempt, attempt < maxAttempts)
		}()

		if attemptErr == nil {
			p.opts.Telemetry.ObserveRequest(target.Name, status, time.Since(start))
			p.logRequest(r, target.Name, status, attempts, start, recorder)
			return
		}

		lastErr = attemptErr
		reason := retryReason(attemptErr)
		p.opts.Telemetry.ObserveRetry(target.Name, reason)
		p.reportAttemptFailure(target.Name, attemptErr)
		excluded[target.Name] = true

		if recorder.WroteHeader() {
			p.opts.Telemetry.ObserveRequest(target.Name, recorder.Status(), time.Since(start))
			p.logRequest(r, target.Name, recorder.Status(), attempts, start, recorder)
			return
		}
		if r.Context().Err() != nil {
			break
		}
		if attempt == maxAttempts {
			break
		}
	}

	status := http.StatusBadGateway
	code := "upstream_failure"
	message := "all upstream attempts failed"
	var extraHeaders map[string]string
	if attempts == 0 {
		status = http.StatusServiceUnavailable
		if errors.Is(lastErr, balancer.ErrSaturated) {
			code = "upstreams_saturated"
			message = "all upstreams are at capacity"
			extraHeaders = map[string]string{"Retry-After": "1"}
		} else {
			code = "no_upstream_available"
			message = "no upstream is currently available"
		}
	}
	if r.Context().Err() != nil && attempts > 0 {
		// The client disconnected; there is no one to answer, but keep the
		// counters and logs accurate.
		status = 499
		code = "client_closed_request"
		message = "client closed the request"
	}
	if lastErr != nil {
		p.opts.Logger.Debug("request failed",
			"method", r.Method, "path", r.URL.Path, "upstream", lastUpstream,
			"attempts", attempts, "error", lastErr)
	}
	if !recorder.WroteHeader() {
		writeJSONError(recorder, status, code, message, lastUpstream, attempts, extraHeaders)
	}
	p.opts.Telemetry.ObserveRequest(labelUpstream(lastUpstream), status, time.Since(start))
	p.logRequest(r, lastUpstream, recorder.Status(), attempts, start, recorder)
}

// proxyOnce performs one attempt. It returns an error only if no usable
// response was produced (transport error or a retryable status while more
// attempts remain).
func (p *Proxy) proxyOnce(w http.ResponseWriter, r *http.Request, target *balancer.Target, attempt int, canRetry bool) (int, error) {
	var attemptErr error
	status := 0

	reverseProxy := &httputil.ReverseProxy{
		Transport:     p.transport,
		FlushInterval: -1,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target.URL)
			pr.SetXForwarded()
			pr.Out.Host = target.URL.Host
			for key, value := range target.Headers {
				pr.Out.Header.Set(key, value)
			}
			if p.opts.InstanceID != "" {
				pr.Out.Header.Set("X-LLM-Router-Instance", p.opts.InstanceID)
			}
		},
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) {
			attemptErr = err
		},
		ModifyResponse: func(response *http.Response) error {
			status = response.StatusCode
			if canRetry && p.retryStatus[status] {
				return &retryableStatusError{status: status}
			}
			response.Header.Set("X-LLM-Router-Upstream", target.Name)
			response.Header.Set("X-LLM-Router-Attempts", strconv.Itoa(attempt))
			if isFailureStatus(status) {
				p.opts.Balancer.ReportFailure(target.Name, fmt.Sprintf("HTTP %d", status))
			} else {
				p.opts.Balancer.ReportSuccess(target.Name)
			}
			return nil
		},
	}
	reverseProxy.ServeHTTP(w, r)

	if attemptErr != nil {
		return status, attemptErr
	}
	return status, nil
}

func (p *Proxy) reportAttemptFailure(upstream string, err error) {
	var retryErr *retryableStatusError
	if errors.As(err, &retryErr) {
		if retryErr.status == http.StatusTooManyRequests {
			// Overload is not a health failure; the retry itself is the
			// mitigation.
			return
		}
		p.opts.Balancer.ReportFailure(upstream, fmt.Sprintf("HTTP %d", retryErr.status))
		return
	}
	p.opts.Balancer.ReportFailure(upstream, err.Error())
}

func retryReason(err error) string {
	var retryErr *retryableStatusError
	if errors.As(err, &retryErr) {
		return "status_" + strconv.Itoa(retryErr.status)
	}
	if errors.Is(err, context.Canceled) {
		return "client_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "transport_error"
}

func isFailureStatus(status int) bool {
	return status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

type retryableStatusError struct {
	status int
}

func (e *retryableStatusError) Error() string {
	return fmt.Sprintf("upstream returned retryable HTTP %d", e.status)
}

func labelUpstream(upstream string) string {
	if upstream == "" {
		return "none"
	}
	return upstream
}

func (p *Proxy) logRequest(r *http.Request, upstream string, status, attempts int, start time.Time, recorder *responseRecorder) {
	if !p.opts.AccessLog {
		return
	}
	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelWarn
	}
	p.opts.Logger.Log(r.Context(), level, "request",
		"method", r.Method,
		"path", r.URL.Path,
		"status", status,
		"upstream", labelUpstream(upstream),
		"attempts", attempts,
		"duration_ms", time.Since(start).Milliseconds(),
		"bytes", recorder.Bytes(),
		"remote", r.RemoteAddr,
	)
}

type bodyBuffer struct {
	data     []byte
	reusable bool
}

func (p *Proxy) prepareBody(r *http.Request) (*bodyBuffer, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return &bodyBuffer{data: []byte{}, reusable: true}, nil
	}
	limit := p.opts.Retry.BufferLimitBytes
	if limit <= 0 {
		return &bodyBuffer{reusable: false}, nil
	}
	if r.ContentLength > limit {
		// Too large to buffer: stream it through once.
		return &bodyBuffer{reusable: false}, nil
	}

	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		_ = r.Body.Close()
		return nil, err
	}
	if int64(len(data)) > limit {
		original := r.Body
		r.Body = &combinedReadCloser{
			Reader: io.MultiReader(bytes.NewReader(data), original),
			Closer: original,
		}
		r.ContentLength = -1
		return &bodyBuffer{reusable: false}, nil
	}
	_ = r.Body.Close()
	return &bodyBuffer{data: data, reusable: true}, nil
}

func (b *bodyBuffer) apply(r *http.Request) {
	if !b.reusable {
		return
	}
	if len(b.data) == 0 {
		r.Body = http.NoBody
		r.ContentLength = 0
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(b.data))
	r.ContentLength = int64(len(b.data))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b.data)), nil
	}
}

type combinedReadCloser struct {
	io.Reader
	io.Closer
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
	wrote  bool
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w}
}

func (w *responseRecorder) WriteHeader(status int) {
	if !w.wrote {
		w.status = status
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseRecorder) Write(data []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += int64(n)
	return n, err
}

func (w *responseRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseRecorder) WroteHeader() bool { return w.wrote }

func (w *responseRecorder) Status() int {
	if !w.wrote {
		return http.StatusOK
	}
	return w.status
}

func (w *responseRecorder) Bytes() int64 { return w.bytes }

func writeJSONError(w *responseRecorder, status int, code, message, upstream string, attempts int, extraHeaders map[string]string) {
	header := w.Header()
	header.Set("Content-Type", "application/json; charset=utf-8")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("X-LLM-Router-Upstream", labelUpstream(upstream))
	if attempts > 0 {
		header.Set("X-LLM-Router-Attempts", strconv.Itoa(attempts))
	}
	for key, value := range extraHeaders {
		header.Set(key, value)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    "router_error",
			"code":    code,
		},
	})
}
