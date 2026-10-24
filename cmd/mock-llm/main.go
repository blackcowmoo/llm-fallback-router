// Command mock-llm is a tiny OpenAI-compatible load simulator used by the
// Docker Compose demo and end-to-end tests.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

type mockServer struct {
	name       string
	model      string
	latency    time.Duration
	baseActive int64
	active     atomic.Int64
	total      atomic.Int64
	started    time.Time
	logger     *slog.Logger
}

func main() {
	name := flag.String("name", env("MOCK_NAME", "mock-a"), "upstream name")
	addr := flag.String("addr", env("MOCK_ADDR", ":8001"), "listen address")
	modelFlag := flag.String("model", env("MOCK_MODEL", ""), "model name exposed in metrics and responses")
	latency := flag.Duration("latency", envDuration("MOCK_LATENCY", 300*time.Millisecond), "simulated generation latency")
	baseActive := flag.Int64("base-active", envInt("MOCK_BASE_ACTIVE", 0), "constant load added to reported active requests")
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	model := *modelFlag
	if model == "" {
		model = "mock-" + *name
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	s := &mockServer{
		name:       *name,
		model:      model,
		latency:    *latency,
		baseActive: *baseActive,
		started:    time.Now(),
		logger:     logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/metrics", s.prometheusMetrics)
	mux.HandleFunc("/dedicated-metrics", s.jsonMetrics)
	mux.HandleFunc("/v1/models", s.models)
	mux.HandleFunc("/v1/chat/completions", s.chatCompletions)
	mux.HandleFunc("/v1/completions", s.completions)
	mux.HandleFunc("/", s.notFound)

	server := &http.Server{
		Addr:              *addr,
		Handler:           logRequests(logger, mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("mock LLM listening", "name", s.name, "addr", *addr, "model", s.model, "latency", s.latency)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		logger.Error("server stopped", "error", err)
		stop()
		os.Exit(1)
	case <-ctx.Done():
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	logger.Info("mock LLM stopped", "name", s.name)
}

func (s *mockServer) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func (s *mockServer) models(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []any{
			map[string]any{"id": s.model, "object": "model", "created": s.started.Unix(), "owned_by": s.name},
		},
	})
}

func (s *mockServer) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.active.Add(1)
	s.total.Add(1)
	defer s.active.Add(-1)

	start := time.Now()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var payload struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &payload)

	delay := s.delayFor(r)
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}

	if payload.Stream {
		s.stream(w, r)
	} else {
		writeJSON(w, http.StatusOK, map[string]any{
			"id":      fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   s.model,
			"choices": []any{
				map[string]any{
					"index": 0,
					"message": map[string]any{
						"role":    "assistant",
						"content": fmt.Sprintf("hello from %s", s.name),
					},
					"finish_reason": "stop",
				},
			},
			"usage": map[string]any{"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14},
		})
	}
	s.logger.Info("completion served",
		"name", s.name, "stream", payload.Stream, "delay_ms", delay.Milliseconds(),
		"duration_ms", time.Since(start).Milliseconds(), "active", s.active.Load())
}

func (s *mockServer) completions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.chatCompletions(w, r)
}

func (s *mockServer) stream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	chunks := []string{
		fmt.Sprintf(`{"id":"chatcmpl-stream","object":"chat.completion.chunk","model":%q,"choices":[{"index":0,"delta":{"role":"assistant","content":"hello "}}]}`, s.model),
		fmt.Sprintf(`{"id":"chatcmpl-stream","object":"chat.completion.chunk","model":%q,"choices":[{"index":0,"delta":{"content":"from %s"}}]}`, s.model, s.name),
	}
	for _, chunk := range chunks {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", chunk); err != nil {
			return
		}
		flusher.Flush()
		select {
		case <-time.After(15 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
	flusher.Flush()
}

func (s *mockServer) prometheusMetrics(w http.ResponseWriter, _ *http.Request) {
	active := s.active.Load() + s.baseActive
	if active < 0 {
		active = 0
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, "# HELP mock_llm_active_requests Requests currently being processed by this upstream.\n")
	_, _ = fmt.Fprintf(w, "# TYPE mock_llm_active_requests gauge\n")
	_, _ = fmt.Fprintf(w, "mock_llm_active_requests{model=%s} %d\n", strconv.Quote(s.model), active)
	_, _ = fmt.Fprintf(w, "# HELP mock_llm_queued_requests Requests waiting in the upstream queue.\n")
	_, _ = fmt.Fprintf(w, "# TYPE mock_llm_queued_requests gauge\n")
	_, _ = fmt.Fprintf(w, "mock_llm_queued_requests{model=%s} 0\n", strconv.Quote(s.model))
	_, _ = fmt.Fprintf(w, "# HELP mock_llm_requests_total Requests served by this upstream.\n")
	_, _ = fmt.Fprintf(w, "# TYPE mock_llm_requests_total counter\n")
	_, _ = fmt.Fprintf(w, "mock_llm_requests_total{model=%s} %d\n", strconv.Quote(s.model), s.total.Load())
	_, _ = fmt.Fprintf(w, "# HELP mock_llm_uptime_seconds Uptime of this upstream.\n")
	_, _ = fmt.Fprintf(w, "# TYPE mock_llm_uptime_seconds gauge\n")
	_, _ = fmt.Fprintf(w, "mock_llm_uptime_seconds %f\n", time.Since(s.started).Seconds())
}

func (s *mockServer) jsonMetrics(w http.ResponseWriter, _ *http.Request) {
	active := s.active.Load() + s.baseActive
	if active < 0 {
		active = 0
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active_requests":  active,
		"waiting_requests": 0,
		"model":            s.model,
		"name":             s.name,
		"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *mockServer) notFound(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, map[string]any{
		"error": map[string]any{
			"message": fmt.Sprintf("mock LLM %s has no route %s", s.name, r.URL.Path),
			"type":    "not_found",
		},
	})
}

func (s *mockServer) delayFor(r *http.Request) time.Duration {
	value := r.Header.Get("X-Mock-Delay")
	if value == "" {
		return s.latency
	}
	delay, err := time.ParseDuration(value)
	if err != nil || delay < 0 || delay > time.Minute {
		return s.latency
	}
	return delay
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func logRequests(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		logger.Info("http request",
			"method", r.Method, "path", r.URL.Path, "status", recorder.status,
			"duration_ms", time.Since(start).Milliseconds(), "remote", r.RemoteAddr)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusRecorder) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func envInt(key string, fallback int64) int64 {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}
