package router

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/balancer"
	"github.com/blackcowmoo/llm-fallback-router/internal/telemetry"
	"github.com/blackcowmoo/llm-fallback-router/internal/tracker"
)

// ServerOptions configures the route table around the proxy.
type ServerOptions struct {
	Proxy         http.Handler
	Balancer      *balancer.Balancer
	Tracker       tracker.Tracker
	Telemetry     *telemetry.Metrics
	RedisPing     func(context.Context) error
	RedisRequired bool
	InstanceID    string
	Strategy      string
	AdminToken    string
	Version       string
	Started       time.Time
}

// NewServer builds the router's HTTP handler: local health/metrics/admin
// endpoints plus the proxy for all other paths.
func NewServer(opts ServerOptions) http.Handler {
	s := &server{opts: opts}
	if s.opts.Started.IsZero() {
		s.opts.Started = time.Now()
	}

	mux := http.NewServeMux()
	mux.Handle("/healthz", getOnly(http.HandlerFunc(s.healthz)))
	mux.Handle("/readyz", getOnly(http.HandlerFunc(s.readyz)))
	mux.Handle("/metrics", getOnly(opts.Telemetry.Handler()))
	mux.Handle("/admin", getOnly(http.HandlerFunc(s.admin)))
	mux.Handle("/admin/", getOnly(http.HandlerFunc(s.admin)))
	mux.Handle("/", opts.Proxy)
	return mux
}

type server struct {
	opts ServerOptions
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	healthy := s.opts.Balancer.HealthyCount()
	total := s.opts.Balancer.UpstreamCount()
	redisEnabled := s.opts.RedisPing != nil
	redisOK := true
	reasons := make([]string, 0, 2)
	if healthy == 0 {
		reasons = append(reasons, "no healthy upstreams")
	}
	if redisEnabled && s.opts.RedisRequired {
		if err := s.opts.RedisPing(ctx); err != nil {
			redisOK = false
			reasons = append(reasons, "redis is required but unavailable")
		}
	}

	status := http.StatusOK
	state := "ok"
	if len(reasons) > 0 {
		status = http.StatusServiceUnavailable
		state = "unavailable"
	}
	writeJSON(w, status, map[string]any{
		"status": state,
		"upstreams": map[string]any{
			"healthy": healthy,
			"total":   total,
		},
		"redis": map[string]any{
			"enabled":  redisEnabled,
			"required": s.opts.RedisRequired,
			"ok":       redisOK,
		},
		"reasons": reasons,
	})
}

func (s *server) admin(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="llm-router"`)
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return
	}
	if r.URL.Path != "/admin/state" {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not found"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"instance_id":    s.opts.InstanceID,
		"version":        s.opts.Version,
		"strategy":       s.opts.Strategy,
		"started_at":     s.opts.Started.UTC(),
		"uptime_seconds": time.Since(s.opts.Started).Seconds(),
		"upstreams":      s.opts.Balancer.States(ctx),
		"redis":          s.opts.Tracker.Status(),
	})
}

func (s *server) authorized(r *http.Request) bool {
	if s.opts.AdminToken == "" {
		return true
	}
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.AdminToken)) == 1
}

func getOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
