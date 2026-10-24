// Command llm-router runs the active-request-aware LLM router.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/blackcowmoo/llm-fallback-router/internal/app"
	"github.com/blackcowmoo/llm-fallback-router/internal/config"
	"github.com/blackcowmoo/llm-fallback-router/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "healthcheck" {
		return runHealthcheck(args[1:])
	}

	flags := flag.NewFlagSet("llm-router", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "config.yaml", "path to the YAML configuration file")
	listen := flags.String("listen", "", "override server.listen (for example :8080)")
	logLevel := flags.String("log-level", "info", "log level: debug, info, warn, error")
	logFormat := flags.String("log-format", "json", "log format: json or text")
	showVersion := flags.Bool("version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Printf("llm-router %s\n", version.String())
		return 0
	}

	logger, err := newLogger(*logFormat, *logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "llm-router: %v\n", err)
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		return 1
	}
	if *listen != "" {
		cfg.Server.Listen = *listen
		if err := cfg.Validate(); err != nil {
			logger.Error("invalid configuration after --listen override", "error", err)
			return 1
		}
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := app.New(signalCtx, cfg, logger)
	if err != nil {
		logger.Error("failed to start router", "error", err)
		return 1
	}
	defer func() {
		if err := application.Close(); err != nil {
			logger.Warn("shutdown cleanup failed", "error", err)
		}
	}()

	server := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           application.Handler(),
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.D(),
		IdleTimeout:       cfg.Server.IdleTimeout.D(),
		MaxHeaderBytes:    cfg.Server.MaxHeaderBytes,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("llm router listening",
			"addr", cfg.Server.Listen,
			"instance_id", application.InstanceID(),
			"version", version.String(),
			"upstreams", len(cfg.Upstreams),
			"strategy", cfg.Routing.Strategy,
			"redis", cfg.Redis.Address != "",
		)
		serverErr <- server.ListenAndServe()
	}()

	exitCode := 0
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped unexpectedly", "error", err)
			exitCode = 1
		}
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.D())
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("graceful HTTP shutdown timed out; forcing close", "error", err)
		_ = server.Close()
	}
	logger.Info("llm router stopped")
	return exitCode
}

func runHealthcheck(args []string) int {
	flags := flag.NewFlagSet("llm-router healthcheck", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	url := flags.String("url", "http://127.0.0.1:8080/healthz", "health endpoint URL")
	timeout := flags.Duration("timeout", 2*time.Second, "request timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	client := &http.Client{Timeout: *timeout}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, *url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck request failed: %v\n", err)
		return 1
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed: %v\n", err)
		return 1
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		fmt.Fprintf(os.Stderr, "healthcheck failed: HTTP %d\n", response.StatusCode)
		return 1
	}
	return 0
}

func newLogger(format, level string) (*slog.Logger, error) {
	var parsedLevel slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		parsedLevel = slog.LevelDebug
	case "info", "":
		parsedLevel = slog.LevelInfo
	case "warn", "warning":
		parsedLevel = slog.LevelWarn
	case "error":
		parsedLevel = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown log level %q", level)
	}

	options := &slog.HandlerOptions{Level: parsedLevel}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json", "":
		return slog.New(slog.NewJSONHandler(os.Stdout, options)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, options)), nil
	default:
		return nil, fmt.Errorf("unknown log format %q", format)
	}
}
