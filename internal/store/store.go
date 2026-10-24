// Package store implements the Redis-backed shared state used by the router:
// per-instance active request counters and a shared metrics reading cache.
package store

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/blackcowmoo/llm-fallback-router/internal/metrics"
)

// Options configures a Store.
type Options struct {
	Address          string
	Username         string
	Password         string
	DB               int
	KeyPrefix        string
	InstanceID       string
	InstanceTTL      time.Duration
	ReadingTTL       time.Duration
	OperationTimeout time.Duration
	DialTimeout      time.Duration
	PoolSize         int
	TLSEnabled       bool
	TLSCAFile        string
	TLSSkipVerify    bool
}

// Store wraps a Redis client with router-specific key conventions.
type Store struct {
	client      *redis.Client
	prefix      string
	instanceID  string
	instanceTTL time.Duration
	readingTTL  time.Duration
	opTimeout   time.Duration
}

// New creates a Store. It does not connect to Redis; call Ping to check
// connectivity. The returned client reconnects automatically.
func New(opts Options) (*Store, error) {
	if opts.Address == "" {
		return nil, errors.New("redis address must not be empty")
	}
	if opts.InstanceID == "" {
		return nil, errors.New("instance id must not be empty")
	}
	if opts.InstanceTTL <= 0 {
		return nil, errors.New("instance TTL must be positive")
	}
	if opts.OperationTimeout <= 0 {
		return nil, errors.New("operation timeout must be positive")
	}
	if opts.PoolSize < 1 {
		opts.PoolSize = 10
	}
	if opts.ReadingTTL <= 0 {
		opts.ReadingTTL = 60 * time.Second
	}
	prefix := opts.KeyPrefix
	if prefix != "" && !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}

	redisOpts := &redis.Options{
		Addr:         opts.Address,
		Username:     opts.Username,
		Password:     opts.Password,
		DB:           opts.DB,
		PoolSize:     opts.PoolSize,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.OperationTimeout,
		WriteTimeout: opts.OperationTimeout,
		PoolTimeout:  opts.OperationTimeout,
		Protocol:     2,
	}
	if opts.TLSEnabled {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: opts.TLSSkipVerify} //nolint:gosec // explicit opt-in
		if opts.TLSCAFile != "" {
			pem, err := os.ReadFile(opts.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("read redis CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("redis CA file %s contains no certificates", opts.TLSCAFile)
			}
			tlsConfig.RootCAs = pool
		}
		redisOpts.TLSConfig = tlsConfig
	}

	return &Store{
		client:      redis.NewClient(redisOpts),
		prefix:      prefix,
		instanceID:  opts.InstanceID,
		instanceTTL: opts.InstanceTTL,
		readingTTL:  opts.ReadingTTL,
		opTimeout:   opts.OperationTimeout,
	}, nil
}

// Ping checks Redis connectivity.
func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.client.Ping(ctx).Err()
}

// Heartbeat publishes this instance's absolute active counts, refreshes the
// instance key TTL, and refreshes its liveness score.
func (s *Store) Heartbeat(ctx context.Context, counts map[string]int64) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	now := time.Now()
	activeKey := s.activeKey(s.instanceID)
	pipe := s.client.TxPipeline()

	if len(counts) > 0 {
		values := make(map[string]any, len(counts))
		for upstream, count := range counts {
			values[upstream] = count
		}
		pipe.HSet(ctx, activeKey, values)
	}
	pipe.PExpire(ctx, activeKey, s.instanceTTL)
	pipe.ZAdd(ctx, s.routersKey(), redis.Z{
		Score:  float64(now.UnixMilli()),
		Member: s.instanceID,
	})
	pipe.ZRemRangeByScore(ctx, s.routersKey(),
		"-inf",
		strconv.FormatInt(now.Add(-s.instanceTTL).UnixMilli(), 10))

	_, err := pipe.Exec(ctx)
	return err
}

// ActiveCounts sums live active counts from all router instances except this
// one. Upstreams with no traffic are present with a zero value.
func (s *Store) ActiveCounts(ctx context.Context, upstreams []string) (map[string]int64, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	counts := make(map[string]int64, len(upstreams))
	for _, upstream := range upstreams {
		counts[upstream] = 0
	}
	if len(upstreams) == 0 {
		return counts, nil
	}

	now := time.Now()
	ids, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     s.routersKey(),
		ByScore: true,
		Start:   strconv.FormatInt(now.Add(-s.instanceTTL).UnixMilli(), 10),
		Stop:    "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return counts, nil
	}

	pipe := s.client.Pipeline()
	commands := make([]*redis.SliceCmd, 0, len(ids))
	for _, id := range ids {
		if id == s.instanceID {
			continue
		}
		commands = append(commands, pipe.HMGet(ctx, s.activeKey(id), upstreams...))
	}
	if len(commands) == 0 {
		return counts, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	for _, command := range commands {
		values, err := command.Result()
		if err != nil {
			continue
		}
		for i, value := range values {
			if i >= len(upstreams) || value == nil {
				continue
			}
			count, err := parseCount(value)
			if err != nil {
				continue
			}
			counts[upstreams[i]] += count
		}
	}
	return counts, nil
}

// Deregister removes this instance's active counts and liveness entry. It is
// best-effort: the TTLs clean up after a crashed instance anyway.
func (s *Store) Deregister(ctx context.Context) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	pipe := s.client.TxPipeline()
	pipe.Del(ctx, s.activeKey(s.instanceID))
	pipe.ZRem(ctx, s.routersKey(), s.instanceID)
	_, err := pipe.Exec(ctx)
	return err
}

// PutReading shares a metrics reading with other router instances.
func (s *Store) PutReading(ctx context.Context, upstream string, reading metrics.Reading) error {
	data, err := json.Marshal(reading)
	if err != nil {
		return err
	}
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.client.Set(ctx, s.readingKey(upstream), data, s.readingTTL).Err()
}

// GetReading retrieves a metrics reading shared by another instance.
func (s *Store) GetReading(ctx context.Context, upstream string) (metrics.Reading, bool, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()

	data, err := s.client.Get(ctx, s.readingKey(upstream)).Bytes()
	if errors.Is(err, redis.Nil) {
		return metrics.Reading{}, false, nil
	}
	if err != nil {
		return metrics.Reading{}, false, err
	}
	var reading metrics.Reading
	if err := json.Unmarshal(data, &reading); err != nil {
		return metrics.Reading{}, false, fmt.Errorf("decode shared reading for %s: %w", upstream, err)
	}
	return reading, true, nil
}

// Close closes the Redis client.
func (s *Store) Close() error { return s.client.Close() }

func (s *Store) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opTimeout)
}

func (s *Store) activeKey(instanceID string) string {
	return s.prefix + "active:" + instanceID
}

func (s *Store) routersKey() string { return s.prefix + "routers" }

func (s *Store) readingKey(upstream string) string {
	return s.prefix + "reading:" + upstream
}

func parseCount(value any) (int64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case int64:
		return v, nil
	default:
		return strconv.ParseInt(fmt.Sprint(v), 10, 64)
	}
}
