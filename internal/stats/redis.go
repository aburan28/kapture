package stats

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisSinkConfig configures the Redis statistics sink.
type RedisSinkConfig struct {
	// Addr is the Redis host:port.
	Addr string
	// Password is optional AUTH credentials.
	Password string
	// DB is the logical database number.
	DB int
	// StreamMaxLen caps each window stream (approximate trimming);
	// 0 = default 1000 entries.
	StreamMaxLen int64
}

// RedisSink publishes statistics to Redis: the latest snapshot as a
// plain key with TTL, completed flow windows as stream entries
// (XADD MAXLEN~).
type RedisSink struct {
	client       *redis.Client
	streamMaxLen int64
}

// NewRedisSink connects to Redis and verifies the connection.
func NewRedisSink(ctx context.Context, cfg RedisSinkConfig) (*RedisSink, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis address is required")
	}
	if cfg.StreamMaxLen <= 0 {
		cfg.StreamMaxLen = 1000
	}
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("connect redis %s: %w", cfg.Addr, err)
	}
	return &RedisSink{client: client, streamMaxLen: cfg.StreamMaxLen}, nil
}

// PublishSnapshot stores the snapshot JSON under key with a TTL.
func (s *RedisSink) PublishSnapshot(ctx context.Context, key string, payload []byte, ttl time.Duration) error {
	return s.client.Set(ctx, key, payload, ttl).Err()
}

// AppendWindow appends one flow window to the stream, trimming to the
// configured approximate length.
func (s *RedisSink) AppendWindow(ctx context.Context, stream string, payload []byte) error {
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: s.streamMaxLen,
		Approx: true,
		Values: map[string]any{"window": payload},
	}).Err()
}

// Close releases the Redis connection.
func (s *RedisSink) Close() error { return s.client.Close() }
