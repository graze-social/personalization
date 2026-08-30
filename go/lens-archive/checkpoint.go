package main

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// checkpoint persists how far the archive pass has got.
//
// A full pass is ~1.9 TB and hours of downloading. Without this, any eviction,
// node drain, or OOM restarts from segment zero — so the job could be restarted
// indefinitely and never finish. The cursor is written only after the rows it
// covers are durable in ClickHouse.
type checkpoint struct {
	client *redis.Client
	key    string
}

func newCheckpoint(redisURL, key string) *checkpoint {
	if redisURL == "" {
		return &checkpoint{key: key}
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		logf("checkpoint disabled, unparseable redis url: %v", err)
		return &checkpoint{key: key}
	}
	return &checkpoint{client: redis.NewClient(opt), key: key}
}

func (c *checkpoint) load(ctx context.Context) (uint64, error) {
	if c.client == nil {
		return 0, nil
	}
	v, err := c.client.Get(ctx, c.key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(v, 10, 64)
}

func (c *checkpoint) save(ctx context.Context, seq uint64) error {
	if c.client == nil {
		return nil
	}
	// No TTL. A cursor that expires mid-run silently restarts the job from the
	// beginning, which is the exact failure this exists to prevent.
	return c.client.Set(ctx, c.key, strconv.FormatUint(seq, 10), 0).Err()
}
