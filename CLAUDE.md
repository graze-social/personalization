# Graze Personalization Service

High-performance microservice for real-time personalized feed ranking using the LinkLonk algorithm. Written in Rust with Axum and deadpool-redis.

## Project Structure

```
src/
├── lib.rs                      # Library exports
├── config.rs                   # Configuration
├── error.rs                    # Error types with Axum integration
├── metrics.rs                  # Prometheus metrics
├── models/                     # Request/response models
│   ├── requests.rs             # API request types
│   ├── responses.rs            # API response types
│   └── atproto.rs              # ATProto-specific types
├── redis/                      # Redis client and utilities
│   ├── client.rs               # Connection pool management
│   ├── keys.rs                 # Key patterns (Keys struct)
│   ├── hash.rs                 # hash_did() implementation
│   └── scripts.rs              # Lua script manager
├── services/                   # External service clients
│   ├── uri_interner.rs         # URI <-> ID mapping with LRU cache
│   └── special_posts.rs        # Pinned/sticky/sponsored posts
├── algorithm/                  # LinkLonk algorithm
│   ├── mod.rs                  # LinkLonkAlgorithm orchestration
│   ├── params.rs               # Algorithm parameters and presets
│   ├── coliker.rs              # Co-liker computation
│   ├── scorer.rs               # Post scoring
│   ├── scoring_core.rs         # Parallel scoring implementation
│   ├── thompson.rs             # Thompson sampling for A/B testing
│   ├── liker_cache.rs          # Hot post liker cache
│   ├── feed_cache.rs           # Feed skeleton cache
│   └── author_affinity.rs      # Author-level personalization
├── api/                        # HTTP handlers
│   ├── mod.rs                  # Router setup
│   ├── personalize.rs          # POST /v1/personalize
│   ├── feed.rs                 # ATProto feed endpoints
│   ├── cursor.rs               # Feed cursor encoding/decoding
│   ├── fallback.rs             # Fallback tranche blending
│   ├── special_posts.rs        # Special posts injection
│   ├── health.rs               # Health and metrics endpoints
│   └── admin.rs                # Admin endpoints
├── workers/                    # Background workers
│   ├── like_streamer.rs        # Jetstream consumer for likes
│   ├── candidate_sync.rs       # ClickHouse sync + fallback tranches
│   └── bot_filter.rs           # Bot detection and filtering
└── bin/                        # Binary entry points
    ├── api.rs                  # graze-api server
    ├── like_streamer.rs        # graze-like-streamer worker
    └── candidate_sync.rs       # graze-candidate-sync worker

lua/
├── graze_compute.lua           # Core scoring algorithm (runs in Redis)
└── graze_compute_inverted.lua  # Inverted scoring variant

scripts/
└── clickhouse-init/            # ClickHouse schema initialization
    └── 01-schema.sql

tests/
├── integration_redis.rs        # Redis integration tests
├── common/                     # Test utilities
└── helpers/                    # Test helpers
```

## Key Concepts

### LinkLonk Algorithm (3-step random walk)
1. Get user's recent likes
2. Find co-likers (users who liked same posts before)
3. Score candidate posts from co-likers' likes

### URI interner (date-sharded)

- New post IDs: `{YYYYMMDD}{seq:010}` (18 digits, no colons)
- `uri2id:{date}` / `id2uri:{date}` / `uri:counter:{date}` — EXPIRE aligned with `LIKE_TTL_DAYS`
- Legacy global `uri2id` / `id2uri` — read-only fallback; orphan GC via `scripts/redis_interner_gc.py`

### Redis Key Patterns (see `src/redis/keys.rs` Keys struct)
- `ul:{hash}` - User likes (ZSET, score=timestamp)
- `pl:{hash}` - Post likers (ZSET, score=timestamp)
- `ap:{algo_id}` - Algorithm eligible posts (SET)
- `ll:{algo_id}:{hash}` - Cached personalization results (ZSET)
- `fsc:{algo_id}:{hash}` - Feed skeleton cache (LIST)
- `colikes:{hash}` - Pre-computed co-liker weights (ZSET)
- `trending:{algo_id}` - Trending posts fallback (ZSET)
- `popular:{algo_id}` - Popular posts fallback (ZSET)
- `velocity:{algo_id}` - Velocity-based fallback (ZSET)
- `discovery:{algo_id}` - Discovery posts fallback (ZSET)

### Algorithm Presets
- `default` - Balanced personalization
- `discovery` - Favor newer, niche content
- `stable` - Broader, more consistent results
- `fast` - Optimized for speed

## Development Commands

```bash
# Run API server
cargo run --bin graze-api

# Run like streamer worker
cargo run --bin graze-like-streamer

# Run candidate sync worker
cargo run --bin graze-candidate-sync

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run clippy
cargo clippy --all-targets --all-features -- -D warnings

# Format code
cargo fmt

# Build release binaries
cargo build --release --bin graze-api --bin graze-like-streamer --bin graze-candidate-sync
```

## Configuration

Key environment variables:
- `HTTP_HOST` - HTTP host to bind services to (default: 0.0.0.0)
- `HTTP_PORT` - HTTP port to bind services to (default: 8080)
- `HTTP_EXTERNAL` - Public hostname the service is running on
- `METRICS_PORT` - HTTP port to publish metrics on
- `REDIS_URL` - Redis connection string
- `CLICKHOUSE_HOST` - ClickHouse host for algorithm posts
- `CLICKHOUSE_PORT` - ClickHouse port (default: 8123)
- `CLICKHOUSE_DATABASE` - ClickHouse database name
- `JETSTREAM_URL` - Jetstream WebSocket URL
- `FEED_GENERATOR_DID` - Service DID for ATProto

## Docker

```bash
# Build image
docker build -t graze .

# Run API server
docker run -e REDIS_URL=redis://host:6379 graze

# Run like streamer
docker run -e REDIS_URL=redis://host:6379 graze /app/graze-like-streamer

# Run candidate sync
docker run -e REDIS_URL=redis://host:6379 graze /app/graze-candidate-sync
```
