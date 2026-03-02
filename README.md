# Graze Personalization Service

A high-performance microservice for real-time personalized feed ranking using the LinkLonk algorithm. Written in Rust with Axum and Redis.

## Overview

The Graze Personalization Service provides personalized feed rankings for Bluesky users based on their like history and the likes of similar users. It uses a 3-step random walk algorithm on the like graph to discover relevant content.

## Architecture

The service consists of three worker types:

- **API (graze-api)**: Serves personalization, feed skeleton, and admin endpoints
- **Like Streamer (graze-like-streamer)**: Consumes likes from Bluesky's Jetstream and maintains the Redis like graph
- **Candidate Sync (graze-candidate-sync)**: Fetches algorithm post candidates from a configurable source (ClickHouse, HTTP, or admin-only) and syncs to Redis

For data-flow diagrams, pluggable backends (interactions writer, candidate source, special posts), and the single ClickHouse module, see **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Quick Start

### HTTP-only (no ClickHouse)

You can run without ClickHouse by using HTTP candidate source and local special posts:

```bash
# API: no interaction persistence, local special posts
export REDIS_URL=redis://localhost:6379
export INTERACTIONS_WRITER=none
export SPECIAL_POSTS_SOURCE=local
cargo run --bin graze-api
```

```bash
# Candidate sync: pull candidates from the API
export REDIS_URL=redis://localhost:6379
export CANDIDATE_SOURCE=http
export CANDIDATE_HTTP_URL=http://localhost:8080
cargo run --bin graze-candidate-sync
```

Populate candidates via admin: `POST /v1/feeds/:algo_id/candidates` with `{ "uris": ["at://..."] }`. Manage special posts via `GET/POST/DELETE /v1/feeds/:algo_id/special-posts/...`. See [docs/ADMIN_API.md](docs/ADMIN_API.md).

### Full stack (with ClickHouse)

For interaction analytics and ClickHouse-backed candidate sync:

```bash
# API with ClickHouse interaction writer
export REDIS_URL=redis://localhost:6379
export INTERACTIONS_WRITER=clickhouse
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_DATABASE=graze
export SPECIAL_POSTS_SOURCE=remote
cargo run --bin graze-api
```

```bash
# Candidate sync from ClickHouse
export REDIS_URL=redis://localhost:6379
export CANDIDATE_SOURCE=clickhouse
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_DATABASE=graze
cargo run --bin graze-candidate-sync
```

All configuration options: **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)**.

### Configuration (summary)

1. **Redis** is required: `REDIS_URL`
2. **ClickHouse** is optional: use when `INTERACTIONS_WRITER=clickhouse` or `CANDIDATE_SOURCE=clickhouse`
3. Set `INTERACTIONS_WRITER=none` and `CANDIDATE_SOURCE=http` or `admin_only` and `SPECIAL_POSTS_SOURCE=local` for a no-ClickHouse setup

### Docker

```bash
# Build and run (uses .env file)
docker compose up -d api

# Check health
curl http://localhost:8080/internal/ready

# Run all workers
docker compose --profile full up -d

# Build and push to DockerHub
docker compose build
docker compose push
```

## API Endpoints

### POST /v1/personalize

Get personalized posts for a user.

```json
{
  "user_did": "did:plc:abc123xyz",
  "algo_id": 12345,
  "limit": 30,
  "cursor": null,
  "params": {
    "preset": "discovery"
  }
}
```

### GET /internal/ready

Health check endpoint.

### POST /v1/invalidate

Invalidate cached results.

### POST /v1/sync

Force sync algorithm posts from ClickHouse.

## Configuration

All configuration is via environment variables. See **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** for the full table (interactions writer, candidate source, special posts, no-ClickHouse setup). Summary:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `INTERACTIONS_WRITER` | `clickhouse` | `clickhouse` or `none` |
| `CANDIDATE_SOURCE` | `clickhouse` | `clickhouse`, `http`, or `admin_only` (sync worker) |
| `SPECIAL_POSTS_SOURCE` | `remote` | `remote` or `local` |
| `CLICKHOUSE_*` | (see docs) | Used when writer/source is ClickHouse |
| `HTTP_HOST` / `HTTP_PORT` | `0.0.0.0` / `8080` | Bind address for API |
| `READ_ONLY_MODE` | `false` | Disable admin writes; 503 for mutating endpoints |

**Feed lifecycle** (add/remove feeds, candidate pool ClickHouse vs manual, special posts, turn off): **[docs/FEED_LIFECYCLE.md](docs/FEED_LIFECYCLE.md)**.  
Admin endpoints (candidates, special posts, Thompson, audit): **[docs/ADMIN_API.md](docs/ADMIN_API.md)**. See `crates/graze-api/src/config.rs` and `crates/graze-candidate-sync/src/config.rs` for the full list of options.

## Algorithm Presets

- **default**: Balanced personalization
- **discovery**: Favor newer, niche content
- **stable**: Broader, more consistent results  
- **fast**: Optimized for speed

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=graze_personalization

# Run load test
python scripts/load_test.py --url http://localhost:8080 --duration 60
```

## Debugging & Inspection

### Feed Status Endpoint

Use the built-in diagnostic endpoint to check feed health:

```bash
curl http://localhost:8080/v1/feeds/status/1  # Replace 1 with your algo_id
```

Returns:
- `algo_posts.exists` / `algo_posts.count` - Whether algorithm posts exist
- `sync_metadata.last_sync_timestamp` - When sync last ran
- `rate_limit.locked` / `rate_limit.ttl_seconds` - Rate limit status
- `fallback_tranches` - Status of trending/popular/velocity/discovery
- `diagnosis` - Human-readable problem description

### Redis Key Inspection

```bash
# Check if algorithm posts exist
redis-cli KEYS "ap:*"

# Count posts for algo_id 1
redis-cli SCARD "ap:1"

# Check sync metadata
redis-cli HGETALL "am:1"

# Check rate limit lock (blocks syncs for 5 min by default)
redis-cli GET "lock:sync:1"
redis-cli TTL "lock:sync:1"

# Check sync queue depth
redis-cli LLEN "queue:sync"

# Check fallback tranches
redis-cli ZCARD "trending:1"
redis-cli ZCARD "popular:1"
redis-cli ZCARD "velocity:1"
redis-cli ZCARD "discovery:1"

# Check user likes (hash is first 12 chars of SHA256 of DID)
redis-cli ZCARD "ul:<user_hash>"
```

### Triggering Algorithm Sync

The candidate sync worker does **not** sync automatically. You must trigger syncs manually:

```bash
# Queue a sync for algo_id 1
curl -X POST http://localhost:8080/v1/sync \
  -H "Content-Type: application/json" \
  -d '{"algo_id": 1}'

# Force sync (bypasses rate limit)
curl -X POST http://localhost:8080/v1/sync \
  -H "Content-Type: application/json" \
  -d '{"algo_id": 1, "force": true}'
```

### Common Sync Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Sync never runs | No trigger sent | POST to `/v1/sync` |
| "Sync rate limited" in logs | Lock active | Wait 5 min or use `force: true` |
| "No posts found" in logs | ClickHouse empty/unreachable | Check ClickHouse config |
| Empty `ap:*` keys | Sync hasn't completed | Check worker logs |
| 503 on `/v1/sync` | Read-only mode enabled | Set `READ_ONLY_MODE=false` |

### ClickHouse Verification

```bash
# Test connectivity
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD \
  "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/?query=SELECT+1"

# Check algorithm_posts_v2 table exists
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD \
  "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" \
  -d "SELECT COUNT(*) FROM $CLICKHOUSE_DATABASE.algorithm_posts_v2"

# Check posts for a specific algo_id
curl -u $CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD \
  "http://$CLICKHOUSE_HOST:$CLICKHOUSE_PORT/" \
  -d "SELECT COUNT(*) FROM $CLICKHOUSE_DATABASE.algorithm_posts_v2 WHERE algo_id = 1"
```

### Enable Debug Logging

```bash
# Full debug output
RUST_LOG=graze=debug cargo run --bin graze-candidate-sync

# Or for specific modules
RUST_LOG=graze::workers::candidate_sync=debug cargo run --bin graze-candidate-sync
```

### Key Log Messages

| Log Message | Meaning |
|-------------|---------|
| "Starting candidate sync worker" | Worker started successfully |
| "Sync starting" | Processing a sync request |
| "Sync completed" | Sync finished successfully |
| "Sync rate limited" | Skipped due to recent sync |
| "Invalid algo_id in sync queue" | Corrupted queue entry |
| "No posts found" | ClickHouse returned empty results |
| "ClickHouse connection error" | Network/auth failure |
| "ClickHouse query failed" | Query error (check table/schema) |

## Deployment

Kubernetes manifests are provided in the `kube/` directory:

```bash
kubectl apply -f kube/
```

## Docker Hub

Images are pushed to: `dgaff/graze-personalization`

```bash
# Pull the image
docker pull dgaff/graze-personalization:latest
```

## License

This project is dual-licensed under the Apache License, Version 2.0 or the MIT License, at your option. See [LICENSE](LICENSE) and [LICENSE-MIT](LICENSE-MIT) for the full text.
