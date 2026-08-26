# Debugging Guide

This guide covers tools and techniques for debugging the Graze Personalization Service.

## Quick Start: Which Tool to Use

| Problem | Tool | Command |
|---------|------|---------|
| Feed returns empty | Feed Status API | `curl https://localhost:8081/v1/feeds/status/{algo_id}` |
| Wrong/unexpected posts | Prove API | `curl -X POST https://localhost:8081/v1/prove -d '{"user_did":"...","algo_id":N}'` |
| Debug specific user | Audit API | `curl -X POST https://localhost:8081/v1/audit/users -d '{"dids":["..."]}'` |
| Step-by-step simulation | Debug Script | `python scripts/debug_feed_query.py <did> <algo_id> --verbose` |
| Performance issues | Metrics | `curl https://localhost:8081/metrics` |

---

## Feed Query Simulator

The `debug_feed_query.py` script simulates the feed query process step-by-step, showing each Redis query, timing, and diagnostic information. This helps debug user experience and performance issues.

### Usage

```bash
python scripts/debug_feed_query.py <user_did> <algo_id> [options]
```

**Arguments:**
- `user_did` - User DID (e.g., `did:plc:abc123xyz`)
- `algo_id` - Algorithm ID (integer)

**Options:**
- `--preset PRESET` - Algorithm preset: `default`, `discovery`, `stable`, `fast`
- `--limit N` - Number of results to simulate (default: 30)
- `--redis-url URL` - Redis URL (overrides `REDIS_URL`)
- `--json` - Output as JSON instead of human-readable
- `--verbose, -v` - Show sample data from each stage

**Default behavior:** Interactive step-by-step mode. After each stage:
- Press `Enter` to continue
- Press `q` to quit
- Press `s` to skip to summary

### Simulation Stages

The tool replicates the LinkLonk algorithm flow:

| Stage | Name | What It Checks |
|-------|------|----------------|
| 0 | Identity Resolution | DID hashing |
| 1 | Prerequisites Check | Algorithm posts, user likes, co-likers, trending |
| 2 | Cache Status | Personalization cache, feed cache TTLs |
| 3 | User Likes Analysis | Recent likes within time window |
| 4 | Co-liker Discovery | Source users who liked same posts |
| 5 | Candidate Collection | Posts from co-likers' likes |
| 6 | Algorithm Membership | Filter candidates to algorithm posts |
| 7 | Scoring & Results | Final scoring with popularity penalty |

### Running with Docker

#### Using `docker run`

```bash
# Basic usage - connect to a Redis instance
docker run --rm -it \
  -e REDIS_URL=redis://your-redis-host:6379 \
  dgaff/graze-personalization:latest \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345

# With verbose output
docker run --rm -it \
  -e REDIS_URL=redis://your-redis-host:6379 \
  dgaff/graze-personalization:latest \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345 --verbose

# JSON output (non-interactive)
docker run --rm \
  -e REDIS_URL=redis://your-redis-host:6379 \
  dgaff/graze-personalization:latest \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345 --json

# With a specific preset
docker run --rm -it \
  -e REDIS_URL=redis://your-redis-host:6379 \
  dgaff/graze-personalization:latest \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345 --preset discovery --verbose

# Override Redis URL via CLI argument
docker run --rm -it \
  dgaff/graze-personalization:latest \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345 \
  --redis-url redis://your-redis-host:6379
```

#### Using Docker Compose

```bash
# Run against the same Redis as your docker-compose setup
docker compose run --rm api \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345 --verbose
```

#### In Kubernetes

```bash
# Execute in an existing pod
kubectl exec -it <pod-name> -- \
  python scripts/debug_feed_query.py did:plc:abc123xyz 12345 --verbose

# Run as a one-off job with JSON output
kubectl run debug-feed --rm -it --restart=Never \
  --image=dgaff/graze-personalization:latest \
  --env="REDIS_URL=redis://redis-service:6379" \
  -- python scripts/debug_feed_query.py did:plc:abc123xyz 12345 --json
```

### Example Output

```
======================================================================
Stage 1: Prerequisites Check [OK]
======================================================================
Status: All prerequisites met
Time: 3.42ms

Redis commands executed:
  > EXISTS ap:12345
  > SCARD ap:12345
  > EXISTS ul:7a8b9c0d1e2f3a4b
  > ZCARD ul:7a8b9c0d1e2f3a4b
  > EXISTS colikes:7a8b9c0d1e2f3a4b
  > TTL colikes:7a8b9c0d1e2f3a4b
  > ZCARD colikes:7a8b9c0d1e2f3a4b
  > EXISTS trending:12345
  > ZCARD trending:12345

Data:
  algo_posts:
    exists: True
    count: 18432
  user_likes:
    exists: True
    count: 347
  colikes:
    exists: True
    ttl_seconds: 2847
    count: 412
  trending:
    exists: True
    count: 500

[Enter=continue, q=quit, s=skip to summary]
```

### Recommendations Engine

The tool provides actionable recommendations based on findings:

| Condition | Recommendation |
|-----------|----------------|
| No algorithm posts | "Sync algorithm posts: POST /v1/sync with algo_id=X" |
| No user likes | "User has no likes - will see trending posts only" |
| < 5 likes | "Cold user - feed will blend 80% trending, 20% personalized" |
| No co-likers | "No pre-computed co-likers - first request will be slower" |
| Low algo hit rate | "Consider increasing max_algo_checks" |

### Troubleshooting

**Redis connection errors:**
```bash
# Verify Redis is reachable
docker run --rm dgaff/graze-personalization:latest \
  python -c "import redis; r = redis.from_url('redis://your-host:6379'); print(r.ping())"
```

**User has no personalized results:**
1. Check if user has likes (Stage 3)
2. Check if algorithm posts are synced (Stage 1)
3. Check co-liker cache status (Stage 4)
4. Verify algorithm membership hit rate (Stage 6)

**Slow performance:**
1. Look at timing breakdown in summary
2. Check if co-liker cache is fresh (Stage 1)
3. Check candidate collection time (Stage 5)
4. Consider using `--preset fast` for debugging

---

## API Debugging Endpoints

These HTTP endpoints provide real-time debugging without needing CLI access.

### Feed Health Check

**Endpoint:** `GET /v1/feeds/status/:algo_id`

Returns comprehensive diagnostic information about a feed's health status. Use this first when feeds return empty.

```bash
curl -s https://localhost:8081/v1/feeds/status/12345 | jq
```

**Response fields:**

| Field | Description |
|-------|-------------|
| `healthy` | Overall health status (true/false) |
| `algo_posts.exists` | Whether algorithm posts are synced |
| `algo_posts.count` | Number of posts in the algorithm |
| `algo_posts.ttl_seconds` | Time until posts expire (-1 = no expiry) |
| `sync_metadata.last_sync_timestamp` | Unix timestamp of last sync |
| `sync_metadata.last_sync_age_seconds` | Seconds since last sync |
| `rate_limit.locked` | Whether sync is rate-limited |
| `fallback_tranches` | Status of trending/popular/velocity/discovery fallbacks |
| `diagnosis` | Human-readable diagnosis message |

**Common diagnosis messages:**

| Message | Meaning | Action |
|---------|---------|--------|
| "Sync has never been triggered" | No data exists | Run `POST /v1/sync` |
| "Posts key expired" | Data is stale | Run `POST /v1/sync` |
| "Posts key exists but is empty" | ClickHouse returned no data | Check ClickHouse query |
| "Posts key will expire in X seconds" | Data expiring soon | Consider preemptive sync |
| "Feed appears healthy" | Everything looks good | Issue is elsewhere |

### Algorithm Explainability (Prove)

**Endpoint:** `POST /v1/prove`

Shows exactly how personalization results were computed for a user. Use this to understand why specific posts appear (or don't appear) in a feed.

```bash
# Basic usage
curl -s -X POST https://localhost:8081/v1/prove \
  -H "Content-Type: application/json" \
  -d '{"user_did": "did:plc:abc123xyz", "algo_id": 12345}' | jq

# With specific preset
curl -s -X POST https://localhost:8081/v1/prove \
  -H "Content-Type: application/json" \
  -d '{"user_did": "did:plc:abc123xyz", "algo_id": 12345, "preset": "discovery"}' | jq
```

**Request body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_did` | string | Yes | User's DID |
| `algo_id` | integer | Yes | Algorithm ID |
| `preset` | string | No | Algorithm preset: `default`, `discovery`, `stable`, `fast` |

**Response structure:**

The response shows the 3-step LinkLonk algorithm:

**Step 1 - User Likes:** Shows the user's recent liked posts used as seeds.
```json
{
  "step1_user_likes": {
    "total_likes": 347,
    "likes_in_window": 50,
    "likes_used": 20,
    "sample_posts": [
      {
        "post_id": 12345,
        "uri": "at://did:plc:.../app.bsky.feed.post/...",
        "like_timestamp": 1704067200,
        "other_likers_count": 42
      }
    ]
  }
}
```

**Step 2 - Co-likers:** Shows users who liked the same posts (weighted by recency).
```json
{
  "step2_colikers": {
    "total_colikers": 412,
    "top_colikers": [
      {
        "coliker_hash": "7a8b9c0d1e2f",
        "weight": 2.847,
        "shared_posts_count": 8,
        "sample_shared_posts": [...]
      }
    ]
  }
}
```

**Step 3 - Post Scoring:** Shows how candidate posts were scored.
```json
{
  "step3_scoring": {
    "candidates_considered": 1500,
    "top_posts": [
      {
        "post_id": 67890,
        "uri": "at://did:plc:.../app.bsky.feed.post/...",
        "raw_score": 4.23,
        "popularity_penalty": 0.85,
        "final_score": 3.60,
        "total_likers": 150,
        "top_contributors": [
          {"coliker_hash": "7a8b9c", "weight": 2.1, "recency": 0.95, "contribution": 1.99}
        ]
      }
    ]
  }
}
```

**Timing breakdown:** The response includes `timing` with millisecond breakdowns for each step.

### User-Level Audit Logging

Enable detailed logging for specific users to debug their feed experience.

**Check audit status:**
```bash
curl -s https://localhost:8081/v1/audit/status | jq
```

**Add users to audit set:**
```bash
curl -s -X POST https://localhost:8081/v1/audit/users \
  -H "Content-Type: application/json" \
  -d '{"dids": ["did:plc:abc123xyz", "did:plc:user2"]}' | jq
```

**List audited users:**
```bash
curl -s https://localhost:8081/v1/audit/users | jq
```

**Remove users from audit:**
```bash
curl -s -X DELETE https://localhost:8081/v1/audit/users \
  -H "Content-Type: application/json" \
  -d '{"dids": ["did:plc:abc123xyz"]}' | jq
```

**Audit configuration (environment variables):**

| Variable | Default | Description |
|----------|---------|-------------|
| `AUDIT_ENABLED` | `false` | Master switch for audit logging |
| `AUDIT_ALL_USERS` | `false` | Audit 100% of traffic |
| `AUDIT_SAMPLE_RATE` | `0.0` | Random sampling rate (0.0-1.0) |
| `AUDIT_LOG_FULL_BREAKDOWN` | `false` | Include per-co-liker details |
| `AUDIT_MAX_CONTRIBUTORS` | `10` | Max co-likers to log per post |

Audit logs are emitted to the `graze_audit` log target as structured JSON.

### Other Useful Endpoints

```bash
# Trigger algorithm sync from ClickHouse
curl -s -X POST https://localhost:8081/v1/sync \
  -H "Content-Type: application/json" \
  -d '{"algo_id": 12345}' | jq

# List all registered feeds
curl -s https://localhost:8081/v1/feeds | jq

# Health check (Redis connectivity)
curl -s https://localhost:8081/internal/ready | jq

# Prometheus metrics
curl -s https://localhost:8081/metrics
```

---

## Troubleshooting Flowchart

### Empty Feed

```
1. Check feed health
   curl https://localhost:8081/v1/feeds/status/{algo_id}
   │
   ├─ "Sync has never been triggered" → POST /v1/sync
   ├─ "Posts key expired" → POST /v1/sync
   ├─ "Posts key exists but is empty" → Check ClickHouse
   └─ "Feed appears healthy" → Continue to step 2

2. Check user data with debug script
   python scripts/debug_feed_query.py <did> <algo_id> --verbose
   │
   ├─ No user likes → User is "cold", will see trending only
   ├─ No co-likers → First request, co-likers will be computed
   └─ Low algo hit rate → Candidates don't match algorithm posts

3. Check user-specific scoring
   curl -X POST https://localhost:8081/v1/prove -d '{"user_did":"...","algo_id":N}'
   │
   └─ Review step1/step2/step3 to identify where the pipeline breaks
```

### Wrong Posts Appearing

```
1. Get proof of ranking
   curl -X POST https://localhost:8081/v1/prove -d '{"user_did":"...","algo_id":N}'

2. Check step3_scoring.top_contributors
   - Which co-likers contributed to unwanted posts?
   - Are weights unexpectedly high for certain users?

3. Check step1_user_likes
   - What posts is the user liking that lead to these co-likers?
```

### Slow Performance

```
1. Check metrics
   curl https://localhost:8081/metrics | grep duration

2. Check timing in prove response
   - step1 slow → Redis ZRANGEBYSCORE on user likes
   - step2 slow → Co-liker aggregation (expected for uncached)
   - step3 slow → Candidate scoring (try reducing max_sources)

3. Check cache status
   python scripts/debug_feed_query.py <did> <algo_id>
   - Stage 2 shows cache TTLs
   - Low/no cache → First request for user, will be faster next time
```
