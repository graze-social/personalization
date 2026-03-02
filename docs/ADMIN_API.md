# Admin API

Admin endpoints require authentication. The API accepts either:

- **Authorization**: `Bearer <token>`
- **X-API-Key**: `<api key>`

Configure valid tokens/keys via your deployment (e.g. `ADMIN_API_KEYS` or equivalent in `graze-api` config). Unauthorized requests receive 401.

Base path for admin routes: `/v1/...`. All request/response bodies are JSON unless noted.

---

## Feeds

### GET /v1/feeds

List registered feeds and their algorithm IDs.

**Response:** `200 OK`

```json
{
  "feeds": {
    "at://did:plc:.../app.bsky.feed.graze/for-you": 1
  }
}
```

### POST /v1/feeds

Register a feed as active (one-to-one `feed_uri` ↔ `algo_id`). Idempotent if already registered for this `algo_id`. The service persists the feed, seeds `feed:access` for rolling sync, and enqueues one candidate sync.

**Body:**

```json
{
  "feed_uri": "at://did:plc:.../app.bsky.feed.generator/my-feed",
  "algo_id": 2237
}
```

**Response:** `201 Created`

```json
{
  "ok": true,
  "registered": true
}
```

**Errors:** `400` if `feed_uri` is missing or empty; `503` in read-only mode.

### DELETE /v1/feeds/:algo_id

Deregister a feed. Removes it from active state and from `feed:access` (stops serving and stops rolling sync). Idempotent if not registered. Late-arriving interaction events for this feed are still accepted (see feed lifecycle docs).

**Response:** `204 No Content` (or `200 OK` with `{ "ok": true }` when already not registered).

**Errors:** `503` in read-only mode.

### GET /v1/feeds/status/:algo_id

Diagnostic status for a feed: algorithm posts count, sync metadata, rate limit, fallback tranches.

**Response:** `200 OK`

```json
{
  "algo_id": 1,
  "healthy": true,
  "algo_posts": { "exists": true, "count": 5000, "ttl_seconds": 86400 },
  "sync_metadata": { "exists": true, "last_sync_timestamp": "1234567890", "last_sync_age_seconds": 3600, "last_sync_post_count": 5000 },
  "rate_limit": { "locked": false, "ttl_seconds": null },
  "sync_queue": { "depth": 0 },
  "fallback_tranches": { "trending": { "exists": true, "count": 100 }, "popular": { ... }, "velocity": { ... }, "discovery": { ... } },
  "diagnosis": "Feed appears healthy."
}
```

---

## Candidates

All candidate endpoints require that the feed exists (algo_id appears in `SUPPORTED_FEEDS`). Otherwise `404 NotFound` is returned.

### GET /v1/feeds/:algo_id/candidates

List candidate post URIs for an algorithm. Used by the candidate-sync worker when `CANDIDATE_SOURCE=http` and for admin debugging.

**Query:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 10000 | Max URIs to return (capped at 50000). |

**Response:** `200 OK`

```json
{
  "uris": ["at://did:plc:.../app.bsky.feed.post/abc", "..."],
  "total": 42
}
```

### POST /v1/feeds/:algo_id/candidates

Add post URIs to the candidate set. Incremental; does not replace the set.

**Body:**

```json
{
  "uris": ["at://did:plc:.../app.bsky.feed.post/abc", "..."]
}
```

**Response:** `200 OK`

```json
{
  "added": 2
}
```

In read-only mode returns `503 ReadOnlyMode`.

### DELETE /v1/feeds/:algo_id/candidates

Remove post URIs from the candidate set.

**Body:**

```json
{
  "uris": ["at://did:plc:.../app.bsky.feed.post/abc", "..."]
}
```

**Response:** `200 OK`

```json
{
  "removed": 1
}
```

In read-only mode returns `503 ReadOnlyMode`.

---

## Special posts

Special posts (pinned, rotating/sticky, sponsored) are stored in Redis `sp:{algo_id}`. When `SPECIAL_POSTS_SOURCE=local`, these endpoints are the source of truth.

### GET /v1/feeds/:algo_id/special-posts

Return the full special-posts payload for the algorithm.

**Response:** `200 OK`

```json
{
  "algorithm_id": 1,
  "pinned": [{ "attribution": "pin1", "post": "at://..." }],
  "sticky": [{ "attribution": "rot1", "post": "at://..." }],
  "sponsored": [{ "attribution": "spon1", "post": "at://...", "cpm_cents": 50 }],
  "sponsorship_saturation_rate": 0.1
}
```

### POST /v1/feeds/:algo_id/special-posts/pinned

Add or replace a pinned post by attribution.

**Body:**

```json
{
  "attribution": "pin1",
  "post": "at://did:plc:.../app.bsky.feed.post/xyz"
}
```

**Response:** `200 OK` — `{ "ok": true }`

### DELETE /v1/feeds/:algo_id/special-posts/pinned/:attribution

Remove a pinned post by attribution. Returns `404` if not found.

### POST /v1/feeds/:algo_id/special-posts/rotating

Add or replace a rotating (sticky) post.

**Body:** Same as pinned: `{ "attribution": "...", "post": "at://..." }`

### DELETE /v1/feeds/:algo_id/special-posts/rotating/:attribution

Remove a rotating post by attribution.

### POST /v1/feeds/:algo_id/special-posts/sponsored

Add or replace a sponsored post.

**Body:**

```json
{
  "attribution": "spon1",
  "post": "at://did:plc:.../app.bsky.feed.post/xyz",
  "cpm_cents": 50
}
```

`cpm_cents` is optional (default 0).

### DELETE /v1/feeds/:algo_id/special-posts/sponsored/:attribution

Remove a sponsored post by attribution.

---

## Thompson sampling

### GET /v1/thompson/stats

Global Thompson learner statistics.

### GET /v1/feeds/:algo_id/thompson-config

Per-feed Thompson interaction config. Returns default if not set.

### PUT /v1/feeds/:algo_id/thompson-config

Set per-feed Thompson config. Body: `FeedThompsonConfig` JSON.

### GET /v1/thompson/search-space

Global Thompson search space. Returns default if not set.

### PUT /v1/thompson/search-space

Set global search space. Body: `ThompsonSearchSpace` JSON.

### GET /v1/thompson/success-criteria

Global success criteria. Returns default if not set.

### PUT /v1/thompson/success-criteria

Set global success criteria. Body: `FeedSuccessConfig` JSON.

### GET /v1/thompson/algorithm/:algo_id

Learned parameters and bandit stats for an algorithm.

---

## Audit

### POST /v1/audit/users

Add user DIDs to the audit set.

**Body:** `{ "dids": ["did:plc:...", "..."] }`

**Response:** `200 OK` — `{ "success": true, "added": N, "total_requested": N }`

### DELETE /v1/audit/users

Remove user DIDs from the audit set. Body same as POST.

**Response:** `200 OK` — `{ "success": true, "removed": N, "total_requested": N }`

### GET /v1/audit/users

List audit set (returns user hashes for privacy).

### GET /v1/audit/status

Current audit configuration (enabled, sample rate, per-user count, etc.).

### POST /v1/audit/personalize

Run personalization for a user/algorithm and return the full audit record (params, post scores, blending, timing). Body: same as `POST /v1/personalize` (`PersonalizeRequest`).

---

## Errors

Admin endpoints return JSON errors with an `error` code and optional `message`:

- `401 Unauthorized` — Missing or invalid auth.
- `404 NotFound` — Feed or resource not found (e.g. algo_id not in SUPPORTED_FEEDS, or special post attribution not found).
- `503 Service Unavailable` — Read-only mode; write operations are disabled.

Standard error shape:

```json
{
  "error": "ReadOnlyMode",
  "message": "Write operations disabled in read-only mode"
}
```
