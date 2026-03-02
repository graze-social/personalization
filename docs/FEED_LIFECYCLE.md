# Feed lifecycle

This document describes the full lifecycle of a feed in the Graze personalization service: how to add it, how candidates are supplied (pull from ClickHouse vs manual), how to manage special posts, and how to turn a feed off.

---

## 1. Adding a feed (making it “tracked”)

A feed is **tracked** when its AT-URI is registered and the service can serve `getFeedSkeleton` for it and map it to an **algorithm ID** (`algo_id`). The mapping is **one-to-one**: one `feed_uri` per `algo_id`.

### Register via HTTP (recommended)

Use the admin API so the personalization service owns all state. Feed-processor (or your app) should call:

**`POST /v1/feeds`**

- **Body:** `{ "feed_uri": "at://did:plc:.../app.bsky.feed.generator/my-feed", "algo_id": 2237 }`
- **Idempotent:** If the feed is already registered for this `algo_id`, returns success.
- The service will:
  - Persist the feed as active (`supported_feeds`, and reverse mapping for deregister).
  - Seed `feed:access` so the rolling sync worker keeps syncing this feed until deregister.
  - Enqueue one candidate sync so the candidate pool and fallback tranches are populated quickly.

**Verify:**

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/feeds
```

### Legacy: register via Redis

The API and workers use the Redis hash **`supported_feeds`** (key `SUPPORTED_FEEDS`): each field is a **feed AT-URI**, each value is the **algo_id** (integer as string). If you are not using the HTTP register endpoint, you can still add a feed manually:

```bash
redis-cli HSET supported_feeds "at://did:plc:xxx/app.bsky.feed.graze/for-you" "1"
```

- The **feed generator DID** and **describeFeedGenerator** response are built from whatever is in `supported_feeds`; clients discover feeds from that endpoint.
- When using HTTP register, the service also maintains `algo_id_to_feed_uri` and `feed_uri_to_algo_writes` (see [Turning off a feed](#4-turning-off-a-feed)).

---

## 2. Candidate pool: pull-based (ClickHouse) vs manual (admin)

The **candidate pool** for an algo is the set of post URIs the engine can rank. It is stored in Redis (`ap:{algo_id}`). How that set is filled depends on the **candidate-sync worker** configuration, not on a per-feed flag.

### Pull-based (periodic sync from ClickHouse)

Use this when candidates are defined in **ClickHouse** (e.g. table `algorithm_posts_v2` with `algo_id` and `uri`).

1. **Run the candidate-sync worker** with:
   - `CANDIDATE_SOURCE=clickhouse`
   - ClickHouse env vars set (`CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, etc.)

2. **Ensure ClickHouse has data** for that algo:
   - Rows in `algorithm_posts_v2` with the correct `algo_id` and recent `bluesky_created_at` (or whatever your pipeline writes).

3. **Trigger syncs** so the worker refills Redis:
   - **Automatic (rolling sync):** With `FEED_ACCESS_SYNC_ENABLED=true` (default), the worker periodically scans the `feed:access` Redis hash. Any algo_id that had feed traffic within the configured window gets synced. So once the feed is registered and users hit it, sync will be scheduled.
   - **On first request:** When a user calls `getFeedSkeleton` and the algo has no posts (or stale), the API **queues a sync** (LPUSH to `queue:sync`). The worker picks it up and runs a sync for that algo.
   - **Manual via Redis:**  
     `SADD pending:syncs <algo_id>` then `LPUSH queue:sync <algo_id>` (same as what the API does when it queues a sync). The worker will process the queue and run a sync for that algo.

So: **“denote that the feed is pull-based via ClickHouse”** means:

- Register the feed (step 1) so it maps to an `algo_id`.
- Run the candidate-sync worker with **`CANDIDATE_SOURCE=clickhouse`** and point it at the ClickHouse that has `algorithm_posts_v2` for that `algo_id`.
- Rely on **rolling sync** (feed traffic) and/or **queue** (first request or manual LPUSH) to run the periodic check-in and refill the candidate pool from ClickHouse.

No per-feed “mode” is stored: if the worker is configured as ClickHouse, **all** algos it syncs get their candidates from ClickHouse.

### Manual candidate management (admin API)

Use this when you want to **add/remove** candidates by hand (or from your own pipeline) instead of from ClickHouse.

1. **Run the candidate-sync worker** with either:
   - `CANDIDATE_SOURCE=admin_only` — worker never overwrites candidates; only admin HTTP (or direct Redis) changes the set, or  
   - `CANDIDATE_SOURCE=http` — worker can **replace** the set by GETting `/v1/feeds/:algo_id/candidates` from the API (so the “source of truth” is whatever that endpoint returns, e.g. after you’ve edited via admin).

2. **Update the candidate set via admin API:**
   - **Add:** `POST /v1/feeds/:algo_id/candidates` with body `{ "uris": ["at://...", ...] }` (incremental add).
   - **Remove:** `DELETE /v1/feeds/:algo_id/candidates` with body `{ "uris": ["at://...", ...] }`.
   - **List:** `GET /v1/feeds/:algo_id/candidates` (optional `?limit=`).

Important: when the worker runs with **ClickHouse** source, a sync **replaces** the candidate set for that algo with the result of the ClickHouse query. So if you mix ClickHouse sync and admin adds, the next ClickHouse sync will overwrite admin-added URIs unless you stop using ClickHouse for that algo (e.g. switch to `admin_only` or `http` and manage entirely via admin/API).

---

## 3. Updating / removing pinned, rotating, and sponsored posts

These are **special posts** (pinned at top, rotating/sticky, sponsored). They are stored in Redis under `sp:{algo_id}` and used when building the feed skeleton.

### When to use local (admin) vs remote

- **`SPECIAL_POSTS_SOURCE=local`** — No external API. You manage everything via the admin endpoints below; Redis is the source of truth.
- **`SPECIAL_POSTS_SOURCE=remote`** — The service fetches special posts from an external URL and caches them in Redis; admin CRUD is typically not used for that mode.

The following assumes **local** mode (or a deployment where you want to edit via admin).

### Endpoints (all require admin auth)

- **List:** `GET /v1/feeds/:algo_id/special-posts` — full payload (pinned, sticky, sponsored).
- **Pinned**
  - Add/update: `POST /v1/feeds/:algo_id/special-posts/pinned`  
    Body: `{ "attribution": "id", "post": "at://..." }`  
    (Same attribution replaces the previous entry.)
  - Remove: `DELETE /v1/feeds/:algo_id/special-posts/pinned/:attribution`
- **Rotating (sticky)**
  - Add/update: `POST /v1/feeds/:algo_id/special-posts/rotating`  
    Body: `{ "attribution": "id", "post": "at://..." }`
  - Remove: `DELETE /v1/feeds/:algo_id/special-posts/rotating/:attribution`
- **Sponsored**
  - Add/update: `POST /v1/feeds/:algo_id/special-posts/sponsored`  
    Body: `{ "attribution": "id", "post": "at://...", "cpm_cents": 50 }`  
    (`cpm_cents` optional.)
  - Remove: `DELETE /v1/feeds/:algo_id/special-posts/sponsored/:attribution`

So: **update** = same endpoint with same `attribution`; **remove** = DELETE with that `attribution`.

See [ADMIN_API.md](ADMIN_API.md) for full request/response details.

---

## 4. Turning off a feed

To stop serving a feed and stop syncing it:

### Deregister via HTTP (recommended)

**`DELETE /v1/feeds/:algo_id`**

- **Path:** `algo_id` is the integer algorithm ID.
- **Idempotent:** If the feed is not registered for this `algo_id`, returns 200/204 success.
- The service will:
  - Remove the feed from `supported_feeds` and from `feed:access` (so rolling sync no longer touches it).
  - **Not** remove the feed from `feed_uri_to_algo_writes`, so late-arriving interaction events (e.g. `sendInteractions`) are still accepted and written for 20–30+ minutes after the feed goes offline.

After deregister, **`getFeedSkeleton`** for that feed URI returns 404 and **describeFeedGenerator** no longer lists it.

### Legacy: stop serving via Redis

Remove the feed’s registration from Redis so the service no longer considers it supported:

```bash
redis-cli HDEL supported_feeds "at://did:plc:xxx/app.bsky.feed.graze/for-you"
# If using HTTP lifecycle, also clear reverse mapping and feed:access:
redis-cli HDEL algo_id_to_feed_uri "<algo_id>"
redis-cli HDEL feed:access "<algo_id>"
```

There is no separate “enabled/disabled” flag per feed; removal from `supported_feeds` is what makes the feed unavailable to clients.

---

## Quick reference

| Goal | Action |
|------|--------|
| Add a feed | **Preferred:** `POST /v1/feeds` with `{ "feed_uri": "at://...", "algo_id": N }`. Legacy: `HSET supported_feeds "<feed_uri>" "<algo_id>"`. |
| Use ClickHouse as candidate source | Run candidate-sync with `CANDIDATE_SOURCE=clickhouse`; ensure ClickHouse has rows for that algo_id; rely on rolling sync or queue. |
| Manually manage candidates | Use `CANDIDATE_SOURCE=admin_only` or `http`; use POST/DELETE `/v1/feeds/:algo_id/candidates`. |
| Add/update pinned/rotating/sponsored | `SPECIAL_POSTS_SOURCE=local`; POST to `/v1/feeds/:algo_id/special-posts/pinned` (or rotating/sponsored) with `attribution` and `post`. |
| Remove a special post | DELETE `/v1/feeds/:algo_id/special-posts/{pinned|rotating|sponsored}/:attribution`. |
| Turn off a feed | **Preferred:** `DELETE /v1/feeds/:algo_id`. Legacy: `HDEL supported_feeds "<feed_uri>"`, and if applicable `HDEL algo_id_to_feed_uri "<algo_id>"`, `HDEL feed:access "<algo_id>"`. |

See [CONFIGURATION.md](CONFIGURATION.md) for env vars and [ADMIN_API.md](ADMIN_API.md) for full endpoint details.
