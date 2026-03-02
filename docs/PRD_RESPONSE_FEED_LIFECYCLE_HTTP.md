# Response to PRD: Personalization Feed Lifecycle via HTTP Admin API

**To:** Feed-processor / Graze platform  
**From:** Personalization team  
**Re:** Formal request — feed lifecycle via HTTP admin API  
**Status:** Implemented

---

## 1. Summary

We have implemented the two requested admin endpoints and the supporting behavior described in your PRD. You can rely on **POST /v1/feeds** (register) and **DELETE /v1/feeds/:algo_id** (deregister) as the single, HTTP-based contract for feed lifecycle. We own all state in Redis (and any related background work); you need only call these endpoints and the existing special-posts endpoints.

---

## 2. Delivered endpoints

### 2.1 Register feed

**Endpoint:** `POST /v1/feeds`

**Request body (JSON):**

```json
{
  "feed_uri": "at://did:plc:.../app.bsky.feed.generator/my-feed",
  "algo_id": 2237
}
```

**Behavior (as requested):**

- **Idempotent:** If the feed is already registered for this `algo_id`, we return success (201).
- **Persist as active:** We store `feed_uri` → `algo_id` in `supported_feeds` and maintain a one-to-one mapping (one feed_uri per algo_id). Re-registering the same algo_id with a different feed_uri replaces the previous mapping.
- **Serve the feed:** We seed `feed:access` so the rolling sync worker picks this feed up and keeps syncing it until deregister. We enqueue one candidate sync (same mechanism as “on first request”) so the candidate pool and fallback tranches are populated quickly.

**Response:** `201 Created` with body `{ "ok": true, "registered": true }`.

**Errors:** `400` if `feed_uri` is missing or empty; `401` if auth missing/invalid; `503` in read-only mode. Other errors return JSON per existing admin API conventions.

---

### 2.2 Deregister feed

**Endpoint:** `DELETE /v1/feeds/:algo_id`

**Path:** `algo_id` is the integer algorithm ID (same as in register).

**Behavior (as requested):**

- **Idempotent:** If no feed is registered for this `algo_id`, we return success (200 with `{ "ok": true }` or 204).
- **Deregister:** We remove the feed from `supported_feeds` and from `feed:access`, so the feed is no longer served and the rolling sync worker no longer syncs it.
- **Stop background work:** By removing the algo_id from `feed:access`, we stop scheduling syncs for this feed. We do not delete algo-scoped Redis keys (e.g. `ap:{algo_id}`, tranches); the feed simply stops being served and no further syncs are scheduled.
- **Interaction writes after deregister:** We continue to accept and persist interaction events (e.g. from `sendInteractions`) for this feed for an indefinite period. We maintain a durable `feed_uri` → `algo_id` mapping used only for resolving interaction writes, so late-arriving events (e.g. over 20–30+ minutes) are still written and not rejected.

**Response:** `204 No Content` when a feed was removed; `200 OK` with `{ "ok": true }` when the feed was already not registered.

**Errors:** `401` if auth missing/invalid; `503` in read-only mode.

---

## 3. Design decisions (for your awareness)

- **One-to-one:** We treat the relationship as one `feed_uri` per `algo_id`. If you register the same algo_id again with a different feed_uri, we replace the previous feed_uri. If you register the same (feed_uri, algo_id) again, we return success (idempotent).
- **Durable interaction mapping:** To support “write any interaction events that leak through” after deregister, we keep a separate Redis mapping (`feed_uri_to_algo_writes`) that we set on register and never remove on deregister. When resolving feed_uri → algo_id for interaction writes, we check active feeds first, then this mapping. So late `sendInteractions` for a deregistered feed still resolve and are persisted.
- **No bulk key cleanup on deregister:** We do not delete algo-scoped keys (candidates, tranches, special posts cache, etc.) on deregister. The feed stops being served and stops being synced; existing keys may expire per existing TTLs or remain until overwritten. If you later need explicit cleanup, we can add it.

---

## 4. Unchanged behavior

As requested, we have not changed:

- **Special posts:** `GET /v1/feeds/:algo_id/special-posts`, `POST`/`DELETE` for pinned, rotating, and sponsored work as before. Your lifecycle (register → push full special posts → add/remove sticky or pinned → deregister) is supported.
- **Auth:** Same as existing admin API: `Authorization: Bearer <key>` or `X-API-Key: <key>`.
- **Base URL:** Your `PERSONALIZATION_ADMIN_API_BASE_URL` (e.g. `https://beta.graze.social`) and `PERSONALIZATION_ADMIN_API_KEY` are unchanged.

---

## 5. Documentation and verification

- **Admin API:** [docs/ADMIN_API.md](ADMIN_API.md) documents `POST /v1/feeds` and `DELETE /v1/feeds/:algo_id` with request/response and errors.
- **Feed lifecycle:** [docs/FEED_LIFECYCLE.md](FEED_LIFECYCLE.md) describes adding a feed (including HTTP register), turning off a feed (including HTTP deregister), and the quick-reference table. Legacy Redis-based steps remain documented for operators who have not yet switched to HTTP.

You can verify registration with `GET /v1/feeds` (lists all active feeds).

---

## 6. Next steps

Once these endpoints are deployed in your personalization service environment, you can remove all use of `PERSONALIZATION_REDIS_URL` for feed lifecycle and rely solely on:

- `POST /v1/feeds` when a feed becomes personalized  
- `DELETE /v1/feeds/:algo_id` when a feed is unpublished or switched away from personalized  
- Existing special-posts endpoints as today  

We will maintain ownership of Redis and all state; your integration is complete from your side once you point at the deployed admin API.

If you need any changes to behavior or additional guarantees (e.g. explicit cleanup of algo keys on deregister), we can iterate.

— Personalization team
