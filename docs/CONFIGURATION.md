# Configuration

All configuration is via environment variables. This document covers the main modules: interactions, candidates, and special posts, plus a no-ClickHouse setup.

## Environment variable reference

### Interactions (graze-api)

| Variable | Default | Description |
|----------|---------|-------------|
| `INTERACTIONS_LOGGING_ENABLED` | `true` | Whether to queue and process interaction events at all. |
| `INTERACTIONS_WRITER` | `clickhouse` | Backend for persisting batches: `clickhouse` or `none`. |
| `INTERACTIONS_BATCH_INTERVAL_MS` | (see config) | Flush interval for batched writes (ms). |
| `INTERACTIONS_BATCH_SIZE` | (see config) | Max events per batch before flush. |
| `INTERACTIONS_QUEUE_CAPACITY` | (see config) | Channel capacity for the interaction queue. |

When `INTERACTIONS_WRITER=clickhouse`, ClickHouse-related vars (e.g. `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE`) must be set. When `none`, they are not used.

### Candidates (graze-candidate-sync)

| Variable | Default | Description |
|----------|---------|-------------|
| `CANDIDATE_SOURCE` | `clickhouse` | Where to fetch candidate URIs: `clickhouse`, `http`, or `admin_only`. |
| `CANDIDATE_HTTP_URL` | — | Base URL for the API when `CANDIDATE_SOURCE=http` (e.g. `http://graze-api:8080`). GET `/v1/feeds/:algo_id/candidates` is appended. |

When `CANDIDATE_SOURCE=clickhouse`, ClickHouse vars are used. When `http`, `CANDIDATE_HTTP_URL` is required. When `admin_only`, the worker does not fetch candidates from anywhere (only admin HTTP or manual triggers populate Redis).

### Special posts (graze-api)

| Variable | Default | Description |
|----------|---------|-------------|
| `SPECIAL_POSTS_SOURCE` | `remote` | `remote` = fetch from external API and cache in Redis; `local` = Redis only (admin CRUD). |
| `SPECIAL_POSTS_API_BASE` | `https://api.graze.social/app/my_feeds` | Base URL for the remote special-posts API when `SPECIAL_POSTS_SOURCE=remote`. |

### ClickHouse (shared when used)

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse HTTP host. |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port. |
| `CLICKHOUSE_DATABASE` | `graze` | Database name. |
| `CLICKHOUSE_USER` | `default` | HTTP basic auth user. |
| `CLICKHOUSE_PASSWORD` | `` | HTTP basic auth password. |
| `CLICKHOUSE_SECURE` | `false` | Use HTTPS for the HTTP interface. |

### Other (graze-api / worker)

See `crates/graze-api/src/config.rs` and `crates/graze-candidate-sync/src/config.rs` for the full list (Redis, HTTP bind, metrics, algorithm tuning, etc.).

---

## No-ClickHouse setup

You can run the service **without ClickHouse** by using HTTP/local backends:

1. **Interactions**: Set `INTERACTIONS_WRITER=none`. Interaction events will still be queued and processed by the worker, but nothing will be written to ClickHouse (no-op writer).
2. **Candidates**: Set `CANDIDATE_SOURCE=http` and `CANDIDATE_HTTP_URL` to your graze-api base URL. The candidate-sync worker will GET `/v1/feeds/:algo_id/candidates` from the API. Alternatively use `CANDIDATE_SOURCE=admin_only` and populate candidates only via admin POST/DELETE to `/v1/feeds/:algo_id/candidates`.
3. **Special posts**: Set `SPECIAL_POSTS_SOURCE=local`. Populate and manage pinned/rotating/sponsored posts via the admin special-posts endpoints (see [ADMIN_API.md](ADMIN_API.md)).

Valid combination example:

- `INTERACTIONS_WRITER=none`
- `CANDIDATE_SOURCE=http` and `CANDIDATE_HTTP_URL=http://graze-api:8080`
- `SPECIAL_POSTS_SOURCE=local`

No ClickHouse connection is required for the API or the candidate-sync worker in this configuration.
