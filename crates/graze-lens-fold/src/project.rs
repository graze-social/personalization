//! Rebuilding the traversal projection.
//!
//! Second-degree queries do not run against `follow_edges`. They run here, and
//! the reason is measured rather than aesthetic: on a 1B-row table the same
//! scored 2-hop read 311M rows at the default granularity, 39M partitioned at
//! granularity 1024, and 6–9M unpartitioned. The projection exists to be the
//! third of those.
//!
//! Three properties carry that, and all three are easy to undo by accident:
//!
//! * **u32 keys.** Which is why the DID interner has to be mirrored into
//!   ClickHouse at all — a query cannot reach into Redis to resolve them.
//! * **`index_granularity = 1024`.** A linear factor on every seed seek.
//! * **No partitioning.** Counter-intuitive, and the biggest single win:
//!   partitioning charges every seed a boundary granule in *every* partition,
//!   which was nearly all of the 39M rows read in the partitioned case.
//!
//! Callers must also pass literal seed lists rather than `IN (subquery)`; a
//! subquery defeats index analysis and full-scans. The builder always has first
//! degree in hand, so this costs it nothing.
//!
//! # Interner growth
//!
//! Roughly 39% of accounts in the graph are not yet interned (measured by
//! sampling). Interning them grows a hash *shared with rust-smasher and
//! membership-service*, on the cache Redis, which is NOEVICTION. At today's
//! scale that is ~580k entries (~35 MB) and unremarkable. At full-network scale
//! it would be ~25M entries and something like 1.8 GB on an instance where
//! depth is an outage budget — so `max_intern_per_run` bounds each pass, and
//! that threshold is the point at which a dedicated id space should be
//! considered instead.

use std::time::Duration;

use deadpool_redis::Pool;
use graze_common::ClickHouseConfig;
use tracing::{info, warn};

pub const LIVE_TABLE: &str = "follow_graph_int";
pub const STAGING_TABLE: &str = "follow_graph_int_next";
pub const MAP_TABLE: &str = "didint_map";
/// Accounts still needing an id, materialised once per run.
pub const PENDING_TABLE: &str = "didint_pending";

/// Interner keys — the **lens-owned** id space, on the lens Redis.
///
/// Not the shared `didint:{didint}:*` space that rust-smasher and
/// membership-service use. The full follow graph is ~25M accounts, and the
/// shared instance is `noeviction`: growing it there does not evict, it makes
/// other services' writes fail. See `graze-lens-builder::interner` for the
/// trade-off this accepts.
///
/// These ids must match the ones the builder stamps into blobs. Both sides
/// declare the space, and a reader refuses a blob from the wrong one — ids from
/// two interners collide silently otherwise, resolving to the wrong accounts
/// rather than to nothing.
const DIDINT_MAP: &str = "lensdid:{lensdid}:map";
const DIDINT_SEQ: &str = "lensdid:{lensdid}:seq";
const INTERN_CHUNK: usize = 1000;

const LUA_GETSET: &str = r#"
local ids = {}
for i = 1, #ARGV do
    local id = redis.call('HGET', KEYS[1], ARGV[i])
    if not id then
        id = redis.call('INCR', KEYS[2])
        redis.call('HSET', KEYS[1], ARGV[i], id)
    end
    ids[i] = id
end
return ids
"#;

pub struct Projector {
    http: reqwest::Client,
    clickhouse: ClickHouseConfig,
    redis: Pool,
    timeout: Duration,
    max_execution_seconds: u64,
    max_intern_per_run: usize,
    min_ratio: f64,
    force: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ProjectReport {
    pub interned: usize,
    pub before: u64,
    pub after: u64,
}

impl Projector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        clickhouse: ClickHouseConfig,
        redis: Pool,
        timeout: Duration,
        max_execution_seconds: u64,
        max_intern_per_run: usize,
        min_ratio: f64,
        force: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            clickhouse,
            redis,
            timeout,
            max_execution_seconds,
            max_intern_per_run,
            min_ratio,
            force,
        })
    }

    pub async fn run(&self) -> anyhow::Result<ProjectReport> {
        let interned = self.extend_interner().await?;
        let report = self.rebuild(interned).await?;
        Ok(report)
    }

    /// Give an id to every account in the graph that lacks one.
    ///
    /// Without this the projection silently drops edges: a join against the
    /// mirror simply loses rows whose DID was never interned, and a lens built
    /// on the result would be quietly short of members with nothing erroring.
    /// Give every account in the graph an id in the lens space.
    ///
    /// The expensive part is working out *which* accounts still need one. That
    /// question costs a full pass over `follow_edges` — twice, since an account
    /// can appear as either endpoint — so it is asked exactly once per run and
    /// the answer materialised.
    ///
    /// It used to be asked once per 50,000-DID batch. At 7M edges that was
    /// unremarkable; at 2.79 billion it is ~900 iterations each scanning 5.6
    /// billion rows, and the run never finishes. The batching is still here,
    /// because the Lua interner should not be handed 45M arguments at once —
    /// but it now pages through a materialised list instead of re-deriving it.
    async fn extend_interner(&self) -> anyhow::Result<usize> {
        let db = &self.clickhouse.database;

        // One pass. Ordered by did so the pager below can walk it with a
        // range scan rather than OFFSET, which would itself become quadratic.
        self.exec(&format!("DROP TABLE IF EXISTS {db}.{PENDING_TABLE}"))
            .await?;
        self.exec(&format!(
            "CREATE TABLE {db}.{PENDING_TABLE} ENGINE = MergeTree ORDER BY did AS
             SELECT did FROM (
                 SELECT follower AS did FROM {db}.follow_edges
                 UNION ALL
                 SELECT followee AS did FROM {db}.follow_edges WHERE followee != ''
             ) AS g
             LEFT ANTI JOIN {db}.{MAP_TABLE} AS m ON g.did = m.did
             GROUP BY did"
        ))
        .await?;

        let pending: u64 = self
            .exec(&format!(
                "SELECT count() FROM {db}.{PENDING_TABLE} FORMAT TabSeparated"
            ))
            .await?
            .trim()
            .parse()
            .unwrap_or(0);
        info!(pending, "accounts needing an id");

        let mut total = 0usize;
        let mut cursor = String::new();

        loop {
            let remaining = self.max_intern_per_run.saturating_sub(total);
            if remaining == 0 {
                warn!(
                    total,
                    pending,
                    "hit max_intern_per_run; projection will be incomplete until the next run"
                );
                break;
            }
            let batch = remaining.min(50_000);

            // Range scan from where the last batch ended.
            let text = self.exec(&pending_page_sql(db, &cursor, batch)).await?;
            let dids: Vec<String> = text
                .lines()
                .map(str::trim)
                .filter(|l| l.starts_with("did:"))
                .map(str::to_string)
                .collect();
            if dids.is_empty() {
                break;
            }
            cursor = dids.last().cloned().unwrap_or_default();

            let pairs = self.intern(&dids).await?;
            self.write_map(&pairs).await?;
            total += pairs.len();
            info!(interned = total, of = pending, "interner extended");
        }

        self.exec(&format!("DROP TABLE IF EXISTS {db}.{PENDING_TABLE}"))
            .await?;
        Ok(total)
    }

    async fn intern(&self, dids: &[String]) -> anyhow::Result<Vec<(String, u32)>> {
        let mut conn = self.redis.get().await?;
        let mut out = Vec::with_capacity(dids.len());
        for chunk in dids.chunks(INTERN_CHUNK) {
            let mut script = deadpool_redis::redis::cmd("EVAL");
            script
                .arg(LUA_GETSET)
                .arg(2)
                .arg(DIDINT_MAP)
                .arg(DIDINT_SEQ);
            for d in chunk {
                script.arg(d);
            }
            let ids: Vec<i64> = script.query_async(&mut conn).await?;
            for (d, id) in chunk.iter().zip(ids) {
                out.push((
                    d.clone(),
                    u32::try_from(id)
                        .map_err(|_| anyhow::anyhow!("interner id {id} out of u32 range"))?,
                ));
            }
        }
        Ok(out)
    }

    async fn write_map(&self, pairs: &[(String, u32)]) -> anyhow::Result<()> {
        let db = &self.clickhouse.database;
        let body: String = pairs
            .iter()
            .map(|(d, i)| format!("{d}\t{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let query = format!("INSERT INTO {db}.{MAP_TABLE} (did, id) FORMAT TabSeparated");
        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.timeout)
            .query(&[("query", query.as_str())])
            .body(body)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let t = response.text().await.unwrap_or_default();
            anyhow::bail!("map insert failed ({status}): {}", &t[..t.len().min(400)]);
        }
        Ok(())
    }

    /// Build into staging and swap.
    ///
    /// Same reasoning as the reverse index: the projection is derived, so it is
    /// replaced wholesale rather than mutated, and readers see the old contents
    /// until the instant they see the new ones.
    async fn rebuild(&self, interned: usize) -> anyhow::Result<ProjectReport> {
        let db = &self.clickhouse.database;

        self.exec(&format!(
            "CREATE TABLE IF NOT EXISTS {db}.{STAGING_TABLE} AS {db}.{LIVE_TABLE}"
        ))
        .await?;
        self.exec(&format!("TRUNCATE TABLE {db}.{STAGING_TABLE}"))
            .await?;

        // The `op` filter runs outside the FINAL subquery: filtering first would
        // keep the create row of an unfollowed pair and resurrect a dead edge.
        // Joins are INNER, so an account still lacking an id drops out rather
        // than projecting to 0 and colliding with a real account.
        self.exec(&format!(
            "INSERT INTO {db}.{STAGING_TABLE} (follower_int, followee_int)
             SELECT m1.id, m2.id
             FROM (
                 SELECT follower, followee FROM (
                     SELECT follower, followee, op FROM {db}.follow_edges FINAL
                 ) WHERE op = 'create' AND followee != ''
             ) AS e
             INNER JOIN {db}.{MAP_TABLE} AS m1 ON e.follower = m1.did
             INNER JOIN {db}.{MAP_TABLE} AS m2 ON e.followee = m2.did"
        ))
        .await?;

        let before = self.count(LIVE_TABLE).await?;
        let after = self.count(STAGING_TABLE).await?;

        // Same anti-wipe guard as the reverse index: EXCHANGE is instant and
        // total, so a rebuild that silently produced nothing would replace the
        // projection with an empty table and every second-degree lens would
        // quietly return nobody.
        if before > 0 {
            let ratio = after as f64 / before as f64;
            if ratio < self.min_ratio && !self.force {
                anyhow::bail!(
                    "refusing to swap: rebuilt {after} rows vs {before} live ({:.1}%, floor {:.0}%). \
                     Re-run with LENS_PROJECT_FORCE=true if this shrinkage is expected.",
                    ratio * 100.0,
                    self.min_ratio * 100.0
                );
            }
        } else if after == 0 {
            anyhow::bail!("refusing to swap: both live and rebuilt projections are empty");
        }

        self.exec(&format!(
            "EXCHANGE TABLES {db}.{LIVE_TABLE} AND {db}.{STAGING_TABLE}"
        ))
        .await?;
        self.exec(&format!("TRUNCATE TABLE {db}.{STAGING_TABLE}"))
            .await?;

        info!(before, after, interned, "traversal projection swapped in");
        Ok(ProjectReport {
            interned,
            before,
            after,
        })
    }

    async fn count(&self, table: &str) -> anyhow::Result<u64> {
        let db = &self.clickhouse.database;
        let t = self
            .exec(&format!("SELECT count() FROM {db}.{table}"))
            .await?;
        Ok(t.trim().parse().unwrap_or(0))
    }

    async fn exec(&self, sql: &str) -> anyhow::Result<String> {
        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.timeout)
            .query(&[
                ("max_execution_time", self.max_execution_seconds.to_string()),
                ("cancel_http_readonly_queries_on_client_close", "1".into()),
            ])
            .body(sql.to_string())
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!(
                "projection query failed ({status}): {}\nSQL: {}",
                &text[..text.len().min(500)],
                &sql[..sql.len().min(200)]
            );
        }
        Ok(text)
    }
}

/// Render a literal seed list for a 2-hop query.
///
/// Exists as its own function to make the rule enforceable by test: callers
/// must inline the ids. `IN (subquery)` reads the whole table.
pub fn seed_list(ids: &[u32]) -> String {
    ids.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// One page of the pending list.
///
/// Ranges on `did` rather than using OFFSET: OFFSET re-reads every row before
/// the window, so paging 45M accounts 50k at a time would re-scan the prefix
/// 900 times and be quadratic in exactly the way this rewrite removed.
fn pending_page_sql(db: &str, cursor: &str, batch: usize) -> String {
    format!(
        "SELECT did FROM {db}.{PENDING_TABLE} WHERE did > '{}' \
         ORDER BY did LIMIT {batch} FORMAT TabSeparated",
        cursor.replace('\'', "''")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_list_is_literal_integers() {
        assert_eq!(seed_list(&[3, 1, 2]), "3,1,2");
        assert_eq!(seed_list(&[]), "");
        // No quoting, no subquery: the two things that would defeat the index.
        let s = seed_list(&[7, 8]);
        assert!(!s.contains('\''));
        assert!(!s.to_uppercase().contains("SELECT"));
    }

    /// The projection must never be written in place; a partial rebuild visible
    /// to readers is a lens that silently loses members.
    #[test]
    fn live_and_staging_are_distinct() {
        assert_ne!(LIVE_TABLE, STAGING_TABLE);
        assert!(STAGING_TABLE.starts_with(LIVE_TABLE));
    }

    /// Paging must range-scan on `did`. OFFSET would re-read the prefix on
    /// every batch — the same quadratic shape that made the old per-batch
    /// anti-join unusable at 2.79 billion edges.
    #[test]
    fn pending_pager_range_scans_and_escapes() {
        let sql = pending_page_sql("default", "did:plc:abc", 50_000);
        assert!(sql.contains("WHERE did > 'did:plc:abc'"));
        assert!(sql.contains("ORDER BY did LIMIT 50000"));
        assert!(!sql.to_uppercase().contains("OFFSET"));
        assert!(sql.contains(PENDING_TABLE));

        // First page starts from the empty cursor, which must still be a valid
        // range predicate rather than a syntax error.
        assert!(pending_page_sql("default", "", 10).contains("did > ''"));

        // A quote in a DID must not terminate the literal.
        let evil = pending_page_sql("default", "did:plc:a'b", 10);
        assert!(evil.contains("did:plc:a''b"));
    }

    #[test]
    fn interner_keys_match_the_lens_space() {
        // These must equal `graze-lens-builder::interner`'s LENSDID_* keys, or
        // the projection and the blobs are interned by two different counters
        // and every id in one means a different account in the other.
        assert_eq!(DIDINT_MAP, "lensdid:{lensdid}:map");
        assert_eq!(DIDINT_SEQ, "lensdid:{lensdid}:seq");
        // And must NOT be the shared space, which belongs to rust-smasher and
        // membership-service.
        assert_ne!(DIDINT_MAP, "didint:{didint}:map");
        assert_ne!(DIDINT_SEQ, "didint:{didint}:seq");
    }
}
