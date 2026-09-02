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
/// The incremental delta the facets union in; compacted here after each swap.
const DELTA_TABLE: &str = crate::delta_projection::DELTA_TABLE;
pub const MAP_TABLE: &str = "didint_map";
/// Per-account degree counts, rebuilt from the projection each run. These are
/// the "priors" the niche/popular lens facets join against.
pub const STATS_TABLE: &str = "account_stats";
pub const STATS_STAGING: &str = "account_stats_next";
/// The last ~90 days of edges, with their date. Small enough (a few hundred
/// million rows) that the velocity facet can filter it by day at build time.
pub const RECENT_TABLE: &str = "follow_graph_recent";
pub const RECENT_STAGING: &str = "follow_graph_recent_next";
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
        // Read BEFORE the rebuild, not after: the rebuild's `FINAL` read of
        // `follow_edges` sees some prefix of the stream, and anything arriving
        // during its ~31 minutes is NOT in the result. Compacting to a watermark
        // taken afterwards would delete delta rows the new base does not contain
        // — a silent hole in the traversal graph until the next night.
        let watermark = self.delta_watermark().await;
        let report = self.rebuild(interned).await?;
        // Best-effort: a delta that keeps its rows is merely larger than it needs
        // to be, and unioning a table this size is free (measured). Failing the
        // whole projection over housekeeping would be the worse trade.
        if let Some(seq) = watermark {
            if let Err(e) = self.compact_delta(seq).await {
                warn!(error = %e, watermark = seq, "could not compact the delta");
            }
        }
        Ok(report)
    }

    /// The highest `seq` the coming rebuild is guaranteed to include.
    async fn delta_watermark(&self) -> Option<u64> {
        let db = &self.clickhouse.database;
        match self
            .exec(&format!(
                "SELECT max(seq) FROM {db}.{DELTA_TABLE} FORMAT TabSeparated"
            ))
            .await
        {
            Ok(text) => text.trim().parse::<u64>().ok(),
            Err(e) => {
                warn!(error = %e, "could not read the delta watermark");
                None
            }
        }
    }

    /// Drop delta rows the freshly swapped base now contains.
    ///
    /// `ALTER DELETE` is a mutation and would be brutal on the 2.78B-row base;
    /// here it runs against a table holding a day of edges, which is what makes
    /// it acceptable. Rows above the watermark are deliberately kept: they
    /// arrived while the rebuild was running and exist nowhere else.
    async fn compact_delta(&self, watermark: u64) -> anyhow::Result<()> {
        let db = &self.clickhouse.database;
        self.exec(&format!(
            "ALTER TABLE {db}.{DELTA_TABLE} DELETE WHERE seq <= {watermark}"
        ))
        .await?;
        info!(watermark, "compacted the traversal delta");
        Ok(())
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

        // The map is a mirror of exactly one interner, never an accumulation
        // across several. It used to be extended incrementally, with an
        // anti-join skipping DIDs already present — which is correct only while
        // the id space never changes. When lenses moved to their own space the
        // ~2.6M rows already in the map kept their old ids, and since both
        // spaces number from 1 those stale ids collided with freshly issued
        // ones: three different accounts all held id 1. The join then fanned
        // out and mapped edges to the wrong people, which showed up as a
        // second-degree reach higher than the viewer's own follow count.
        //
        // Rebuilding is cheap because interning is get-or-create: accounts that
        // already have an id in Redis get the same one back.
        self.exec(&format!("TRUNCATE TABLE IF EXISTS {db}.{MAP_TABLE}"))
            .await?;

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

        // Collapse the map once, here, where it is 42M rows on its own rather
        // than one side of a billion-row join. Every DID is written exactly
        // once per run, so this should be a no-op — but "should" is not a
        // guarantee across a truncate and a few hundred batched inserts, and an
        // unmerged duplicate silently doubles an edge in the projection.
        self.exec(&format!("OPTIMIZE TABLE {db}.{MAP_TABLE} FINAL"))
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
            // grace_hash, because the default hash join builds both 42M-row
            // sides in memory while streaming 2.79 billion rows through them
            // and ClickHouse refused at 21.6 GiB. grace_hash buckets to disk
            // and stays bounded; the extra IO is irrelevant for a job that
            // already runs for twenty minutes. max_threads caps how many
            // in-flight blocks pile up alongside it.
            //
            // The map is collapsed by OPTIMIZE before this runs, so no FINAL
            // here. FINAL inside the join looked right and cost 21.7 GiB — it
            // turns the 42M-row side into an aggregating transform feeding a
            // 2.79B-row join. Collapsing the small table once beforehand is the
            // same guarantee for a fraction of the memory.
            //
            // The edges themselves are deliberately NOT deduplicated here. `follow_edges` is keyed on
            // (follower, rkey), and a follow → unfollow → refollow cycle issues
            // a new rkey, so an unwitnessed delete leaves two live creates for
            // one pair. Collapsing them with GROUP BY means holding 2.77
            // billion distinct pairs in a hash table, which exceeded
            // ClickHouse's 21.6 GiB limit outright.
            //
            // The duplicates only matter because they would make one follow
            // count twice in a reach total, so they are counted away where the
            // working set is small — `second_degree::reach_query` counts
            // distinct followers over one viewer's seeds, tens of thousands of
            // rows rather than billions.
            "INSERT INTO {db}.{STAGING_TABLE} (follower_int, followee_int)
             SELECT m1.id, m2.id
             FROM (
                 SELECT follower, followee FROM (
                     SELECT follower, followee, op FROM {db}.follow_edges FINAL
                 ) WHERE op = 'create' AND followee != ''
             ) AS e
             INNER JOIN {db}.{MAP_TABLE} AS m1 ON e.follower = m1.did
             INNER JOIN {db}.{MAP_TABLE} AS m2 ON e.followee = m2.did
             SETTINGS join_algorithm = 'grace_hash', max_threads = 4"
        ))
        .await?;

        // Two DIFFERENT accounts sharing an id is silent: the join fans out,
        // edges are attributed to the wrong people, and the projection still
        // looks plausibly sized. Cheap to check, and it is the failure that
        // actually happened.
        //
        // `uniqExact(did)`, not `count()`. The map is a ReplacingMergeTree, so
        // the same (did, id) row appearing twice before a merge is ordinary and
        // harmless — an earlier version of this guard counted rows and blocked
        // a perfectly good rebuild on 50,000 of them.
        let collisions: u64 = self
            .exec(&format!(
                "SELECT count() FROM (SELECT id FROM {db}.{MAP_TABLE} \
                 GROUP BY id HAVING uniqExact(did) > 1) FORMAT TabSeparated"
            ))
            .await?
            .trim()
            .parse()
            .unwrap_or(0);
        if collisions > 0 {
            anyhow::bail!(
                "{collisions} id(s) map to more than one account; refusing to swap in a \
                 projection built from a corrupt id map"
            );
        }

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

    /// Rebuild `account_stats` from the freshly swapped projection.
    ///
    /// One pass over both endpoints of every edge. The invariant that makes
    /// this verifiable end to end: `sum(followers) == sum(following) ==` the
    /// projection's row count — every edge contributes exactly one to each.
    pub async fn rebuild_stats(&self) -> anyhow::Result<u64> {
        let db = &self.clickhouse.database;

        self.exec(&format!("TRUNCATE TABLE {db}.{STATS_STAGING}"))
            .await?;
        self.exec(&format!(
            "INSERT INTO {db}.{STATS_STAGING} (account_int, followers, following)
             SELECT account_int, sum(fin), sum(fout) FROM (
                 SELECT followee_int AS account_int, toUInt64(1) AS fin, toUInt64(0) AS fout
                 FROM {db}.{LIVE_TABLE}
                 UNION ALL
                 SELECT follower_int, 0, 1 FROM {db}.{LIVE_TABLE}
             ) GROUP BY account_int
             SETTINGS max_threads = 4, max_bytes_before_external_group_by = 8000000000"
        ))
        .await?;

        let edges = self.count(LIVE_TABLE).await?;
        let followers_sum: u64 = self
            .exec(&format!(
                "SELECT sum(followers) FROM {db}.{STATS_STAGING} FORMAT TabSeparated"
            ))
            .await?
            .trim()
            .parse()
            .unwrap_or(0);
        // Every edge has exactly one followee, so the sums must reconcile. A
        // mismatch means the aggregation dropped or double-counted rows, and a
        // prior built from that silently mis-ranks every fame-weighted lens.
        if followers_sum != edges {
            anyhow::bail!(
                "account_stats does not reconcile: sum(followers)={followers_sum} vs {edges} edges"
            );
        }

        self.exec(&format!(
            "EXCHANGE TABLES {db}.{STATS_TABLE} AND {db}.{STATS_STAGING}"
        ))
        .await?;
        self.exec(&format!("TRUNCATE TABLE {db}.{STATS_STAGING}"))
            .await?;
        let rows = self.count(STATS_TABLE).await?;
        info!(rows, edges, "account_stats swapped in");
        Ok(rows)
    }

    /// Rebuild the recency slice: edges created in the last ~90 days, with
    /// their date, so the velocity facet can ask "reached via follows made this
    /// week" as a plain WHERE. Kept separate from the main projection because
    /// carrying a date there would grow the hot table 50% for a column only
    /// this one facet reads.
    pub async fn rebuild_recent(&self) -> anyhow::Result<u64> {
        let db = &self.clickhouse.database;

        self.exec(&format!("TRUNCATE TABLE {db}.{RECENT_STAGING}"))
            .await?;
        self.exec(&format!(
            "INSERT INTO {db}.{RECENT_STAGING} (follower_int, followee_int, followed_at)
             SELECT m1.id, m2.id, toDate(e.created_at)
             FROM (
                 SELECT follower, followee, created_at FROM (
                     SELECT follower, followee, op, created_at FROM {db}.follow_edges FINAL
                 ) WHERE op = 'create' AND followee != ''
                   AND created_at > now() - INTERVAL 90 DAY
                   AND created_at < now() + INTERVAL 1 DAY
             ) AS e
             INNER JOIN {db}.{MAP_TABLE} AS m1 ON e.follower = m1.did
             INNER JOIN {db}.{MAP_TABLE} AS m2 ON e.followee = m2.did
             SETTINGS join_algorithm = 'grace_hash', max_threads = 4"
        ))
        .await?;

        let after = self.count(RECENT_STAGING).await?;
        let before = self.count(RECENT_TABLE).await?;
        // A recency slice legitimately shrinks day to day, so the anti-wipe
        // floor is looser than the main projection's — but a sudden collapse to
        // near-nothing still means the source query broke, not the network.
        if before > 1_000_000 && (after as f64) < before as f64 * 0.2 {
            anyhow::bail!("refusing to swap follow_graph_recent: rebuilt {after} vs {before} live");
        }

        self.exec(&format!(
            "EXCHANGE TABLES {db}.{RECENT_TABLE} AND {db}.{RECENT_STAGING}"
        ))
        .await?;
        self.exec(&format!("TRUNCATE TABLE {db}.{RECENT_STAGING}"))
            .await?;
        info!(rows = after, "follow_graph_recent swapped in");
        Ok(after)
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
