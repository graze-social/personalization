//! Rebuilding the reverse follow index.
//!
//! `follow_edges` answers "who does X follow". `mutuals` needs the opposite, and
//! that cannot be derived row-by-row: a follow *delete* carries no followee, so
//! a materialized view over the forward table would receive unfollows as edges
//! pointing at `''` and never retract the real one. The reverse direction has to
//! come from the *folded* forward table, which means a periodic rebuild.
//!
//! # Why a swap rather than an insert
//!
//! ReplacingMergeTree replaces; it never deletes. Rebuilding by inserting the
//! current edge set into the live table would leave every retracted edge behind
//! forever — the table would only ever grow, and `mutuals` would slowly fill
//! with people who unfollowed. So the rebuild fills an empty staging table and
//! `EXCHANGE TABLES` swaps it in atomically: readers see the old contents right
//! up to the swap and the new contents immediately after, never a partial one.
//!
//! # Why it is chunked
//!
//! `FINAL` over the whole forward table is a large merge. The chunks align with
//! the forward table's own partition key, so each pass reads one partition and
//! peak memory is bounded by the largest partition rather than the table.

use std::time::Duration;

use graze_common::ClickHouseConfig;
use tracing::{info, warn};

/// Must match `PARTITION BY cityHash64(follower) % 32` on `follow_edges`.
/// A mismatch is not a correctness bug — every row is still visited exactly
/// once — but it stops each chunk aligning to a single partition, which is the
/// entire point of chunking.
pub const CHUNKS: u64 = 32;

pub const LIVE_TABLE: &str = "follow_edges_rev";
pub const STAGING_TABLE: &str = "follow_edges_rev_next";

pub struct RevRebuilder {
    http: reqwest::Client,
    clickhouse: ClickHouseConfig,
    timeout: Duration,
    max_execution_seconds: u64,
    /// Refuse to swap when the rebuild produced less than this fraction of the
    /// rows currently live. See `swap` for why.
    min_ratio: f64,
    force: bool,
}

impl RevRebuilder {
    pub fn new(
        clickhouse: ClickHouseConfig,
        timeout: Duration,
        max_execution_seconds: u64,
        min_ratio: f64,
        force: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            clickhouse,
            timeout,
            max_execution_seconds,
            min_ratio,
            force,
        })
    }

    /// Full rebuild: prepare staging, fill it chunk by chunk, then swap.
    pub async fn run(&self) -> anyhow::Result<RebuildReport> {
        let db = self.clickhouse.database.clone();

        self.exec(&format!(
            "CREATE TABLE IF NOT EXISTS {db}.{STAGING_TABLE} AS {db}.{LIVE_TABLE}"
        ))
        .await?;
        // Staging may hold the *previous* live table from the last swap.
        self.exec(&format!("TRUNCATE TABLE {db}.{STAGING_TABLE}"))
            .await?;

        for chunk in 0..CHUNKS {
            self.exec(&self.chunk_query(chunk)).await?;
            info!(chunk = chunk + 1, of = CHUNKS, "rebuilt chunk");
        }

        let before = self.count(LIVE_TABLE).await?;
        let after = self.count(STAGING_TABLE).await?;
        self.swap(before, after).await?;

        // The old live table is now in staging; leave it empty rather than
        // holding a second copy of the graph on disk until the next run.
        self.exec(&format!("TRUNCATE TABLE {db}.{STAGING_TABLE}"))
            .await?;

        Ok(RebuildReport { before, after })
    }

    /// One chunk of the forward table, folded and reversed.
    ///
    /// The `op` filter runs outside the `FINAL` subquery: filtering before the
    /// fold would keep the create row of an unfollowed pair and resurrect the
    /// edge — the same trap as the forward read path.
    fn chunk_query(&self, chunk: u64) -> String {
        let db = &self.clickhouse.database;
        format!(
            "INSERT INTO {db}.{STAGING_TABLE} (followee, follower, refreshed_at) \
             SELECT followee, follower, now() FROM ( \
                SELECT followee, follower, op FROM {db}.follow_edges FINAL \
                WHERE cityHash64(follower) % {CHUNKS} = {chunk} \
             ) WHERE op = 'create' AND followee != ''"
        )
    }

    /// Swap staging in, unless the result looks like a failed rebuild.
    ///
    /// The guard exists because `EXCHANGE` is instant and total: a rebuild that
    /// silently produced nothing — an empty source, a partial run, a chunk loop
    /// that errored in a way we mishandled — would otherwise replace the live
    /// index with an empty table, and `mutuals` would quietly return nobody for
    /// every viewer. Shrinkage is legitimate in principle, so the threshold is
    /// overridable; it is just never something to do by accident.
    async fn swap(&self, before: u64, after: u64) -> anyhow::Result<()> {
        let db = &self.clickhouse.database;

        if before > 0 {
            let ratio = after as f64 / before as f64;
            if ratio < self.min_ratio && !self.force {
                anyhow::bail!(
                    "refusing to swap: rebuild produced {after} rows vs {before} live \
                     ({:.1}% of current, below the {:.0}% floor). Re-run with \
                     LENS_REV_FORCE=true if this shrinkage is expected.",
                    ratio * 100.0,
                    self.min_ratio * 100.0
                );
            }
            if ratio < 1.0 {
                warn!(before, after, "reverse index shrank");
            }
        } else if after == 0 {
            anyhow::bail!("refusing to swap: both the live and rebuilt tables are empty");
        }

        self.exec(&format!(
            "EXCHANGE TABLES {db}.{LIVE_TABLE} AND {db}.{STAGING_TABLE}"
        ))
        .await?;
        info!(before, after, "reverse index swapped in");
        Ok(())
    }

    async fn count(&self, table: &str) -> anyhow::Result<u64> {
        let db = &self.clickhouse.database;
        let text = self
            .exec(&format!("SELECT count() FROM {db}.{table}"))
            .await?;
        Ok(text.trim().parse().unwrap_or(0))
    }

    async fn exec(&self, sql: &str) -> anyhow::Result<String> {
        let max_execution = self.max_execution_seconds.to_string();
        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.timeout)
            .query(&[
                ("max_execution_time", max_execution.as_str()),
                // An abandoned-but-still-running query has cost us real money
                // before; make ClickHouse drop it if we disconnect.
                ("cancel_http_readonly_queries_on_client_close", "1"),
            ])
            .body(sql.to_string())
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!(
                "clickhouse query failed ({}): {}\nSQL: {}",
                status,
                &text[..text.len().min(600)],
                &sql[..sql.len().min(300)]
            );
        }
        Ok(text)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RebuildReport {
    pub before: u64,
    pub after: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rebuilder() -> RevRebuilder {
        RevRebuilder::new(
            ClickHouseConfig {
                host: "localhost".into(),
                port: 8123,
                database: "default".into(),
                user: "u".into(),
                password: "p".into(),
                secure: false,
            },
            Duration::from_secs(60),
            600,
            0.5,
            false,
        )
        .unwrap()
    }

    /// Same ordering trap as the forward read: fold first, filter second.
    #[test]
    fn chunk_query_folds_before_filtering_op() {
        let sql = rebuilder().chunk_query(0);
        let final_at = sql.find("FINAL").expect("must use FINAL");
        let filter_at = sql.find("WHERE op = 'create'").expect("must filter op");
        assert!(
            final_at < filter_at,
            "op filter must run outside the FINAL subquery"
        );
    }

    /// Reversed on the way out: the target table is keyed (followee, follower).
    ///
    /// The live-table check compares the parsed insert target rather than
    /// substring-matching, because `follow_edges_rev_next` *contains*
    /// `follow_edges_rev` — a naive `!contains` here fails on correct SQL.
    #[test]
    fn chunk_query_writes_the_reverse_direction() {
        let sql = rebuilder().chunk_query(0);
        assert!(sql.contains("(followee, follower, refreshed_at)"));

        let target = insert_target(&sql).expect("query must be an INSERT");
        assert_eq!(target, format!("default.{STAGING_TABLE}"));
        assert_ne!(
            target,
            format!("default.{LIVE_TABLE}"),
            "the rebuild must never write directly to the live table"
        );
    }

    /// The table name between `INSERT INTO` and the column list.
    fn insert_target(sql: &str) -> Option<String> {
        let rest = sql.split("INSERT INTO ").nth(1)?;
        Some(rest.split_whitespace().next()?.to_string())
    }

    /// Every chunk must be distinct and cover the whole modulus, or the rebuild
    /// silently drops a slice of the graph.
    #[test]
    fn chunks_cover_the_full_modulus_exactly_once() {
        let r = rebuilder();
        let mut seen = std::collections::HashSet::new();
        for chunk in 0..CHUNKS {
            let sql = r.chunk_query(chunk);
            assert!(sql.contains(&format!("% {CHUNKS} = {chunk}")));
            assert!(seen.insert(chunk));
        }
        assert_eq!(seen.len(), CHUNKS as usize);
    }

    /// Excluding empty followees matters here too: delete rows carry `''`, and a
    /// reverse index full of `''` entries would be both useless and large.
    #[test]
    fn chunk_query_excludes_empty_followees() {
        assert!(rebuilder().chunk_query(0).contains("followee != ''"));
    }
}
