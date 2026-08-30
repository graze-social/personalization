//! Has this viewer's follow history actually been backfilled?
//!
//! `graze-lens-fold` only sees follows made *since it connected*. So for anyone
//! who was never backfilled, `follow_edges` holds a handful of incidental edges
//! rather than their graph — and a lens built from that is not a narrower feed,
//! it is the wrong feed. Nothing errors. The reader simply sees posts filtered
//! against a graph that isn't theirs, and we cannot tell that apart from
//! someone who genuinely follows very few people.
//!
//! This is the check that makes the difference legible: a viewer is only
//! eligible for a lens once we have deliberately pulled their follow records
//! from their own PDS and recorded that we did.
//!
//! # Scope
//!
//! Records that ONE account's own follows are complete. It says nothing about
//! the wider graph — second-degree facets need everyone else's edges too, and
//! only the archive replay can supply those. Do not reuse this marker to gate
//! anything beyond first degree.

use std::time::Duration;

use graze_common::ClickHouseConfig;
use tracing::debug;

const TABLE: &str = "lens_backfill_state";

/// Where a viewer's backfill came from, for auditing a suspicious lens later.
pub const SOURCE_PDS: &str = "pds";

pub struct CompletenessStore {
    http: reqwest::Client,
    clickhouse: ClickHouseConfig,
    timeout: Duration,
    max_execution_seconds: u64,
}

impl CompletenessStore {
    pub fn new(
        clickhouse: ClickHouseConfig,
        timeout: Duration,
        max_execution_seconds: u64,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            clickhouse,
            timeout,
            max_execution_seconds,
        })
    }

    /// True when this viewer's own follows have been backfilled.
    ///
    /// Errs on the side of "not complete": a ClickHouse blip makes us backfill
    /// again, which is wasteful but idempotent, whereas assuming completeness
    /// would publish a wrong lens.
    pub async fn is_complete(&self, viewer: &str) -> bool {
        match self.lookup(viewer).await {
            Ok(found) => found,
            Err(e) => {
                debug!(error = %e, viewer, "completeness lookup failed; treating as incomplete");
                false
            }
        }
    }

    async fn lookup(&self, viewer: &str) -> anyhow::Result<bool> {
        let db = &self.clickhouse.database;
        let sql = format!(
            "SELECT count() FROM {db}.{TABLE} WHERE viewer = {{viewer:String}} \
             AND edge_count > 0"
        );
        let text = self.exec(&sql, Some(viewer)).await?;
        Ok(text.trim().parse::<u64>().unwrap_or(0) > 0)
    }

    /// Record that we backfilled this viewer.
    pub async fn mark_complete(&self, viewer: &str, edge_count: usize) -> anyhow::Result<()> {
        let db = &self.clickhouse.database;
        // Written to the base table rather than a buffer: the very next build
        // reads this back, and a buffered row would still be invisible.
        let sql = format!(
            "INSERT INTO {db}.{TABLE} (viewer, backfilled_at, edge_count, source) \
             VALUES ({{viewer:String}}, now(), {{edges:UInt32}}, '{SOURCE_PDS}')"
        );
        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.timeout)
            .query(&[
                ("max_execution_time", self.max_execution_seconds.to_string()),
                ("param_viewer", viewer.to_string()),
                ("param_edges", edge_count.to_string()),
            ])
            .body(sql)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "marking completeness failed ({}): {}",
                status,
                &body[..body.len().min(400)]
            );
        }
        Ok(())
    }

    async fn exec(&self, sql: &str, viewer: Option<&str>) -> anyhow::Result<String> {
        let mut query: Vec<(&str, String)> = vec![
            ("max_execution_time", self.max_execution_seconds.to_string()),
            ("cancel_http_readonly_queries_on_client_close", "1".into()),
        ];
        if let Some(v) = viewer {
            query.push(("param_viewer", v.to_string()));
        }

        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.timeout)
            .query(&query)
            .body(sql.to_string())
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!(
                "completeness query failed ({}): {}",
                status,
                &text[..text.len().min(400)]
            );
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> CompletenessStore {
        CompletenessStore::new(
            ClickHouseConfig {
                host: "localhost".into(),
                port: 8123,
                database: "default".into(),
                user: "u".into(),
                password: "p".into(),
                secure: false,
            },
            Duration::from_secs(30),
            20,
        )
        .unwrap()
    }

    /// A viewer DID is caller-supplied and must never be interpolated.
    #[test]
    fn queries_bind_the_viewer() {
        let s = store();
        let sql = format!(
            "SELECT count() FROM {}.{} WHERE viewer = {{viewer:String}} AND edge_count > 0",
            s.clickhouse.database, TABLE
        );
        assert!(sql.contains("{viewer:String}"));
        assert!(!sql.contains("did:plc:"));
    }

    /// A row recorded with zero edges is not evidence of a usable backfill —
    /// it is evidence the account had nothing we could read. Treating it as
    /// complete would permanently pin that viewer to an empty lens.
    #[test]
    fn zero_edge_rows_do_not_count_as_complete() {
        let s = store();
        let sql = format!(
            "SELECT count() FROM {}.{} WHERE viewer = {{viewer:String}} AND edge_count > 0",
            s.clickhouse.database, TABLE
        );
        assert!(sql.contains("edge_count > 0"));
    }
}
