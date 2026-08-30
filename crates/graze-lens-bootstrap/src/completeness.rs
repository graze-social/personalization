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
use tracing::{debug, warn};

const TABLE: &str = "lens_backfill_state";

/// Where a viewer's backfill came from, for auditing a suspicious lens later.
///
/// Both paths write real markers and both count as complete; the distinction is
/// purely so an audit can tell a lazily-repaired viewer from one warmed in bulk.
/// `SOURCE_PDS` is the builder repairing a single viewer inline, on their own
/// first lens request; `SOURCE_BOOTSTRAP` is the bulk Job warming a cohort.
pub const SOURCE_PDS: &str = "pds";
pub const SOURCE_BOOTSTRAP: &str = "bootstrap";

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

    /// Record many viewers at once.
    ///
    /// The bulk Job backfills thousands of accounts in one run, and one INSERT
    /// per account is exactly the tiny-insert pattern that drives this cluster's
    /// cost. Rows go over as TabSeparated in batches, matching the Sink.
    ///
    /// Callers must only pass viewers whose edges are already durable. A marker
    /// written ahead of its edges is worse than no marker: the next build trusts
    /// it, skips the backfill, and publishes a lens from a graph that was never
    /// written.
    pub async fn mark_many(&self, entries: &[(String, usize)], source: &str) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // `backfilled_at` has no DEFAULT and is the ReplacingMergeTree *version*
        // column, so it has to be sent: an omitted value would write epoch zero,
        // which both destroys the audit trail the column exists for and loses
        // every dedup race against any other row for that viewer. Sent as a unix
        // integer, which TabSeparated accepts for DateTime, and read once per
        // batch so a batch is internally consistent.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let body = entries
            .iter()
            .map(|(viewer, edges)| {
                format!(
                    "{}\t{}\t{}\t{}",
                    escape_tsv(viewer),
                    now,
                    edges,
                    escape_tsv(source)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let query = format!(
            "INSERT INTO {}.{TABLE} (viewer, backfilled_at, edge_count, source) \
             FORMAT TabSeparated",
            self.clickhouse.database
        );

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
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "marking {} viewer(s) complete failed ({}): {}",
                entries.len(),
                status,
                &text[..text.len().min(400)]
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

/// TabSeparated escaping, matching the Sink's.
///
/// A DID should never contain a tab, but this is untrusted network input and one
/// stray control character would shift every later column silently rather than
/// failing.
fn escape_tsv(value: &str) -> String {
    if !value
        .as_bytes()
        .iter()
        .any(|b| matches!(b, b'\t' | b'\n' | b'\r' | b'\\'))
    {
        return value.to_string();
    }
    warn!(value, "control characters in a viewer DID; escaping");
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
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

    /// The batched insert must name columns in the order its rows carry them.
    /// A silent column shift here would write the timestamp into `edge_count`,
    /// which the guard reads as "complete" for every viewer forever.
    #[test]
    fn batched_insert_columns_match_the_row_order() {
        let s = store();
        let query = format!(
            "INSERT INTO {}.{TABLE} (viewer, backfilled_at, edge_count, source) \
             FORMAT TabSeparated",
            s.clickhouse.database
        );
        let row = format!(
            "{}\t{}\t{}\t{}",
            escape_tsv("did:plc:a"),
            1788000000,
            42,
            escape_tsv(SOURCE_BOOTSTRAP)
        );

        let cols: Vec<&str> = query
            .split_once('(')
            .and_then(|(_, r)| r.split_once(')'))
            .map(|(c, _)| c.split(',').map(str::trim).collect())
            .expect("column list");
        assert_eq!(
            cols,
            vec!["viewer", "backfilled_at", "edge_count", "source"]
        );
        assert_eq!(row.split('\t').count(), cols.len());
        assert_eq!(row.split('\t').nth(2), Some("42"), "edge_count is third");
    }

    /// `backfilled_at` is the ReplacingMergeTree version column and has no
    /// DEFAULT, so it must be sent. Omitting it writes epoch zero, which both
    /// destroys the audit trail and loses every dedup race for that viewer.
    #[test]
    fn backfilled_at_is_always_sent() {
        let s = store();
        let query = format!(
            "INSERT INTO {}.{TABLE} (viewer, backfilled_at, edge_count, source) FORMAT TabSeparated",
            s.clickhouse.database
        );
        assert!(query.contains("backfilled_at"));
    }

    /// The two write paths must stay distinguishable, so an audit can tell a
    /// lazily-repaired viewer from one warmed in bulk.
    #[test]
    fn sources_are_distinct() {
        assert_ne!(SOURCE_PDS, SOURCE_BOOTSTRAP);
    }

    /// A control character in a DID would shift every later column silently.
    #[test]
    fn tsv_escaping_protects_the_column_boundaries() {
        assert_eq!(escape_tsv("did:plc:ok"), "did:plc:ok");
        assert_eq!(escape_tsv("did:plc:a\tb"), "did:plc:a\\tb");
        assert_eq!(escape_tsv("did:plc:a\nb"), "did:plc:a\\nb");
        assert!(!escape_tsv("did:plc:a\tb").contains('\t'));
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
