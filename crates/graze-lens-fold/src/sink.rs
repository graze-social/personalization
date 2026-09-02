//! Batched writes into `follow_edges_buffer`.
//!
//! Follows the house ClickHouse write pattern: plain `reqwest` HTTP with
//! `INSERT ... FORMAT TabSeparated` (`graze-common/src/clickhouse/writer.rs`),
//! aimed at the Buffer table rather than the base table. Per-event inserts are
//! what drove the tiny-insert cost problem on this cluster.

use std::time::Duration;

use graze_common::ClickHouseConfig;
use tracing::warn;

use crate::event::FollowEdge;

const TABLE: &str = "follow_edges_buffer";
const COLUMNS: &str = "follower, rkey, followee, op, seq, created_at";

pub struct Sink {
    http: reqwest::Client,
    clickhouse: ClickHouseConfig,
    timeout: Duration,
    table: String,
}

impl Sink {
    pub fn new(clickhouse: ClickHouseConfig, timeout: Duration) -> anyhow::Result<Self> {
        Self::new_with_table(clickhouse, timeout, TABLE)
    }

    /// Target a different table. Production always uses the default; this exists
    /// so tests can exercise the real insert path against a scratch table.
    pub fn new_with_table(
        clickhouse: ClickHouseConfig,
        timeout: Duration,
        table: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            clickhouse,
            timeout,
            table: table.to_string(),
        })
    }

    /// Resolve `(follower, rkey)` pairs to their followee.
    ///
    /// A jetstream follow-delete names only the record, never its subject, so an
    /// unfollow cannot be turned into a retraction without asking what that
    /// record said. This is the point lookup that asks.
    ///
    /// Deliberately **without `FINAL`**, and matching `op = 'create'` explicitly.
    /// `follow_edges` is a ReplacingMergeTree keyed on `(follower, rkey)`, so
    /// `FINAL` collapses the pair to the row that arrived last — the delete,
    /// whose `followee` is empty. The create row is the only one that carries the
    /// subject, and it is the older of the two, so it is always already in the
    /// base table by the time its delete arrives.
    ///
    /// `(follower, rkey)` is the sort key and `follower` drives the partition, so
    /// a tuple `IN` prunes rather than scans.
    pub async fn resolve_followees(
        &self,
        pairs: &[(String, String)],
    ) -> anyhow::Result<Vec<(String, String, String)>> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let query = resolve_query(&self.clickhouse.database, pairs);

        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.timeout)
            .body(query)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "followee resolution failed ({}): {}",
                status,
                &text[..text.len().min(600)]
            );
        }

        let body = response.text().await?;
        Ok(body
            .lines()
            .filter_map(|line| {
                let mut cols = line.split('\t');
                match (cols.next(), cols.next(), cols.next()) {
                    (Some(a), Some(b), Some(c)) if !c.is_empty() => {
                        Some((a.to_string(), b.to_string(), c.to_string()))
                    }
                    _ => None,
                }
            })
            .collect())
    }

    /// Insert projected delta rows: `(follower_int, followee_int, seq)`.
    ///
    /// Straight to `follow_graph_int_delta`, NOT through a Buffer table. The
    /// buffer exists to coalesce the raw stream's one-row-at-a-time writes; these
    /// arrive already batched by the caller, and a Buffer would only add up to
    /// 100s of invisibility to the freshness this whole path exists to provide.
    ///
    /// `op` is written explicitly per row: additions and the resolved tombstones
    /// share this path, and an implicit enum default would make them
    /// indistinguishable.
    pub async fn insert_delta(&self, rows: &[(u32, u32, &'static str, u64)]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let body = rows
            .iter()
            .map(|(follower, followee, op, seq)| format!("{follower}\t{followee}\t{op}\t{seq}"))
            .collect::<Vec<_>>()
            .join("\n");
        let query = format!(
            "INSERT INTO {}.{} (follower_int, followee_int, op, seq) FORMAT TabSeparated",
            self.clickhouse.database,
            crate::delta_projection::DELTA_TABLE
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

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "delta insert failed ({}): {}",
                status,
                &text[..text.len().min(600)]
            );
        }
        Ok(())
    }

    /// Insert a batch. Returns `Ok(())` only when ClickHouse accepted every row.
    pub async fn insert(&self, edges: &[FollowEdge]) -> anyhow::Result<()> {
        if edges.is_empty() {
            return Ok(());
        }

        let body = edges.iter().map(render_row).collect::<Vec<_>>().join("\n");
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT TabSeparated",
            self.clickhouse.database, self.table, COLUMNS
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

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "insert failed ({}): {}",
                status,
                &text[..text.len().min(600)]
            );
        }
        Ok(())
    }
}

/// One TabSeparated row.
///
/// Every field is escaped: a DID should never contain a tab or newline, but this
/// is untrusted network input and one stray control character would shift every
/// subsequent column silently rather than failing.
fn render_row(edge: &FollowEdge) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        escape(&edge.follower),
        escape(&edge.rkey),
        escape(&edge.followee),
        edge.op,
        edge.seq,
        escape(&edge.created_at),
    )
}

/// TabSeparated escaping, matching `graze-common`'s `escape_tab_value`.
fn escape(value: &str) -> String {
    if !value
        .as_bytes()
        .iter()
        .any(|b| matches!(b, b'\t' | b'\n' | b'\r' | b'\\'))
    {
        return value.to_string();
    }
    warn!(value, "control characters in a DID or rkey; escaping");
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// The `(follower, rkey) -> followee` lookup, as text.
///
/// Pure so its shape can be pinned: the `FINAL`-free, `op = 'create'` form is
/// load-bearing and easy to "tidy" into something that silently returns nothing.
fn resolve_query(database: &str, pairs: &[(String, String)]) -> String {
    let tuples = pairs
        .iter()
        .map(|(follower, rkey)| {
            format!(
                "('{}','{}')",
                escape_literal(follower),
                escape_literal(rkey)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT follower, rkey, followee FROM {database}.follow_edges \
         WHERE (follower, rkey) IN ({tuples}) AND op = 'create' AND followee != '' \
         FORMAT TabSeparated"
    )
}

/// Escape a value for a single-quoted ClickHouse literal.
///
/// DIDs and rkeys are wire data. They should never contain a quote or a
/// backslash, and if one ever does this must not become a way to end the string
/// early.
fn escape_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

#[cfg(test)]
mod tests {
    use super::{escape_literal, resolve_query};

    /// `FINAL` here would return nothing useful and still look correct.
    ///
    /// `follow_edges` is a ReplacingMergeTree keyed on `(follower, rkey)`, so
    /// `FINAL` collapses a create/delete pair to whichever arrived last — the
    /// delete, whose `followee` is empty. The create row is the only one holding
    /// the subject, which is the entire point of this lookup.
    #[test]
    fn resolution_reads_the_create_row_without_final() {
        let q = resolve_query("default", &[("did:plc:a".to_string(), "rk1".to_string())]);
        assert!(
            !q.to_uppercase().contains("FINAL"),
            "FINAL would collapse to the delete row and lose the subject"
        );
        assert!(q.contains("op = 'create'"));
        assert!(q.contains("followee != ''"));
        // (follower, rkey) is the sort key; a tuple IN prunes rather than scans.
        assert!(
            q.contains("(follower, rkey) IN (('did:plc:a','rk1'))"),
            "got: {q}"
        );
    }

    /// DIDs and rkeys are wire data interpolated into a quoted literal.
    #[test]
    fn escaping_cannot_end_the_string_early() {
        assert_eq!(escape_literal("plain"), "plain");
        assert_eq!(escape_literal("it's"), r"it\'s");
        let q = resolve_query("default", &[("a'--".to_string(), "b".to_string())]);
        assert!(q.contains(r"('a\'--','b')"), "got: {q}");
    }

    use super::*;

    fn edge(op: &'static str, followee: &str) -> FollowEdge {
        FollowEdge {
            follower: "did:plc:a".into(),
            rkey: "r1".into(),
            followee: followee.into(),
            op,
            seq: 42,
            created_at: "2026-08-28 15:41:17.774".into(),
        }
    }

    #[test]
    fn create_row_has_six_tab_separated_columns() {
        let row = render_row(&edge("create", "did:plc:b"));
        assert_eq!(row.split('\t').count(), 6);
        assert_eq!(
            row,
            "did:plc:a\tr1\tdid:plc:b\tcreate\t42\t2026-08-28 15:41:17.774"
        );
    }

    /// A delete's empty followee must still occupy its column, or every later
    /// field shifts left and `op` lands in the wrong place.
    #[test]
    fn delete_row_keeps_the_empty_followee_column() {
        let row = render_row(&edge("delete", ""));
        let cols: Vec<&str> = row.split('\t').collect();
        assert_eq!(cols.len(), 6);
        assert_eq!(cols[2], "");
        assert_eq!(cols[3], "delete");
    }

    #[test]
    fn control_characters_are_escaped_not_passed_through() {
        let row = render_row(&edge("create", "did:plc:b\tinjected"));
        assert_eq!(row.split('\t').count(), 6, "escaping must preserve arity");
        assert!(row.contains("\\t"));
    }
}
