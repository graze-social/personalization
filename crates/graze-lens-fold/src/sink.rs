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

#[cfg(test)]
mod tests {
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
