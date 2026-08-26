//! ClickHouse analytics sink.
//!
//! Ports `insert_clickhouse_data_http` + the two active writers from
//! `feed_stats_runner.py`. Preserves the wire contract exactly:
//!   * HTTP `INSERT ... FORMAT TabSeparated`
//!   * `None` values encoded as the empty string (NOT `\N`), matching Python
//!   * `X-ClickHouse-User` / basic-auth against the same host
//!
//! The generic [`insert_rows`] helper is the public equivalent of the private
//! `ClickHouseInteractionWriter::insert_tabseparated` in graze-common; we keep a
//! local copy so the column list / table name stay arbitrary.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use chrono::NaiveDateTime;
use graze_common::ClickHouseConfig;
use reqwest::Client;

use crate::parse::{AttributableRow, PostView};

/// A single TSV cell. `Null` renders as the empty string (Python `None -> ""`).
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Str(String),
    Int(i64),
    DateTime(NaiveDateTime),
}

impl Cell {
    fn render(&self) -> String {
        match self {
            Cell::Null => String::new(),
            Cell::Str(s) => escape_tab_value(s),
            Cell::Int(i) => i.to_string(),
            Cell::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        }
    }
}

fn opt_int(v: Option<i64>) -> Cell {
    v.map(Cell::Int).unwrap_or(Cell::Null)
}
fn opt_str(v: Option<String>) -> Cell {
    v.map(Cell::Str).unwrap_or(Cell::Null)
}

pub struct ClickHouseSink {
    http: Client,
    config: ClickHouseConfig,
    table_prefix: String,
}

impl ClickHouseSink {
    pub fn new(config: ClickHouseConfig, table_prefix: String) -> Self {
        Self {
            http: Client::new(),
            config,
            table_prefix,
        }
    }

    fn table(&self, name: &str) -> String {
        format!("{}{}", self.table_prefix, name)
    }

    /// Generic TSV insert into `{database}.{table}` with an explicit column list.
    pub async fn insert_rows(
        &self,
        table: &str,
        columns: &[&str],
        rows: Vec<Vec<Cell>>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT TabSeparated",
            self.config.database,
            self.table(table),
            columns.join(", ")
        );
        let body = rows
            .iter()
            .map(|row| row.iter().map(Cell::render).collect::<Vec<_>>().join("\t"))
            .collect::<Vec<_>>()
            .join("\n");

        let resp = self
            .http
            .post(self.config.base_url())
            .basic_auth(&self.config.user, Some(&self.config.password))
            .header("Content-Type", "text/plain")
            .query(&[("query", query.as_str())])
            .body(body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse connection error: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "ClickHouse insert into {} failed: {} {}",
                self.table(table),
                status,
                &text[..text.len().min(500)]
            ));
        }
        Ok(())
    }

    /// Port of `insert_into_post_render_log` — exact 16-column order.
    pub async fn insert_post_render_log(
        &self,
        views: &[PostView],
        campaign_accounts: &HashMap<i64, i64>,
        algorithm_accounts: &HashMap<i64, i64>,
    ) -> Result<usize> {
        if views.is_empty() {
            return Ok(0);
        }
        const COLS: &[&str] = &[
            "attribution_id",
            "campaign_id",
            "paying_account_id",
            "post_id",
            "position",
            "uuid",
            "algorithm_id",
            "paid_account_id",
            "user_did",
            "cpm_cents",
            "created_at",
            "feed_operator_did",
            "slug",
            "cursor",
            "limit",
            "post_count",
        ];
        let rows: Vec<Vec<Cell>> = views
            .iter()
            .map(|v| {
                let paying = v
                    .campaign_id
                    .and_then(|c| campaign_accounts.get(&c).copied());
                // Python only looks up paid_account when algorithm_id is truthy.
                let paid = if v.algorithm_id != 0 {
                    algorithm_accounts.get(&v.algorithm_id).copied()
                } else {
                    None
                };
                vec![
                    opt_int(v.attribution_id),
                    opt_int(v.campaign_id),
                    opt_int(paying),
                    Cell::Str(v.post_id.clone()),
                    Cell::Int(v.position as i64),
                    Cell::Str(v.uuid.clone()),
                    Cell::Int(v.algorithm_id),
                    opt_int(paid),
                    Cell::Str(v.user_did.clone()),
                    opt_int(v.cpm_cents),
                    Cell::DateTime(v.created_at),
                    Cell::Str(v.feed_operator_did.clone()),
                    Cell::Str(v.slug.clone()),
                    opt_str(v.cursor.clone()),
                    Cell::Int(v.limit),
                    Cell::Int(v.post_count as i64),
                ]
            })
            .collect();
        let n = rows.len();
        self.insert_rows("post_render_log", COLS, rows).await?;
        Ok(n)
    }

    /// Port of `insert_into_clickhouse_sponsored_feed_impressions` — 10 columns.
    pub async fn insert_sponsored_feed_impressions(
        &self,
        rows_in: &[AttributableRow],
        campaign_accounts: &HashMap<i64, i64>,
        algorithm_accounts: &HashMap<i64, i64>,
    ) -> Result<usize> {
        if rows_in.is_empty() {
            return Ok(0);
        }
        const COLS: &[&str] = &[
            "post_uri",
            "feed_view_uuid",
            "attribution_id",
            "campaign_id",
            "algorithm_id",
            "user_did",
            "created_at",
            "cpm_cents",
            "paying_account_id",
            "paid_account_id",
        ];
        let rows: Vec<Vec<Cell>> = rows_in
            .iter()
            .map(|r| {
                let paying = r
                    .campaign_id
                    .and_then(|c| campaign_accounts.get(&c).copied());
                let paid = algorithm_accounts.get(&r.algorithm_id).copied();
                vec![
                    Cell::Str(r.post_id.clone()),
                    Cell::Str(r.uuid.clone()),
                    Cell::Int(r.attribution_id),
                    opt_int(r.campaign_id),
                    Cell::Int(r.algorithm_id),
                    opt_str(r.user_did.clone()),
                    Cell::DateTime(r.created_at),
                    opt_int(r.cpm_cents),
                    opt_int(paying),
                    opt_int(paid),
                ]
            })
            .collect();
        let n = rows.len();
        self.insert_rows("sponsored_feed_impressions", COLS, rows)
            .await?;
        Ok(n)
    }
}

/// Backslash-escape TSV control characters (from graze-common's writer).
fn escape_tab_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
