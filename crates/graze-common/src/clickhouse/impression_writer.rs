//! ClickHouse HTTP INSERT client for the feed_impressions table.
//!
//! Implements `ImpressionWriter` for async batch inserts of ML feature rows.
//! Enable via `ML_IMPRESSIONS_ENABLED=true`.

use std::sync::Arc;

use reqwest::Client;
use tracing::error;

use crate::clickhouse::config::ClickHouseConfig;
use crate::error::{GrazeError, Result};
use crate::models::FeedImpressionRow;

/// Writes `FeedImpressionRow` batches to `feed_impressions_buffer`.
#[async_trait::async_trait]
pub trait ImpressionWriter: Send + Sync {
    async fn persist_impressions(&self, rows: Vec<FeedImpressionRow>) -> Result<()>;
}

/// ClickHouse-backed impression writer.
pub struct ClickHouseImpressionWriter {
    http_client: Client,
    config: Arc<ClickHouseConfig>,
}

impl ClickHouseImpressionWriter {
    pub fn new(config: Arc<ClickHouseConfig>) -> Self {
        Self {
            http_client: Client::new(),
            config,
        }
    }

    async fn insert_tabseparated(
        &self,
        table_name: &str,
        columns: &str,
        data: Vec<Vec<String>>,
    ) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let query = format!(
            "INSERT INTO {}.{} ({}) FORMAT TabSeparated",
            self.config.database, table_name, columns
        );

        let data_payload = data
            .iter()
            .map(|row| row.join("\t"))
            .collect::<Vec<_>>()
            .join("\n");

        let url = self.config.base_url();
        let response = self
            .http_client
            .post(&url)
            .basic_auth(&self.config.user, Some(&self.config.password))
            .header("Content-Type", "text/plain")
            .query(&[("query", query.as_str())])
            .body(data_payload)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| GrazeError::Internal(format!("ClickHouse connection error: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!(
                table = %table_name,
                status = %status,
                body = &body[..body.len().min(500)],
                "ClickHouse impression insert failed"
            );
            return Err(GrazeError::Internal(format!(
                "ClickHouse insert failed: {}",
                status
            )));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ImpressionWriter for ClickHouseImpressionWriter {
    async fn persist_impressions(&self, rows: Vec<FeedImpressionRow>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let columns = concat!(
            "impression_id, user_hash, post_id, algo_id, served_at, ",
            "depth, source, is_holdout, is_exploration, response_time_ms, is_first_page, ",
            "raw_score, final_score, num_paths, liker_count, popularity_penalty, paths_boost, ",
            "max_contribution, score_concentration, newest_like_age_hours, oldest_like_age_hours, ",
            "was_liker_cache_hit, ",
            "coliker_count, top_coliker_weight, top5_weight_sum, mean_coliker_weight, weight_concentration, ",
            "user_like_count, user_segment, richness_ratio, hour_of_day, day_of_week"
        );

        let data: Vec<Vec<String>> = rows
            .into_iter()
            .map(|r| {
                vec![
                    escape_tab_value(&r.impression_id),
                    escape_tab_value(&r.user_hash),
                    escape_tab_value(&r.post_id),
                    r.algo_id.to_string(),
                    r.served_at.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                    r.depth.to_string(),
                    escape_tab_value(&r.source),
                    bool_to_ch(r.is_holdout),
                    bool_to_ch(r.is_exploration),
                    r.response_time_ms.to_string(),
                    bool_to_ch(r.is_first_page),
                    r.raw_score.to_string(),
                    r.final_score.to_string(),
                    r.num_paths.to_string(),
                    r.liker_count.to_string(),
                    r.popularity_penalty.to_string(),
                    r.paths_boost.to_string(),
                    r.max_contribution.to_string(),
                    r.score_concentration.to_string(),
                    r.newest_like_age_hours.to_string(),
                    r.oldest_like_age_hours.to_string(),
                    bool_to_ch(r.was_liker_cache_hit),
                    r.coliker_count.to_string(),
                    r.top_coliker_weight.to_string(),
                    r.top5_weight_sum.to_string(),
                    r.mean_coliker_weight.to_string(),
                    r.weight_concentration.to_string(),
                    r.user_like_count.to_string(),
                    escape_tab_value(&r.user_segment),
                    r.richness_ratio.to_string(),
                    r.hour_of_day.to_string(),
                    r.day_of_week.to_string(),
                ]
            })
            .collect();

        self.insert_tabseparated("feed_impressions_buffer", columns, data)
            .await
    }
}

/// No-op impression writer (drops all rows silently).
pub struct NoOpImpressionWriter;

#[async_trait::async_trait]
impl ImpressionWriter for NoOpImpressionWriter {
    async fn persist_impressions(&self, _rows: Vec<FeedImpressionRow>) -> Result<()> {
        Ok(())
    }
}

fn escape_tab_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn bool_to_ch(b: bool) -> String {
    if b {
        "1".to_string()
    } else {
        "0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    // ─── NoOpImpressionWriter ─────────────────────────────────────────────────

    #[tokio::test]
    async fn test_noop_writer_accepts_empty_vec() {
        let w = NoOpImpressionWriter;
        assert!(w.persist_impressions(Vec::new()).await.is_ok());
    }

    #[tokio::test]
    async fn test_noop_writer_accepts_populated_vec() {
        let w = NoOpImpressionWriter;
        let rows = vec![FeedImpressionRow {
            impression_id: "abc123".to_string(),
            served_at: Utc::now(),
            ..Default::default()
        }];
        assert!(w.persist_impressions(rows).await.is_ok());
    }

    // ─── Column list completeness ─────────────────────────────────────────────

    /// The column string used in the INSERT must name exactly the columns that
    /// `persist_impressions` serialises into each data row. We verify column
    /// count matches the value-vector length by running the serialisation path
    /// against a single test row without actually hitting ClickHouse.
    #[test]
    fn test_column_count_matches_value_count() {
        let columns = concat!(
            "impression_id, user_hash, post_id, algo_id, served_at, ",
            "depth, source, is_holdout, is_exploration, response_time_ms, is_first_page, ",
            "raw_score, final_score, num_paths, liker_count, popularity_penalty, paths_boost, ",
            "max_contribution, score_concentration, newest_like_age_hours, oldest_like_age_hours, ",
            "was_liker_cache_hit, ",
            "coliker_count, top_coliker_weight, top5_weight_sum, mean_coliker_weight, weight_concentration, ",
            "user_like_count, user_segment, richness_ratio, hour_of_day, day_of_week"
        );
        let column_count = columns.split(',').count();

        let row = FeedImpressionRow {
            impression_id: "id".to_string(),
            user_hash: "uh".to_string(),
            post_id: "pid".to_string(),
            algo_id: 1,
            served_at: Utc::now(),
            depth: 0,
            source: "personalized".to_string(),
            is_holdout: false,
            is_exploration: false,
            response_time_ms: 50.0,
            is_first_page: true,
            raw_score: 1.0,
            final_score: 0.9,
            num_paths: 3,
            liker_count: 100,
            popularity_penalty: 0.95,
            paths_boost: 1.1,
            max_contribution: 0.5,
            score_concentration: 0.5,
            newest_like_age_hours: 2.0,
            oldest_like_age_hours: 48.0,
            was_liker_cache_hit: true,
            coliker_count: 5,
            top_coliker_weight: 2.0,
            top5_weight_sum: 7.0,
            mean_coliker_weight: 1.4,
            weight_concentration: 1.43,
            user_like_count: 200,
            user_segment: "warm".to_string(),
            richness_ratio: 0.8,
            hour_of_day: 14,
            day_of_week: 2,
        };

        // Replicate the serialisation from persist_impressions to count values
        let values: Vec<String> = vec![
            escape_tab_value(&row.impression_id),
            escape_tab_value(&row.user_hash),
            escape_tab_value(&row.post_id),
            row.algo_id.to_string(),
            row.served_at.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            row.depth.to_string(),
            escape_tab_value(&row.source),
            bool_to_ch(row.is_holdout),
            bool_to_ch(row.is_exploration),
            row.response_time_ms.to_string(),
            bool_to_ch(row.is_first_page),
            row.raw_score.to_string(),
            row.final_score.to_string(),
            row.num_paths.to_string(),
            row.liker_count.to_string(),
            row.popularity_penalty.to_string(),
            row.paths_boost.to_string(),
            row.max_contribution.to_string(),
            row.score_concentration.to_string(),
            row.newest_like_age_hours.to_string(),
            row.oldest_like_age_hours.to_string(),
            bool_to_ch(row.was_liker_cache_hit),
            row.coliker_count.to_string(),
            row.top_coliker_weight.to_string(),
            row.top5_weight_sum.to_string(),
            row.mean_coliker_weight.to_string(),
            row.weight_concentration.to_string(),
            row.user_like_count.to_string(),
            escape_tab_value(&row.user_segment),
            row.richness_ratio.to_string(),
            row.hour_of_day.to_string(),
            row.day_of_week.to_string(),
        ];

        assert_eq!(
            column_count,
            values.len(),
            "column list has {} items but serialised row has {} values",
            column_count,
            values.len()
        );
    }

    // ─── Tab-escape helper ───────────────────────────────────────────────────

    #[test]
    fn test_escape_tab_value_replaces_tab() {
        assert_eq!(escape_tab_value("a\tb"), "a\\tb");
    }

    #[test]
    fn test_escape_tab_value_replaces_newline() {
        assert_eq!(escape_tab_value("a\nb"), "a\\nb");
    }

    #[test]
    fn test_escape_tab_value_replaces_carriage_return() {
        assert_eq!(escape_tab_value("a\rb"), "a\\rb");
    }

    #[test]
    fn test_escape_tab_value_replaces_backslash() {
        assert_eq!(escape_tab_value("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_escape_tab_value_clean_string_unchanged() {
        assert_eq!(escape_tab_value("hello_world"), "hello_world");
    }

    // ─── bool_to_ch ──────────────────────────────────────────────────────────

    #[test]
    fn test_bool_to_ch_true() {
        assert_eq!(bool_to_ch(true), "1");
    }

    #[test]
    fn test_bool_to_ch_false() {
        assert_eq!(bool_to_ch(false), "0");
    }
}
