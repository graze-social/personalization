//! Main `log_tasks` consumer — port of `fetch_logs` + `process_logs` +
//! `worker()` from feed_stats_runner.py.
//!
//! Flow per batch (order preserved from Python):
//!   1. BLPOP up to `log_batch_size` events (or `log_batch_timeout_secs`).
//!   2. Resolve algorithms for the batch's feed_uris (one Postgres round-trip).
//!   3. Expand each log → post_render rows + attributable rows + credit pairs.
//!   4. Insert post_render_log, then sponsored_feed_impressions (ClickHouse).
//!   5. Bump the four Redis ad counters.
//!   6. Decrement sticky-post credits + write CreditUsage (Postgres).
//!
//! Parity note: feed_stats_runner.py does NOT consult EXCLUSION_LIST (the env var
//! is set on the deployment but the script never reads it), so we do not filter
//! on it either. On a sink error we log + count a metric and continue to the next
//! batch rather than crashing the pod — the events are already popped, so this
//! matches Python's effective delivery semantics without killing the worker.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use deadpool_redis::redis;
use graze_common::RedisClient;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::clickhouse_sink::ClickHouseSink;
use crate::config::Config;
use crate::metrics::FeedStatsMetrics;
use crate::parse::{expand_log, RawLog};
use crate::pg::PgStore;
use crate::redis_counters::RedisCounters;

pub struct LogWorker {
    redis: Arc<RedisClient>,
    ch: Arc<ClickHouseSink>,
    pg: Arc<PgStore>,
    counters: RedisCounters,
    metrics: Arc<FeedStatsMetrics>,
    config: Arc<Config>,
}

impl LogWorker {
    pub fn new(
        redis: Arc<RedisClient>,
        ch: Arc<ClickHouseSink>,
        pg: Arc<PgStore>,
        counters: RedisCounters,
        metrics: Arc<FeedStatsMetrics>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            redis,
            ch,
            pg,
            counters,
            metrics,
            config,
        }
    }

    /// Run forever: fetch a batch, process it, repeat.
    pub async fn run(&self) -> Result<()> {
        loop {
            let batch = self.fetch_logs().await?;
            if batch.is_empty() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            if let Err(e) = self.process_logs(batch).await {
                error!(error = %e, "process_logs failed for batch");
            }
            self.metrics.batches_processed.inc();
        }
    }

    /// BLPOP up to `log_batch_size` events, bounded by `log_batch_timeout_secs`.
    async fn fetch_logs(&self) -> Result<Vec<String>> {
        let key = &self.config.shadow.log_tasks_key;
        let mut batch = Vec::new();
        let start = Instant::now();
        let deadline = Duration::from_secs(self.config.log_batch_timeout_secs);

        while batch.len() < self.config.log_batch_size {
            let mut conn = self.redis.get().await?;
            let result: Option<(String, String)> = redis::cmd("BLPOP")
                .arg(key)
                .arg(5) // 5s server-side block, matching Python
                .query_async(&mut conn)
                .await?;
            match result {
                Some((_, raw)) => batch.push(raw),
                None => break, // timeout, no events waiting
            }
            if start.elapsed() >= deadline {
                break;
            }
        }
        Ok(batch)
    }

    async fn process_logs(&self, batch: Vec<String>) -> Result<()> {
        // Decode JSON; a decode failure skips that event (Python fetch_logs).
        let mut raws: Vec<RawLog> = Vec::with_capacity(batch.len());
        for raw in &batch {
            match serde_json::from_str::<RawLog>(raw) {
                Ok(r) => raws.push(r),
                Err(e) => {
                    warn!(error = %e, "skipping undecodable log_tasks entry");
                    self.metrics.log_parse_errors.inc();
                }
            }
        }
        if raws.is_empty() {
            return Ok(());
        }

        // Resolve algorithms for every distinct feed_uri in the batch.
        let uris: Vec<String> = raws
            .iter()
            .filter_map(|r| r.feed_uri.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let algorithms = match self.pg.algorithms_by_uri(&uris).await {
            Ok(m) => m,
            Err(e) => {
                self.metrics.pg_errors.inc();
                return Err(e);
            }
        };

        let now = Utc::now().naive_utc();
        let mut post_views = Vec::new();
        let mut attributable_rows = Vec::new();
        let mut post_algo_pairs: Vec<(String, i64)> = Vec::new();

        for raw in &raws {
            let mut expanded = match expand_log(raw, now) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error = %e, "skipping log on expand error");
                    self.metrics.log_parse_errors.inc();
                    continue;
                }
            };
            // Resolve this log's algorithm and stamp the id onto its rows.
            let algo = algorithms.get(&expanded.feed_uri).copied();
            let algo_id = algo.map(|a| a.id).unwrap_or(0);
            for v in expanded.post_views.iter_mut() {
                v.algorithm_id = algo_id;
            }
            for r in expanded.attributable_rows.iter_mut() {
                r.algorithm_id = algo_id;
            }
            // post_algo_pairs: one per post_id, only when the feed has an algo.
            if algo.is_some() {
                for post_id in &raw.post_ids {
                    post_algo_pairs.push((post_id.clone(), algo_id));
                }
            }

            post_views.append(&mut expanded.post_views);
            attributable_rows.append(&mut expanded.attributable_rows);
            self.metrics.logs_processed.inc();
        }

        // Account maps (union of ids across both row sets — keyed lookups).
        let mut campaign_ids: HashSet<i64> = HashSet::new();
        let mut algorithm_ids: HashSet<i64> = HashSet::new();
        for v in &post_views {
            if let Some(c) = v.campaign_id {
                campaign_ids.insert(c);
            }
            if v.algorithm_id != 0 {
                algorithm_ids.insert(v.algorithm_id);
            }
        }
        for r in &attributable_rows {
            if let Some(c) = r.campaign_id {
                campaign_ids.insert(c);
            }
            if r.algorithm_id != 0 {
                algorithm_ids.insert(r.algorithm_id);
            }
        }
        let campaign_ids: Vec<i64> = campaign_ids.into_iter().collect();
        let algorithm_ids: Vec<i64> = algorithm_ids.into_iter().collect();

        let campaign_accounts = self
            .pg
            .campaign_account_map(&campaign_ids)
            .await
            .unwrap_or_else(|e| {
                self.metrics.pg_errors.inc();
                error!(error = %e, "campaign_account_map failed");
                HashMap::new()
            });
        let algorithm_accounts = self
            .pg
            .algorithm_account_map(&algorithm_ids)
            .await
            .unwrap_or_else(|e| {
                self.metrics.pg_errors.inc();
                error!(error = %e, "algorithm_account_map failed");
                HashMap::new()
            });

        // 1) post_render_log
        match self
            .ch
            .insert_post_render_log(&post_views, &campaign_accounts, &algorithm_accounts)
            .await
        {
            Ok(n) => {
                self.metrics.post_render_log_inserts.inc_by(n as u64);
            }
            Err(e) => {
                self.metrics.clickhouse_errors.inc();
                error!(error = %e, "post_render_log insert failed");
            }
        }

        // 2) sponsored_feed_impressions
        match self
            .ch
            .insert_sponsored_feed_impressions(
                &attributable_rows,
                &campaign_accounts,
                &algorithm_accounts,
            )
            .await
        {
            Ok(n) => {
                self.metrics
                    .sponsored_feed_impressions_inserts
                    .inc_by(n as u64);
            }
            Err(e) => {
                self.metrics.clickhouse_errors.inc();
                error!(error = %e, "sponsored_feed_impressions insert failed");
            }
        }

        // 3) Redis ad counters
        if let Err(e) = self.counters.update(&self.redis, &attributable_rows).await {
            self.metrics.redis_errors.inc();
            error!(error = %e, "redis counter update failed");
        }

        // 4) Postgres: sticky-credit decrement + CreditUsage
        if let Err(e) = self
            .pg
            .decrement_available_impressions(&post_algo_pairs)
            .await
        {
            self.metrics.pg_errors.inc();
            error!(error = %e, "decrement_available_impressions failed");
        }

        Ok(())
    }
}
