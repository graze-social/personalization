//! `feed_requests` drain + periodic `last_delivered_time` flush.
//!
//! Port of `feed_requests_worker` / `drain_feed_requests` / `flush_last_access_times`.
//! Runs as its own tokio task (Python ran it as a daemon thread): it keeps the
//! newest `requested_at` per feed in memory and flushes to Postgres every
//! `feed_requests_flush_interval_secs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, NaiveDateTime};
use deadpool_redis::redis;
use graze_common::RedisClient;
use serde::Deserialize;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::config::Config;
use crate::metrics::FeedStatsMetrics;
use crate::pg::PgStore;

#[derive(Debug, Deserialize)]
struct FeedRequestEvent {
    feed_uri: String,
    requested_at: String,
}

pub struct FeedRequestsWorker {
    redis: Arc<RedisClient>,
    pg: Arc<PgStore>,
    metrics: Arc<FeedStatsMetrics>,
    config: Arc<Config>,
}

impl FeedRequestsWorker {
    pub fn new(
        redis: Arc<RedisClient>,
        pg: Arc<PgStore>,
        metrics: Arc<FeedStatsMetrics>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            redis,
            pg,
            metrics,
            config,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let interval = Duration::from_secs(self.config.feed_requests_flush_interval_secs);
        let mut latest: HashMap<String, NaiveDateTime> = HashMap::new();
        let mut last_flush = Instant::now();

        loop {
            if let Err(e) = self.drain(&mut latest).await {
                error!(error = %e, "feed_requests drain error");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            if last_flush.elapsed() >= interval {
                let to_flush = std::mem::take(&mut latest);
                match self.pg.flush_last_delivered(&to_flush).await {
                    Ok(n) => {
                        if n > 0 {
                            self.metrics.feed_requests_flushed.inc_by(n as u64);
                        }
                    }
                    Err(e) => {
                        self.metrics.pg_errors.inc();
                        error!(error = %e, "flush_last_delivered failed");
                    }
                }
                last_flush = Instant::now();
            }
        }
    }

    /// Drain up to 500 events, keeping the max `requested_at` per feed_uri.
    async fn drain(&self, latest: &mut HashMap<String, NaiveDateTime>) -> Result<()> {
        let key = &self.config.shadow.feed_requests_key;
        let mut count = 0;
        while count < 500 {
            let mut conn = self.redis.get().await?;
            let result: Option<(String, String)> = redis::cmd("BLPOP")
                .arg(key)
                .arg(1) // 1s block, matching Python
                .query_async(&mut conn)
                .await?;
            let raw = match result {
                Some((_, raw)) => raw,
                None => break,
            };
            match serde_json::from_str::<FeedRequestEvent>(&raw) {
                Ok(ev) => {
                    if let Some(ts) = parse_ts(&ev.requested_at) {
                        latest
                            .entry(ev.feed_uri)
                            .and_modify(|e| {
                                if ts > *e {
                                    *e = ts;
                                }
                            })
                            .or_insert(ts);
                    }
                }
                Err(e) => warn!(error = %e, "bad feed_request event"),
            }
            count += 1;
        }
        Ok(())
    }
}

/// Parse `requested_at` (ISO-8601, with or without timezone / `Z`).
fn parse_ts(s: &str) -> Option<NaiveDateTime> {
    let normalized = s.replace('Z', "+00:00");
    if let Ok(dt) = DateTime::parse_from_rfc3339(&normalized) {
        return Some(dt.naive_utc());
    }
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}
