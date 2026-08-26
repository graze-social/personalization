//! Redis ad-spend counters.
//!
//! Port of `update_sponsored_impressions`. Writes four hashes, consumed
//! (destructively) by `clickhouse_materializer.py` every ~10 min:
//!   * `campaign_algo:{campaign_id}`      HINCRBY      field=algorithm_id
//!   * `campaign_algo_cpm:{campaign_id}`  HINCRBYFLOAT field=algorithm_id
//!   * `attribution_count`                HINCRBY      field=attribution_id
//!   * `attribution_cpm`                  HINCRBYFLOAT field=attribution_id
//!
//! In shadow mode every key is prefixed (default `shadow:`) so the materializer
//! never sees the shadow counters.
//!
//! Parity note: Python computes the CPM contribution as `cpm_cents / 1000`. When
//! `cpm_cents` is null that expression raises and crashes the batch — a latent
//! bug. We instead treat a null CPM as a `0.0` contribution (the impression is
//! still counted). This is a deliberate, documented deviation; sponsored rows in
//! practice always carry a CPM, so the golden-diff should stay clean.

use std::collections::HashMap;

use anyhow::Result;
use deadpool_redis::redis;
use graze_common::RedisClient;

use crate::parse::AttributableRow;

pub struct RedisCounters {
    key_prefix: String,
}

impl RedisCounters {
    pub fn new(key_prefix: String) -> Self {
        Self { key_prefix }
    }

    fn key(&self, k: &str) -> String {
        format!("{}{}", self.key_prefix, k)
    }

    /// Increment the four counter hashes for a batch of attributable rows.
    pub async fn update(&self, redis: &RedisClient, rows: &[AttributableRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // (campaign_id -> (algorithm_id -> (count, cpm_sum)))
        let mut campaign_algo: HashMap<String, HashMap<i64, (i64, f64)>> = HashMap::new();
        // attribution_id -> (count, cpm_sum)
        let mut attribution: HashMap<i64, (i64, f64)> = HashMap::new();

        for r in rows {
            let cpm = r.cpm_cents.unwrap_or(0) as f64 / 1000.0;

            // campaign_id renders as "None" when absent, matching Python's f-string.
            let campaign_key = match r.campaign_id {
                Some(c) => c.to_string(),
                None => "None".to_string(),
            };
            let entry = campaign_algo
                .entry(campaign_key)
                .or_default()
                .entry(r.algorithm_id)
                .or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += cpm;

            let attr = attribution.entry(r.attribution_id).or_insert((0, 0.0));
            attr.0 += 1;
            attr.1 += cpm;
        }

        let mut conn = redis.get().await?;

        for (campaign_id, algos) in &campaign_algo {
            let count_key = self.key(&format!("campaign_algo:{campaign_id}"));
            let cpm_key = self.key(&format!("campaign_algo_cpm:{campaign_id}"));
            for (algo_id, (count, cpm_sum)) in algos {
                let _: () = redis::cmd("HINCRBY")
                    .arg(&count_key)
                    .arg(*algo_id)
                    .arg(*count)
                    .query_async(&mut conn)
                    .await?;
                let _: () = redis::cmd("HINCRBYFLOAT")
                    .arg(&cpm_key)
                    .arg(*algo_id)
                    .arg(*cpm_sum)
                    .query_async(&mut conn)
                    .await?;
            }
        }

        let attribution_count_key = self.key("attribution_count");
        let attribution_cpm_key = self.key("attribution_cpm");
        for (attribution_id, (count, cpm_sum)) in &attribution {
            let _: () = redis::cmd("HINCRBY")
                .arg(&attribution_count_key)
                .arg(*attribution_id)
                .arg(*count)
                .query_async(&mut conn)
                .await?;
            let _: () = redis::cmd("HINCRBYFLOAT")
                .arg(&attribution_cpm_key)
                .arg(*attribution_id)
                .arg(*cpm_sum)
                .query_async(&mut conn)
                .await?;
        }

        Ok(())
    }
}
