//! Postgres billing + lookup layer.
//!
//! Ports the SQLAlchemy work in `feed_stats_runner.py`: algorithm/account
//! lookups, sticky-post credit decrement, `CreditUsage` inserts, and the
//! `last_delivered_time` flush. graze-common has no SQL driver, so this is the
//! one net-new dependency (sqlx).
//!
//! Reads always execute (they are side-effect free). Writes are gated by
//! `dry_run`: when set (shadow / mirror mode) the intended statement + params
//! are logged instead of applied, so no production row is mutated.

use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDateTime;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use tracing::info;

/// Resolved algorithm: its numeric id and owning user.
#[derive(Debug, Clone, Copy)]
pub struct AlgoRef {
    pub id: i64,
    pub user_id: i64,
}

pub struct PgStore {
    pool: PgPool,
    dry_run: bool,
}

impl PgStore {
    pub async fn connect(database_url: &str, max_connections: u32, dry_run: bool) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool, dry_run })
    }

    pub fn dry_run(&self) -> bool {
        self.dry_run
    }

    /// `select Algorithm where algorithm_uri in (:uris)` → uri → {id, user_id}.
    pub async fn algorithms_by_uri(&self, uris: &[String]) -> Result<HashMap<String, AlgoRef>> {
        let mut map = HashMap::new();
        if uris.is_empty() {
            return Ok(map);
        }
        let rows = sqlx::query(
            "SELECT algorithm_uri, id, user_id FROM algorithms WHERE algorithm_uri = ANY($1)",
        )
        .bind(uris)
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let uri: String = row.get("algorithm_uri");
            let id: i32 = row.get("id");
            let user_id: i32 = row.get("user_id");
            map.insert(
                uri,
                AlgoRef {
                    id: id as i64,
                    user_id: user_id as i64,
                },
            );
        }
        Ok(map)
    }

    /// `Campaign.id JOIN Account ON Campaign.user_id == Account.user_id` filtered
    /// to the given campaign ids → campaign_id → account_id.
    pub async fn campaign_account_map(&self, campaign_ids: &[i64]) -> Result<HashMap<i64, i64>> {
        self.id_account_map(
            "SELECT c.id, a.id AS account_id FROM campaigns c \
             JOIN accounts a ON c.user_id = a.user_id WHERE c.id = ANY($1)",
            campaign_ids,
        )
        .await
    }

    /// `Algorithm.id JOIN Account ON Algorithm.user_id == Account.user_id`
    /// filtered to the given algorithm ids → algorithm_id → account_id.
    pub async fn algorithm_account_map(&self, algorithm_ids: &[i64]) -> Result<HashMap<i64, i64>> {
        self.id_account_map(
            "SELECT al.id, a.id AS account_id FROM algorithms al \
             JOIN accounts a ON al.user_id = a.user_id WHERE al.id = ANY($1)",
            algorithm_ids,
        )
        .await
    }

    async fn id_account_map(&self, sql: &str, ids: &[i64]) -> Result<HashMap<i64, i64>> {
        let mut map = HashMap::new();
        if ids.is_empty() {
            return Ok(map);
        }
        let ids32: Vec<i32> = ids.iter().map(|&v| v as i32).collect();
        let rows = sqlx::query(sql).bind(&ids32).fetch_all(&self.pool).await?;
        for row in rows {
            let id: i32 = row.get(0);
            let account_id: i32 = row.get("account_id");
            map.insert(id as i64, account_id as i64);
        }
        Ok(map)
    }

    /// Port of `decrement_available_impressions` + `process_results`:
    /// resolve which (algo, post) pairs correspond to injected sticky posts,
    /// decrement their `credits_remaining`, deactivate exhausted ones, and record
    /// `CreditUsage` rows. Everything runs inside one transaction.
    pub async fn decrement_available_impressions(
        &self,
        post_algo_pairs: &[(String, i64)],
    ) -> Result<()> {
        if post_algo_pairs.is_empty() {
            return Ok(());
        }
        let algo_ids: Vec<i32> = post_algo_pairs.iter().map(|(_, a)| *a as i32).collect();
        let uris: Vec<String> = post_algo_pairs.iter().map(|(u, _)| u.clone()).collect();

        // input_cases CTE → (algorithm_id, uri, sticky_post_id) rows.
        let results = sqlx::query(
            "WITH input_cases AS (\
                 SELECT UNNEST($1::int[]) AS algorithm_id, UNNEST($2::text[]) AS uri\
             )\
             SELECT i.algorithm_id, i.uri, a.sticky_post_id \
             FROM input_cases i \
             JOIN algorithms_sticky_posts_association a ON i.algorithm_id = a.algorithm_id \
             JOIN sticky_posts s ON s.id = a.sticky_post_id AND s.uri = i.uri",
        )
        .bind(&algo_ids)
        .bind(&uris)
        .fetch_all(&self.pool)
        .await?;

        if results.is_empty() {
            return Ok(());
        }

        // (algorithm_id, sticky_post_id) pairs for the credit-decrement UPDATE.
        let mut upd_algo: Vec<i32> = Vec::with_capacity(results.len());
        let mut upd_sticky: Vec<i32> = Vec::with_capacity(results.len());
        // per-algo impression counts (→ CreditUsage.credit_amount)
        let mut algo_hits: HashMap<i64, i64> = HashMap::new();
        let mut result_algo_ids: Vec<i32> = Vec::new();
        for row in &results {
            let algorithm_id: i32 = row.get("algorithm_id");
            let sticky_post_id: i32 = row.get("sticky_post_id");
            upd_algo.push(algorithm_id);
            upd_sticky.push(sticky_post_id);
            *algo_hits.entry(algorithm_id as i64).or_insert(0) += 1;
            result_algo_ids.push(algorithm_id);
        }

        // Map the matched algos → their owning user for CreditUsage rows.
        let mut algo_user: HashMap<i64, i64> = HashMap::new();
        let user_rows = sqlx::query("SELECT id, user_id FROM algorithms WHERE id = ANY($1)")
            .bind(&result_algo_ids)
            .fetch_all(&self.pool)
            .await?;
        for row in user_rows {
            let id: i32 = row.get("id");
            let user_id: i32 = row.get("user_id");
            algo_user.insert(id as i64, user_id as i64);
        }

        // Build (user_id, algorithm_id, count) usage records.
        let mut usage: Vec<(i64, i64, i64)> = Vec::new();
        for (algo_id, count) in &algo_hits {
            if let Some(user_id) = algo_user.get(algo_id) {
                usage.push((*user_id, *algo_id, *count));
            }
        }

        if self.dry_run {
            info!(
                target: "feed_stats::pg_dry_run",
                matched = results.len(),
                credit_decrements = upd_algo.len(),
                credit_usage_rows = usage.len(),
                "DRY-RUN decrement_available_impressions (no rows mutated)"
            );
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        // process_results: decrement credits_remaining by 1 and toggle is_active.
        sqlx::query(
            "UPDATE algorithms_sticky_posts_association t \
             SET stopping_criteria = jsonb_set(\
                     t.stopping_criteria, '{credits_remaining}', \
                     to_jsonb((t.stopping_criteria->>'credits_remaining')::int - 1), false), \
                 is_active = ((t.stopping_criteria->>'credits_remaining')::int - 1) > 0 \
             FROM (SELECT UNNEST($1::int[]) AS algorithm_id, UNNEST($2::int[]) AS sticky_post_id) p \
             WHERE t.sticky_type = 'injected' \
               AND t.stopping_criteria ? 'credits_remaining' \
               AND t.algorithm_id = p.algorithm_id \
               AND t.sticky_post_id = p.sticky_post_id",
        )
        .bind(&upd_algo)
        .bind(&upd_sticky)
        .execute(&mut *tx)
        .await?;

        for (user_id, algorithm_id, count) in &usage {
            sqlx::query(
                "INSERT INTO credit_usages (credit_amount, user_id, algorithm_id, created_at, updated_at) \
                 VALUES ($1, $2, $3, now(), now())",
            )
            .bind(*count as i32)
            .bind(*user_id as i32)
            .bind(*algorithm_id as i32)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Port of `flush_last_access_times`: set `settings.last_delivered_time` on
    /// each algorithm to the newest observed request time.
    pub async fn flush_last_delivered(
        &self,
        latest: &HashMap<String, NaiveDateTime>,
    ) -> Result<usize> {
        if latest.is_empty() {
            return Ok(0);
        }
        if self.dry_run {
            info!(
                target: "feed_stats::pg_dry_run",
                feeds = latest.len(),
                "DRY-RUN flush_last_delivered (no rows mutated)"
            );
            return Ok(0);
        }
        let mut updated = 0usize;
        for (uri, ts) in latest {
            let iso = format_isoformat(ts);
            let res = sqlx::query(
                "UPDATE algorithms \
                 SET settings = jsonb_set(COALESCE(settings, '{}'::jsonb), \
                                          '{last_delivered_time}', to_jsonb($1::text), true), \
                     updated_at = now() \
                 WHERE algorithm_uri = $2",
            )
            .bind(&iso)
            .bind(uri)
            .execute(&self.pool)
            .await?;
            updated += res.rows_affected() as usize;
        }
        Ok(updated)
    }
}

/// Match Python `datetime.isoformat()`: omit the fractional part when it is zero.
fn format_isoformat(ts: &NaiveDateTime) -> String {
    if ts.and_utc().timestamp_subsec_nanos() == 0 {
        ts.format("%Y-%m-%dT%H:%M:%S").to_string()
    } else {
        ts.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
    }
}
