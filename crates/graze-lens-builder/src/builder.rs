//! Turning a viewer DID into a published lens set.
//!
//! One build is: fold `follow_edges` to the viewer's current followees in
//! ClickHouse, then publish the result to Redis as the set feeder-rs reads.
//!
//! The publish is deliberately ordered so a reader never sees a half-built set:
//! the new members go into a temporary key, and only then does a single
//! transaction RENAME it over the live key and stamp `lensmeta` as ready. A
//! reader arriving mid-build sees either the previous set or `building`, never
//! a partial one.

use std::time::Duration;

use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;
use graze_common::ClickHouseConfig;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::config::Config;

/// Viewers whose sets graze-lens-fold should keep fresh. Shared key; the fold
/// side owns the constant in `graze_lens_fold::delta`.
const ACTIVE_KEY: &str = "lens:active";

/// Current followees of one viewer.
///
/// Two things here are load-bearing and easy to "simplify" into a bug:
///
/// `FINAL` collapses the create/delete rows ReplacingMergeTree keeps per
/// (follower, rkey), and the `op` filter must run *after* that fold -- hence the
/// subquery. Filtering first would keep the create row of an unfollowed pair and
/// resurrect a dead edge.
///
/// The fold is keyed on rkey because a Jetstream follow delete does not name the
/// followee (verified on live traffic; see the DDL script). A deleted row
/// therefore carries an empty followee and is dropped by the `op` filter -- but
/// only because the surviving row for that rkey *is* the delete.
const FOLLOWS_QUERY: &str = r#"
SELECT followee
FROM (
    SELECT followee, op
    FROM {database:Identifier}.follow_edges FINAL
    WHERE follower = {follower:String}
)
WHERE op = 'create' AND followee != ''
LIMIT {limit:UInt64}
"#;

#[derive(Deserialize)]
struct ChResponse {
    data: Vec<Row>,
}

#[derive(Deserialize)]
struct Row {
    followee: String,
}

/// Why a build did not publish a usable set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildOutcome {
    /// Set published and marked ready.
    Published,
    /// The viewer follows nobody. Marked ready-and-empty so the serve path
    /// stops re-enqueueing them; there is nothing to build.
    Empty,
    /// Over `max_set_size`. Marked failed so it is not retried in a loop.
    TooLarge,
}

pub struct Builder {
    redis: Pool,
    http: reqwest::Client,
    clickhouse: ClickHouseConfig,
    config: Config,
}

impl Builder {
    pub fn new(redis: Pool, config: Config) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            redis,
            http,
            clickhouse: config.clickhouse.clone(),
            config,
        })
    }

    pub fn set_key(facet: &str, viewer: &str) -> String {
        format!("lens:v1:{facet}:{viewer}")
    }

    pub fn meta_key(viewer: &str) -> String {
        format!("lensmeta:{viewer}")
    }

    fn staging_key(facet: &str, viewer: &str) -> String {
        format!("lens:v1:{facet}:{viewer}:building")
    }

    /// Build and publish one lens.
    pub async fn build(&self, viewer: &str, facet: &str) -> anyhow::Result<BuildOutcome> {
        self.mark_state(viewer, "building").await?;

        let followees = self.fetch_follows(viewer).await?;
        debug!(viewer, facet, count = followees.len(), "fetched follows");

        if followees.is_empty() {
            // Ready-and-empty, not failed: this is a correct answer about a
            // viewer, and marking it ready is what stops the serve path from
            // enqueueing them on every request.
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }

        if followees.len() > self.config.max_set_size {
            warn!(
                viewer,
                facet,
                count = followees.len(),
                max = self.config.max_set_size,
                "lens set over size budget; refusing to publish"
            );
            self.mark_state(viewer, "failed").await?;
            return Ok(BuildOutcome::TooLarge);
        }

        self.publish(viewer, facet, &followees).await?;
        Ok(BuildOutcome::Published)
    }

    async fn fetch_follows(&self, viewer: &str) -> anyhow::Result<Vec<String>> {
        let max_execution = self.config.max_execution_seconds.to_string();
        let limit = (self.config.max_set_size + 1).to_string();

        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.config.query_timeout)
            .query(&[
                ("default_format", "JSON"),
                ("max_execution_time", max_execution.as_str()),
                // Abandoned-but-still-running queries have cost us real money
                // before; make ClickHouse drop this one if we disconnect.
                ("cancel_http_readonly_queries_on_client_close", "1"),
                ("param_database", self.clickhouse.database.as_str()),
                // Bound, never interpolated: a DID is caller-supplied data.
                ("param_follower", viewer),
                ("param_limit", limit.as_str()),
            ])
            .body(FOLLOWS_QUERY)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "follow query failed ({}): {}",
                status,
                &body[..body.len().min(600)]
            );
        }

        let parsed: ChResponse = response.json().await?;
        Ok(parsed.data.into_iter().map(|r| r.followee).collect())
    }

    /// Stage, then swap. See the module comment for why the order matters.
    ///
    /// Public so the bootstrap job can publish a set it already has in hand
    /// without going back through ClickHouse for it.
    pub async fn publish(
        &self,
        viewer: &str,
        facet: &str,
        members: &[String],
    ) -> anyhow::Result<()> {
        let staging = Self::staging_key(facet, viewer);
        let live = Self::set_key(facet, viewer);
        let ttl = self.config.set_ttl.as_secs();

        let mut conn = self.redis.get().await?;

        // Staging is rebuilt from scratch each time; a leftover from a crashed
        // build would otherwise union into this one.
        let _: () = conn.del(&staging).await?;
        for chunk in members.chunks(1_000) {
            let _: () = conn.sadd(&staging, chunk).await?;
        }

        deadpool_redis::redis::pipe()
            .atomic()
            .rename(&staging, &live)
            .expire(&live, ttl as i64)
            .hset(Self::meta_key(viewer), "state", "ready")
            .hset(Self::meta_key(viewer), "facet", facet)
            .hset(Self::meta_key(viewer), "count", members.len())
            .expire(Self::meta_key(viewer), ttl as i64)
            // Register the viewer for live maintenance. graze-lens-fold reads
            // this to decide whose sets to keep current from the follow stream,
            // which is what lets the TTL above be long rather than minutes.
            // It prunes its own entries when a set turns out to be gone.
            .sadd(ACTIVE_KEY, viewer)
            .query_async::<()>(&mut conn)
            .await?;

        Ok(())
    }

    async fn mark_state(&self, viewer: &str, state: &str) -> anyhow::Result<()> {
        let mut conn = self.redis.get().await?;
        let ttl = self.config.set_ttl.as_secs() as i64;
        deadpool_redis::redis::pipe()
            .atomic()
            .hset(Self::meta_key(viewer), "state", state)
            .expire(Self::meta_key(viewer), ttl)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// feeder-rs reads these exact key shapes (`feeder-rs/src/lens.rs`). They
    /// are duplicated across two repos by necessity; this pins our half.
    #[test]
    fn key_shapes_match_the_reader() {
        assert_eq!(
            Builder::set_key("follows", "did:plc:abc"),
            "lens:v1:follows:did:plc:abc"
        );
        assert_eq!(Builder::meta_key("did:plc:abc"), "lensmeta:did:plc:abc");
    }

    /// The fold declares this same key in `graze_lens_fold::delta::ACTIVE_KEY`.
    /// If they drift, the builder registers viewers nobody watches and every
    /// lens silently goes back to rotting until its TTL.
    #[test]
    fn active_key_matches_the_fold_side() {
        assert_eq!(ACTIVE_KEY, graze_lens_fold::delta::ACTIVE_KEY);
    }

    /// Staging must not collide with the live key, or the rename would be a
    /// no-op onto itself and readers would see the set vanish.
    #[test]
    fn staging_key_is_distinct_from_live() {
        assert_ne!(
            Builder::staging_key("follows", "did:plc:abc"),
            Builder::set_key("follows", "did:plc:abc")
        );
    }

    /// The fold has to happen before the op filter. Filtering first would keep
    /// the create row of an unfollowed pair and resurrect dead edges.
    #[test]
    fn query_folds_before_filtering_op() {
        let final_at = FOLLOWS_QUERY.find("FINAL").expect("query must use FINAL");
        let filter_at = FOLLOWS_QUERY
            .find("WHERE op = 'create'")
            .expect("query must filter op");
        assert!(final_at < filter_at);
    }

    /// DIDs are caller-supplied; they must reach ClickHouse as bound params.
    #[test]
    fn query_binds_rather_than_interpolates() {
        assert!(FOLLOWS_QUERY.contains("{follower:String}"));
        assert!(!FOLLOWS_QUERY.contains("format!"));
    }

    /// A follow delete on the wire has no followee, so the delete row's
    /// `followee` is empty. Without this guard an empty-string "DID" would be
    /// published into the lens set, where it matches nothing but inflates the
    /// count and makes a follows-nobody viewer look built.
    #[test]
    fn query_excludes_empty_followees() {
        assert!(FOLLOWS_QUERY.contains("followee != ''"));
    }
}
