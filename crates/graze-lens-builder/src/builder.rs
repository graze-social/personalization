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
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::interner::Interner;
use crate::second_degree;
use graze_lens_bootstrap::CompletenessStore;
use graze_lens_bootstrap::{Backfiller, Resolver};
use graze_lens_fold::Sink;

/// Viewers whose sets graze-lens-fold should keep fresh. Shared key; the fold
/// side owns the constant in `graze_lens_fold::delta`.
const ACTIVE_KEY: &str = "lens:active";

/// Facet names on the wire. These match `SUPPORTED_FACETS` in feeder-rs, which
/// turns them into Redis key names — a name that disagrees between the two
/// builds a key nobody reads.
pub const FACET_FOLLOWS: &str = "follows";
pub const FACET_FOLLOWS2: &str = "follows2";

/// Facets this builder can actually produce.
///
/// Deliberately not every facet the *reader* accepts: `network` is a blend the
/// reader composes from these two, so it is a legal thing to ask a feed for and
/// an illegal thing to ask the builder for.
pub fn is_buildable_facet(facet: &str) -> bool {
    matches!(facet, FACET_FOLLOWS | FACET_FOLLOWS2)
}

fn now_secs() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0)
}

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
    /// This viewer has no backfill on record and backfill is not configured, so
    /// any lens we built would be based on incidental stream data. Nothing is
    /// published; the feed serves unlensed.
    NeedsBackfill,
    /// The requested facet is not one this builder produces. Nothing is
    /// published and no state is written, so the viewer is left exactly as they
    /// were rather than stuck mid-build.
    UnknownFacet,
}

pub struct Builder {
    redis: Pool,
    http: reqwest::Client,
    clickhouse: ClickHouseConfig,
    config: Config,
    /// Absent means the completeness guard is off and every viewer is treated
    /// as complete — only appropriate for a backfill-free test setup.
    completeness: Option<CompletenessStore>,
    backfiller: Option<Backfiller>,
    sink: Option<Sink>,
    /// Shared DID interner, on the cache Redis. Required for v2/follows2.
    interner: Option<Interner>,
}

impl Builder {
    /// A builder with no completeness guard. Publishes whatever ClickHouse
    /// holds for a viewer, complete or not.
    pub fn new(redis: Pool, config: Config) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            redis,
            http,
            clickhouse: config.clickhouse.clone(),
            config,
            completeness: None,
            backfiller: None,
            sink: None,
            interner: None,
        })
    }

    /// The production builder: refuses to publish a lens for a viewer whose
    /// follow history has not been backfilled, and backfills them instead.
    ///
    /// `interner_redis` is the *cache* Redis, where the shared DID id space
    /// lives — a different instance from the one lens blobs are published to.
    pub fn with_backfill(
        redis: Pool,
        interner_redis: Option<Pool>,
        config: Config,
    ) -> anyhow::Result<Self> {
        let mut builder = Self::new(redis, config)?;
        builder.interner = interner_redis.map(Interner::new);
        let cfg = &builder.config;

        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("graze-lens-builder/", env!("CARGO_PKG_VERSION")))
            .build()?;

        builder.completeness = Some(CompletenessStore::new(
            cfg.clickhouse.clone(),
            cfg.query_timeout,
            cfg.max_execution_seconds,
        )?);
        builder.backfiller = Some(Backfiller::new(
            http.clone(),
            Resolver::new(http, cfg.plc_directory.clone()),
            cfg.backfill_request_timeout,
            cfg.backfill_page_delay,
            cfg.backfill_max_pages,
        ));
        // Straight to the base table, NOT through follow_edges_buffer.
        //
        // The buffer exists to coalesce the *stream's* one-row-at-a-time
        // inserts; a backfill is already a bulk write of hundreds or thousands
        // of rows, so it gains nothing there and loses something important.
        // Buffered rows are invisible to a plain SELECT on the base table until
        // the buffer flushes (10k rows or 100s), so a rebuild landing inside
        // that window would read back almost nothing and republish the
        // truncated set this guard exists to prevent. Measured: a viewer
        // backfilled to 681 follows had their set regress to 1 on the very next
        // build. Writing directly closes the window entirely.
        builder.sink = Some(Sink::new_with_table(
            cfg.clickhouse.clone(),
            cfg.query_timeout,
            "follow_edges",
        )?);
        Ok(builder)
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
    ///
    /// A viewer whose follow history was never backfilled is backfilled here
    /// first. `graze-lens-fold` only sees follows made since it connected, so
    /// building from ClickHouse alone would publish a handful of incidental
    /// edges as if it were their graph — a wrong feed, not a narrow one, and
    /// indistinguishable from someone who genuinely follows almost nobody.
    pub async fn build(&self, viewer: &str, facet: &str) -> anyhow::Result<BuildOutcome> {
        // Reject an unknown facet before anything else, and in particular before
        // marking the viewer "building" — a state nothing would ever clear.
        //
        // The dispatch below falls through to the first-degree publisher for any
        // name it does not recognise, so an unknown facet would not fail: it
        // would cheerfully publish the viewer's follows under that name, and the
        // serve path would read a lens that answers a different question than
        // the one its key claims. A composite like `network` is expanded by the
        // reader into its underlying facets and never arrives here as itself, so
        // seeing one is already a bug worth surfacing.
        if !is_buildable_facet(facet) {
            warn!(viewer, facet, "unknown facet; refusing to build");
            return Ok(BuildOutcome::UnknownFacet);
        }

        self.mark_state(viewer, "building").await?;

        if let Some(store) = &self.completeness {
            if !store.is_complete(viewer).await {
                info!(viewer, facet, "no backfill on record; backfilling first");
                return self.backfill_then_publish(viewer, facet).await;
            }
        }

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

        // `follows` publishes a v1 set (what feeder-rs reads today) and also a
        // v2 scored blob, so the reader can change formats without a flag day.
        // `follows²` is v2 only — a set cannot carry reach.
        self.publish_for_facet(viewer, facet, &followees).await
    }

    /// Build and publish `follows²` from the traversal projection.
    async fn publish_second_degree(
        &self,
        viewer: &str,
        first_degree: &[String],
    ) -> anyhow::Result<BuildOutcome> {
        let Some(interner) = &self.interner else {
            warn!(viewer, "no interner configured; cannot build follows2");
            self.mark_state(viewer, "failed").await?;
            return Ok(BuildOutcome::NeedsBackfill);
        };

        // Seeds must be ids, and must be inlined into the query. Interning the
        // viewer's own follows is cheap and mostly cache-hits after the first
        // build.
        let ids = interner.intern_many(first_degree).await?;
        let seeds: Vec<u32> = ids.values().copied().collect();
        if seeds.is_empty() {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }

        let sql = second_degree::reach_query(
            &self.clickhouse.database,
            &seeds,
            self.config.second_degree_cap,
        );
        let text = self.query_text(&sql).await?;

        let mut map = second_degree::parse_reach_tsv(&text, self.config.second_degree_top_k);
        second_degree::exclude_first_degree(&mut map, &seeds);
        second_degree::warn_if_thin(viewer, seeds.len(), map.all_ids.len());

        if map.is_empty() {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }

        let built_at = now_secs();
        let blob = map.encode(built_at);
        info!(
            viewer,
            seeds = seeds.len(),
            reached = map.all_ids.len(),
            scored = map.entries.len(),
            max_reach = map.max_reach,
            bytes = blob.len(),
            "built second degree"
        );

        self.publish_blob(viewer, FACET_FOLLOWS2, &blob).await?;
        Ok(BuildOutcome::Published)
    }

    /// Also publish `follows` as a v2 blob, at uniform full weight.
    ///
    /// A follow carries no strength signal, so every entry is full confidence;
    /// the value is format uniformity, so the reader has one decoder and the
    /// blend has one shape to weight. Best-effort: the v1 set is still the
    /// authority until the reader moves over, so a failure here must not fail
    /// the build.
    async fn publish_v2_uniform(&self, viewer: &str, facet: &str, followees: &[String]) {
        let Some(interner) = &self.interner else {
            return;
        };
        let result = async {
            let ids = interner.intern_many(followees).await?;
            let entries: Vec<(u32, u16)> = ids
                .values()
                .map(|id| (*id, crate::scored::WEIGHT_MAX))
                .collect();
            let blob = crate::scored::encode(crate::scored::FACET_FOLLOWS, now_secs(), entries);
            self.publish_blob(viewer, facet, &blob).await
        }
        .await;
        if let Err(e) = result {
            warn!(error = %e, viewer, facet, "v2 mirror publish failed; v1 set still authoritative");
        }
    }

    /// Write a v2 blob and mark the viewer ready.
    async fn publish_blob(&self, viewer: &str, facet: &str, blob: &[u8]) -> anyhow::Result<()> {
        let key = format!("lens:v2:{facet}:{viewer}");
        let ttl = self.config.set_ttl.as_secs();
        let mut conn = self.redis.get().await?;
        deadpool_redis::redis::pipe()
            .atomic()
            .set_ex(&key, blob, ttl)
            .hset(Self::meta_key(viewer), "state", "ready")
            .hset(
                Self::meta_key(viewer),
                format!("v2_{facet}_bytes"),
                blob.len(),
            )
            .expire(Self::meta_key(viewer), ttl as i64)
            .sadd(ACTIVE_KEY, viewer)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    /// Run a query that returns raw text (TSV).
    async fn query_text(&self, sql: &str) -> anyhow::Result<String> {
        let response = self
            .http
            .post(self.clickhouse.base_url())
            .basic_auth(&self.clickhouse.user, Some(&self.clickhouse.password))
            .header("Content-Type", "text/plain")
            .timeout(self.config.query_timeout)
            .query(&[
                (
                    "max_execution_time",
                    self.config.max_execution_seconds.to_string(),
                ),
                ("cancel_http_readonly_queries_on_client_close", "1".into()),
            ])
            .body(sql.to_string())
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("query failed ({status}): {}", &text[..text.len().min(400)]);
        }
        Ok(text)
    }

    /// Pull a viewer's follows from their own PDS, persist them, and publish
    /// the lens from what we just read.
    ///
    /// Publishing from memory rather than re-querying ClickHouse is deliberate.
    /// The edges are written through `follow_edges_buffer`, which flushes on
    /// 10k rows or 100 seconds — so a read immediately after the write would
    /// see almost none of them and publish the very truncated lens this guard
    /// exists to prevent. The rows still land for durability and for later
    /// rebuilds; this build just does not depend on them being visible yet.
    async fn backfill_then_publish(
        &self,
        viewer: &str,
        facet: &str,
    ) -> anyhow::Result<BuildOutcome> {
        let (backfiller, sink, store) = match (&self.backfiller, &self.sink, &self.completeness) {
            (Some(b), Some(s), Some(c)) => (b, s, c),
            // Backfill is not configured; refusing to publish beats publishing
            // a lens we know may be built on nothing.
            _ => {
                warn!(viewer, "backfill unavailable; refusing to publish a lens");
                self.mark_state(viewer, "needs_backfill").await?;
                return Ok(BuildOutcome::NeedsBackfill);
            }
        };

        let backfilled = backfiller.edges_for(viewer).await?;
        let edges = backfilled.edges;
        let count = edges.len();
        info!(
            viewer,
            count,
            truncated = backfilled.truncated,
            "backfilled follow history"
        );

        if !edges.is_empty() {
            sink.insert(&edges).await?;
        }

        // Marked before publishing: if the process dies between the two, the
        // next build finds the marker, skips the (already persisted) backfill,
        // and builds from ClickHouse. Marking after would re-walk the PDS.
        //
        // Except when the backfill was truncated. These edges are a prefix of
        // the account's follows, not their graph, and a marker would make that
        // prefix permanent: every later build would trust it and skip the walk.
        // Leaving the marker off costs a re-walk per request for a handful of
        // very large accounts, and buys the chance to get them whole once
        // `backfill_max_pages` is raised. The lens still publishes meanwhile —
        // a wide-but-partial lens is a reasonable feed, an invisibly frozen one
        // is not.
        if backfilled.truncated {
            warn!(
                viewer,
                count, "backfill truncated; not recording completeness"
            );
        } else {
            store.mark_complete(viewer, count).await?;
        }

        if count == 0 {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }
        if count > self.config.max_set_size {
            warn!(
                viewer,
                count, "lens set over size budget; refusing to publish"
            );
            self.mark_state(viewer, "failed").await?;
            return Ok(BuildOutcome::TooLarge);
        }

        let followees: Vec<String> = edges.into_iter().map(|e| e.followee).collect();
        // Route through the same facet dispatch as a normal build. Publishing
        // directly here would ignore the facet entirely: a `follows2` request
        // that happened to arrive for an un-backfilled viewer would silently
        // publish their FIRST degree under the second-degree name — a lens that
        // looks built, reports a plausible count, and is answering a different
        // question than the one asked.
        self.publish_for_facet(viewer, facet, &followees).await
    }

    /// Publish `followees` under whichever facet was requested.
    ///
    /// The single place that decides what a facet's output looks like, so the
    /// normal and post-backfill paths cannot drift apart.
    async fn publish_for_facet(
        &self,
        viewer: &str,
        facet: &str,
        followees: &[String],
    ) -> anyhow::Result<BuildOutcome> {
        if facet == FACET_FOLLOWS2 {
            return self.publish_second_degree(viewer, followees).await;
        }
        self.publish(viewer, facet, followees).await?;
        self.publish_v2_uniform(viewer, facet, followees).await;
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
    /// The reader accepts `network` and expands it into these two before
    /// enqueueing; the builder must never accept it directly, or it would
    /// publish a first-degree set under a name that promises a blend.
    #[test]
    fn only_real_facets_are_buildable() {
        assert!(is_buildable_facet(FACET_FOLLOWS));
        assert!(is_buildable_facet(FACET_FOLLOWS2));
        assert!(
            !is_buildable_facet("network"),
            "network is composed by the reader, not built here"
        );
        assert!(!is_buildable_facet("mutuals"), "not implemented yet");
        assert!(!is_buildable_facet(""));
        assert!(!is_buildable_facet("FOLLOWS"), "matching is exact");
    }

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
