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
use crate::domain;
use crate::interner::Interner;
use crate::priors;
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
pub const FACET_NICHE: &str = "niche";
pub const FACET_POPULAR: &str = "popular";
pub const FACET_VELOCITY: &str = "velocity";
pub const FACET_COMMUNITY: &str = "community";
/// Feed-scoped: who this feed's recent authors collectively follow. Built per
/// algorithm id, not per viewer; the reader sums it with viewer facets.
pub const FACET_DOMAIN: &str = "domain";

/// Facets this builder can actually produce.
///
/// Deliberately not every facet the *reader* accepts: `network` is a blend the
/// reader composes from these two, so it is a legal thing to ask a feed for and
/// an illegal thing to ask the builder for.
pub fn is_buildable_facet(facet: &str) -> bool {
    matches!(
        facet,
        FACET_FOLLOWS
            | FACET_FOLLOWS2
            | FACET_NICHE
            | FACET_POPULAR
            | FACET_VELOCITY
            | FACET_COMMUNITY
            | FACET_DOMAIN
    )
}

/// The header byte for each stored facet name. One place, so the dispatch and
/// the wire format cannot disagree.
fn facet_header_id(name: &str) -> Option<u8> {
    match name {
        FACET_FOLLOWS => Some(crate::scored::FACET_FOLLOWS),
        FACET_FOLLOWS2 => Some(crate::scored::FACET_FOLLOWS2),
        FACET_NICHE => Some(crate::scored::FACET_NICHE),
        FACET_POPULAR => Some(crate::scored::FACET_POPULAR),
        FACET_VELOCITY => Some(crate::scored::FACET_VELOCITY),
        FACET_COMMUNITY => Some(crate::scored::FACET_COMMUNITY),
        FACET_DOMAIN => Some(crate::scored::FACET_DOMAIN),
        _ => None,
    }
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
    /// The lens-owned id space lives on the **same Redis as the blobs**, so the
    /// interner is derived from `redis` here rather than passed in.
    ///
    /// It used to be a separate `Option<Pool>` argument, and that is exactly how
    /// it broke: `Interner::lens` names the lens keys, but the caller built the
    /// pool from `REDIS_URL` (the *cache* Redis). So `lensdid:{lensdid}:map`
    /// existed on two instances at once — 42,049,737 entries on the lens Valkey,
    /// where `project_rebuild` builds it and feeder-rs reads it, and a private
    /// 9,499-entry map on the cache Redis that only the builder ever touched.
    /// Every v2 blob was filled with ids from the small map and stamped with the
    /// lens idspace byte, so it looked structurally perfect while no author
    /// lookup on the serve path could ever match. Deriving the interner from the
    /// blob pool makes the ids and the blobs impossible to separate again.
    pub fn with_backfill(redis: Pool, config: Config) -> anyhow::Result<Self> {
        let mut builder = Self::new(redis.clone(), config)?;
        builder.interner = Some(Interner::lens(redis));
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
            cfg.backfill_max_retries,
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
    /// Build the feed-scoped `domain` blob for one algorithm id.
    ///
    /// Deliberately not routed through `build()`: that path is a viewer
    /// pipeline — completeness guard, PDS backfill, lensmeta state — and none
    /// of it means anything for a feed. A feed with no recent authors is
    /// Empty, not NeedsBackfill.
    pub async fn build_domain(&self, algo_id: u32) -> anyhow::Result<BuildOutcome> {
        let Some(interner) = &self.interner else {
            anyhow::bail!("no interner configured; cannot build domain");
        };

        let authors_sql = domain::author_weights_query(&self.clickhouse.database, algo_id);
        let authors = domain::parse_author_weights(&self.query_text(&authors_sql).await?);
        if authors.is_empty() {
            info!(algo_id, "feed has no recent authors; nothing to build");
            return Ok(BuildOutcome::Empty);
        }

        // Intern the authors, carrying each one's decayed weight through.
        let dids: Vec<String> = authors.iter().map(|(d, _)| d.clone()).collect();
        let ids = interner.intern_many(&dids).await?;
        let seeds: Vec<(u32, f64)> = authors
            .iter()
            .filter_map(|(d, w)| ids.get(d).map(|id| (*id, *w)))
            .collect();
        if seeds.is_empty() {
            return Ok(BuildOutcome::Empty);
        }

        let reach_sql = domain::weighted_reach_query(
            &self.clickhouse.database,
            &seeds,
            self.config.second_degree_cap,
        );
        let map = domain::parse_domain_tsv(
            &self.query_text(&reach_sql).await?,
            self.config.second_degree_top_k,
        );
        if map.is_empty() {
            return Ok(BuildOutcome::Empty);
        }
        // Same arithmetic impossibility as every reach facet: an account
        // cannot be followed by more of the feed's authors than there are.
        if (map.max_reach as usize) > seeds.len() {
            anyhow::bail!(
                "domain: max_reach {} exceeds {} seed authors — id map or projection corrupt",
                map.max_reach,
                seeds.len()
            );
        }

        let blob = map.encode_as(crate::scored::FACET_DOMAIN, interner.idspace(), now_secs());
        info!(
            algo_id,
            authors = seeds.len(),
            reached = map.all_ids.len(),
            scored = map.entries.len(),
            max_reach = map.max_reach,
            bytes = blob.len(),
            "built domain facet"
        );

        // Keyed by algorithm id. Numeric, so it cannot collide with the
        // DID-keyed viewer blobs sharing the prefix.
        let key = format!("lens:v2:{FACET_DOMAIN}:{algo_id}");
        let ttl = self.config.set_ttl.as_secs();
        let mut conn = self.redis.get().await?;
        deadpool_redis::redis::cmd("SET")
            .arg(&key)
            .arg(blob)
            .arg("EX")
            .arg(ttl)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(BuildOutcome::Published)
    }

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
        let blob = map.encode(interner.idspace(), built_at);
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
            let blob = crate::scored::encode_in_space(
                crate::scored::FACET_FOLLOWS,
                interner.idspace(),
                now_secs(),
                entries,
            );
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
                // Seed lists are inlined as literals — required for primary-key
                // index analysis — and a whale's 50k follows is ~450 KB of SQL,
                // over ClickHouse's 256 KB default parse budget. "Any user at
                // any scale" means the parser budget scales with the whale, not
                // the whale failing to build.
                ("max_query_size", "10485760".to_string()),
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
        // Exhaustive on purpose. A fallthrough here once published a viewer's
        // FIRST degree under the second-degree name; with six facets the same
        // slip would be six ways to serve the wrong signal under a plausible
        // count. An unmatched name is a bug upstream — is_buildable_facet
        // admitted something this dispatch does not know.
        match facet {
            FACET_FOLLOWS => {
                self.publish(viewer, facet, followees).await?;
                self.publish_v2_uniform(viewer, facet, followees).await;
                Ok(BuildOutcome::Published)
            }
            FACET_FOLLOWS2 => self.publish_second_degree(viewer, followees).await,
            FACET_NICHE => {
                self.publish_prior(viewer, followees, priors::Prior::Niche)
                    .await
            }
            FACET_POPULAR => {
                self.publish_prior(viewer, followees, priors::Prior::Popular)
                    .await
            }
            FACET_VELOCITY => self.publish_velocity(viewer, followees).await,
            FACET_COMMUNITY => self.publish_community(viewer, followees).await,
            other => {
                warn!(viewer, facet = other, "facet admitted but not dispatched");
                Ok(BuildOutcome::UnknownFacet)
            }
        }
    }

    /// Seeds for any graph facet: the viewer's follows, as lens-space ids.
    async fn seeds_for(&self, viewer: &str, first_degree: &[String]) -> anyhow::Result<Vec<u32>> {
        let Some(interner) = &self.interner else {
            anyhow::bail!("no interner configured");
        };
        let ids = interner.intern_many(first_degree).await?;
        let _ = viewer;
        Ok(ids.values().copied().collect())
    }

    /// Shared tail for every scored facet: sanity-check, encode, publish.
    async fn publish_scored(
        &self,
        viewer: &str,
        facet_name: &str,
        map: second_degree::SecondDegree,
        seeds: &[u32],
    ) -> anyhow::Result<BuildOutcome> {
        let Some(interner) = &self.interner else {
            self.mark_state(viewer, "failed").await?;
            return Ok(BuildOutcome::NeedsBackfill);
        };
        if map.is_empty() {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }
        // The invariant that caught a corrupt id map once already: an author
        // cannot be reached by more of your follows than you have.
        if (map.max_reach as usize) > seeds.len() {
            self.mark_state(viewer, "failed").await?;
            anyhow::bail!(
                "{facet_name}: max_reach {} exceeds {} seeds — id map or projection corrupt",
                map.max_reach,
                seeds.len()
            );
        }
        let Some(header_id) = facet_header_id(facet_name) else {
            anyhow::bail!("{facet_name} has no header id; dispatch and wire format disagree");
        };
        let blob = map.encode_as(header_id, interner.idspace(), now_secs());
        info!(
            viewer,
            facet = facet_name,
            seeds = seeds.len(),
            reached = map.all_ids.len(),
            scored = map.entries.len(),
            max_reach = map.max_reach,
            bytes = blob.len(),
            "built scored facet"
        );
        self.publish_blob(viewer, facet_name, &blob).await?;
        Ok(BuildOutcome::Published)
    }

    /// niche / popular: reach reweighted by global fame from `account_stats`.
    async fn publish_prior(
        &self,
        viewer: &str,
        first_degree: &[String],
        prior: priors::Prior,
    ) -> anyhow::Result<BuildOutcome> {
        let facet_name = match prior {
            priors::Prior::Niche => FACET_NICHE,
            priors::Prior::Popular => FACET_POPULAR,
        };
        let seeds = self.seeds_for(viewer, first_degree).await?;
        if seeds.is_empty() {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }
        let sql = priors::prior_reach_query(
            &self.clickhouse.database,
            &seeds,
            self.config.second_degree_cap,
            prior,
        );
        let text = self.query_text(&sql).await?;
        let mut map = priors::parse_prior_tsv(&text, self.config.second_degree_top_k);
        second_degree::exclude_first_degree(&mut map, &seeds);
        self.publish_scored(viewer, facet_name, map, &seeds).await
    }

    /// velocity: reach over the recency slice only.
    async fn publish_velocity(
        &self,
        viewer: &str,
        first_degree: &[String],
    ) -> anyhow::Result<BuildOutcome> {
        let seeds = self.seeds_for(viewer, first_degree).await?;
        if seeds.is_empty() {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }
        let sql = priors::velocity_query(
            &self.clickhouse.database,
            &seeds,
            self.config.second_degree_cap,
            self.config.velocity_days,
        );
        let text = self.query_text(&sql).await?;
        let mut map = second_degree::parse_reach_tsv(&text, self.config.second_degree_top_k);
        second_degree::exclude_first_degree(&mut map, &seeds);
        self.publish_scored(viewer, FACET_VELOCITY, map, &seeds)
            .await
    }

    /// community: members of the viewer's top LPA communities.
    async fn publish_community(
        &self,
        viewer: &str,
        first_degree: &[String],
    ) -> anyhow::Result<BuildOutcome> {
        let seeds = self.seeds_for(viewer, first_degree).await?;
        if seeds.is_empty() {
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }
        let affinity_sql = priors::community_affinity_query(
            &self.clickhouse.database,
            &seeds,
            self.config.community_top,
        );
        let affinity =
            priors::parse_affinity_tsv(&self.query_text(&affinity_sql).await?, seeds.len());
        if affinity.is_empty() {
            // No LPA labels yet (job has not run) or none of the follows are
            // labelled. Ready-and-empty, not failed: the serve path falls open
            // and the next scheduled build picks the labels up.
            self.mark_state(viewer, "ready").await?;
            return Ok(BuildOutcome::Empty);
        }
        let communities: Vec<u32> = affinity.iter().map(|(c, _)| *c).collect();
        let members_sql = priors::community_members_query(
            &self.clickhouse.database,
            &communities,
            self.config.second_degree_cap,
        );
        let mut map = priors::parse_members_tsv(
            &self.query_text(&members_sql).await?,
            &affinity,
            self.config.second_degree_top_k,
        );
        second_degree::exclude_first_degree(&mut map, &seeds);
        self.publish_scored(viewer, FACET_COMMUNITY, map, &seeds)
            .await
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
        for f in [
            FACET_FOLLOWS,
            FACET_FOLLOWS2,
            FACET_NICHE,
            FACET_POPULAR,
            FACET_VELOCITY,
            FACET_COMMUNITY,
            FACET_DOMAIN,
        ] {
            assert!(is_buildable_facet(f), "{f} should build");
        }
        // Composites are composed by the reader from stored facets; a build
        // request for one is a bug, and accepting it would publish first
        // degree under a name that promises a blend.
        assert!(!is_buildable_facet("network"));
        assert!(!is_buildable_facet("discover"));
        assert!(!is_buildable_facet("expertise"));
        assert!(!is_buildable_facet("mutuals"), "not implemented yet");
        assert!(!is_buildable_facet(""));
        assert!(!is_buildable_facet("FOLLOWS"), "matching is exact");
    }

    /// Facet header ids must be distinct — a collision would let the reader
    /// accept a blob built for a different signal.
    #[test]
    fn facet_header_ids_are_distinct() {
        let ids = [
            crate::scored::FACET_FOLLOWS,
            crate::scored::FACET_FOLLOWS2,
            crate::scored::FACET_NICHE,
            crate::scored::FACET_POPULAR,
            crate::scored::FACET_VELOCITY,
            crate::scored::FACET_COMMUNITY,
            crate::scored::FACET_DOMAIN,
        ];
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());
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

    fn test_pool() -> Pool {
        // deadpool builds lazily; nothing connects until a call is made.
        deadpool_redis::Config::from_url("redis://127.0.0.1:6379")
            .builder()
            .expect("pool builder")
            .max_size(1)
            .runtime(deadpool_redis::Runtime::Tokio1)
            .build()
            .expect("pool")
    }

    /// The interner must come from the SAME pool the blobs are published to.
    ///
    /// This is the invariant whose violation filled every v2 blob with ids from
    /// a private 9,499-entry map on the cache Redis while the serve path looked
    /// authors up in the 42M-entry lens space — a lens that could never match,
    /// behind a blob that decoded perfectly. `with_backfill` no longer accepts a
    /// separate pool, so the only thing left to pin is that it always builds one
    /// and builds it in the lens space.
    #[test]
    fn interner_is_always_present_and_lens_spaced() {
        let builder = Builder::with_backfill(test_pool(), Config::for_test()).expect("builder");
        let interner = builder
            .interner
            .as_ref()
            .expect("v2 must never be silently disabled: no interner means no v2 or follows2");
        assert_eq!(
            interner.idspace(),
            crate::scored::IDSPACE_LENS,
            "blobs are stamped with this byte; it must match the space the ids came from"
        );
    }
}
