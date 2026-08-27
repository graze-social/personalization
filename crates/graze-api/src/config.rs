//! Configuration for the Graze API service.
//!
//! All configuration is loaded from environment variables.

use std::collections::HashSet;
use std::sync::Arc;

use graze_common::{exclusion_set_from_env_opt, RedisConfig};

/// Application settings loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    // ═══════════════════════════════════════════════════════════════════════════════
    // Redis Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub redis_url: String,
    pub redis_pool_size: usize,
    pub redis_connect_max_retries: u32,
    pub redis_connect_initial_delay_ms: u64,
    /// Optional Redis URL for post-render / request logging (e.g. log_tasks queue).
    /// When unset, post-render logging is skipped quietly.
    pub redis_requests_logger_url: Option<String>,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Inverted Algorithm Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub inverted_algorithm_enabled: bool,
    pub inverted_min_post_likes: usize,
    pub inverted_max_likers_per_post: usize,
    pub inverted_max_posts_to_score: usize,
    /// Skip personalization entirely when the feed's candidate pool
    /// (`SCARD ap:{algo_id}`) is below this size. 0 disables the gate.
    ///
    /// Rationale (measured in prod, 25-min window, 359 scorer runs): candidate
    /// pool size is the dominant predictor of how many posts get personalized —
    /// r=0.604 (0.777 log-log) vs r=0.141 for co-liker source count. Requests
    /// against a pool under 500 posts returned a MEDIAN OF ZERO scored posts
    /// with a 74% zero rate, and accounted for 41% of all personalization
    /// attempts. Those runs still paid for co-liker computation (27-106ms),
    /// author-affinity supplementation (~106ms), a full SMEMBERS of the pool and
    /// an HMGET of every liker count — to produce nothing. The 500-2k band, by
    /// contrast, returned a median of 18, so the useful cut is at ~500, not
    /// higher.
    pub min_candidate_pool_for_personalization: usize,
    pub min_overlapping_colikers: usize,

    /// How many days of like history the "does this user have seed?" gate looks back.
    ///
    /// Must match what the scorer actually reads (`DEFAULT_RETENTION_DAYS`), or users get
    /// turned away with `fallback_reason=no_user_data` despite having seed the scorer would
    /// have used. This was previously hardcoded to 2 days (today + yesterday) while the
    /// scorer read 6, which measured as **14.0% of all daily-active users — 22.1% of every
    /// seeded user — losing personalization for no reason.**
    ///
    /// Lower it to 2 to restore the old behaviour without a rebuild.
    pub user_data_check_days: u32,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Randomized parameter experiment + Thompson persistence — see THEORIES T19
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Force one Thompson dimension to a randomized value chosen by hashing the user DID.
    ///
    /// Exists because Thompson assignment is adaptive, which makes retrospective arm comparisons
    /// confounded — the `max_total_sources` signal appeared just as strongly on fallback posts the
    /// parameter cannot affect. Hash assignment is orthogonal to time and bandit state.
    pub ab_experiment_enabled: bool,
    /// Bandit dimension to randomize: `min_likes`, `max_likers`, `max_sources`, `max_checks`,
    /// `min_colikes`, `max_user_likes`, `max_src_per_post`, `seed_pool`, `corater_decay`.
    pub ab_experiment_dimension: String,
    /// Comma-separated values to randomize between, e.g. `250,10000`.
    pub ab_experiment_values: Vec<usize>,
    /// Percentage of users enrolled. Enrolled users are excluded from bandit learning.
    pub ab_experiment_traffic_pct: u32,
    /// Bump to re-randomize assignment.
    pub ab_experiment_salt: String,

    /// Persist Thompson bandit evidence to Redis and merge across replicas.
    ///
    /// Without this, arms are in-memory per pod and every deploy discards all learning across
    /// three replicas — which is both why the search never converged and why arm selection
    /// correlates with time.
    pub thompson_persist_enabled: bool,
    /// Seconds between flush-and-reload cycles.
    pub thompson_persist_interval_seconds: u64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Inverted (co-liker-first) lookup — see THEORIES-personalization.md T7
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Compute the co-liker-first arm alongside the post-first arm, log the comparison, and
    /// **serve the post-first result**. Safe to leave on; it cannot change a response.
    pub inverted_lookup_shadow_mode: bool,

    /// Serve from the co-liker-first arm. Keep false until shadow mode confirms the coverage
    /// and latency numbers on live traffic.
    pub inverted_lookup_enabled: bool,

    /// How many days of each co-liker's likes to read.
    ///
    /// Pool posts are capped at `SYNC_PREFERRED_MAX_AGE_HOURS` (72h), so a like on a pool post
    /// cannot be older than that plus a margin; older shards only return posts that fail the
    /// pool-membership test. 4 keeps a day of headroom over the 3-day pool.
    pub inverted_coliker_like_days: u32,

    /// Max recent likes to read per co-liker.
    ///
    /// This is where truncation moves to, and it is a much weaker bias than the post-first
    /// `max_likers_per_post`: one cap per person rather than per post, the median seeded user
    /// has only ~16 likes in the window, and a single ZREVRANGEBYSCORE returns 500 members as
    /// cheaply as 30 — so raising it costs essentially nothing.
    pub inverted_coliker_like_limit: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Sampled random walk (`SampledWalk` ranker) — see algorithm/walk.rs
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Total walks to distribute across the user's seed posts.
    ///
    /// This is the *entire* cost knob: unlike both enumerative paths, work here is bounded by the
    /// budget rather than by graph degree, which is what makes the 5,157-co-liker case affordable.
    /// Salt for the per-user personalization-holdout assignment.
    ///
    /// Deliberately distinct from the Thompson and interleaving salts: sharing one would correlate
    /// holdout membership with bandit arms, so a holdout readout would partly measure the bandit.
    /// Changing this reshuffles who is held out, which starts a new experiment.
    pub personalization_holdout_salt: String,

    /// Minimum share of a feed's candidate pool that must clear `min_post_likes` before the engine
    /// will spend a co-liker walk on that feed. `0.0` disables the gate.
    ///
    /// Measured across 64 live pools: 22 feeds have >=20% scoreable candidates, 27 are 5-20%, and 15
    /// are below 5%. The distribution is not driven by pool size — algo 8352 has 596 posts and 22%
    /// scoreable while algo 5395's 1,000 posts are 2% — so the existing
    /// `min_candidate_pool_for_personalization` gate cannot separate them. Density can.
    ///
    /// This matters because algo 5395 is ~83% of all scoring traffic at 2% scoreable, so most of the
    /// engine's compute currently goes to a feed that structurally cannot produce a ranking.
    pub min_pool_scoreable_share: f64,

    pub walk_count: usize,

    /// Distinct co-likers whose like lists may be fetched in the second phase.
    ///
    /// Bounds the one pipelined fetch that dominates latency. The inverted path failed precisely
    /// because it had no such bound.
    pub walk_max_users: usize,

    /// Likes to read per sampled co-liker.
    pub walk_user_like_limit: usize,

    /// Early stop once this many candidates have been visited `walk_early_stop_nv` times.
    pub walk_early_stop_np: usize,

    /// Visit threshold for [`Config::walk_early_stop_np`]. Zero disables early stopping.
    pub walk_early_stop_nv: u32,

    /// Exponent on the popularity discount. Zero disables it.
    pub walk_popularity_power: f64,

    /// Minimum visits before a candidate is ranked at all — the noise floor of a sampled estimate.
    pub walk_min_visits: u32,

    /// Below this many seed posts, fall back to exhaustive scoring.
    ///
    /// Sampling variance is worst exactly where the graph is smallest, and enumeration is cheap
    /// there, so there is nothing to buy and accuracy to lose.
    pub walk_min_seed_for_sampling: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Durable Co-liker Profiles (`ucl:`) — see DESIGN-durable-coliker-profiles.md
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Phase B: compute a durable-profile arm alongside the live one, log the comparison,
    /// and **serve nothing** from it. Safe to leave on — it cannot change any response.
    ///
    /// Costs one `GET` plus one extra scoring pass, and only for requests that would
    /// otherwise early-exit with zero co-likers.
    pub durable_profile_shadow_mode: bool,

    /// Phase C: actually serve from the durable profile when the live path finds no
    /// co-likers. Keep false until shadow mode shows the `overlap_count` distribution is
    /// sane — `overlap_count` feeds a nonlinear `paths_boost`, so a ~128-member profile can
    /// distort ranking rather than merely rescale it.
    pub durable_profile_enabled: bool,

    /// `min_post_likes` used **only** when scoring against a durable profile.
    ///
    /// Measured on algo 396 (10,703 candidates) with the global
    /// `INVERTED_MIN_POST_LIKES=10`: 1,866 candidates had no likers, **5,048 (47%) were
    /// dropped by this filter**, and of the ~3,789 survivors ~3,777 had no overlap with the
    /// 128-member profile — leaving 12 scored. The filter is the single largest loss in the
    /// funnel, and 10 is already stricter than the modal 5 seen in `feedContext`.
    ///
    /// Kept separate from `inverted_min_post_likes` so relaxing it cannot change what the
    /// live arm serves: users the live path can already personalize never reach the durable
    /// arm at all.
    pub durable_profile_min_post_likes: usize,

    /// `max_likers_per_post` used **only** when scoring against a durable profile.
    ///
    /// This is the dominant constraint on durable-profile reach, not `min_post_likes`. The
    /// scorer fetches the N *most recent* likers per candidate, so a co-liker who liked
    /// early is invisible — detection probability is `min(1, N/L)` for a post with L likers.
    /// Measured on algo 396: **72–100% of a lurker's eligible overlapping candidates have
    /// more than 30 likers**, and modelling `min(1, 30/L)` reproduced production almost
    /// exactly (predicted 16/589/1/21 vs observed 12/500/2/24).
    ///
    /// Expected scoreable candidates by N for four sampled lurkers:
    ///
    /// | N | 30 | 100 | 200 | 500 | ∞ |
    /// |---|---|---|---|---|---|
    /// | A | 18 | 25 | 29 | 33 | 41 |
    /// | B | 679 | 979 | 1064 | 1112 | 1141 |
    /// | C | 1 | 2 | 2 | 4 | 7 |
    /// | D | 21 | 37 | 43 | 52 | 67 |
    ///
    /// 100 captures most of the available gain (+39% to +100%) before returns flatten.
    ///
    /// Raising this for the durable arm **requires a separate liker cache** — the scorer
    /// writes fetched liker lists back into its cache, so sharing one would leak longer
    /// lists into the live arm and silently change what it scores.
    pub durable_profile_max_likers_per_post: usize,

    /// Target for the largest weight derived from a durable profile.
    ///
    /// Stored profile scores are `Σ 1/L_j` (order 1e-3..1e2), while live co-liker weights
    /// are ~2e-7 and `max_coliker_weight` clamps at 1e-6. Weights are rescaled
    /// rank-preservingly so the top entry lands here; absolute scale is load-bearing only
    /// at that clamp and at `score > 0.0`, so this keeps the profile arm inside the same
    /// regime as the live arm.
    pub durable_profile_weight_target: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Liker Cache Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub liker_cache_enabled: bool,
    pub liker_cache_max_size: usize,
    pub liker_cache_ttl_seconds: u64,
    pub liker_cache_prewarm_count: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Local Algo Cache Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub local_algo_cache_ttl_seconds: u64,
    pub local_algo_cache_max_algos: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Bloom Filter Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub seen_bloom_enabled: bool,
    pub seen_bloom_expected_items: usize,
    pub seen_bloom_false_positive_rate: f64,
    pub seen_bloom_max_users: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Trending Posts Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub trending_posts_limit: usize,
    pub trending_posts_ttl_hours: u32,
    pub trending_recency_decay_hours: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Fallback Tranches Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub fallback_personalization_ratio: f64,
    pub fallback_popular_ratio: f64,
    pub fallback_trending_ratio: f64,
    pub fallback_discovery_ratio: f64,
    pub fallback_stagger_factor: f64,
    pub popular_posts_limit: usize,
    pub popular_decay_hours: f64,
    pub popular_min_likes: usize,
    pub velocity_window_hours: f64,
    pub velocity_min_likes: usize,
    pub velocity_posts_limit: usize,
    pub author_success_min_posts: usize,
    pub author_success_decay_hours: f64,
    pub discovery_posts_limit: usize,
    pub discovery_max_post_likes: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Progressive Blending Thresholds
    // ═══════════════════════════════════════════════════════════════════════════════
    pub cold_user_max_likes: usize,
    pub warm_user_max_likes: usize,
    pub cold_user_trending_ratio: f64,
    pub warm_user_trending_ratio: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Liked Posts Filter Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// When true, filter out already-liked posts from every feed response.
    pub liked_posts_filter_enabled: bool,
    /// Max number of liked posts to load for the universal filter (higher than scorer's cap).
    pub liked_posts_filter_max: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Seen Posts Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub seen_posts_ttl_hours: u32,
    pub seen_posts_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Interactions Logging Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Enable logging of all interactions (queue + worker).
    pub interactions_logging_enabled: bool,
    /// Backend for interaction writes: `clickhouse` or `none`.
    pub interactions_writer: String,
    /// Interval for batched ClickHouse writes (ms).
    pub interactions_batch_interval_ms: u64,
    /// Max interactions to batch before flushing.
    pub interactions_batch_size: usize,
    /// Channel capacity for the interaction queue.
    pub interactions_queue_capacity: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Special Posts Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Source for special posts: `remote` (fetch from API) or `local` (Redis only, admin CRUD).
    pub special_posts_source: String,
    /// API base URL when special_posts_source is remote.
    pub special_posts_api_base: String,
    /// Bearer token for authenticating with the special posts API.
    pub special_posts_api_token: String,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Access Sync Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_access_sync_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Cache Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_cache_ttl_seconds: u64,
    pub feed_cache_size: usize,
    pub feed_cache_enabled: bool,
    pub feed_cache_stale_threshold_seconds: u64,
    pub feed_cache_batch_size: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Co-liker Pre-computation Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub coliker_ttl_seconds: u64,
    pub coliker_refresh_threshold_seconds: u64,
    pub coliker_max_sources: usize,
    pub coliker_enabled: bool,
    pub linklonk_normalization_enabled: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Author-Affinity Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    /// TTL for the per-algorithm candidate-pool cache. `0` disables it, which is the rollback
    /// path: it restores fetching `ap:{algo_id}` on every request without a rebuild.
    pub pool_cache_ttl_seconds: u64,
    /// Total cached pool members across all algos. Bounded on members rather than entries because
    /// pools range from 1 to ~40,000, so an entry cap would bound almost nothing.
    pub pool_cache_max_members: usize,
    /// Seed author-affinity from `uf:{hash}` (followed authors) when the like-based seed is empty.
    ///
    /// Defaults OFF: this is the read path for follow seeds, and it changes what users are served.
    /// Turning it on is a treatment change and therefore resets the holdout experiment window.
    pub follow_seed_read_enabled: bool,
    /// How a followed author is weighted: `uniform` or `inverse_popularity`.
    ///
    /// A follow carries no strength signal the way a like count does, so unlike the like path there
    /// is no weight to inherit -- one has to be chosen, and the choice needs its own validation.
    pub follow_seed_weight_mode: String,
    pub author_affinity_enabled: bool,
    pub max_liked_authors_per_user: usize,
    pub max_likers_per_author: usize,
    pub author_affinity_max_authors: usize,
    pub author_affinity_max_colikers: usize,
    pub author_affinity_max_likers_per_author: usize,
    pub author_affinity_time_window_hours: u32,
    pub author_affinity_ttl_seconds: u64,
    pub author_affinity_refresh_threshold_seconds: u64,
    pub author_affinity_max_posts_to_score: usize,
    pub author_affinity_min_author_likes: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Personalization Defaults
    // ═══════════════════════════════════════════════════════════════════════════════
    pub stale_refresh_threshold_seconds: u64,
    pub default_max_user_likes: usize,
    pub default_max_sources_per_post: usize,
    pub default_min_co_likes: usize,
    pub default_time_window_hours: f64,
    pub default_recency_half_life_hours: f64,
    pub default_specificity_power: f64,
    pub default_popularity_power: f64,
    pub default_num_paths_power: f64,
    pub max_coliker_weight: f64,
    pub prove_max_posts_to_sample: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Author Diversity Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub diversity_enabled: bool,
    pub max_posts_per_author: usize,
    pub author_diminishing_factor: f64,
    pub diversity_mmr_lambda: f64,
    /// Keep ranking order through diversity instead of re-sorting by adjusted score.
    ///
    /// Needed by interleaving, whose team-draft order carries the ranker assignment. Off by
    /// default; the per-author cap still applies when on.
    pub diversity_preserve_order: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Interleaving (see experiment/interleave.rs, RESEARCH-personalization-directions.md)
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Blend two rankers into one response and attribute engagement per item.
    ///
    /// This is the measurement unlock: within-user clustering inflates A/B variance 6.2x, so a
    /// 20% effect needs ~48 days. Interleaving makes each user their own control — reported at
    /// 50-100x sensitivity (Airbnb: same conclusion on 0.5% of the duration, 4% of traffic).
    ///
    /// Unlike the shadow modes, this changes what users see: both rankers really contribute.
    pub interleave_enabled: bool,
    /// Control ranker name (see `Ranker::parse`). Invalid names disable the experiment rather
    /// than silently measuring the control against itself.
    pub interleave_control: String,
    /// Treatment ranker name.
    pub interleave_treatment: String,
    /// Percentage of eligible users enrolled.
    pub interleave_traffic_pct: u32,
    /// Bump to re-randomize both enrolment and the per-user draft coin flip.
    pub interleave_salt: String,

    /// Allow control and treatment to be the SAME ranker.
    ///
    /// Normally rejected as pointless, but it is the one production exposure that is provably
    /// feed-neutral: two identical lists produce all-shared, all-untagged items in the original
    /// order, so no user's feed changes while the full path (enrolment, draft, cache write/read,
    /// provenance) is exercised. Use it to validate the harness before pointing it at a real
    /// treatment.
    pub interleave_self_check: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // HTTP Server Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub http_host: String,
    pub http_port: u16,
    pub http_external: String,
    pub http_workers: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Generator Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub feed_generator_did: String,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Metrics Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub metrics_enabled: bool,
    pub metrics_port: u16,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Read-Only Mode Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub read_only_mode: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Admin API Authentication
    // ═══════════════════════════════════════════════════════════════════════════════
    /// When set, all non-ATProto / non-well-known endpoints require this key via Authorization: Bearer or X-API-Key.
    pub admin_api_key: Option<String>,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Personalization A/B Test Holdout
    // ═══════════════════════════════════════════════════════════════════════════════
    /// Fraction of first-page requests (0.0–1.0) served with non-personalized fallback blend
    /// for A/B testing. Default 0.5 = 50/50 split. Enables downstream comparison of engagement vs personalized.
    pub personalization_holdout_rate: f64,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Audit Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub audit_enabled: bool,
    pub audit_all_users: bool,
    pub audit_sample_rate: f64,
    pub audit_log_full_breakdown: bool,
    pub audit_max_contributors: usize,

    // ═══════════════════════════════════════════════════════════════════════════════
    // ClickHouse Configuration
    // ═══════════════════════════════════════════════════════════════════════════════
    pub clickhouse_host: String,
    pub clickhouse_port: u16,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_secure: bool,

    // ═══════════════════════════════════════════════════════════════════════════════
    // Privacy / opt-out (EXCLUSION_LIST)
    // ═══════════════════════════════════════════════════════════════════════════════
    pub exclusion_dids: Arc<HashSet<String>>,
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        Self {
            // Redis
            redis_url: default_env("REDIS_URL", "redis://localhost:6379"),
            redis_pool_size: parse_usize_env("REDIS_POOL_SIZE", 100),
            redis_connect_max_retries: parse_u32_env("REDIS_CONNECT_MAX_RETRIES", 10),
            redis_connect_initial_delay_ms: parse_u64_env("REDIS_CONNECT_INITIAL_DELAY_MS", 500),
            redis_requests_logger_url: std::env::var("REDIS_REQUESTS_LOGGER").ok().and_then(|s| {
                if s.trim().is_empty() {
                    None
                } else {
                    Some(s)
                }
            }),

            // Inverted Algorithm
            inverted_algorithm_enabled: parse_bool_env("INVERTED_ALGORITHM_ENABLED", true),
            inverted_min_post_likes: parse_usize_env("INVERTED_MIN_POST_LIKES", 10),
            inverted_max_likers_per_post: parse_usize_env("INVERTED_MAX_LIKERS_PER_POST", 30),
            inverted_max_posts_to_score: parse_usize_env("INVERTED_MAX_POSTS_TO_SCORE", 0),
            // Defaults to 0 (gate disabled) so merging cannot silently change what
            // users are served. Recommended rollout value: 500.
            min_candidate_pool_for_personalization: parse_usize_env(
                "MIN_CANDIDATE_POOL_FOR_PERSONALIZATION",
                0,
            ),
            min_overlapping_colikers: parse_usize_env("MIN_OVERLAPPING_COLIKERS", 1),
            user_data_check_days: parse_u32_env(
                "USER_DATA_CHECK_DAYS",
                graze_common::DEFAULT_RETENTION_DAYS,
            ),
            ab_experiment_enabled: parse_bool_env("AB_EXPERIMENT_ENABLED", false),
            ab_experiment_dimension: default_env("AB_EXPERIMENT_DIMENSION", "max_sources"),
            ab_experiment_values: default_env("AB_EXPERIMENT_VALUES", "250,10000")
                .split(',')
                .filter_map(|v| v.trim().parse::<usize>().ok())
                .collect(),
            ab_experiment_traffic_pct: parse_u32_env("AB_EXPERIMENT_TRAFFIC_PCT", 100).min(100),
            ab_experiment_salt: default_env("AB_EXPERIMENT_SALT", "v1"),
            thompson_persist_enabled: parse_bool_env("THOMPSON_PERSIST_ENABLED", false),
            thompson_persist_interval_seconds: parse_u64_env(
                "THOMPSON_PERSIST_INTERVAL_SECONDS",
                60,
            ),
            inverted_lookup_shadow_mode: parse_bool_env("INVERTED_LOOKUP_SHADOW_MODE", false),
            inverted_lookup_enabled: parse_bool_env("INVERTED_LOOKUP_ENABLED", false),
            inverted_coliker_like_days: parse_u32_env("INVERTED_COLIKER_LIKE_DAYS", 4),
            inverted_coliker_like_limit: parse_usize_env("INVERTED_COLIKER_LIKE_LIMIT", 500),
            min_pool_scoreable_share: parse_f64_env("MIN_POOL_SCOREABLE_SHARE", 0.0),
            personalization_holdout_salt: std::env::var("PERSONALIZATION_HOLDOUT_SALT")
                .unwrap_or_else(|_| "pholdout-v1".to_string()),
            walk_count: parse_usize_env("WALK_COUNT", 2000),
            walk_max_users: parse_usize_env("WALK_MAX_USERS", 256),
            walk_user_like_limit: parse_usize_env("WALK_USER_LIKE_LIMIT", 200),
            walk_early_stop_np: parse_usize_env("WALK_EARLY_STOP_NP", 200),
            walk_early_stop_nv: parse_u32_env("WALK_EARLY_STOP_NV", 3),
            walk_popularity_power: parse_f64_env("WALK_POPULARITY_POWER", 0.5),
            walk_min_visits: parse_u32_env("WALK_MIN_VISITS", 1),
            walk_min_seed_for_sampling: parse_usize_env("WALK_MIN_SEED_FOR_SAMPLING", 5),
            durable_profile_shadow_mode: parse_bool_env("DURABLE_PROFILE_SHADOW_MODE", false),
            durable_profile_enabled: parse_bool_env("DURABLE_PROFILE_ENABLED", false),
            durable_profile_min_post_likes: parse_usize_env("DURABLE_PROFILE_MIN_POST_LIKES", 5),
            durable_profile_max_likers_per_post: parse_usize_env(
                "DURABLE_PROFILE_MAX_LIKERS_PER_POST",
                100,
            ),
            durable_profile_weight_target: parse_f64_env(
                "DURABLE_PROFILE_WEIGHT_TARGET",
                0.0000002,
            ),

            // Liker Cache
            liker_cache_enabled: parse_bool_env("LIKER_CACHE_ENABLED", true),
            liker_cache_max_size: parse_usize_env("LIKER_CACHE_MAX_SIZE", 400000),
            liker_cache_ttl_seconds: parse_u64_env("LIKER_CACHE_TTL_SECONDS", 600),
            liker_cache_prewarm_count: parse_usize_env("LIKER_CACHE_PREWARM_COUNT", 10000),

            // Local Algo Cache
            local_algo_cache_ttl_seconds: parse_u64_env("LOCAL_ALGO_CACHE_TTL_SECONDS", 120),
            local_algo_cache_max_algos: parse_usize_env("LOCAL_ALGO_CACHE_MAX_ALGOS", 100),

            // Bloom Filter
            seen_bloom_enabled: parse_bool_env("SEEN_BLOOM_ENABLED", true),
            seen_bloom_expected_items: parse_usize_env("SEEN_BLOOM_EXPECTED_ITEMS", 10000),
            seen_bloom_false_positive_rate: parse_f64_env("SEEN_BLOOM_FALSE_POSITIVE_RATE", 0.01),
            seen_bloom_max_users: parse_usize_env("SEEN_BLOOM_MAX_USERS", 20000),

            // Trending Posts
            trending_posts_limit: parse_usize_env("TRENDING_POSTS_LIMIT", 500),
            trending_posts_ttl_hours: parse_u32_env("TRENDING_POSTS_TTL_HOURS", 1),
            trending_recency_decay_hours: parse_f64_env("TRENDING_RECENCY_DECAY_HOURS", 24.0),

            // Fallback Tranches
            fallback_personalization_ratio: parse_f64_env("FALLBACK_PERSONALIZATION_RATIO", 0.80),
            fallback_popular_ratio: parse_f64_env("FALLBACK_POPULAR_RATIO", 0.34),
            fallback_trending_ratio: parse_f64_env("FALLBACK_TRENDING_RATIO", 0.33),
            fallback_discovery_ratio: parse_f64_env("FALLBACK_DISCOVERY_RATIO", 0.33),
            fallback_stagger_factor: parse_f64_env("FALLBACK_STAGGER_FACTOR", 0.3),
            popular_posts_limit: parse_usize_env("POPULAR_POSTS_LIMIT", 500),
            popular_decay_hours: parse_f64_env("POPULAR_DECAY_HOURS", 48.0),
            popular_min_likes: parse_usize_env("POPULAR_MIN_LIKES", 10),
            velocity_window_hours: parse_f64_env("VELOCITY_WINDOW_HOURS", 6.0),
            velocity_min_likes: parse_usize_env("VELOCITY_MIN_LIKES", 3),
            velocity_posts_limit: parse_usize_env("VELOCITY_POSTS_LIMIT", 500),
            author_success_min_posts: parse_usize_env("AUTHOR_SUCCESS_MIN_POSTS", 3),
            author_success_decay_hours: parse_f64_env("AUTHOR_SUCCESS_DECAY_HOURS", 48.0),
            discovery_posts_limit: parse_usize_env("DISCOVERY_POSTS_LIMIT", 500),
            discovery_max_post_likes: parse_usize_env("DISCOVERY_MAX_POST_LIKES", 5),

            // Progressive Blending
            cold_user_max_likes: parse_usize_env("COLD_USER_MAX_LIKES", 5),
            warm_user_max_likes: parse_usize_env("WARM_USER_MAX_LIKES", 20),
            cold_user_trending_ratio: parse_f64_env("COLD_USER_TRENDING_RATIO", 0.8),
            warm_user_trending_ratio: parse_f64_env("WARM_USER_TRENDING_RATIO", 0.5),

            // Liked Posts Filter
            liked_posts_filter_enabled: parse_bool_env("LIKED_POSTS_FILTER_ENABLED", true),
            liked_posts_filter_max: parse_usize_env("LIKED_POSTS_FILTER_MAX", 2000),

            // Seen Posts
            seen_posts_ttl_hours: parse_u32_env("SEEN_POSTS_TTL_HOURS", 48),
            seen_posts_enabled: parse_bool_env("SEEN_POSTS_ENABLED", true),

            // Interactions Logging
            interactions_logging_enabled: parse_bool_env("INTERACTIONS_LOGGING_ENABLED", true),
            interactions_writer: default_env("INTERACTIONS_WRITER", "clickhouse")
                .to_lowercase()
                .trim()
                .to_string(),
            interactions_batch_interval_ms: parse_u64_env("INTERACTIONS_BATCH_INTERVAL_MS", 3000),
            interactions_batch_size: parse_usize_env("INTERACTIONS_BATCH_SIZE", 200),
            interactions_queue_capacity: parse_usize_env("INTERACTIONS_QUEUE_CAPACITY", 5000),

            // Special Posts
            special_posts_source: default_env("SPECIAL_POSTS_SOURCE", "remote")
                .to_lowercase()
                .trim()
                .to_string(),
            special_posts_api_base: default_env(
                "SPECIAL_POSTS_API_BASE",
                "https://api.graze.social/app/my_feeds",
            )
            .trim()
            .to_string(),
            special_posts_api_token: std::env::var("SPECIAL_POSTS_API_TOKEN")
                .unwrap_or_default()
                .trim()
                .to_string(),

            // Feed Access Sync
            feed_access_sync_enabled: parse_bool_env("FEED_ACCESS_SYNC_ENABLED", true),

            // Feed Cache
            feed_cache_ttl_seconds: parse_u64_env("FEED_CACHE_TTL_SECONDS", 600),
            feed_cache_size: parse_usize_env("FEED_CACHE_SIZE", 1000),
            feed_cache_enabled: parse_bool_env("FEED_CACHE_ENABLED", true),
            feed_cache_stale_threshold_seconds: parse_u64_env(
                "FEED_CACHE_STALE_THRESHOLD_SECONDS",
                120,
            ),
            feed_cache_batch_size: parse_usize_env("FEED_CACHE_BATCH_SIZE", 300),

            // Co-liker Pre-computation
            coliker_ttl_seconds: parse_u64_env("COLIKER_TTL_SECONDS", 21600),
            coliker_refresh_threshold_seconds: parse_u64_env(
                "COLIKER_REFRESH_THRESHOLD_SECONDS",
                3600,
            ),
            coliker_max_sources: parse_usize_env("COLIKER_MAX_SOURCES", 1000),
            coliker_enabled: parse_bool_env("COLIKER_ENABLED", true),
            linklonk_normalization_enabled: parse_bool_env("LINKLONK_NORMALIZATION_ENABLED", true),

            // Author-Affinity
            pool_cache_ttl_seconds: parse_u64_env("POOL_CACHE_TTL_SECONDS", 30),
            pool_cache_max_members: parse_usize_env("POOL_CACHE_MAX_MEMBERS", 600_000),
            follow_seed_read_enabled: parse_bool_env("FOLLOW_SEED_READ_ENABLED", false),
            follow_seed_weight_mode: std::env::var("FOLLOW_SEED_WEIGHT_MODE")
                .unwrap_or_else(|_| "uniform".to_string()),
            author_affinity_enabled: parse_bool_env("AUTHOR_AFFINITY_ENABLED", true),
            max_liked_authors_per_user: parse_usize_env("MAX_LIKED_AUTHORS_PER_USER", 500),
            max_likers_per_author: parse_usize_env("MAX_LIKERS_PER_AUTHOR", 1000),
            author_affinity_max_authors: parse_usize_env("AUTHOR_AFFINITY_MAX_AUTHORS", 100),
            author_affinity_max_colikers: parse_usize_env("AUTHOR_AFFINITY_MAX_COLIKERS", 300),
            author_affinity_max_likers_per_author: parse_usize_env(
                "AUTHOR_AFFINITY_MAX_LIKERS_PER_AUTHOR",
                100,
            ),
            author_affinity_time_window_hours: parse_u32_env(
                "AUTHOR_AFFINITY_TIME_WINDOW_HOURS",
                168,
            ),
            author_affinity_ttl_seconds: parse_u64_env("AUTHOR_AFFINITY_TTL_SECONDS", 3600),
            author_affinity_refresh_threshold_seconds: parse_u64_env(
                "AUTHOR_AFFINITY_REFRESH_THRESHOLD_SECONDS",
                600,
            ),
            author_affinity_max_posts_to_score: parse_usize_env(
                "AUTHOR_AFFINITY_MAX_POSTS_TO_SCORE",
                500,
            ),
            author_affinity_min_author_likes: parse_usize_env(
                "AUTHOR_AFFINITY_MIN_AUTHOR_LIKES",
                2,
            ),

            // Personalization Defaults
            stale_refresh_threshold_seconds: parse_u64_env("STALE_REFRESH_THRESHOLD_SECONDS", 60),
            default_max_user_likes: parse_usize_env("DEFAULT_MAX_USER_LIKES", 750),
            default_max_sources_per_post: parse_usize_env("DEFAULT_MAX_SOURCES_PER_POST", 100),
            default_min_co_likes: parse_usize_env("DEFAULT_MIN_CO_LIKES", 1),
            default_time_window_hours: parse_f64_env("DEFAULT_TIME_WINDOW_HOURS", 144.0),
            default_recency_half_life_hours: parse_f64_env("DEFAULT_RECENCY_HALF_LIFE_HOURS", 24.0),
            default_specificity_power: parse_f64_env("DEFAULT_SPECIFICITY_POWER", 1.0),
            default_popularity_power: parse_f64_env("DEFAULT_POPULARITY_POWER", 0.6),
            default_num_paths_power: parse_f64_env("DEFAULT_NUM_PATHS_POWER", 0.3),
            max_coliker_weight: parse_f64_env("MAX_COLIKER_WEIGHT", 0.000001),
            prove_max_posts_to_sample: parse_usize_env("PROVE_MAX_POSTS_TO_SAMPLE", 0),

            // Author Diversity
            diversity_enabled: parse_bool_env("DIVERSITY_ENABLED", true),
            max_posts_per_author: parse_usize_env("MAX_POSTS_PER_AUTHOR", 3),
            author_diminishing_factor: parse_f64_env("AUTHOR_DIMINISHING_FACTOR", 0.5),
            diversity_mmr_lambda: parse_f64_env("DIVERSITY_MMR_LAMBDA", 0.3),
            diversity_preserve_order: parse_bool_env("DIVERSITY_PRESERVE_ORDER", false),
            interleave_enabled: parse_bool_env("INTERLEAVE_ENABLED", false),
            interleave_control: default_env("INTERLEAVE_CONTROL", "post_first"),
            interleave_treatment: default_env("INTERLEAVE_TREATMENT", "post_first"),
            interleave_traffic_pct: parse_u32_env("INTERLEAVE_TRAFFIC_PCT", 100).min(100),
            interleave_salt: default_env("INTERLEAVE_SALT", "v1"),
            interleave_self_check: parse_bool_env("INTERLEAVE_SELF_CHECK", false),

            // HTTP Server
            http_host: default_env("HTTP_HOST", "0.0.0.0"),
            http_port: parse_u16_env("HTTP_PORT", 8080),
            http_external: default_env("HTTP_EXTERNAL", ""),
            http_workers: parse_usize_env("HTTP_WORKERS", 4),

            // Feed Generator
            feed_generator_did: default_env("FEED_GENERATOR_DID", "did:web:labs.graze.social"),

            // Metrics
            metrics_enabled: parse_bool_env("METRICS_ENABLED", true),
            metrics_port: parse_u16_env("METRICS_PORT", 9090),

            // Read-Only Mode
            read_only_mode: parse_bool_env("READ_ONLY_MODE", false),

            // Admin API key (empty string = auth disabled)
            admin_api_key: std::env::var("ADMIN_API_KEY").ok().and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s)
                }
            }),

            // Personalization Holdout (A/B test)
            // Cut from 0.5: half of authenticated first-page requests were skipping
            // personalization entirely, which halved the traffic any experiment could learn
            // from. 0.05 keeps a control group for absolute-lift measurement.
            personalization_holdout_rate: parse_f64_env("PERSONALIZATION_HOLDOUT_RATE", 0.05),

            // Audit
            audit_enabled: parse_bool_env("AUDIT_ENABLED", false),
            audit_all_users: parse_bool_env("AUDIT_ALL_USERS", false),
            audit_sample_rate: parse_f64_env("AUDIT_SAMPLE_RATE", 0.0),
            audit_log_full_breakdown: parse_bool_env("AUDIT_LOG_FULL_BREAKDOWN", false),
            audit_max_contributors: parse_usize_env("AUDIT_MAX_CONTRIBUTORS", 10),

            // ClickHouse
            clickhouse_host: default_env("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port: parse_u16_env("CLICKHOUSE_PORT", 8123),
            clickhouse_user: default_env("CLICKHOUSE_USER", "default"),
            clickhouse_password: default_env("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: default_env("CLICKHOUSE_DATABASE", "default"),
            clickhouse_secure: parse_bool_env("CLICKHOUSE_SECURE", false),

            exclusion_dids: exclusion_set_from_env_opt(std::env::var("EXCLUSION_LIST").ok()),
        }
    }

    /// Convert to RedisConfig for graze-common.
    pub fn redis_config(&self) -> RedisConfig {
        RedisConfig {
            url: self.redis_url.clone(),
            pool_size: self.redis_pool_size,
            connect_max_retries: self.redis_connect_max_retries,
            connect_initial_delay_ms: self.redis_connect_initial_delay_ms,
        }
    }
}

// Environment variable helper functions

fn default_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u16_env(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u32_env(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_f64_env(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(v.to_lowercase().as_str(), "true" | "1" | "yes"),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deploying the Phase B image must not change behaviour on its own. Both durable
    /// profile flags have to be opt-in, so shipping the binary is inert until someone
    /// sets the env var.
    #[test]
    fn durable_profile_flags_are_off_unless_set() {
        // Guard against a stray value in the ambient environment invalidating the check.
        if std::env::var("DURABLE_PROFILE_SHADOW_MODE").is_ok()
            || std::env::var("DURABLE_PROFILE_ENABLED").is_ok()
        {
            return;
        }
        let c = Config::from_env();
        assert!(
            !c.durable_profile_shadow_mode,
            "shadow mode must default off"
        );
        assert!(!c.durable_profile_enabled, "serving must default off");
    }

    /// The rescale target has to sit strictly below `max_coliker_weight`, or the scorer's
    /// `weight.min(cap)` clamps every profile entry to the same value and ranking collapses
    /// into arbitrary tie order.
    #[test]
    fn weight_target_stays_under_the_coliker_weight_cap() {
        if std::env::var("DURABLE_PROFILE_WEIGHT_TARGET").is_ok()
            || std::env::var("MAX_COLIKER_WEIGHT").is_ok()
        {
            return;
        }
        let c = Config::from_env();
        assert!(
            c.durable_profile_weight_target > 0.0
                && c.durable_profile_weight_target < c.max_coliker_weight,
            "target {} must be in (0, {})",
            c.durable_profile_weight_target,
            c.max_coliker_weight
        );
    }
}

#[cfg(test)]
mod durable_arm_tests {
    use super::*;

    /// The durable arm must not be able to change what the live arm serves. The only
    /// intended difference is `min_post_likes`; if a future edit accidentally points the
    /// live scorer at the relaxed value, this catches it.
    #[test]
    fn profile_min_post_likes_is_independent_of_the_live_filter() {
        if std::env::var("DURABLE_PROFILE_MIN_POST_LIKES").is_ok()
            || std::env::var("INVERTED_MIN_POST_LIKES").is_ok()
        {
            return;
        }
        let c = Config::from_env();
        assert_eq!(c.inverted_min_post_likes, 10, "live arm default unchanged");
        assert_eq!(c.durable_profile_min_post_likes, 5, "durable arm relaxed");
        assert!(
            c.durable_profile_min_post_likes <= c.inverted_min_post_likes,
            "relaxing the durable arm is the whole point; {} > {}",
            c.durable_profile_min_post_likes,
            c.inverted_min_post_likes
        );
    }

    /// `max_likers_per_post` is the dominant reach constraint: detection probability for a
    /// co-liker on a post with L likers is `min(1, N/L)`, and 72-100% of eligible candidates
    /// have L > 30. The durable arm must relax it, and the live arm must not move.
    #[test]
    fn profile_max_likers_is_relaxed_without_touching_the_live_arm() {
        if std::env::var("DURABLE_PROFILE_MAX_LIKERS_PER_POST").is_ok()
            || std::env::var("INVERTED_MAX_LIKERS_PER_POST").is_ok()
        {
            return;
        }
        let c = Config::from_env();
        assert_eq!(c.inverted_max_likers_per_post, 30, "live arm unchanged");
        assert_eq!(c.durable_profile_max_likers_per_post, 100);
        assert!(c.durable_profile_max_likers_per_post > c.inverted_max_likers_per_post);
    }
}

#[cfg(test)]
mod seed_gate_tests {
    use super::*;

    /// The seed gate must look back at least as far as the scorer reads, or users are turned
    /// away with `no_user_data` despite having seed the scorer would have used. Measured: a
    /// 2-day gate against a 6-day scorer lost 14.0% of all DAU (22.1% of seeded users).
    #[test]
    fn seed_gate_window_matches_what_the_scorer_reads() {
        if std::env::var("USER_DATA_CHECK_DAYS").is_ok() {
            return;
        }
        let c = Config::from_env();
        assert_eq!(
            c.user_data_check_days,
            graze_common::DEFAULT_RETENTION_DAYS,
            "gate window must equal the scorer's retention window"
        );
        assert!(
            c.user_data_check_days >= 2,
            "must not regress below the old 2-day behaviour"
        );
    }
}

#[cfg(test)]
mod inverted_lookup_tests {
    use super::*;

    /// Deploying must not change behaviour on its own: both arms opt-in.
    #[test]
    fn inverted_lookup_flags_default_off() {
        if std::env::var("INVERTED_LOOKUP_SHADOW_MODE").is_ok()
            || std::env::var("INVERTED_LOOKUP_ENABLED").is_ok()
        {
            return;
        }
        let c = Config::from_env();
        assert!(!c.inverted_lookup_shadow_mode);
        assert!(!c.inverted_lookup_enabled);
    }

    /// The co-liker like window must cover the pool's age span, or the inverted arm cannot see
    /// likes on the oldest pool posts. Pool age is capped at SYNC_PREFERRED_MAX_AGE_HOURS (72h
    /// = 3 days), so 4 days leaves a day of headroom.
    #[test]
    fn coliker_like_window_covers_the_pool_age_span() {
        if std::env::var("INVERTED_COLIKER_LIKE_DAYS").is_ok() {
            return;
        }
        let c = Config::from_env();
        assert!(
            c.inverted_coliker_like_days >= 4,
            "must cover the 3-day pool span plus headroom, got {}",
            c.inverted_coliker_like_days
        );
        assert!(
            c.inverted_coliker_like_days <= graze_common::DEFAULT_RETENTION_DAYS,
            "reading beyond retention wastes ops on keys that cannot exist"
        );
    }

    /// Truncation moves to the co-liker side, so the per-co-liker cap must be far more generous
    /// than the per-post cap it replaces — that is the whole point of the inversion.
    #[test]
    fn coliker_like_limit_is_generous_relative_to_the_per_post_cap() {
        if std::env::var("INVERTED_COLIKER_LIKE_LIMIT").is_ok() {
            return;
        }
        let c = Config::from_env();
        assert!(
            c.inverted_coliker_like_limit >= 10 * c.inverted_max_likers_per_post,
            "per-co-liker cap {} should dwarf the per-post cap {}",
            c.inverted_coliker_like_limit,
            c.inverted_max_likers_per_post
        );
    }
}

#[cfg(test)]
mod interleave_config_tests {
    use super::*;

    /// Deploying the image must not change any feed on its own.
    #[test]
    fn interleaving_defaults_off_and_neutral() {
        for k in [
            "INTERLEAVE_ENABLED",
            "INTERLEAVE_SELF_CHECK",
            "DIVERSITY_PRESERVE_ORDER",
            "INTERLEAVE_TREATMENT",
        ] {
            if std::env::var(k).is_ok() {
                return;
            }
        }
        let c = Config::from_env();
        assert!(!c.interleave_enabled);
        assert!(!c.interleave_self_check);
        assert!(!c.diversity_preserve_order);
        // Control == treatment by default, so even a stray INTERLEAVE_ENABLED=1 is a no-op
        // rather than an accidental live comparison against an unspecified ranker.
        assert_eq!(c.interleave_control, c.interleave_treatment);
    }

    /// The holdout cut is what makes interleaving worth running; guard it against regression.
    #[test]
    fn personalization_holdout_is_no_longer_half_the_traffic() {
        if std::env::var("PERSONALIZATION_HOLDOUT_RATE").is_ok() {
            return;
        }
        let c = Config::from_env();
        assert!(
            c.personalization_holdout_rate <= 0.1,
            "holdout {} would halve experiment throughput",
            c.personalization_holdout_rate
        );
        assert!(
            c.personalization_holdout_rate > 0.0,
            "keep a control group for absolute-lift measurement"
        );
    }
}

#[cfg(test)]
mod density_gate_guards {
    use super::*;

    #[test]
    fn density_gate_defaults_to_disabled() {
        // Must ship inert. Enabling it by default would silently stop personalizing feeds before we
        // have measured what the gate actually skips.
        let c = Config::from_env();
        assert_eq!(
            c.min_pool_scoreable_share, 0.0,
            "MIN_POOL_SCOREABLE_SHARE must default to 0.0 (disabled)"
        );
    }

    #[test]
    fn density_gate_would_separate_the_feeds_we_measured() {
        // Real measured shares at min_post_likes=10. A gate at 0.05 must keep the viable feeds and
        // skip the dead ones — including the pair that pool SIZE cannot separate (8352 is smaller
        // than 5395 but far denser).
        let measured: [(i32, f64, bool); 6] = [
            (1988, 0.29, true),
            (396, 0.36, true),
            (8352, 0.22, true),  // 596 posts  — small but dense
            (5395, 0.02, false), // 1,000 posts — larger but sparse
            (4051, 0.01, false),
            (30237, 0.003, false),
        ];
        let gate = 0.05;
        for (algo, share, expect_personalize) in measured {
            assert_eq!(
                share >= gate,
                expect_personalize,
                "algo {algo} at share {share} classified wrongly by gate {gate}"
            );
        }
    }
}
