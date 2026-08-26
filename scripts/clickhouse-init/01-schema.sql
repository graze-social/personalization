-- =============================================================================
-- Graze Personalization Service — ClickHouse Schema
-- =============================================================================
-- All tables use the Buffer → MergeTree pattern for high-throughput inserts.
-- Buffer parameters: (num_layers, min_time, max_time, min_rows, max_rows,
--                     min_bytes, max_bytes)
--   flush conditions: ANY of (elapsed >= max_time) OR (rows >= max_rows)
--                               OR (bytes >= max_bytes)
-- =============================================================================

-- =============================================================================
-- feed_interactions: raw ATProto sendInteractions events
-- =============================================================================
CREATE TABLE IF NOT EXISTS feed_interactions
(
    did                        String,
    impression_id              String,        -- join key with feed_impressions
    interaction_feed_context   String,        -- raw base64 feedContext blob
    feed_uri                   String,
    attribution                LowCardinality(String),
    interaction_item           String,        -- AT-URI of the post
    interaction_event          LowCardinality(String),  -- e.g. app.bsky.feed.defs#interactionLike
    interaction_request_id     String,
    occurred                   DateTime64(3, 'UTC'),

    INDEX idx_impression_id    impression_id   TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_did              did             TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_event            interaction_event TYPE set(20) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(occurred)
ORDER BY (interaction_event, occurred)
TTL occurred + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS feed_interactions_buffer AS feed_interactions
ENGINE = Buffer(
    currentDatabase(), 'feed_interactions',
    16,         -- num_layers
    10, 100,    -- min_time, max_time (seconds)
    10000, 1000000,   -- min_rows, max_rows
    10485760, 104857600  -- min_bytes (10MB), max_bytes (100MB)
);

-- =============================================================================
-- user_action_logs: per-user action aggregates for analytics
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_action_logs
(
    algo_id          Int32,
    user_did         String,
    action_type      LowCardinality(String),
    action_identifier String,
    action_time      DateTime64(3, 'UTC'),
    action_count     UInt32,

    INDEX idx_user_did user_did TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(action_time)
ORDER BY (algo_id, user_did, action_time)
TTL action_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS user_action_logs_buffer AS user_action_logs
ENGINE = Buffer(
    currentDatabase(), 'user_action_logs',
    8,
    10, 100,
    10000, 500000,
    10485760, 52428800  -- 10MB, 50MB
);

-- =============================================================================
-- algorithm_posts_v2: candidate posts per algorithm (read by candidate_sync)
-- =============================================================================
CREATE TABLE IF NOT EXISTS algorithm_posts_v2
(
    algo_id              Int32,
    uri                  String,
    bluesky_created_at   DateTime64(3, 'UTC'),
    created_at           DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(bluesky_created_at)
ORDER BY (algo_id, bluesky_created_at DESC)
TTL bluesky_created_at + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- hidden_status: per-algorithm hidden posts (read by candidate_sync to exclude)
--
-- May be created and maintained by another service. candidate_sync joins to
-- this table so that hidden posts are not synced into Redis. One row per
-- (algo_id, uri) with hidden = true when the post is hidden for that feed.
-- =============================================================================
CREATE TABLE IF NOT EXISTS hidden_status
(
    algo_id    UInt32,
    uri        String,
    hidden     Bool DEFAULT false,
    updated_at DateTime DEFAULT now(),

    INDEX idx_uri_bloom uri TYPE bloom_filter(0.01) GRANULARITY 3
)
ENGINE = MergeTree()
ORDER BY (algo_id, uri)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- feed_impressions: ML feature vectors logged at feed serve time
--
-- One row per (request, post) pair — i.e. every post position in every served
-- feed. Joined to feed_interactions on impression_id to build training labels.
--
-- Scaling: at 100M req/day × 30 posts/req = 3B rows/day → partition by month,
-- with 90-day TTL gives ~9B rows peak. ClickHouse handles this comfortably with
-- MergeTree + monthly partitioning.
-- =============================================================================
CREATE TABLE IF NOT EXISTS feed_impressions
(
    -- Identity
    impression_id          String,        -- 16-char hex, unique per (request, post)
    user_hash              String,        -- hashed DID (anonymized)
    post_id                String,        -- internal integer ID as string
    algo_id                Int32,
    served_at              DateTime64(3, 'UTC'),

    -- Feed context (from FeedContextProvenance)
    depth                  UInt8,         -- 0-based position in feed response
    source                 LowCardinality(String),  -- personalized|fallback|author_affinity|...
    is_holdout             Bool,
    is_exploration         Bool,
    response_time_ms       Float32,
    is_first_page          Bool,

    -- Scoring features (from scorer hot loop)
    raw_score              Float32,       -- sum of co-liker weighted contributions
    final_score            Float32,       -- after paths_boost and popularity_penalty
    num_paths              UInt16,        -- distinct co-likers who matched this post
    liker_count            UInt32,        -- global post popularity (total likes)
    popularity_penalty     Float32,       -- (1/liker_count)^(pop_power*0.5)
    paths_boost            Float32,       -- num_paths^num_paths_power
    max_contribution       Float32,       -- largest single co-liker contribution
    score_concentration    Float32,       -- max_contribution / raw_score
    newest_like_age_hours  Float32,       -- age of most recent matching co-liker like
    oldest_like_age_hours  Float32,       -- age of oldest matching co-liker like
    was_liker_cache_hit    Bool,          -- whether liker data came from in-process cache

    -- Network features (computed from source_weights before scoring loop)
    coliker_count          UInt16,        -- total co-likers in the user's graph
    top_coliker_weight     Float32,       -- weight of strongest co-liker
    top5_weight_sum        Float32,       -- sum of top-5 co-liker weights
    mean_coliker_weight    Float32,       -- mean co-liker weight
    weight_concentration   Float32,       -- top1 / mean (Herfindahl-style)

    -- User features (from blending logic)
    user_like_count        UInt32,        -- total likes by this user in retention window
    user_segment           LowCardinality(String),  -- cold|warm|active

    -- Request quality (from ScoringResult)
    richness_ratio         Float32,       -- posts_scored / posts_checked

    -- Time features
    hour_of_day            UInt8,
    day_of_week            UInt8,         -- 0=Monday ... 6=Sunday

    INDEX idx_impression_id impression_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_user_hash     user_hash     TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_algo_id       algo_id       TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(served_at)
ORDER BY (algo_id, user_hash, served_at)
TTL served_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS feed_impressions_buffer AS feed_impressions
ENGINE = Buffer(
    currentDatabase(), 'feed_impressions',
    16,
    10, 100,
    10000, 1000000,
    10485760, 104857600
);

-- =============================================================================
-- Training helper view
--
-- Joins impressions with interactions to produce labelled training rows.
-- Use in Python training script as:
--   SELECT * FROM training_labels WHERE served_at BETWEEN x AND y
--
-- Positive examples: any_positive = 1
-- Explicit negatives: see_less = 1
-- Implicit negatives: seen_no_engage = 1 (seen but no positive + no see_less)
-- =============================================================================
CREATE VIEW IF NOT EXISTS training_labels AS
SELECT
    i.impression_id,
    i.user_hash,
    i.post_id,
    i.algo_id,
    i.served_at,
    i.depth,
    i.source,
    i.is_holdout,
    i.is_exploration,
    i.response_time_ms,
    i.is_first_page,
    i.raw_score,
    i.final_score,
    i.num_paths,
    i.liker_count,
    i.popularity_penalty,
    i.paths_boost,
    i.max_contribution,
    i.score_concentration,
    i.newest_like_age_hours,
    i.oldest_like_age_hours,
    i.was_liker_cache_hit,
    i.coliker_count,
    i.top_coliker_weight,
    i.top5_weight_sum,
    i.mean_coliker_weight,
    i.weight_concentration,
    i.user_like_count,
    i.user_segment,
    i.richness_ratio,
    i.hour_of_day,
    i.day_of_week,
    -- Outcome counts per interaction type
    countIf(ie.interaction_event LIKE '%interactionLike')    AS like_count,
    countIf(ie.interaction_event LIKE '%interactionRepost')  AS repost_count,
    countIf(ie.interaction_event LIKE '%interactionReply')   AS reply_count,
    countIf(ie.interaction_event LIKE '%interactionQuote')   AS quote_count,
    countIf(ie.interaction_event LIKE '%interactionSeeMore') AS see_more_count,
    countIf(ie.interaction_event LIKE '%interactionSeen')    AS seen_count,
    countIf(ie.interaction_event LIKE '%seeLess%')           AS see_less_count,
    -- Binary labels
    (like_count > 0)     AS liked,
    (repost_count > 0)   AS reposted,
    (reply_count > 0)    AS replied,
    (quote_count > 0)    AS quoted,
    (see_more_count > 0) AS see_more,
    (see_less_count > 0) AS see_less,
    -- Composite positive: any deliberate positive signal
    (liked OR reposted OR replied OR quoted OR see_more) AS any_positive,
    -- Implicit negative: seen by client but no positive and no explicit dislike
    (seen_count > 0 AND NOT any_positive AND NOT see_less)   AS seen_no_engage
FROM feed_impressions i
LEFT JOIN feed_interactions ie
    ON ie.impression_id = i.impression_id
GROUP BY
    i.impression_id, i.user_hash, i.post_id, i.algo_id, i.served_at,
    i.depth, i.source, i.is_holdout, i.is_exploration, i.response_time_ms,
    i.is_first_page, i.raw_score, i.final_score, i.num_paths, i.liker_count,
    i.popularity_penalty, i.paths_boost, i.max_contribution, i.score_concentration,
    i.newest_like_age_hours, i.oldest_like_age_hours, i.was_liker_cache_hit,
    i.coliker_count, i.top_coliker_weight, i.top5_weight_sum, i.mean_coliker_weight,
    i.weight_concentration, i.user_like_count, i.user_segment, i.richness_ratio,
    i.hour_of_day, i.day_of_week;
