-- LinkLonk Personalization Algorithm
--
-- Performs a 3-step random walk on the like graph:
-- 1. Get user's recent likes
-- 2. Find other users who liked those posts (sources) OR use pre-computed co-likers
--    - Optionally filter sources to only those who have liked algo posts (1-hop neighborhood)
-- 3. Recommend posts that sources have liked
--
-- KEYS:
--   1: User likes key (ul:{user_hash})
--   2: Algorithm posts key (ap:{algo_id})
--   3: Result key (ll:{algo_id}:{user_hash})
--   4: URI to ID mapping (uri2id)
--   5: ID to URI mapping (id2uri)
--   6: User seen posts key (seen:{user_hash})
--   7: Pre-computed co-likers key (colikes:{user_hash}) - optional
--   8: Algo likers key (al:{algo_id}) - optional, 1-hop neighborhood for source pruning
--
-- ARGV:
--   1: max_user_likes
--   2: max_sources_per_post
--   3: max_total_sources
--   4: min_co_likes (unused in this version, for future)
--   5: time_window_seconds
--   6: recency_half_life_seconds
--   7: specificity_power
--   8: popularity_power
--   9: result_ttl_seconds
--   10: current_timestamp (unix seconds)
--   11: max_algo_checks (max SISMEMBER calls for algo posts)
--   12: use_precomputed_colikes (1 to use, 0 to compute fresh)
--   13: max_seen_posts (max seen posts to load for filtering)
--   14: use_algo_likers (1 to filter sources by algo neighborhood, 0 to skip)
--   15: num_paths_power (exponent for boosting posts with more distinct paths, default 0.3)
--   16: seed_sample_pool (0 = disabled; >0 = fetch this many likes, randomly sample max_user_likes)
--   17: corater_decay (0.0-1.0; per-rank decay factor for co-liker contributions)
--
-- Returns: Number of posts scored

local user_likes_key = KEYS[1]
local algo_posts_key = KEYS[2]
local result_key = KEYS[3]
local uri2id_key = KEYS[4]
local id2uri_key = KEYS[5]
local user_seen_key = KEYS[6]
local colikes_key = KEYS[7]  -- Pre-computed co-likers (optional)
local algo_likers_key = KEYS[8]  -- 1-hop neighborhood for source pruning (optional)

local max_user_likes = tonumber(ARGV[1])
local max_sources_per_post = tonumber(ARGV[2])
local max_total_sources = tonumber(ARGV[3])
-- local min_co_likes = tonumber(ARGV[4])  -- Reserved for future use
local time_window_seconds = tonumber(ARGV[5])
local recency_half_life_seconds = tonumber(ARGV[6])
local specificity_power = tonumber(ARGV[7])
local popularity_power = tonumber(ARGV[8])
local result_ttl = tonumber(ARGV[9])
local now = tonumber(ARGV[10])
local max_algo_checks = tonumber(ARGV[11])
local use_precomputed_colikes = tonumber(ARGV[12]) or 0
local max_seen_posts = tonumber(ARGV[13])
local use_algo_likers = tonumber(ARGV[14]) or 0
local num_paths_power = tonumber(ARGV[15]) or 0.3
local seed_sample_pool = tonumber(ARGV[16]) or 0
local corater_decay = tonumber(ARGV[17]) or 0

-- Calculate time window boundaries
local min_time = now - time_window_seconds

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 0: Load user's seen posts (to exclude from recommendations)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Get most recent N seen posts (ZREVRANGE returns highest scores = most recent timestamps first)
local seen_posts_raw = redis.call('ZREVRANGE', user_seen_key, 0, max_seen_posts - 1)
local seen_posts = {}
for _, post_id in ipairs(seen_posts_raw) do
    seen_posts[post_id] = true
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: Get user's recent likes (post IDs they've liked)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Get user's likes within time window, most recent first
-- Stored as: ZADD ul:{hash} <timestamp> <post_id>
-- If seed_sample_pool > 0, fetch a larger pool and randomly sample max_user_likes
local fetch_limit = max_user_likes
if seed_sample_pool > 0 and seed_sample_pool > max_user_likes then
    fetch_limit = seed_sample_pool
end
local user_likes = redis.call('ZREVRANGEBYSCORE', user_likes_key, now, min_time, 'WITHSCORES', 'LIMIT', 0, fetch_limit)

if #user_likes == 0 then
    -- User has no likes, return empty result
    redis.call('DEL', result_key)
    return 0
end

-- Random seed sampling: Fisher-Yates shuffle on pairs then truncate
if seed_sample_pool > 0 and #user_likes > max_user_likes * 2 then
    -- Build array of (post_id, like_time) pairs
    local pairs_count = #user_likes / 2
    local pair_indices = {}
    for i = 1, pairs_count do
        pair_indices[i] = i
    end
    -- Fisher-Yates shuffle
    for i = pairs_count, 2, -1 do
        local j = math.random(1, i)
        pair_indices[i], pair_indices[j] = pair_indices[j], pair_indices[i]
    end
    -- Take first max_user_likes pairs
    local sampled = {}
    local take = math.min(max_user_likes, pairs_count)
    for k = 1, take do
        local idx = pair_indices[k]
        local base = (idx - 1) * 2
        sampled[#sampled + 1] = user_likes[base + 1]
        sampled[#sampled + 1] = user_likes[base + 2]
    end
    user_likes = sampled
end

-- Parse likes into {post_id -> like_timestamp}
local liked_posts = {}
local liked_post_ids = {}
for i = 1, #user_likes, 2 do
    local post_id = user_likes[i]
    local like_time = tonumber(user_likes[i + 1])
    liked_posts[post_id] = like_time
    table.insert(liked_post_ids, post_id)
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: Find source users (people who liked the same posts BEFORE the user)
--         OR use pre-computed co-likers if available
--         Optionally filter to only sources in the algo's 1-hop neighborhood
-- ═══════════════════════════════════════════════════════════════════════════════

-- source_weights[did_hash] = accumulated weight
local source_weights = {}
local total_sources = 0
local used_precomputed = false
local sources_pruned = 0
local sources_kept = 0

-- Check if algo_likers filtering is enabled and the set exists
local algo_likers_enabled = false
if use_algo_likers == 1 and algo_likers_key and algo_likers_key ~= '' then
    local algo_likers_exists = redis.call('EXISTS', algo_likers_key)
    if algo_likers_exists == 1 then
        algo_likers_enabled = true
    end
end

-- Helper function to check if a source should be included
-- Returns true if source is in algo neighborhood (or filtering disabled)
local function should_include_source(source_did)
    if not algo_likers_enabled then
        return true  -- No filtering, include all
    end
    -- O(1) check: is this source in the algo's 1-hop neighborhood?
    return redis.call('SISMEMBER', algo_likers_key, source_did) == 1
end

-- Check if we should use pre-computed co-likers
if use_precomputed_colikes == 1 and colikes_key and colikes_key ~= '' then
    local colikes_exists = redis.call('EXISTS', colikes_key)
    if colikes_exists == 1 then
        -- Use pre-computed co-likers (O(1) lookup)
        local precomputed = redis.call('ZREVRANGEBYSCORE', colikes_key, '+inf', '0', 'WITHSCORES', 'LIMIT', 0, max_total_sources * 2)

        for i = 1, #precomputed, 2 do
            local source_did = precomputed[i]
            local weight = tonumber(precomputed[i + 1])
            
            -- Apply algo_likers filter: only keep sources who have liked algo posts
            if should_include_source(source_did) then
                source_weights[source_did] = weight
                total_sources = total_sources + 1
                sources_kept = sources_kept + 1
            else
                sources_pruned = sources_pruned + 1
            end
            
            if total_sources >= max_total_sources then
                break
            end
        end

        used_precomputed = true
    end
end

-- Fall back to real-time computation if pre-computed data not available
if not used_precomputed then
    for _, post_id in ipairs(liked_post_ids) do
        if total_sources >= max_total_sources then
            break
        end

        local user_like_time = liked_posts[post_id]

        -- Post ID is sufficient for pl: key lookup
        local post_likers_key = 'pl:' .. post_id
        local exists = redis.call('EXISTS', post_likers_key)
        if exists == 1 then

            -- Get users who liked this post BEFORE the current user
            -- Fetch extra to account for pruning
            local fetch_limit = algo_likers_enabled and (max_sources_per_post * 2) or max_sources_per_post
            local likers = redis.call('ZRANGEBYSCORE', post_likers_key, min_time, user_like_time - 1, 'WITHSCORES', 'LIMIT', 0, fetch_limit)

            for i = 1, #likers, 2 do
                local source_did = likers[i]
                local source_like_time = tonumber(likers[i + 1])

                -- Apply algo_likers filter: only keep sources who have liked algo posts
                if should_include_source(source_did) then
                    -- Calculate recency weight for this source's like
                    local age_seconds = now - source_like_time
                    local recency_weight = math.exp(-0.693 * age_seconds / recency_half_life_seconds)

                    -- Accumulate weight for this source
                    source_weights[source_did] = (source_weights[source_did] or 0) + recency_weight
                    total_sources = total_sources + 1
                    sources_kept = sources_kept + 1
                else
                    sources_pruned = sources_pruned + 1
                end

                if total_sources >= max_total_sources then
                    break
                end
            end
        end
    end
end

if total_sources == 0 then
    -- No sources found
    redis.call('DEL', result_key)
    return 0
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: Get posts liked by sources and compute scores (TWO-PASS APPROACH)
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Instead of loading all ~20k algorithm posts via SMEMBERS, we use a two-pass
-- approach that only checks SISMEMBER for top-scoring candidates:
--   Pass 1: Collect all candidate posts with scores (without algo check)
--   Pass 2: Sort by score, check SISMEMBER only for top N candidates
--

-- PASS 1: Collect ALL candidate posts with scores (no algo filter yet)
-- candidate_scores[post_id] = accumulated raw score
local candidate_scores = {}
-- candidate_liker_counts[post_id] = number of sources who liked it
local candidate_liker_counts = {}

for source_did, source_weight in pairs(source_weights) do
    local source_likes_key = 'ul:' .. source_did

    -- Get source's recent likes
    local source_likes = redis.call('ZREVRANGEBYSCORE', source_likes_key, now, min_time, 'WITHSCORES', 'LIMIT', 0, max_user_likes)

    local source_like_count = #source_likes / 2

    -- Apply specificity penalty: users who like everything get less weight
    local specificity_penalty = 1.0
    if specificity_power > 0 and source_like_count > 0 then
        specificity_penalty = math.pow(100 / math.max(source_like_count, 100), specificity_power)
    end

    local adjusted_weight = source_weight * specificity_penalty

    for i = 1, #source_likes, 2 do
        local post_id = source_likes[i]
        local like_time = tonumber(source_likes[i + 1])

        -- Skip posts the user has already liked or seen
        -- Note: We do NOT check algo membership here - that's Pass 2
        if not liked_posts[post_id] and not seen_posts[post_id] then
            -- Calculate recency weight for this recommendation
            local age_seconds = now - like_time
            local recency_weight = math.exp(-0.693 * age_seconds / recency_half_life_seconds)

            -- Apply corater decay: (1 - decay)^rank where rank 0 = most recent
            local decay_mult = 1.0
            if corater_decay > 0 then
                local rank = (i - 1) / 2
                decay_mult = math.pow(1.0 - corater_decay, rank)
            end

            local score = adjusted_weight * recency_weight * decay_mult
            candidate_scores[post_id] = (candidate_scores[post_id] or 0) + score
            candidate_liker_counts[post_id] = (candidate_liker_counts[post_id] or 0) + 1
        end
    end
end

-- Convert candidates to sortable array
local candidates = {}
for post_id, score in pairs(candidate_scores) do
    table.insert(candidates, {id = post_id, score = score})
end

-- Sort by score descending (highest scores first)
table.sort(candidates, function(a, b) return a.score > b.score end)

-- PASS 2: Check SISMEMBER for top N candidates only
-- This avoids loading all 20k algo posts into memory
local post_scores = {}
local post_liker_counts = {}
local checks_made = 0

for _, candidate in ipairs(candidates) do
    if checks_made >= max_algo_checks then
        break
    end

    -- Lazy SISMEMBER check - only for top candidates
    local is_algo_post = redis.call('SISMEMBER', algo_posts_key, candidate.id)
    checks_made = checks_made + 1

    if is_algo_post == 1 then
        post_scores[candidate.id] = candidate.score
        post_liker_counts[candidate.id] = candidate_liker_counts[candidate.id]
    end
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: Apply popularity penalty and store results
-- ═══════════════════════════════════════════════════════════════════════════════

-- Delete existing result
redis.call('DEL', result_key)

local scored_count = 0
local results = {}

for post_id, raw_score in pairs(post_scores) do
    local num_paths = post_liker_counts[post_id] or 1
    local liker_count = num_paths  -- same as path count from our candidate set (for popularity we use algo liker count when available; here we use path count as proxy)
    
    -- (1) Num paths exponent: boost posts with more distinct co-liker paths
    local paths_boost = 1.0
    if num_paths_power > 0 and num_paths > 0 then
        paths_boost = math.pow(math.max(1, num_paths), num_paths_power)
    end
    local score_after_paths = raw_score * paths_boost
    
    -- (2) Popularity exponent: demote viral posts
    local popularity_penalty = 1.0
    if popularity_power > 0 and liker_count > 1 then
        popularity_penalty = math.pow(1 / liker_count, popularity_power * 0.5)
    end
    
    local final_score = score_after_paths * popularity_penalty
    
    -- Add to results (will be stored in sorted set)
    table.insert(results, final_score)
    table.insert(results, post_id)
    scored_count = scored_count + 1
end

-- Store results in sorted set (batched to avoid unpack() stack limits)
if #results > 0 then
    local batch_size = 1000  -- 500 score+member pairs
    for i = 1, #results, batch_size do
        local batch = {}
        for j = i, math.min(i + batch_size - 1, #results) do
            table.insert(batch, results[j])
        end
        if #batch > 0 then
            redis.call('ZADD', result_key, unpack(batch))
        end
    end
    redis.call('EXPIRE', result_key, result_ttl)
end

return scored_count
