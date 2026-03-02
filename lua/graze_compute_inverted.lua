-- LinkLonk Personalization Algorithm (INVERTED)
--
-- This is an inverted version of the algorithm optimized for small-to-medium feeds (<=20K posts).
-- Instead of starting from sources and filtering to algo posts (97% waste),
-- we start from algo posts and check which have co-liker engagement (0% waste).
--
-- Performance: O(algo_posts) instead of O(sources * likes_per_source)
-- For a 2.5K post feed: ~2.5K post checks instead of ~150K candidate evaluations
--
-- KEYS:
--   1: User likes key (ul:{user_hash})
--   2: Algorithm posts key (ap:{algo_id})
--   3: Result key (ll:{algo_id}:{user_hash})
--   4: URI to ID mapping (uri2id) - unused in inverted
--   5: ID to URI mapping (id2uri) - unused in inverted
--   6: User seen posts key (seen:{user_hash})
--   7: Pre-computed co-likers key (colikes:{user_hash}) - optional
--   8: Algo likers key (al:{algo_id}) - unused in inverted (we iterate algo posts directly)
--
-- ARGV:
--   1: max_user_likes
--   2: max_sources_per_post
--   3: max_total_sources
--   4: min_co_likes (unused in this version)
--   5: time_window_seconds
--   6: recency_half_life_seconds
--   7: specificity_power
--   8: popularity_power
--   9: result_ttl_seconds
--   10: current_timestamp (unix seconds)
--   11: max_likers_per_post (default 30, reduced from 100 for speedup)
--   12: use_precomputed_colikes (1 to use, 0 to compute fresh)
--   13: max_seen_posts (max seen posts to load for filtering)
--   14: use_algo_likers - unused in inverted mode
--   15: max_posts_to_score (limit algo posts to process, 0 = all)
--   16: min_post_likes (skip posts with fewer likes, default 5)
--   17: num_paths_power (exponent for boosting posts with more distinct paths, default 0.3)
--
-- Optimizations in this script:
--   1. Skip posts with 0 likes (can never score > 0)
--   2. Skip posts with < min_post_likes (statistically unlikely to have co-liker overlap)
--   3. Reduced max_likers_per_post from 100 to 30 (top likers have most signal)
--   4. Early ZCARD check before fetching full liker list
--
-- Returns: Number of posts scored

local user_likes_key = KEYS[1]
local algo_posts_key = KEYS[2]
local result_key = KEYS[3]
-- local uri2id_key = KEYS[4]  -- unused
-- local id2uri_key = KEYS[5]  -- unused
local user_seen_key = KEYS[6]
local colikes_key = KEYS[7]

local max_user_likes = tonumber(ARGV[1])
local max_sources_per_post = tonumber(ARGV[2])
local max_total_sources = tonumber(ARGV[3])
-- local min_co_likes = tonumber(ARGV[4])  -- Reserved
local time_window_seconds = tonumber(ARGV[5])
local recency_half_life_seconds = tonumber(ARGV[6])
local specificity_power = tonumber(ARGV[7])
local popularity_power = tonumber(ARGV[8])
local result_ttl = tonumber(ARGV[9])
local now = tonumber(ARGV[10])
local max_likers_per_post = tonumber(ARGV[11]) or 30  -- Reduced from 100 for speedup
local use_precomputed_colikes = tonumber(ARGV[12]) or 0
local max_seen_posts = tonumber(ARGV[13]) or 1000
-- use_algo_likers unused in inverted
local max_posts_to_score = tonumber(ARGV[15]) or 0  -- 0 = all posts
local min_post_likes = tonumber(ARGV[16]) or 5  -- Skip posts with fewer likes (speedup)
local num_paths_power = tonumber(ARGV[17]) or 0.3

-- Calculate time window boundaries
local min_time = now - time_window_seconds

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 0: Load user's seen posts (to exclude from recommendations)
-- ═══════════════════════════════════════════════════════════════════════════════

local seen_posts_raw = redis.call('ZREVRANGE', user_seen_key, 0, max_seen_posts - 1)
local seen_posts = {}
for _, post_id in ipairs(seen_posts_raw) do
    seen_posts[post_id] = true
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1: Get user's recent likes (to exclude from recommendations)
-- ═══════════════════════════════════════════════════════════════════════════════

local user_likes = redis.call('ZREVRANGEBYSCORE', user_likes_key, now, min_time, 'WITHSCORES', 'LIMIT', 0, max_user_likes)

if #user_likes == 0 then
    redis.call('DEL', result_key)
    return 0
end

-- Parse likes into set for fast lookup
local liked_posts = {}
local liked_post_ids = {}
for i = 1, #user_likes, 2 do
    local post_id = user_likes[i]
    local like_time = tonumber(user_likes[i + 1])
    liked_posts[post_id] = like_time
    table.insert(liked_post_ids, post_id)
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2: Build co-likers table with weights
-- Same as original algorithm - we need to know WHO the user's taste-mates are
-- ═══════════════════════════════════════════════════════════════════════════════

local source_weights = {}  -- source_hash -> weight
local total_sources = 0
local used_precomputed = false

-- Try pre-computed co-likers first
if use_precomputed_colikes == 1 and colikes_key and colikes_key ~= '' then
    local colikes_exists = redis.call('EXISTS', colikes_key)
    if colikes_exists == 1 then
        local precomputed = redis.call('ZREVRANGEBYSCORE', colikes_key, '+inf', '0', 'WITHSCORES', 'LIMIT', 0, max_total_sources)
        
        for i = 1, #precomputed, 2 do
            local source_did = precomputed[i]
            local weight = tonumber(precomputed[i + 1])
            source_weights[source_did] = weight
            total_sources = total_sources + 1
        end
        used_precomputed = true
    end
end

-- Fall back to real-time computation
if not used_precomputed then
    for _, post_id in ipairs(liked_post_ids) do
        if total_sources >= max_total_sources then
            break
        end
        
        local user_like_time = liked_posts[post_id]
        local post_likers_key = 'pl:' .. post_id
        local exists = redis.call('EXISTS', post_likers_key)
        
        if exists == 1 then
            local likers = redis.call('ZRANGEBYSCORE', post_likers_key, min_time, user_like_time - 1, 'WITHSCORES', 'LIMIT', 0, max_sources_per_post)
            
            for i = 1, #likers, 2 do
                local source_did = likers[i]
                local source_like_time = tonumber(likers[i + 1])
                
                local age_seconds = now - source_like_time
                local recency_weight = math.exp(-0.693 * age_seconds / recency_half_life_seconds)
                
                source_weights[source_did] = (source_weights[source_did] or 0) + recency_weight
                total_sources = total_sources + 1
                
                if total_sources >= max_total_sources then
                    break
                end
            end
        end
    end
end

if total_sources == 0 then
    redis.call('DEL', result_key)
    return 0
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3: INVERTED APPROACH - Iterate algo posts, check co-liker overlap
-- 
-- Instead of: sources -> their likes -> filter to algo (97% waste)
-- We do:      algo posts -> their likers -> match co-likers (0% waste)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Get all algo posts (bounded by 20K hard limit)
local algo_posts = redis.call('SMEMBERS', algo_posts_key)

if #algo_posts == 0 then
    redis.call('DEL', result_key)
    return 0
end

-- Optionally limit posts to score (for very large feeds)
local posts_to_process = algo_posts
if max_posts_to_score > 0 and #algo_posts > max_posts_to_score then
    -- Shuffle and take first N (simple random sampling)
    -- For deterministic results, we just take first N
    posts_to_process = {}
    for i = 1, max_posts_to_score do
        posts_to_process[i] = algo_posts[i]
    end
end

-- Score each algo post based on co-liker engagement
local post_scores = {}
local post_liker_counts = {}
local posts_with_engagement = 0
local posts_skipped_no_likers = 0
local posts_skipped_few_likers = 0

for _, post_id in ipairs(posts_to_process) do
    -- Skip posts user already liked or seen
    if not liked_posts[post_id] and not seen_posts[post_id] then
        local post_likers_key = 'pl:' .. post_id
        
        -- OPTIMIZATION 1: Skip posts with zero likes (they can never score > 0)
        local liker_count = redis.call('ZCARD', post_likers_key)
        
        if liker_count == 0 then
            posts_skipped_no_likers = posts_skipped_no_likers + 1
        elseif liker_count < min_post_likes then
            -- OPTIMIZATION 2: Skip posts with very few likes
            -- Probability of co-liker overlap with <5 likes is <1%
            posts_skipped_few_likers = posts_skipped_few_likers + 1
        else
            -- Get recent likers of this post (OPTIMIZATION 3: reduced max_likers_per_post)
            local likers = redis.call('ZRANGEBYSCORE', post_likers_key, min_time, now, 'WITHSCORES', 'LIMIT', 0, max_likers_per_post)
            
            local post_score = 0
            local matching_likers = 0
            
            for i = 1, #likers, 2 do
                local liker_hash = likers[i]
                local like_time = tonumber(likers[i + 1])
                
                -- O(1) lookup: is this liker one of the user's co-likers?
                local source_weight = source_weights[liker_hash]
                if source_weight then
                    -- Calculate recency weight for this like
                    local age_seconds = now - like_time
                    local recency_weight = math.exp(-0.693 * age_seconds / recency_half_life_seconds)
                    
                    post_score = post_score + source_weight * recency_weight
                    matching_likers = matching_likers + 1
                end
            end
            
            if post_score > 0 then
                post_scores[post_id] = post_score
                post_liker_counts[post_id] = matching_likers
                posts_with_engagement = posts_with_engagement + 1
            end
        end
    end
end

-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4: Apply popularity penalty and store results
-- ═══════════════════════════════════════════════════════════════════════════════

redis.call('DEL', result_key)

local scored_count = 0
local results = {}

for post_id, raw_score in pairs(post_scores) do
    local num_paths = post_liker_counts[post_id] or 1
    local liker_count = num_paths
    
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
    
    table.insert(results, final_score)
    table.insert(results, post_id)
    scored_count = scored_count + 1
end

-- Store results in sorted set (batched)
if #results > 0 then
    local batch_size = 1000
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
