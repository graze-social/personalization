//! Redis key patterns used throughout the service.

/// Redis key patterns for the Graze service.
///
/// All keys follow consistent naming conventions:
/// - Short prefixes for memory efficiency
/// - Hash suffixes for user/post identification
/// - Algorithm IDs for feed-specific data
/// - Date suffixes (YYYYMMDD) for time-based partitioning
pub struct Keys;

/// Default number of days to retain like data.
pub const DEFAULT_RETENTION_DAYS: u32 = 8;

/// Convert a unix timestamp to a YYYYMMDD date string (UTC).
#[inline]
pub fn date_from_timestamp(timestamp_secs: f64) -> String {
    // Convert to days since epoch, then to year/month/day
    let secs = timestamp_secs as i64;
    let days_since_epoch = secs / 86400;

    // Algorithm to convert days since epoch to year/month/day
    // Based on Howard Hinnant's date algorithms
    let z = days_since_epoch + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{:04}{:02}{:02}", y, m, d)
}

/// Get today's date as YYYYMMDD string (UTC).
#[inline]
pub fn today_date() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    date_from_timestamp(now)
}

/// Get the list of dates to query for the retention window.
/// Returns dates from today back N days (inclusive).
#[inline]
pub fn retention_dates(retention_days: u32) -> Vec<String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    (0..retention_days)
        .map(|days_ago| {
            let timestamp = now - (days_ago as f64 * 86400.0);
            date_from_timestamp(timestamp)
        })
        .collect()
}

/// Calculate TTL for a key based on when it should expire.
/// Keys should expire at the end of the retention window.
/// For example, if retention is 8 days, a key for today should expire in 8 days.
#[inline]
pub fn ttl_for_date(date: &str, retention_days: u32) -> i64 {
    // Parse the date to get the timestamp at start of that day (UTC)
    let year: i32 = date[0..4].parse().unwrap_or(2020);
    let month: u32 = date[4..6].parse().unwrap_or(1);
    let day: u32 = date[6..8].parse().unwrap_or(1);

    // Convert back to epoch timestamp (simplified calculation)
    // Days from year
    let mut total_days: i64 = 0;
    for y in 1970..year {
        let leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        total_days += if leap { 366 } else { 365 };
    }
    // Days from months
    let days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    for m in 1..month {
        total_days += days_in_months[(m - 1) as usize] as i64;
        if m == 2 && is_leap {
            total_days += 1;
        }
    }
    total_days += (day - 1) as i64;

    let key_date_epoch = total_days * 86400;
    let key_expiry_epoch = key_date_epoch + (retention_days as i64 * 86400);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let ttl = key_expiry_epoch - now;
    if ttl > 0 {
        ttl
    } else {
        1
    } // Minimum 1 second TTL
}

// ═══════════════════════════════════════════════════════════════════════════════
// Legacy compatibility - these will be removed after migration
// ═══════════════════════════════════════════════════════════════════════════════

/// DEPRECATED: Number of day tranches (d0-d7 format).
pub const DAY_TRANCHES: u8 = 8;

/// DEPRECATED: Use date_from_timestamp instead.
#[inline]
pub fn day_offset_from_timestamp(timestamp_secs: f64) -> u8 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    let days_ago = ((now - timestamp_secs) / 86400.0).floor() as i64;

    if days_ago < 0 {
        0
    } else if days_ago >= DAY_TRANCHES as i64 {
        DAY_TRANCHES - 1
    } else {
        days_ago as u8
    }
}

/// DEPRECATED: Use ttl_for_date instead.
#[inline]
pub fn ttl_for_day(day: u8, ttl_days: u32) -> i64 {
    let remaining_days = ttl_days.saturating_sub(day as u32);
    (remaining_days as i64) * 86400
}

impl Keys {
    // ═══════════════════════════════════════════════════════════════════════════════
    // Like Graph Keys (Date-Based: YYYYMMDD)
    // ═══════════════════════════════════════════════════════════════════════════════

    /// User's likes sorted set for a specific date: `ul:{hash}:{YYYYMMDD}`
    ///
    /// ZSET with post_id as member and timestamp as score.
    #[inline]
    pub fn user_likes_date(user_did_hash: &str, date: &str) -> String {
        format!("ul:{}:{}", user_did_hash, date)
    }

    /// Get all user_likes keys for a user within retention window.
    #[inline]
    pub fn user_likes_retention(user_did_hash: &str, retention_days: u32) -> Vec<String> {
        retention_dates(retention_days)
            .into_iter()
            .map(|d| Self::user_likes_date(user_did_hash, &d))
            .collect()
    }

    /// Post's likers sorted set for a specific date: `pl:{id}:{YYYYMMDD}`
    ///
    /// ZSET with user_hash as member and timestamp as score.
    #[inline]
    pub fn post_likers_date(post_id: &str, date: &str) -> String {
        format!("pl:{}:{}", post_id, date)
    }

    /// Get all post_likers keys for a post within retention window.
    #[inline]
    pub fn post_likers_retention(post_id: &str, retention_days: u32) -> Vec<String> {
        retention_dates(retention_days)
            .into_iter()
            .map(|d| Self::post_likers_date(post_id, &d))
            .collect()
    }

    /// Author's likers sorted set for a specific date: `authl:{hash}:{YYYYMMDD}`
    ///
    /// ZSET with user_hash as member and timestamp as score.
    #[inline]
    pub fn author_likers_date(author_did_hash: &str, date: &str) -> String {
        format!("authl:{}:{}", author_did_hash, date)
    }

    /// Get all author_likers keys for an author within retention window.
    #[inline]
    pub fn author_likers_retention(author_did_hash: &str, retention_days: u32) -> Vec<String> {
        retention_dates(retention_days)
            .into_iter()
            .map(|d| Self::author_likers_date(author_did_hash, &d))
            .collect()
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Like Graph Keys (Legacy d0-d7 format - for migration)
    // ═══════════════════════════════════════════════════════════════════════════════

    /// DEPRECATED: Use user_likes_date() for new writes.
    #[inline]
    pub fn user_likes_day(user_did_hash: &str, day: u8) -> String {
        format!("ul:{}:d{}", user_did_hash, day)
    }

    /// DEPRECATED: Use user_likes_retention() for reads.
    #[inline]
    pub fn user_likes_all_days(user_did_hash: &str) -> Vec<String> {
        (0..DAY_TRANCHES)
            .map(|d| Self::user_likes_day(user_did_hash, d))
            .collect()
    }

    /// DEPRECATED: Use post_likers_date() for new writes.
    #[inline]
    pub fn post_likers_day(post_id: &str, day: u8) -> String {
        format!("pl:{}:d{}", post_id, day)
    }

    /// DEPRECATED: Use post_likers_retention() for reads.
    #[inline]
    pub fn post_likers_all_days(post_id: &str) -> Vec<String> {
        (0..DAY_TRANCHES)
            .map(|d| Self::post_likers_day(post_id, d))
            .collect()
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Like Graph Keys (Legacy - original format for migration)
    // ═══════════════════════════════════════════════════════════════════════════════

    /// DEPRECATED: User's likes sorted set (legacy): `ul:{hash}`
    #[inline]
    pub fn user_likes(user_did_hash: &str) -> String {
        format!("ul:{}", user_did_hash)
    }

    /// DEPRECATED: Post's likers sorted set (legacy): `pl:{hash}`
    #[inline]
    pub fn post_likers(post_id: &str) -> String {
        format!("pl:{}", post_id)
    }

    /// User's seen posts sorted set: `seen:{hash}`
    ///
    /// ZSET with post_id as member and timestamp as score.
    #[inline]
    pub fn user_seen(user_did_hash: &str) -> String {
        format!("seen:{}", user_did_hash)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Algorithm Post Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Algorithm's eligible posts set: `ap:{algo_id}`
    ///
    /// SET containing post_ids that are eligible for this algorithm.
    #[inline]
    pub fn algo_posts(algo_id: i32) -> String {
        format!("ap:{}", algo_id)
    }

    /// Algorithm posts liker counts hash: `apc:{algo_id}`
    ///
    /// HASH mapping post_id -> liker_count for fast filtering.
    #[inline]
    pub fn algo_posts_counts(algo_id: i32) -> String {
        format!("apc:{}", algo_id)
    }

    /// Algorithm metadata hash: `am:{algo_id}`
    ///
    /// HASH containing last_sync timestamp and other metadata.
    #[inline]
    pub fn algo_meta(algo_id: i32) -> String {
        format!("am:{}", algo_id)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Cached Results
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Cached personalization result: `ll:{algo_id}:{hash}`
    ///
    /// ZSET with post_id as member and personalization score as score.
    #[inline]
    pub fn cached_result(algo_id: i32, user_did_hash: &str) -> String {
        format!("ll:{}:{}", algo_id, user_did_hash)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // URI Interning
    // ═══════════════════════════════════════════════════════════════════════════════

    /// URI to ID mapping hash.
    pub const URI_TO_ID: &'static str = "uri2id";

    /// ID to URI mapping hash.
    pub const ID_TO_URI: &'static str = "id2uri";

    /// URI counter for generating new IDs.
    pub const URI_COUNTER: &'static str = "uri:counter";

    // ═══════════════════════════════════════════════════════════════════════════════
    // Operational Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Jetstream cursor for resuming from last position.
    pub const JETSTREAM_CURSOR: &'static str = "cursor:jetstream";

    /// Pending cursor for crash recovery (saved before batch processing).
    ///
    /// If this key exists on startup, it means a batch was in progress when
    /// the process crashed. Resume from this cursor to avoid data loss.
    pub const JETSTREAM_CURSOR_PENDING: &'static str = "cursor:jetstream:pending";

    /// Sync queue for algorithm sync requests.
    pub const SYNC_QUEUE: &'static str = "queue:sync";

    /// Post-render log task queue (compatible with consumers expecting "log_tasks").
    /// One JSON payload per feed hit; consumed elsewhere for post_render_log etc.
    pub const LOG_TASKS: &'static str = "log_tasks";

    /// Feed access tracking HSET: `feed:access`
    ///
    /// HASH mapping algo_id -> timestamp for rolling sync scheduling.
    /// The candidate sync worker periodically scans this and syncs feeds
    /// that have been accessed within the rolling window.
    pub const FEED_ACCESS: &'static str = "feed:access";

    /// Supported feeds mapping: feed_uri -> algo_id (active feeds only).
    pub const SUPPORTED_FEEDS: &'static str = "supported_feeds";

    /// Reverse mapping algo_id -> feed_uri for deregister (one-to-one).
    pub const ALGO_ID_TO_FEED_URI: &'static str = "algo_id_to_feed_uri";

    /// Durable feed_uri -> algo_id for interaction writes; never removed on deregister
    /// so late-arriving sendInteractions are still accepted.
    pub const FEED_URI_TO_ALGO_WRITES: &'static str = "feed_uri_to_algo_writes";

    /// Per-feed Thompson config: feed_thompson_config:{algo_id}
    #[inline]
    pub fn feed_thompson_config(algo_id: i32) -> String {
        format!("feed_thompson_config:{}", algo_id)
    }

    /// Global Thompson search space: thompson_search_space
    #[inline]
    pub fn thompson_search_space() -> &'static str {
        "thompson_search_space"
    }

    /// Global Thompson success criteria: thompson_success_criteria
    #[inline]
    pub fn thompson_success_criteria() -> &'static str {
        "thompson_success_criteria"
    }

    /// Sync lock for rate limiting: `lock:sync:{algo_id}`
    #[inline]
    pub fn sync_lock(algo_id: i32) -> String {
        format!("lock:sync:{}", algo_id)
    }

    /// Algorithm parameters cache: `params:{algo_id}`
    #[inline]
    pub fn algo_params(algo_id: i32) -> String {
        format!("params:{}", algo_id)
    }

    /// Cached special posts for a feed: `sp:{algo_id}`
    #[inline]
    pub fn special_posts(algo_id: i32) -> String {
        format!("sp:{}", algo_id)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Co-liker Pre-computation Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Pre-computed co-liker weights: `colikes:{hash}`
    ///
    /// ZSET with co_liker_hash as member and weight as score.
    #[inline]
    pub fn colikes(user_did_hash: &str) -> String {
        format!("colikes:{}", user_did_hash)
    }

    /// Co-liker computation metadata: `colikes:meta:{hash}`
    #[inline]
    pub fn colikes_meta(user_did_hash: &str) -> String {
        format!("colikes:meta:{}", user_did_hash)
    }

    /// Co-liker computation timestamp: `colikes:ts:{hash}`
    ///
    /// STRING containing the timestamp of user's most recent like when
    /// co-likers were computed. Used to detect if user has new likes
    /// that would invalidate the cached co-likers.
    #[inline]
    pub fn colikes_timestamp(user_did_hash: &str) -> String {
        format!("colikes:ts:{}", user_did_hash)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Author-Level Affinity Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Authors the user has liked content from: `ula:{hash}`
    ///
    /// ZSET with author_hash as member and accumulated_weight as score.
    /// Note: ula keys are NOT day-tranched since they accumulate counts, not timestamps.
    #[inline]
    pub fn user_liked_authors(user_did_hash: &str) -> String {
        format!("ula:{}", user_did_hash)
    }

    /// DEPRECATED: Use author_likers_date() for new writes.
    #[inline]
    pub fn author_likers_day(author_did_hash: &str, day: u8) -> String {
        format!("authl:{}:d{}", author_did_hash, day)
    }

    /// DEPRECATED: Use author_likers_retention() for reads.
    #[inline]
    pub fn author_likers_all_days(author_did_hash: &str) -> Vec<String> {
        (0..DAY_TRANCHES)
            .map(|d| Self::author_likers_day(author_did_hash, d))
            .collect()
    }

    /// DEPRECATED: Users who have liked this author's content (legacy): `authl:{hash}`
    #[inline]
    pub fn author_likers(author_did_hash: &str) -> String {
        format!("authl:{}", author_did_hash)
    }

    /// Pre-computed author-level co-liker weights: `acolikes:{hash}`
    ///
    /// ZSET with co_liker_hash as member and weight as score.
    #[inline]
    pub fn author_colikes(user_did_hash: &str) -> String {
        format!("acolikes:{}", user_did_hash)
    }

    /// Author co-liker computation metadata: `acolikes:meta:{hash}`
    #[inline]
    pub fn author_colikes_meta(user_did_hash: &str) -> String {
        format!("acolikes:meta:{}", user_did_hash)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Fallback Tranches Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Trending posts sorted set (legacy): `trending:{algo_id}`
    #[inline]
    pub fn trending_posts(algo_id: i32) -> String {
        format!("trending:{}", algo_id)
    }

    /// Alias for trending_posts for compatibility.
    #[inline]
    pub fn trending(algo_id: i32) -> String {
        Self::trending_posts(algo_id)
    }

    /// Trending posts metadata: `trending:meta:{algo_id}`
    #[inline]
    pub fn trending_meta(algo_id: i32) -> String {
        format!("trending:meta:{}", algo_id)
    }

    /// Popular posts sorted set: `popular:{algo_id}`
    ///
    /// Score = like_count * slow_recency_factor.
    #[inline]
    pub fn popular_posts(algo_id: i32) -> String {
        format!("popular:{}", algo_id)
    }

    /// Popular posts metadata: `popular:meta:{algo_id}`
    #[inline]
    pub fn popular_meta(algo_id: i32) -> String {
        format!("popular:meta:{}", algo_id)
    }

    /// Velocity trending posts sorted set: `velocity:{algo_id}`
    ///
    /// Score = likes_in_window / hours_since_first_like.
    #[inline]
    pub fn velocity_posts(algo_id: i32) -> String {
        format!("velocity:{}", algo_id)
    }

    /// Velocity posts metadata: `velocity:meta:{algo_id}`
    #[inline]
    pub fn velocity_meta(algo_id: i32) -> String {
        format!("velocity:meta:{}", algo_id)
    }

    /// Author success scores hash: `authors:{algo_id}`
    ///
    /// HASH mapping author_did -> success_score.
    #[inline]
    pub fn author_success(algo_id: i32) -> String {
        format!("authors:{}", algo_id)
    }

    /// Author success metadata: `authors:meta:{algo_id}`
    #[inline]
    pub fn author_success_meta(algo_id: i32) -> String {
        format!("authors:meta:{}", algo_id)
    }

    /// Discovery posts sorted set: `discovery:{algo_id}`
    ///
    /// Score = author_success_score for posts with low engagement.
    #[inline]
    pub fn discovery_posts(algo_id: i32) -> String {
        format!("discovery:{}", algo_id)
    }

    /// Discovery posts metadata: `discovery:meta:{algo_id}`
    #[inline]
    pub fn discovery_meta(algo_id: i32) -> String {
        format!("discovery:meta:{}", algo_id)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Algo Likers Keys (1-hop neighborhood)
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Users who have liked posts in this algo: `al:{algo_id}`
    ///
    /// SET of user hashes.
    #[inline]
    pub fn algo_likers(algo_id: i32) -> String {
        format!("al:{}", algo_id)
    }

    /// Algo likers metadata: `al:meta:{algo_id}`
    #[inline]
    pub fn algo_likers_meta(algo_id: i32) -> String {
        format!("al:meta:{}", algo_id)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Bot Filtering Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// ZSET of filtered user hashes.
    pub const BOT_FILTERED: &'static str = "bot:filtered";

    /// Bot metadata hash: `bot:meta:{hash}`
    #[inline]
    pub fn bot_meta(user_did_hash: &str) -> String {
        format!("bot:meta:{}", user_did_hash)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // Feed Cache Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Cached feed posts list: `fsc:{algo_id}:{hash}`
    ///
    /// LIST containing post URIs for pagination.
    #[inline]
    pub fn feed_cache(algo_id: i32, user_did_hash: &str) -> String {
        format!("fsc:{}:{}", algo_id, user_did_hash)
    }

    // ═══════════════════════════════════════════════════════════════════════════════
    // User Like Counts (for LinkLonk normalization)
    // ═══════════════════════════════════════════════════════════════════════════════

    /// User like counts hash: `ulc`
    ///
    /// HASH mapping user_did_hash -> total_like_count.
    /// Used for LinkLonk's 1/|items_s_liked| normalization factor.
    pub const USER_LIKE_COUNTS: &'static str = "ulc";

    // ═══════════════════════════════════════════════════════════════════════════════
    // Audit Keys
    // ═══════════════════════════════════════════════════════════════════════════════

    /// Set of user hashes to audit: `audit:users`
    ///
    /// SET containing user_did_hashes that should have audit logging enabled.
    pub const AUDIT_USERS: &'static str = "audit:users";

    /// Get the audit users key.
    #[inline]
    pub fn audit_users() -> &'static str {
        Self::AUDIT_USERS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_likes_key() {
        assert_eq!(Keys::user_likes("abc123"), "ul:abc123");
    }

    #[test]
    fn test_user_likes_day_key() {
        assert_eq!(Keys::user_likes_day("abc123", 0), "ul:abc123:d0");
        assert_eq!(Keys::user_likes_day("abc123", 7), "ul:abc123:d7");
    }

    #[test]
    fn test_user_likes_all_days() {
        let keys = Keys::user_likes_all_days("abc123");
        assert_eq!(keys.len(), 8);
        assert_eq!(keys[0], "ul:abc123:d0");
        assert_eq!(keys[7], "ul:abc123:d7");
    }

    #[test]
    fn test_post_likers_day_key() {
        assert_eq!(Keys::post_likers_day("12345", 0), "pl:12345:d0");
        assert_eq!(Keys::post_likers_day("12345", 3), "pl:12345:d3");
    }

    #[test]
    fn test_author_likers_day_key() {
        assert_eq!(Keys::author_likers_day("xyz789", 0), "authl:xyz789:d0");
        assert_eq!(Keys::author_likers_day("xyz789", 5), "authl:xyz789:d5");
    }

    #[test]
    fn test_day_offset_from_timestamp() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // Today
        assert_eq!(day_offset_from_timestamp(now), 0);
        assert_eq!(day_offset_from_timestamp(now - 100.0), 0);

        // Yesterday
        assert_eq!(day_offset_from_timestamp(now - 86400.0), 1);
        assert_eq!(day_offset_from_timestamp(now - 86400.0 * 1.5), 1);

        // 7 days ago (clamped to 7)
        assert_eq!(day_offset_from_timestamp(now - 86400.0 * 7.0), 7);
        assert_eq!(day_offset_from_timestamp(now - 86400.0 * 30.0), 7);
    }

    #[test]
    fn test_ttl_for_day() {
        // With 8 day TTL
        assert_eq!(ttl_for_day(0, 8), 8 * 86400); // Day 0: 8 days
        assert_eq!(ttl_for_day(1, 8), 7 * 86400); // Day 1: 7 days
        assert_eq!(ttl_for_day(7, 8), 86400); // Day 7: 1 day
        assert_eq!(ttl_for_day(8, 8), 0); // Day 8: 0 (expired)
    }

    #[test]
    fn test_cached_result_key() {
        assert_eq!(Keys::cached_result(42, "def456"), "ll:42:def456");
    }

    #[test]
    fn test_algo_posts_key() {
        assert_eq!(Keys::algo_posts(123), "ap:123");
    }
}
