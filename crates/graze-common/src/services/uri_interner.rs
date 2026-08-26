//! URI interning with date-sharded Redis storage.
//!
//! New mappings live in per-day hashes with TTL aligned to like-graph retention:
//! - `uri2id:{YYYYMMDD}` / `id2uri:{YYYYMMDD}` / `uri:counter:{YYYYMMDD}`
//! - Post IDs: `{YYYYMMDD}{seq:010}` (see [`crate::post_id`])
//!
//! Legacy global `uri2id` / `id2uri` are read for lookups only; new writes use shards.

use std::collections::HashMap;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::error::Result;
use crate::post_id::{intern_date_from_post_id, is_legacy_numeric};
use crate::redis::{retention_dates, ttl_for_date, Keys, RedisClient, DEFAULT_RETENTION_DAYS};

/// Default size for each LRU cache.
const DEFAULT_CACHE_SIZE: usize = 50_000;

/// Batch get-or-create within one intern date shard.
const SHARD_BATCH_GET_OR_CREATE_SCRIPT: &str = r#"
local uri_to_id_key = KEYS[1]
local id_to_uri_key = KEYS[2]
local counter_key = KEYS[3]
local date = ARGV[1]
local results = {}
for i = 2, #ARGV do
    local uri = ARGV[i]
    local existing = redis.call('HGET', uri_to_id_key, uri)
    if existing then
        results[i - 1] = existing
    else
        local seq = redis.call('INCR', counter_key)
        local id = date .. string.format('%010d', seq)
        redis.call('HSET', uri_to_id_key, uri, id)
        redis.call('HSET', id_to_uri_key, id, uri)
        results[i - 1] = id
    end
end
return results
"#;

/// Manages URI <-> post ID mapping.
pub struct UriInterner {
    redis: Arc<RedisClient>,
    retention_days: u32,
    id_cache: Mutex<LruCache<String, String>>,
    uri_cache: Mutex<LruCache<String, String>>,
}

impl UriInterner {
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self::with_retention(redis, DEFAULT_RETENTION_DAYS)
    }

    pub fn with_cache_size(redis: Arc<RedisClient>, cache_size: usize) -> Self {
        Self {
            redis,
            retention_days: DEFAULT_RETENTION_DAYS,
            id_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(cache_size).unwrap(),
            )),
            uri_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(cache_size).unwrap(),
            )),
        }
    }

    pub fn with_retention(redis: Arc<RedisClient>, retention_days: u32) -> Self {
        Self {
            redis,
            retention_days,
            id_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap(),
            )),
            uri_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap(),
            )),
        }
    }

    /// Get or create a post ID for `uri` on intern date `YYYYMMDD`.
    pub async fn get_or_create_id(&self, uri: &str, intern_date: &str) -> Result<String> {
        let map = self
            .get_or_create_ids_batch(&[uri.to_string()], intern_date)
            .await?;
        map.get(uri)
            .cloned()
            .ok_or_else(|| crate::error::GrazeError::Internal("interner missing id".into()))
    }

    /// Batch intern URIs under `intern_date` (`YYYYMMDD`).
    pub async fn get_or_create_ids_batch(
        &self,
        uris: &[String],
        intern_date: &str,
    ) -> Result<HashMap<String, String>> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result = HashMap::with_capacity(uris.len());
        let mut uris_to_fetch: Vec<String> = Vec::new();

        {
            let mut cache = self.id_cache.lock();
            for uri in uris {
                if let Some(id) = cache.get(uri) {
                    result.insert(uri.clone(), id.clone());
                } else {
                    uris_to_fetch.push(uri.clone());
                }
            }
        }

        if !uris_to_fetch.is_empty() {
            // Reuse legacy or any retention shard ID so ap:* stays aligned with ul:/pl:
            let existing = self.get_ids_batch(&uris_to_fetch).await?;
            let mut still_missing: Vec<String> = Vec::new();
            {
                let mut id_cache = self.id_cache.lock();
                let mut uri_cache = self.uri_cache.lock();
                for uri in uris_to_fetch {
                    if let Some(id) = existing.get(&uri) {
                        result.insert(uri.clone(), id.clone());
                        id_cache.put(uri.clone(), id.clone());
                        uri_cache.put(id.clone(), uri.clone());
                    } else {
                        still_missing.push(uri);
                    }
                }
            }

            if still_missing.is_empty() {
                return Ok(result);
            }

            let mut eval_args: Vec<&str> = Vec::with_capacity(1 + still_missing.len());
            eval_args.push(intern_date);
            for u in &still_missing {
                eval_args.push(u);
            }
            let uri2id_key = Keys::uri_to_id_date(intern_date);
            let id2uri_key = Keys::id_to_uri_date(intern_date);
            let counter_key = Keys::uri_counter_date(intern_date);
            let ids: Vec<String> = self
                .redis
                .eval(
                    SHARD_BATCH_GET_OR_CREATE_SCRIPT,
                    &[&uri2id_key, &id2uri_key, &counter_key],
                    &eval_args,
                )
                .await?;

            let ttl = ttl_for_date(intern_date, self.retention_days);
            if ttl > 0 {
                self.redis.expire(&uri2id_key, ttl).await?;
                self.redis.expire(&id2uri_key, ttl).await?;
                self.redis.expire(&counter_key, ttl).await?;
            }

            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();
            for (uri, id) in still_missing.iter().zip(ids.iter()) {
                result.insert(uri.clone(), id.clone());
                id_cache.put(uri.clone(), id.clone());
                uri_cache.put(id.clone(), uri.clone());
            }
        }

        Ok(result)
    }

    /// Intern URIs using each entry's event date (`YYYYMMDD`).
    pub async fn get_or_create_ids_by_event_date(
        &self,
        uri_and_dates: &[(String, String)],
    ) -> Result<HashMap<String, String>> {
        if uri_and_dates.is_empty() {
            return Ok(HashMap::new());
        }

        let mut by_date: HashMap<String, Vec<String>> = HashMap::new();
        for (uri, date) in uri_and_dates {
            by_date.entry(date.clone()).or_default().push(uri.clone());
        }

        let mut result = HashMap::with_capacity(uri_and_dates.len());
        for (date, mut uris) in by_date {
            uris.sort();
            uris.dedup();
            let chunk = self.get_or_create_ids_batch(&uris, &date).await?;
            result.extend(chunk);
        }
        Ok(result)
    }

    /// Look up an existing post ID for `uri` (legacy global hash, then retention shards).
    pub async fn get_id(&self, uri: &str) -> Result<Option<String>> {
        {
            let mut cache = self.id_cache.lock();
            if let Some(id) = cache.get(uri) {
                return Ok(Some(id.clone()));
            }
        }

        if let Some(s) = self.redis.hget(Keys::URI_TO_ID, uri).await? {
            self.cache_pair(uri, &s);
            return Ok(Some(s));
        }

        for date in retention_dates(self.retention_days) {
            let key = Keys::uri_to_id_date(&date);
            if let Some(s) = self.redis.hget(&key, uri).await? {
                self.cache_pair(uri, &s);
                return Ok(Some(s));
            }
        }

        Ok(None)
    }

    /// Resolve post ID to AT-URI (dated shard, then legacy global).
    pub async fn get_uri(&self, post_id: &str) -> Result<Option<String>> {
        {
            let mut cache = self.uri_cache.lock();
            if let Some(uri) = cache.get(post_id) {
                return Ok(Some(uri.clone()));
            }
        }

        if let Some(date) = intern_date_from_post_id(post_id) {
            if let Some(uri) = self
                .redis
                .hget(&Keys::id_to_uri_date(date), post_id)
                .await?
            {
                self.cache_pair(&uri, post_id);
                return Ok(Some(uri));
            }
        }

        if is_legacy_numeric(post_id) {
            if let Some(uri) = self.redis.hget(Keys::ID_TO_URI, post_id).await? {
                self.cache_pair(&uri, post_id);
                return Ok(Some(uri));
            }
        }

        Ok(None)
    }

    /// Batch resolve post IDs to URIs.
    pub async fn get_uris_batch(&self, post_ids: &[String]) -> Result<HashMap<String, String>> {
        if post_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result = HashMap::with_capacity(post_ids.len());
        let mut to_fetch: Vec<String> = Vec::new();

        {
            let mut cache = self.uri_cache.lock();
            for id in post_ids {
                if let Some(uri) = cache.get(id) {
                    result.insert(id.clone(), uri.clone());
                } else {
                    to_fetch.push(id.clone());
                }
            }
        }

        if to_fetch.is_empty() {
            return Ok(result);
        }

        let mut legacy_ids: Vec<&str> = Vec::new();
        let mut by_date: HashMap<String, Vec<String>> = HashMap::new();

        for id in &to_fetch {
            if let Some(date) = intern_date_from_post_id(id) {
                by_date
                    .entry(date.to_string())
                    .or_default()
                    .push(id.clone());
            } else if is_legacy_numeric(id) {
                legacy_ids.push(id.as_str());
            }
        }

        if !legacy_ids.is_empty() {
            let refs: Vec<&str> = legacy_ids.to_vec();
            let uris = self.redis.hmget(Keys::ID_TO_URI, &refs).await?;
            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();
            for (id, uri_opt) in legacy_ids.iter().zip(uris.iter()) {
                if let Some(uri) = uri_opt {
                    result.insert((*id).to_string(), uri.clone());
                    uri_cache.put((*id).to_string(), uri.clone());
                    id_cache.put(uri.clone(), (*id).to_string());
                }
            }
        }

        for (date, ids) in by_date {
            let key = Keys::id_to_uri_date(&date);
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            let uris = self.redis.hmget(&key, &refs).await?;
            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();
            for (id, uri_opt) in ids.iter().zip(uris.iter()) {
                if let Some(uri) = uri_opt {
                    result.insert(id.clone(), uri.clone());
                    uri_cache.put(id.clone(), uri.clone());
                    id_cache.put(uri.clone(), id.clone());
                }
            }
        }

        Ok(result)
    }

    /// Batch lookup URI -> post ID without creating entries.
    pub async fn get_ids_batch(&self, uris: &[String]) -> Result<HashMap<String, String>> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result = HashMap::with_capacity(uris.len());
        let mut to_fetch: Vec<String> = Vec::new();

        {
            let mut cache = self.id_cache.lock();
            for uri in uris {
                if let Some(id) = cache.get(uri) {
                    result.insert(uri.clone(), id.clone());
                } else {
                    to_fetch.push(uri.clone());
                }
            }
        }

        for uri in to_fetch {
            if let Some(id) = self.get_id(&uri).await? {
                result.insert(uri, id);
            }
        }

        Ok(result)
    }

    /// Total entries in legacy + visible date shards (approximate; for metrics).
    pub async fn get_table_size(&self) -> Result<usize> {
        let mut total = self.redis.hlen(Keys::URI_TO_ID).await?;
        for date in retention_dates(self.retention_days) {
            total += self.redis.hlen(&Keys::uri_to_id_date(&date)).await?;
        }
        Ok(total)
    }

    pub fn cache_sizes(&self) -> (usize, usize) {
        let id_cache = self.id_cache.lock();
        let uri_cache = self.uri_cache.lock();
        (id_cache.len(), uri_cache.len())
    }

    fn cache_pair(&self, uri: &str, post_id: &str) {
        let mut id_cache = self.id_cache.lock();
        let mut uri_cache = self.uri_cache.lock();
        id_cache.put(uri.to_string(), post_id.to_string());
        uri_cache.put(post_id.to_string(), uri.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::post_id::{format_post_id, is_dated};

    #[test]
    fn shard_script_references_keys() {
        assert!(SHARD_BATCH_GET_OR_CREATE_SCRIPT.contains("uri_to_id_key"));
        assert!(SHARD_BATCH_GET_OR_CREATE_SCRIPT.contains("%010d"));
    }

    #[test]
    fn dated_id_format_in_script_matches_rust() {
        let id = format_post_id("20260513", 7);
        assert_eq!(id.len(), crate::post_id::DATED_ID_LEN);
        assert!(is_dated(&id));
    }
}
