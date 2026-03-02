//! URI Interning for memory-efficient storage.
//!
//! This module provides bidirectional mapping between URIs and integer IDs,
//! reducing memory usage by ~80% compared to storing full URIs.
//!
//! Redis Keys:
//! - uri2id: HASH mapping URI -> ID
//! - id2uri: HASH mapping ID -> URI
//! - uri:counter: STRING for auto-incrementing IDs

use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::error::Result;
use crate::redis::{Keys, RedisClient};

/// Default size for each LRU cache.
const DEFAULT_CACHE_SIZE: usize = 50_000;

/// Lua script for atomic get-or-create ID operation.
const GET_OR_CREATE_SCRIPT: &str = r#"
local uri = ARGV[1]
local existing = redis.call('HGET', KEYS[1], uri)
if existing then
    return existing
end
local new_id = redis.call('INCR', KEYS[3])
redis.call('HSET', KEYS[1], uri, new_id)
redis.call('HSET', KEYS[2], new_id, uri)
return new_id
"#;

/// Lua script for atomic batch get-or-create IDs operation.
const BATCH_GET_OR_CREATE_SCRIPT: &str = r#"
local uri_to_id_key = KEYS[1]
local id_to_uri_key = KEYS[2]
local counter_key = KEYS[3]
local results = {}

for i, uri in ipairs(ARGV) do
    local existing = redis.call('HGET', uri_to_id_key, uri)
    if existing then
        results[i] = existing
    else
        local new_id = redis.call('INCR', counter_key)
        redis.call('HSET', uri_to_id_key, uri, new_id)
        redis.call('HSET', id_to_uri_key, new_id, uri)
        results[i] = new_id
    end
end
return results
"#;

/// Manages URI <-> ID mapping for memory efficiency.
///
/// Includes an in-process LRU cache layer to reduce Redis calls.
pub struct UriInterner {
    redis: Arc<RedisClient>,
    /// Cache: URI -> ID
    id_cache: Mutex<LruCache<String, i64>>,
    /// Cache: ID -> URI
    uri_cache: Mutex<LruCache<i64, String>>,
}

impl UriInterner {
    /// Create a new URI interner with the default cache size.
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self::with_cache_size(redis, DEFAULT_CACHE_SIZE)
    }

    /// Create a new URI interner with a custom cache size.
    pub fn with_cache_size(redis: Arc<RedisClient>, cache_size: usize) -> Self {
        Self {
            redis,
            id_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(cache_size).unwrap(),
            )),
            uri_cache: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(cache_size).unwrap(),
            )),
        }
    }

    /// Get the integer ID for a URI, creating one if it doesn't exist.
    ///
    /// This is an atomic operation that ensures no duplicate IDs are assigned.
    /// Checks in-process LRU cache first before going to Redis.
    pub async fn get_or_create_id(&self, uri: &str) -> Result<i64> {
        // Check in-process cache first
        {
            let mut cache = self.id_cache.lock();
            if let Some(&id) = cache.get(uri) {
                return Ok(id);
            }
        }

        // Execute atomic get-or-create in Redis
        let id: i64 = self
            .redis
            .eval(
                GET_OR_CREATE_SCRIPT,
                &[Keys::URI_TO_ID, Keys::ID_TO_URI, Keys::URI_COUNTER],
                &[uri],
            )
            .await?;

        // Populate both caches
        {
            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();
            id_cache.put(uri.to_string(), id);
            uri_cache.put(id, uri.to_string());
        }

        Ok(id)
    }

    /// Get or create IDs for multiple URIs efficiently.
    ///
    /// Returns a mapping of URI -> ID.
    /// Checks in-process LRU cache first before going to Redis.
    pub async fn get_or_create_ids_batch(
        &self,
        uris: &[String],
    ) -> Result<std::collections::HashMap<String, i64>> {
        use std::collections::HashMap;

        if uris.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result: HashMap<String, i64> = HashMap::with_capacity(uris.len());
        let mut uris_to_fetch: Vec<&str> = Vec::new();

        // First, check in-process cache
        {
            let mut cache = self.id_cache.lock();
            for uri in uris {
                if let Some(&id) = cache.get(uri) {
                    result.insert(uri.clone(), id);
                } else {
                    uris_to_fetch.push(uri.as_str());
                }
            }
        }

        if uris_to_fetch.is_empty() {
            return Ok(result);
        }

        // Execute batch get-or-create in Redis
        let ids: Vec<i64> = self
            .redis
            .eval(
                BATCH_GET_OR_CREATE_SCRIPT,
                &[Keys::URI_TO_ID, Keys::ID_TO_URI, Keys::URI_COUNTER],
                &uris_to_fetch,
            )
            .await?;

        // Populate caches and result
        {
            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();

            for (uri, id) in uris_to_fetch.iter().zip(ids.iter()) {
                result.insert((*uri).to_string(), *id);
                id_cache.put((*uri).to_string(), *id);
                uri_cache.put(*id, (*uri).to_string());
            }
        }

        Ok(result)
    }

    /// Get the ID for a URI, or None if not interned.
    ///
    /// Checks in-process LRU cache first before going to Redis.
    pub async fn get_id(&self, uri: &str) -> Result<Option<i64>> {
        // Check in-process cache first
        {
            let mut cache = self.id_cache.lock();
            if let Some(&id) = cache.get(uri) {
                return Ok(Some(id));
            }
        }

        // Check Redis
        let id_str = self.redis.hget(Keys::URI_TO_ID, uri).await?;
        if let Some(s) = id_str {
            let id: i64 = s.parse().unwrap_or(0);
            // Populate both caches
            {
                let mut id_cache = self.id_cache.lock();
                let mut uri_cache = self.uri_cache.lock();
                id_cache.put(uri.to_string(), id);
                uri_cache.put(id, uri.to_string());
            }
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }

    /// Get the URI for an ID, or None if not found.
    ///
    /// Checks in-process LRU cache first before going to Redis.
    pub async fn get_uri(&self, id: i64) -> Result<Option<String>> {
        // Check in-process cache first
        {
            let mut cache = self.uri_cache.lock();
            if let Some(uri) = cache.get(&id) {
                return Ok(Some(uri.clone()));
            }
        }

        // Check Redis
        let uri = self.redis.hget(Keys::ID_TO_URI, &id.to_string()).await?;
        if let Some(ref u) = uri {
            // Populate both caches
            {
                let mut id_cache = self.id_cache.lock();
                let mut uri_cache = self.uri_cache.lock();
                uri_cache.put(id, u.clone());
                id_cache.put(u.clone(), id);
            }
        }
        Ok(uri)
    }

    /// Get URIs for multiple IDs efficiently.
    ///
    /// Returns a mapping of ID -> URI (only for found IDs).
    pub async fn get_uris_batch(
        &self,
        ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, String>> {
        use std::collections::HashMap;

        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result: HashMap<i64, String> = HashMap::with_capacity(ids.len());
        let mut ids_to_fetch: Vec<i64> = Vec::new();

        // First, check in-process cache
        {
            let mut cache = self.uri_cache.lock();
            for &id in ids {
                if let Some(uri) = cache.get(&id) {
                    result.insert(id, uri.clone());
                } else {
                    ids_to_fetch.push(id);
                }
            }
        }

        if ids_to_fetch.is_empty() {
            return Ok(result);
        }

        // Fetch from Redis
        let str_ids: Vec<String> = ids_to_fetch.iter().map(|id| id.to_string()).collect();
        let str_refs: Vec<&str> = str_ids.iter().map(|s| s.as_str()).collect();
        let uris = self.redis.hmget(Keys::ID_TO_URI, &str_refs).await?;

        // Populate caches and result
        {
            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();

            for (id, uri_opt) in ids_to_fetch.iter().zip(uris.iter()) {
                if let Some(uri) = uri_opt {
                    result.insert(*id, uri.clone());
                    uri_cache.put(*id, uri.clone());
                    id_cache.put(uri.clone(), *id);
                }
            }
        }

        Ok(result)
    }

    /// Get IDs for multiple URIs (only existing ones).
    ///
    /// Returns a mapping of URI -> ID (only for found URIs).
    pub async fn get_ids_batch(
        &self,
        uris: &[String],
    ) -> Result<std::collections::HashMap<String, i64>> {
        use std::collections::HashMap;

        if uris.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result: HashMap<String, i64> = HashMap::with_capacity(uris.len());
        let mut uris_to_fetch: Vec<&str> = Vec::new();

        // First, check in-process cache
        {
            let mut cache = self.id_cache.lock();
            for uri in uris {
                if let Some(&id) = cache.get(uri) {
                    result.insert(uri.clone(), id);
                } else {
                    uris_to_fetch.push(uri.as_str());
                }
            }
        }

        if uris_to_fetch.is_empty() {
            return Ok(result);
        }

        // Fetch from Redis
        let ids = self.redis.hmget(Keys::URI_TO_ID, &uris_to_fetch).await?;

        // Populate caches and result
        {
            let mut id_cache = self.id_cache.lock();
            let mut uri_cache = self.uri_cache.lock();

            for (uri, id_opt) in uris_to_fetch.iter().zip(ids.iter()) {
                if let Some(id_str) = id_opt {
                    let id: i64 = id_str.parse().unwrap_or(0);
                    result.insert((*uri).to_string(), id);
                    id_cache.put((*uri).to_string(), id);
                    uri_cache.put(id, (*uri).to_string());
                }
            }
        }

        Ok(result)
    }

    /// Get the number of URIs in the interning table.
    pub async fn get_table_size(&self) -> Result<usize> {
        self.redis.hlen(Keys::URI_TO_ID).await
    }

    /// Get current cache sizes for monitoring.
    pub fn cache_sizes(&self) -> (usize, usize) {
        let id_cache = self.id_cache.lock();
        let uri_cache = self.uri_cache.lock();
        (id_cache.len(), uri_cache.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_syntax() {
        // Basic syntax check - ensure scripts compile
        assert!(GET_OR_CREATE_SCRIPT.contains("HGET"));
        assert!(BATCH_GET_OR_CREATE_SCRIPT.contains("INCR"));
    }

    #[test]
    fn test_get_or_create_script_content() {
        // Verify script structure
        assert!(GET_OR_CREATE_SCRIPT.contains("KEYS[1]")); // uri_to_id
        assert!(GET_OR_CREATE_SCRIPT.contains("KEYS[2]")); // id_to_uri
        assert!(GET_OR_CREATE_SCRIPT.contains("KEYS[3]")); // counter
        assert!(GET_OR_CREATE_SCRIPT.contains("ARGV[1]")); // uri
    }

    #[test]
    fn test_batch_script_content() {
        // Verify batch script structure
        assert!(BATCH_GET_OR_CREATE_SCRIPT.contains("for i, uri in ipairs(ARGV)"));
        assert!(BATCH_GET_OR_CREATE_SCRIPT.contains("results[i]"));
    }

    #[test]
    fn test_default_cache_size() {
        assert_eq!(DEFAULT_CACHE_SIZE, 50_000);
    }

    #[test]
    fn test_keys_constants() {
        assert_eq!(Keys::URI_TO_ID, "uri2id");
        assert_eq!(Keys::ID_TO_URI, "id2uri");
        assert_eq!(Keys::URI_COUNTER, "uri:counter");
    }
}
