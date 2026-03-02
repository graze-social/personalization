//! Hashing utilities for DIDs and URIs.

use sha2::{Digest, Sha256};
use std::sync::LazyLock;

use dashmap::DashMap;

/// Cache for DID hashes to avoid recomputing SHA256.
static DID_HASH_CACHE: LazyLock<DashMap<String, String>> = LazyLock::new(DashMap::new);

/// Hash a DID to 16-char hex string for memory efficiency.
///
/// Cached to avoid recomputing SHA256 for frequently seen DIDs.
pub fn hash_did(did: &str) -> String {
    if let Some(cached) = DID_HASH_CACHE.get(did) {
        return cached.clone();
    }

    let hash = compute_hash(did);

    // Only cache if we haven't exceeded reasonable size
    if DID_HASH_CACHE.len() < 100_000 {
        DID_HASH_CACHE.insert(did.to_string(), hash.clone());
    }

    hash
}

/// Hash a post URI to 16-char hex string for memory efficiency.
///
/// Not cached as URIs are less frequently repeated than DIDs.
pub fn hash_uri(uri: &str) -> String {
    compute_hash(uri)
}

/// Compute SHA256 hash and return first 16 hex chars.
fn compute_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    hex::encode(&result[..8]) // 8 bytes = 16 hex chars
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_did() {
        let did = "did:plc:testuser123";
        let hash = hash_did(did);
        assert_eq!(hash.len(), 16);

        // Should be cached
        let hash2 = hash_did(did);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_uri() {
        let uri = "at://did:plc:user/app.bsky.feed.post/abc123";
        let hash = hash_uri(uri);
        assert_eq!(hash.len(), 16);
    }

    #[test]
    fn test_hash_consistency() {
        // Same input should always produce same hash
        let input = "test-input";
        let hash1 = compute_hash(input);
        let hash2 = compute_hash(input);
        assert_eq!(hash1, hash2);
    }
}
