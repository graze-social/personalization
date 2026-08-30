//! Shared DID interner: `did:plc:… -> u32`.
//!
//! Not ours. This is the same mapping, in the same Redis, under the same keys
//! that `rust-smasher` and `membership-service` already use — 4.8M DIDs deep in
//! production at time of writing. Sharing it means a lens map and a
//! membership-service bitmap speak about the same accounts by the same ids, and
//! anything either side publishes stays interpretable by the other. Allocating
//! a private id space here would have been easier and would have quietly made
//! the two incomparable forever.
//!
//! # Why intern at all
//!
//! Scored lens maps are (id, weight) pairs. As raw DIDs a second-degree map is
//! ~35 bytes per entry; as u32 ids it is 6. At 250k entries that is 8.75 MB
//! versus 1.5 MB, per viewer, on a Redis the serve path reads.
//!
//! # Two spaces, and why lenses got their own
//!
//! The shared space above is still the default and still what rust-smasher and
//! membership-service use. Lenses now allocate from a private space
//! (`IDSPACE_LENS`) on the lens Redis instead, because the full follow graph is
//! ~25M accounts against the shared instance's remaining headroom, and that
//! instance is `noeviction` — filling it does not evict, it makes other
//! services' writes fail.
//!
//! The cost is real and worth remembering: a lens map and a membership-service
//! bitmap no longer speak about accounts by the same ids, so a future
//! `my-lists` facet that wanted to intersect the two needs a translation step
//! rather than a direct bitmap operation.
//!
//! Ids are only meaningful relative to the interner that issued them, so every
//! blob stamps its space in the header and readers refuse a mismatch. Without
//! that, a blob built in one space and read in another resolves every lookup to
//! the wrong account and looks like a lens with strange taste, not a broken one.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use deadpool_redis::Pool;

/// Hash mapping `did -> u32`. Hash-tagged so the get-or-create script, which
/// touches both keys, stays on one Cluster slot.
pub const DIDINT_MAP: &str = "didint:{didint}:map";
/// Monotonic id sequence.
pub const DIDINT_SEQ: &str = "didint:{didint}:seq";

/// The lens-owned space, on the lens Redis. Same hash-tag trick so the
/// get-or-create script stays on one Cluster slot.
pub const LENSDID_MAP: &str = "lensdid:{lensdid}:map";
pub const LENSDID_SEQ: &str = "lensdid:{lensdid}:seq";

/// Bound on DIDs per Lua call, so the script does not hold the Redis slot long.
const INTERN_CHUNK: usize = 1000;

/// Atomic get-or-create. Two processes interning the same new DID concurrently
/// still agree on its id, which a read-then-write from the client would not
/// guarantee.
const LUA_GETSET: &str = r#"
local ids = {}
for i = 1, #ARGV do
    local id = redis.call('HGET', KEYS[1], ARGV[i])
    if not id then
        id = redis.call('INCR', KEYS[2])
        redis.call('HSET', KEYS[1], ARGV[i], id)
    end
    ids[i] = id
end
return ids
"#;

pub struct Interner {
    redis: Pool,
    map_key: &'static str,
    seq_key: &'static str,
    /// Stamped into every blob built from these ids.
    idspace: u8,
    /// Repeated interning during one rebuild should not re-hit Redis; a
    /// viewer's second-degree map revisits the same accounts constantly.
    cache: Arc<DashMap<String, u32>>,
}

impl Interner {
    /// The shared space, as rust-smasher and membership-service use it.
    pub fn shared(redis: Pool) -> Self {
        Self {
            redis,
            map_key: DIDINT_MAP,
            seq_key: DIDINT_SEQ,
            idspace: crate::scored::IDSPACE_SHARED,
            cache: Arc::new(DashMap::new()),
        }
    }

    /// The lens-owned space.
    pub fn lens(redis: Pool) -> Self {
        Self {
            redis,
            map_key: LENSDID_MAP,
            seq_key: LENSDID_SEQ,
            idspace: crate::scored::IDSPACE_LENS,
            cache: Arc::new(DashMap::new()),
        }
    }

    pub fn idspace(&self) -> u8 {
        self.idspace
    }

    pub fn new(redis: Pool) -> Self {
        Self::shared(redis)
    }

    /// Ids for every DID given, allocating for any that are new.
    ///
    /// Order is not preserved; the caller gets a map, because every caller here
    /// wants lookup rather than position.
    pub async fn intern_many(&self, dids: &[String]) -> anyhow::Result<HashMap<String, u32>> {
        let mut out = HashMap::with_capacity(dids.len());
        let mut missing: Vec<String> = Vec::new();

        for did in dids {
            match self.cache.get(did) {
                Some(id) => {
                    out.insert(did.clone(), *id);
                }
                None => missing.push(did.clone()),
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }

        let mut conn = self.redis.get().await?;
        for chunk in missing.chunks(INTERN_CHUNK) {
            let mut script = deadpool_redis::redis::cmd("EVAL");
            script
                .arg(LUA_GETSET)
                .arg(2)
                .arg(self.map_key)
                .arg(self.seq_key);
            for did in chunk {
                script.arg(did);
            }
            let ids: Vec<i64> = script.query_async(&mut conn).await?;
            if ids.len() != chunk.len() {
                anyhow::bail!(
                    "interner returned {} ids for {} dids",
                    ids.len(),
                    chunk.len()
                );
            }
            for (did, id) in chunk.iter().zip(ids) {
                // Ids are allocated by INCR from 1 and will not exceed u32 for
                // the foreseeable network; a negative or oversized id means the
                // shared counter has been tampered with, and silently wrapping
                // it would corrupt every consumer of this id space.
                let id = u32::try_from(id)
                    .map_err(|_| anyhow::anyhow!("interner id {id} out of u32 range"))?;
                self.cache.insert(did.clone(), id);
                out.insert(did.clone(), id);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These key strings are the interoperability contract with rust-smasher
    /// and membership-service. Production holds ~4.8M entries under exactly
    /// these names; a typo here allocates a fresh, empty, incompatible id space
    /// and nothing would report an error.
    #[test]
    fn keys_match_the_shared_id_space() {
        assert_eq!(DIDINT_MAP, "didint:{didint}:map");
        assert_eq!(DIDINT_SEQ, "didint:{didint}:seq");
    }

    /// The hash tag must be present and identical on both keys, or the
    /// get-or-create script spans two Cluster slots and fails.
    #[test]
    fn both_keys_share_one_hash_tag() {
        assert!(DIDINT_MAP.contains("{didint}"));
        assert!(DIDINT_SEQ.contains("{didint}"));
    }

    /// Get-or-create must be one atomic script. A client-side read-then-write
    /// lets two processes allocate different ids for the same new DID.
    #[test]
    fn allocation_is_atomic() {
        assert!(LUA_GETSET.contains("HGET"));
        assert!(LUA_GETSET.contains("INCR"));
        assert!(LUA_GETSET.contains("HSET"));
    }
}
