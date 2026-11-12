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

/// Blob header byte for the space rust-smasher and membership-service share.
pub const IDSPACE_SHARED: u8 = 0;
/// Blob header byte for the lens-owned space.
///
/// ⚠️ This records the space the builder *intended*, not where the ids came
/// from. It is not a provenance guard — a blob filled from the wrong Redis still
/// carries whichever byte the code stamped. See
/// `memory/community-facet-not-personalized.md`.
pub const IDSPACE_LENS: u8 = 1;

use std::collections::HashMap;
use std::sync::Mutex;

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

/// Default ceiling on DIDs held in one process's id cache, across both
/// generations. ~110 B each once malloc's overhead is counted (101 B requested,
/// measured), so ~110 MiB at the ceiling.
pub const DEFAULT_CACHE_ENTRIES: usize = 1_000_000;

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
    ///
    /// 🔴 BOUNDED, and it has to be. This was an unbounded map for the life of
    /// the process, and the nightly lens refresh walks the follows (and now the
    /// followers) of every active viewer — ~145k builds — so each builder pod
    /// accumulated every distinct DID it had ever seen, at ~110 B apiece, until
    /// it hit its 512Mi limit and was OOMKilled mid-refresh, roughly daily per
    /// pod. The ids live in Redis; this is only a cache of them, and a miss
    /// costs one batched round trip.
    cache: IdCache,
}

/// A `did -> id` cache bounded by two generations.
///
/// New and re-used entries go into `young`. When `young` reaches half the
/// ceiling it becomes `old`, and the previous `old` is dropped whole. A hit in
/// `old` is promoted back into `young`, so an account that keeps coming up —
/// a popular account in many viewers' follows — survives every rotation, and
/// what ages out is the long tail seen once. Memory never exceeds the ceiling,
/// and eviction costs nothing per entry: no LRU list, no timestamps.
struct IdCache {
    inner: Mutex<Generations>,
    per_generation: usize,
}

#[derive(Default)]
struct Generations {
    young: HashMap<String, u32>,
    old: HashMap<String, u32>,
}

impl IdCache {
    fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(Generations::default()),
            // Zero turns the cache off: every lookup goes to Redis.
            per_generation: max_entries / 2,
        }
    }

    /// Split `dids` into ids already cached (into `out`) and the DIDs that
    /// are not. One lock for the whole batch rather than one per DID.
    fn partition<'a>(&self, dids: &'a [String], out: &mut HashMap<String, u32>) -> Vec<&'a String> {
        let mut missing = Vec::new();
        let mut generations = self.lock();
        let mut retired = None;
        for did in dids {
            if let Some(&id) = generations.young.get(did) {
                out.insert(did.clone(), id);
            } else if let Some((key, id)) = generations.old.remove_entry(did) {
                out.insert(did.clone(), id);
                generations.young.insert(key, id);
                retired = retired.or(self.rotate_if_full(&mut generations));
            } else {
                missing.push(did);
            }
        }
        drop(generations);
        // Dropped outside the lock: freeing half a million strings is not
        // something every other build should wait behind.
        drop(retired);
        missing
    }

    fn insert_all(&self, entries: impl IntoIterator<Item = (String, u32)>) {
        if self.per_generation == 0 {
            return;
        }
        let mut generations = self.lock();
        let mut retired = Vec::new();
        for (did, id) in entries {
            generations.young.insert(did, id);
            retired.extend(self.rotate_if_full(&mut generations));
        }
        drop(generations);
        drop(retired);
    }

    /// When `young` is full, it becomes `old` and the previous `old` is
    /// returned for the caller to drop once the lock is released.
    fn rotate_if_full(&self, generations: &mut Generations) -> Option<HashMap<String, u32>> {
        if generations.young.len() < self.per_generation {
            return None;
        }
        let young = std::mem::take(&mut generations.young);
        Some(std::mem::replace(&mut generations.old, young))
    }

    fn len(&self) -> usize {
        let generations = self.lock();
        generations.young.len() + generations.old.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Generations> {
        // A panic while holding this lock cannot leave the maps inconsistent
        // in any way that matters — the worst case is a missing cache entry —
        // so a poisoned lock is recovered rather than propagated.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Interner {
    /// The shared space, as rust-smasher and membership-service use it.
    pub fn shared(redis: Pool) -> Self {
        Self {
            redis,
            map_key: DIDINT_MAP,
            seq_key: DIDINT_SEQ,
            idspace: IDSPACE_SHARED,
            cache: IdCache::new(DEFAULT_CACHE_ENTRIES),
        }
    }

    /// The lens-owned space.
    pub fn lens(redis: Pool) -> Self {
        Self {
            redis,
            map_key: LENSDID_MAP,
            seq_key: LENSDID_SEQ,
            idspace: IDSPACE_LENS,
            cache: IdCache::new(DEFAULT_CACHE_ENTRIES),
        }
    }

    /// Replace the id cache's ceiling (entries across both generations).
    /// Zero disables the cache.
    pub fn with_cache_entries(mut self, max_entries: usize) -> Self {
        self.cache = IdCache::new(max_entries);
        self
    }

    /// DIDs currently cached in this process. For logging memory.
    pub fn cached_entries(&self) -> usize {
        self.cache.len()
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
    /// Ids for the DIDs that ALREADY have one. Allocates nothing.
    ///
    /// The counterpart to `intern_many`, and the right primitive for anything
    /// reading the firehose. Interning every account that flies past would grow
    /// this hash by roughly 19M entries a day — it already holds ~42M, on a Redis
    /// where depth is an outage budget.
    ///
    /// Dropping unknown accounts is also the *correct* semantics rather than a
    /// concession: `follow_graph_int` is built with INNER joins against the id
    /// map, so an account without an id is already absent from the projection.
    /// A delta that invented ids would describe edges the base table cannot, and
    /// the two would disagree about who exists. Unknown accounts are picked up by
    /// the nightly job's bounded interning, exactly as today.
    pub async fn lookup_many(&self, dids: &[String]) -> anyhow::Result<HashMap<String, u32>> {
        let mut out = HashMap::with_capacity(dids.len());
        let unknown = self.cache.partition(dids, &mut out);
        if unknown.is_empty() {
            return Ok(out);
        }

        let mut conn = self.redis.get().await?;
        for chunk in unknown.chunks(INTERN_CHUNK) {
            let mut cmd = deadpool_redis::redis::cmd("HMGET");
            cmd.arg(self.map_key);
            for did in chunk {
                cmd.arg(did.as_str());
            }
            let ids: Vec<Option<u32>> = cmd.query_async(&mut conn).await?;
            let found: Vec<(String, u32)> = chunk
                .iter()
                .zip(ids)
                .filter_map(|(did, id)| id.map(|id| ((*did).clone(), id)))
                .collect();
            for (did, id) in &found {
                out.insert(did.clone(), *id);
            }
            self.cache.insert_all(found);
        }
        Ok(out)
    }

    pub async fn intern_many(&self, dids: &[String]) -> anyhow::Result<HashMap<String, u32>> {
        let mut out = HashMap::with_capacity(dids.len());
        let missing = self.cache.partition(dids, &mut out);
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
            let mut fresh = Vec::with_capacity(chunk.len());
            for (did, id) in chunk.iter().zip(ids) {
                // Ids are allocated by INCR from 1 and will not exceed u32 for
                // the foreseeable network; a negative or oversized id means the
                // shared counter has been tampered with, and silently wrapping
                // it would corrupt every consumer of this id space.
                let id = u32::try_from(id)
                    .map_err(|_| anyhow::anyhow!("interner id {id} out of u32 range"))?;
                out.insert((*did).clone(), id);
                fresh.push(((*did).clone(), id));
            }
            self.cache.insert_all(fresh);
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
    fn dids(range: std::ops::Range<u32>) -> Vec<String> {
        range.map(|i| format!("did:plc:{i:024}")).collect()
    }

    fn cache_with(cache: &IdCache, range: std::ops::Range<u32>) {
        cache.insert_all(dids(range.clone()).into_iter().zip(range));
    }

    /// The whole point: however many distinct DIDs pass through, the cache
    /// never holds more than its ceiling. Unbounded, it OOMKilled the builder.
    #[test]
    fn cache_never_exceeds_its_ceiling() {
        let cache = IdCache::new(1_000);
        cache_with(&cache, 0..50_000);
        assert!(cache.len() <= 1_000, "held {}", cache.len());
        assert!(cache.len() >= 500, "keeps at least a generation");
    }

    /// An account that keeps coming up outlives any number of rotations; the
    /// tail seen once is what ages out.
    #[test]
    fn repeatedly_seen_dids_survive_rotation() {
        let cache = IdCache::new(100);
        let hot = dids(0..10);
        cache_with(&cache, 0..10);
        for round in 0..20u32 {
            let mut out = HashMap::new();
            let missing = cache.partition(&hot, &mut out);
            assert!(missing.is_empty(), "hot DIDs evicted in round {round}");
            assert_eq!(out[&hot[3]], 3);
            let start = 1_000 + round * 40;
            cache_with(&cache, start..start + 40);
        }
        // And the one-off tail from early rounds is gone.
        let mut out = HashMap::new();
        assert_eq!(cache.partition(&dids(1_000..1_040), &mut out).len(), 40);
    }

    #[test]
    fn partition_returns_hits_and_misses() {
        let cache = IdCache::new(100);
        cache_with(&cache, 0..5);
        let asked = dids(3..8);
        let mut out = HashMap::new();
        let missing = cache.partition(&asked, &mut out);
        assert_eq!(out.len(), 2);
        assert_eq!(missing, asked[2..].iter().collect::<Vec<_>>());
    }

    #[test]
    fn zero_ceiling_disables_the_cache() {
        let cache = IdCache::new(0);
        cache_with(&cache, 0..10);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn allocation_is_atomic() {
        assert!(LUA_GETSET.contains("HGET"));
        assert!(LUA_GETSET.contains("INCR"));
        assert!(LUA_GETSET.contains("HSET"));
    }
}
