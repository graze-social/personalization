# Durable co-liker profiles — design proposal

**Status:** proposal, 2026-08-11. Supersedes the accumulate-in-API design in
`~/.claude/plans/proceed-with-this-proposal-joyful-dream.md` (Phase 1).

**Goal:** serve highly relevant content to users who like *infrequently*, by using their
longitudinal like history to find taste-neighbours, then ranking the live candidate pool by
what those neighbours liked recently.

---

## 1. What the measurements established

**The historical like graph exists.** `default.user_action_logs`, `action_type =
'app.bsky.feed.like'` — **141,040,158 likes across 985,880 distinct users**, with `user_did`
(actor), `action_identifier` (post AT-URI) and `action_time`. 10.08 GiB, continuous at 5–8M
likes/month. (An earlier gate wrongly used `feed_interactions`, which records only likes on
Graze-*served* posts — 13× fewer likes — and produced a false negative. `global_action_logs`
has no actor column and is useless here.)

**The gap is large.** Of 357,575 feed requesters in 30 days, **64,241 have ≥20 likes of
history**. Of 3-day requesters, **13,196 have zero likes in the live 6-day window but ≥20 in
history** — versus 21,597 currently personalizable. That is a **+61% larger addressable
population**.

**The chain yields abundant candidates.** Five randomly sampled real lurkers (0 likes in 6d,
120–1,200 in 365d) produced 280–39,900 co-likers reaching 7,236–36,982 posts liked in the last
3 days. Against a **live** pool — lurker `65lxax66` (real user of algo 396, 367 impressions/7d,
`ap:396` = 10,473 candidates):

| | today | with historical co-likers |
|---|---|---|
| scoreable pool posts | **0** | **4,736 (45% of pool)** |
| ≥2 overlapping co-likers | 0 | 2,950 |
| ≥3 | 0 | 2,096 |

Active users reach only ~1,700–1,860 scoreable. **The lurker beats them 2.5×**, because a year
of history yields 3,550 co-likers where a 6-day window yields 127.

**Coverage saturates fast, which is what makes this cheap.** Same lurker, top-K co-likers
ranked by overlap:

| K | scoreable posts | ≥2 overlap | random-K baseline | % of full |
|---|---|---|---|---|
| 25 | 1,054 | 213 | 139 | 22% |
| 50 | 1,672 | 446 | 257 | 35% |
| 100 | 2,113 | 753 | 429 | 45% |
| 200 | 2,886 | 1,278 | 931 | 61% |
| 3,550 | 4,738 | 2,954 | 4,738 | 100% |

Ranking beats random by **7.6× at K=25**, so the ordering carries real signal. And **K=128
lands ~2,300 scoreable candidates — already above what active users get today.** A feed page is
30–50 posts, so that is 45–75× oversupply. We do not need full coverage.

---

## 2. Design

### 2.1 Nightly batch job, chunked

The co-liker self-join full-scans 141M rows — impossible on the request path, viable as a
nightly job. **It does not fit in one query.** Measured: a naive single query succeeded at 500
users but **OOMed at 2,000** (`Code: 241 ... would use 18.01 GiB ... While executing
JoiningTransform`, after 2m53s). Two changes fix it, and both *improve* the signal:

1. **Cap seed at the 128 most recent liked posts per user** (`LIMIT 128 BY u` on the seed CTE).
   Production already caps `max_user_likes`. For a lurker with 124 lifetime likes this is a
   no-op; for heavy users 128 recent posts is ample.
2. **Drop ultra-popular seed posts (`HAVING L <= 500`).** A post with 5,000 likers drags 5,000
   rows into the join while contributing `1/5000 = 0.0002` to `Σ 1/L_j` — negligible against a
   niche post's `1/5 = 0.2`. This is the dominant cost term, and cutting it *raises* average
   specificity, which is exactly what we want for relevance.

Measured result at 2,000 users: **3m50s, no OOM, 1,999 of 2,000 users profiled, average 123.8
co-likers of max 128** — the cap still binds, so the popularity filter does not starve profiles.

**Chunk size: prefer large.** The per-chunk 141M-row scan is a fixed cost, so cost is strongly
sublinear in users:

| users/chunk | wall clock | users profiled | avg co-likers |
|---|---|---|---|
| 2,000 | 3m50s | 1,999 | 123.8 |
| 8,000 | **4m50s** | 7,994 | 123.7 |

4× the users for 1.26× the time. Fitting `t ≈ 210s + 10s per 1,000 users` implies ~14 min for
all 64,241 in a single query — but memory, not time, is the binding constraint, and 18 GiB is
the observed ceiling. **Plan on 8 chunks of 8,000 ≈ 40 minutes total**, and probe upward from
there. Bucket on `cityHash64(user_did) % N` so chunks are stable across runs and independently
retryable; pipeline the Valkey writes per chunk so nothing large is held client-side.

**Floor on profile size.** `min_cl` was 1 in the 8,000-user run — a few users derive almost no
co-likers. Serving a 1-member profile yields near-zero coverage and gives `overlap_count` no
room to discriminate. Gate with a `MIN_PROFILE_SIZE` (start ~10) and skip the write below it;
those users keep today's fallback behaviour.

The query, with both fixes:

```sql
WITH targets AS (                       -- profile-eligible users
  SELECT user_did FROM (
    SELECT user_did, count() c FROM user_action_logs
    WHERE action_type='app.bsky.feed.like' AND action_time >= now()-INTERVAL 365 DAY
    GROUP BY user_did HAVING c BETWEEN 20 AND 5000      -- excl. 959 hyper-likers
  ) WHERE user_did IN (SELECT DISTINCT user_did FROM user_action_logs
        WHERE action_type='app.bsky.feed.defs#interactionSeen'
          AND action_time >= now()-INTERVAL 30 DAY)),
seed0 AS (                              -- cap: 128 most recent liked posts per user
  SELECT user_did u, action_identifier post, min(action_time) t
  FROM user_action_logs
  WHERE action_type='app.bsky.feed.like' AND action_time >= now()-INTERVAL 365 DAY
    AND user_did IN (SELECT user_did FROM targets)
  GROUP BY u, post
  ORDER BY u, t DESC LIMIT 128 BY u),
Lj AS (                                 -- post specificity 1/L_j; drop viral posts
  SELECT action_identifier post, count() L FROM user_action_logs
  WHERE action_type='app.bsky.feed.like'
    AND action_identifier IN (SELECT DISTINCT post FROM seed0)
  GROUP BY post HAVING L <= 500),
seed AS (
  SELECT s.u u, s.post post, s.t t, L.L L
  FROM seed0 s INNER JOIN Lj L ON L.post = s.post)
SELECT s.u AS u, o.user_did AS cl, sum(1.0/s.L) AS score
FROM user_action_logs o
INNER JOIN seed s ON o.action_identifier = s.post
WHERE o.action_type='app.bsky.feed.like'
  AND o.user_did != s.u
  AND o.action_time < s.t               -- "liked before me", matches coliker.rs:223-232
GROUP BY u, cl
ORDER BY u, score DESC
LIMIT 128 BY u                          -- ClickHouse per-group top-K
SETTINGS join_algorithm='parallel_hash'
```

`LIMIT 128 BY u` does top-K per user server-side, so nothing oversized is ever materialised
client-side. Folding `Lj` into `seed` (rather than joining it a second time in the main query)
removes one of the two chained joins that caused the OOM.

**Score = `Σ 1/L_j` over overlapping posts.** This is the specificity-weighted measure from the
prior design: a neighbour who overlaps on 3 obscure posts outranks one who overlaps on 5 viral
posts. It is exactly the one factor in `scoring_core.rs:480` that is stable at profile-build
time (`likers.len()` is a property of the post). The activity term (`1/source_total_likes`) and
recency stay at read time, as today.

**This eliminates the entire accumulation machinery.** The prior design needed `ZINCRBY`, a
watermark for idempotency, a `SET NX` lock, and throttled `EXPIRE` — because it accumulated
incrementally in the request path. A nightly full rebuild over 365 days of history *inherently*
includes yesterday's likes, so:

- **no changes to `graze-like-streamer`**
- **no changes to the request write path**
- no watermark, no lock, no replay-idempotency problem
- staleness ≤24h on the *co-liker set*, which is irrelevant — taste-neighbours are durable, and
  the candidates being ranked come from live data

### 2.2 Storage: packed binary string, not a zset

Measured on the production Valkey instance:

| representation | bytes/user @K=128 | encoding |
|---|---|---|
| ZSET (16-hex member + float score) | 3,632 | listpack |
| ZSET @K=300 | 25,512 | **skiplist** |
| **packed string (8B hash + 4B f32) × 128** | **1,840** | raw |
| packed string @K=64 | 944 | raw |

Two things fall out:

1. **K=300 costs 7× the memory of K=128 for 2.3× the data** — the listpack→skiplist cliff at
   128 members (28.4 vs 85.0 B/member). K=128 is the natural design point, and the coverage
   curve says it is more than sufficient.
2. **A packed string halves K=128 again**, because a nightly wholesale overwrite never needs
   `ZADD`, `ZINCRBY`, or `ZREMRANGEBYRANK`. We were paying zset overhead for capabilities the
   batch design makes worthless. The read path wants all 128 entries anyway — never a range or
   a partial — so one `GET` of 1,840 B beats a `ZREVRANGE` of 128 members on allocations and
   round-trip payload too.

```
Key:    ucl:{user_hash}                       (flat, not date-sharded)
Value:  128 × [ 8-byte co-liker hash | 4-byte f32 score ]   = 1,536 B, big-endian
TTL:    7 days                                (survives several failed nightly runs)
```

Flat key with a flat TTL, following the `ula:` precedent (`keys.rs:439`). **Must not** live
under the `colikes:` prefix: `scripts/redis_prune_retention.py:38-49` treats that prefix as
safe-to-delete, and `coliker.rs:80-82` gates the cache-hit branch on `ttl > 0` so a persistent
key would fall through to a full recompute on every request.

### 2.3 Memory budget

| | |
|---|---|
| 64,241 profiles × 1,840 B | **118 MB** |
| same as ZSET (for comparison) | 233 MB |
| `ula:` reclaimed on retirement | **−23,700 MB** |
| **net change** | **≈ −23.6 GB** |

Also removes `zincrby`'s **7.8% of Valkey command CPU** — it is used *only* for `ula:`.

### 2.4 Read path: strictly additive

Integration point is the existing early-exit at `mod.rs:286`:

```rust
if coliker_weights.is_empty() {
    // NEW: fall back to the durable profile instead of returning nothing
    if let Some(w) = self.coliker.get_durable_profile(user_hash).await? { ... }
    else { return Ok(ScoringResult::empty()); }
}
```

**This is the key risk property.** Only users who *today* get an early-exit and 100% fallback
are affected. The 21,597 currently-personalized users take a byte-identical path. There is no
regression surface for existing traffic, and the new segment's comparison baseline is fallback,
not better personalization.

`scorer.score()` takes `source_weights: &HashMap<String, f64>` (`scorer.rs:77-84`), so the
profile just needs to produce that map. Absolute score magnitude is load-bearing in exactly two
places — `weight.min(max_coliker_weight)` (`scorer.rs:326,397`, cap `1e-6`) and `score > 0.0`
(`:358,419`) — so a **rank-preserving rescale to a max just under the cap** is provably safe and
needs no calibration against live `colikes:` values.

---

## 3. On compression: roaring bitmaps, bloom filters, embeddings

Directly addressing whether a compression play helps. Short version: **memory is not the
constraint** — 118 MB in a service where one dead key family wastes 23.7 GB. But one of these
ideas is strategically the most important thing on this list.

**Packed binary string — yes, adopt.** Measured 2× win over the zset (1,840 vs 3,632 B),
already folded into the design above. This is the whole of the available "compression" win, and
it comes from removing unused capability rather than from clever encoding.

**Roaring bitmaps — no for v1, but the right escape hatch.** Roaring compresses sets of
*integers*, and wins when they are dense or clustered. Our members are 64-bit hashes — uniformly
random and maximally sparse, roaring's worst case; at 128 members it would be *larger* than
packed raw. It becomes genuinely attractive only if we ever want full coverage (K=3,550 →
42 KB/user → 2.7 GB packed). Then the move is to intern co-liker DIDs into dense sequential
integer IDs — exactly what this codebase already does for post URIs via the URI interner — so
~1M active likers fit in 20-bit IDs and roaring's run-length encoding starts paying. Worth
keeping in the back pocket; not worth building while K=128 already exceeds active-user coverage.

**Bloom filters — no.** A bloom answers "is X in the set?" probabilistically and cannot
enumerate or carry weights. We need *both* the co-liker identity and its score to compute
relevance, and false positives would inject neighbours the user has no affinity with — directly
against the relevance goal. As a cheap pre-filter ("does this candidate have any of my
co-likers?") it is technically apt, given 87% of candidates have zero likers, but that test is
already done server-side via `pl:` and is not the measured bottleneck. Optimization without a
demonstrated cost.

**Graph embeddings — not compression, but the strategically right v2.** Instead of storing 128
neighbour IDs, learn one vector per user from the like graph (e.g. 64 floats = 256 B) and one
per post, then score by similarity. This is smaller than the packed profile, but the size is
beside the point. What matters is that it is **dense by construction**: every candidate gets a
score, with no overlap required. That dissolves the wall behind all of this work — **87% of
candidates have zero likers and the median pool post has 1 like** — which no amount of seed
history can fix, because the sparsity is on the candidate side. It also generalises past direct
intersection: two users who share *no* posts can still sit close in the graph if their tastes
meet one hop away. We have 141M edges to train on. Costs are real (training pipeline, ANN index,
much harder to audit and debug), so this is a follow-on, not a v1 — but it subsumes the co-liker
profile entirely and is where the ceiling actually is.

---

## 4. Risks

1. **Coverage ≠ engagement — the main unknown.** The +13% personalized lift (within-user,
   z≈2.9, p≈0.004) was measured on *active* users. This segment is defined by not liking things
   recently. More rankable candidates is proven; more engagement is not. Ship behind a flag with
   a holdback and measure.
2. **`overlap_count` distortion.** It is weight-independent (`scorer.rs:286,354,415`) and feeds
   a nonlinear `paths_boost = overlap_count^num_paths_power` (`:360-362`). Lurker sets are ~28×
   larger than active-user sets and one post drew 170 overlapping co-likers, so ranking will
   distort, not merely rescale. Must be compared in shadow mode before serving. Mitigated by
   §2.4: nobody currently personalized is exposed.
3. **The bot filter is inert.** `bot:filtered` holds **13 users**, because
   `BOT_LIKE_THRESHOLD=5000` per 6-day window ≈ 833/day. Sampling turned up accounts with
   216,073 and 101,288 likes/year. The batch job must not rely on it — hence the explicit
   `HAVING c BETWEEN 20 AND 5000` band, which excludes 959 hyper-likers. Also consider capping
   any single co-liker's contribution; one co-liker supplied 572 of 10,473 pool posts.
4. **Selection optimises coverage, not relevance.** A greedy coverage oracle reaches 88% with
   100 co-likers vs 45% for overlap-ranked — but the co-likers it picks are prolific likers,
   which is precisely the low-specificity signal `Σ 1/L_j` is designed to down-rank. Do **not**
   chase the oracle; it optimises the wrong objective.
5. **`user_action_logs` has dirty timestamps** — `app.bsky.feed.like` spans 2017→2038. Bound
   every window with `BETWEEN now()-INTERVAL N DAY AND now()`.

---

## 5. Phases

**Phase A — batch job, dark. ✅ BUILT AND VALIDATED 2026-08-11.**

Shipped as `graze-build-coliker-profiles`
(`crates/graze-candidate-sync/src/bin/build_coliker_profiles.rs`, logic in
`src/coliker_profiles.rs`, codec in `graze-common/src/coliker_profile.rs`). Writes `ucl:` keys
and nothing else — no read path exists, so it cannot change what any user is served.

Validated end-to-end against production (bucket 0 of 256, i.e. ~1/256 of the population):

| check | result |
|---|---|
| users returned / profiles written | 242 / 241 (1 skipped below `MIN_PROFILE_SIZE`) |
| mean profile size | **124.7** of a 128 cap — matches the 123.7 the SQL predicted |
| `STRLEN` of a full profile | **1,536 B** (128 × 12), exactly as designed |
| `MEMORY USAGE` | **1,840 B**, matching the synthetic measurement exactly |
| `TTL` / `OBJECT ENCODING` | 604,152 s (6.99 d) / `raw` |
| stored co-likers' live activity | all 8 sampled have **55–1,733 likes in the live 6-day window** |

That last row is the one that matters: profiles built from a dormant user's year-old history point
at co-likers who are active *now*, whose recent likes are in `ul:`/`pl:` — so the set is directly
usable by the scorer.

Runtime: 129 s for 242 users at `PROFILE_CHUNK_COUNT=256`. The ~120 s scan is a fixed per-chunk
cost (242 users → 129 s vs 8,000 → 290 s), so run few, large chunks: **`PROFILE_CHUNK_COUNT=8`
projects to ~39 min for all 64,241 users.** Confirm on the first full run.

Remaining before Phase B: nothing blocking. `bin/` defaults are the measured design points; start
with `PROFILE_DRY_RUN=1` on a new environment.

**Phase B — shadow. ✅ BUILT 2026-08-11, not yet enabled.**

Attaches at the `coliker_weights.is_empty()` early-exit in `algorithm/mod.rs` via
`try_durable_profile`, which loads `ucl:`, runs a full scoring pass, emits a
`durable_profile_shadow` log line, and then **discards the result** so the response is
byte-identical to today. Serving is a separate flag.

| piece | where |
|---|---|
| `get_durable_profile` + `profile_weights_from_scores` | `graze-api/src/algorithm/coliker.rs` |
| overlap observability on `ScoringResult` | `graze-api/src/algorithm/scorer.rs` |
| shadow arm | `graze-api/src/algorithm/mod.rs` (`try_durable_profile`) |
| flags | `graze-api/src/config.rs` |

**Flags (both default off — deploying the image changes nothing):**

- `DURABLE_PROFILE_SHADOW_MODE=1` — compute + log, serve nothing. This is Phase B.
- `DURABLE_PROFILE_ENABLED=1` — actually serve. This is Phase C; leave off.
- `DURABLE_PROFILE_WEIGHT_TARGET` — default `2e-7`, must stay under `MAX_COLIKER_WEIGHT` (1e-6).

**Weight conversion.** Stored `Σ 1/L_j` scores (order 1e-3..1e2) are rescaled so the top entry
lands on `durable_profile_weight_target`. Dividing by the *max* (not the sum) preserves ranking
exactly, keeps every weight below the `weight.min(max_coliker_weight)` clamp — which would
otherwise flatten the profile into arbitrary ties — and stops a 12-member profile from being
handed larger per-entry weights than a 128-member one. Six unit tests cover these properties;
a config test asserts `target < max_coliker_weight`.

**What to read in the logs.** `durable_profile_shadow` carries `profile_size`, `scored_count`,
`posts_checked`, all three skip reasons, and `overlap_mean` / `overlap_max` / `overlap_hist`
(buckets `1,2,3-4,5-8,9-16,17-32,33-64,65+`). Compare its `overlap_hist` against the same fields
now emitted on `scorer_completed` for the live arm. **If the profile arm's distribution is shifted
well right of the live arm's, `paths_boost = overlap_count^num_paths_power` is distorting rather
than rescaling, and Phase C needs `num_paths_power` retuned before it serves.** A real lurker
profile was observed with 170 overlapping co-likers on a single post, so this is a live concern.

Also newly visible on both arms: **`posts_skipped_low_overlap`**, the count dropped by
`min_overlapping_colikers`, which was previously invisible and is what separates "no reachable
candidates" from "reachable but too thinly overlapped".

**Expected firing rate.** Sampling 29 profiled users, **6 (21%) had zero live 6-day likes**, i.e.
the shadow fires for roughly a fifth of profiled users; the rest are served by the live path and
never reach this branch.

### Phase B deployed and first results — 2026-08-11

Image `dgaff/personalization:phaseb-ucl-20260811` (digest `sha256:1db15bc0…`) on
`personalization-api` (DO sfo3), `DURABLE_PROFILE_SHADOW_MODE=1`, `DURABLE_PROFILE_ENABLED`
unset. Phase A run across all 8 buckets first: **62,905 profiles, mean size 124.24, 89.4 MB
wire (~116 MB in Valkey), 23.4 min, 0 chunks failed.**

Four known profiled lurkers driven through `POST /v1/personalize` on algo 396
(`ap:396` = 10,703 candidates). All defaults in play: `INVERTED_MIN_POST_LIKES=10`,
`INVERTED_MAX_LIKERS_PER_POST=30`, `MIN_OVERLAPPING_COLIKERS=1`.

| user | profile | scored | no_likers | few_likers | **low_overlap** | ovl_mean | ovl_max | hist |
|---|---|---|---|---|---|---|---|---|
| 2557e27f | 128 | 12 | 1,866 | 5,048 | 3,777 | 1.00 | 1 | `1:12` |
| 4ca71711 | 128 | **500** (capped) | 1,866 | 5,048 | 3,102 | 1.10 | 3 | `1:620,2:62,3-4:5` |
| abcd3a4e | 128 | 2 | 1,866 | 5,048 | 3,787 | 1.00 | 1 | `1:2` |
| 260c3db6 | 128 | 24 | 1,866 | 5,048 | 3,765 | 1.04 | 2 | `1:23,2:1` |

All four served nothing (`serving=false`) and still logged `no coliker weights found`, so the
response was unchanged — shadow mode behaved exactly as specified.

**Risk 2 (`overlap_count` distortion) is resolved — it is a non-issue.** Live-arm runs in the
same window showed `overlap_max` of 8, 1, 8, 2; the profile arm showed 1, 3, 1, 2. The profile
distribution is *not* shifted right of live — it is slightly lower. With `overlap_mean ≈ 1.0`,
`paths_boost = overlap_count^num_paths_power ≈ 1`, so there is nothing to retune. The fear was
that 128 co-likers would inflate overlap; in reality `pl:` is so sparse (p50 = 1 liker/post,
truncated at 30) that two co-likers rarely land on the same candidate.

**The real bottleneck is now visible, and it is not overlap.** Of 10,703 candidates:
1,866 have no likers, **5,048 (47%) are killed by `INVERTED_MIN_POST_LIKES=10`**, and of the
~3,789 survivors, **~3,777 have zero overlap with the 128-member profile**. So 12 score. The
`min_post_likes=10` filter is the largest single loss and the most promising Phase C lever —
note the modal `min_post_likes` observed in `feedContext` was 5, so 10 is already stricter than
typical.

**Correction to the offline coverage estimate.** §1's curve predicted ~2,300 scoreable at K=128,
but production delivers 2–500. The offline harness intersected `ul:` directly and modelled
neither `min_post_likes` (−47%) nor `max_likers_per_post=30` truncation. **The 45% / ~2,300
figures overstate what production yields; treat 2–500 as the real range**, of which one of four
users got a full feed page and one got a usable 24.

Still 0 → non-zero for every user tested, which is the point. But the honest read is that the
median lurker gets a partial feed, not a full one.

### Correction: `min_post_likes` is NOT the bottleneck — liker truncation is

The inference above ("`min_post_likes=10` is the largest loss") was **wrong**, and offline
measurement against the four stored profiles disproves it. `few_likers` fires *before* the
overlap check, so it was discarding candidates that had no profile overlap anyway. Scoreable
candidates by threshold:

| user | overlapping | ≥10 | ≥5 | ≥3 | ≥1 |
|---|---|---|---|---|---|
| 2557e27f | 41 | 39 | 41 | 41 | 41 |
| 4ca71711 | 1,236 | 1,051 | 1,141 | 1,190 | 1,236 |
| abcd3a4e | 7 | 7 | 7 | 7 | 7 |
| 260c3db6 | 67 | 67 | 67 | 67 | 67 |

Going 10 → 1 buys +5%, +18%, 0%, 0%. Not the lever.

**The real constraint is `INVERTED_MAX_LIKERS_PER_POST=30`.** The scorer fetches the N *most
recent* likers per candidate, so a co-liker who liked early is invisible — detection probability
is `min(1, N/L)`. **72–100% of eligible overlapping candidates have L > 30**, and modelling that
truncation reproduces production almost exactly:

| user | predicted at N=30 | observed production |
|---|---|---|
| 2557e27f | 16 | 12 |
| 4ca71711 | 589 | 500 (hit the cap) |
| abcd3a4e | 1 | 2 |
| 260c3db6 | 21 | 24 |

Expected scoreable by N: 30 → 100 gains **+39%, +44%, +100%, +76%**, flattening past 100–200.

**Shipped:** `DURABLE_PROFILE_MIN_POST_LIKES=5` and `DURABLE_PROFILE_MAX_LIKERS_PER_POST=100`,
applied via a **separate `Scorer` instance with its own `LikerCache`**. The separate cache is
load-bearing, not tidiness: `scorer.rs:520` writes fetched liker lists back into the cache, so a
shared one would leak 100-liker lists into the live arm and silently change what it scores. Two
config tests assert the live arm stays at 10/30.

**Phase C — serve behind a flag,** for a fraction of the affected segment, with a holdback.
Measure engagement by source class the established way: decode
`JSONExtractString(tryBase64Decode(interaction_feed_context),'source')` from
`default.feed_interactions`, **within-user** and depth-banded. (The naive cross-population
comparison overstates the effect ~2×.) Do not measure on a short quiet-period sample — two
comparisons in this investigation were invalidated that way.

**Phase D — retire `ula:`.** Remove writes (`streamer.rs:648-710`) and the read path in the same
PR so the flags cannot drift; a stale `AUTHOR_AFFINITY_ENABLED=true` against a missing `ula:`
fails *open* (silently returns zero posts). Retire `bin/backfill_ula.rs`. Reclaims 23.7 GB and
7.8% of Valkey command CPU. Independently justified — `ula:` measured 0.77% like rate vs
fallback's 6.55% and is already read-disabled — so it need not wait on A–C.

## 6. Out of scope

- Extending `DEFAULT_RETENTION_DAYS` or raw `pl:`/`ul:` retention. ~1.6 GB/day, and the 24h
  recency half-life makes a 14-day-old like worth 0.006% of a fresh one. The point of this
  design is that the *derived* signal is durable while the raw window stays short.
- Widening the seed window for users who already have recent likes: measured 1.04–1.10× for 23×
  more seed. Saturation makes that pointless. The value here is entirely 0 → N.
- Author-affinity scoring in any form. A user's 500 liked authors overlap a feed's 8,851 pool
  authors by 1–2.
