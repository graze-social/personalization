# Personalization theory ledger

Running tally of theories about what drives personalization coverage/quality, and how to reach
more users more cheaply. Started 2026-08-11 (overnight autonomous session).

**Goal:** personalize for as many users as possible, performantly and CPU-cheaply, while keeping
quality. Search space: how far back likes are considered, and what is in/out of bounds.

**Status key:** 🟢 confirmed · 🔴 refuted · 🟡 partial / conditional · ⬜ untested · 🔵 in progress

---

# ☀️ READ THIS FIRST — overnight summary

## The one-sentence version
The seed side is not the lever and neither are like-window widths — but be careful which
denominator you use: **zero-seed users are ~23% of people yet ~50% of requests, because they
paginate at a 1-second cadence.** Per-request personalization metrics are systematically distorted
by pagination; the real reach ceiling is the ~23%, and the remaining wins are in the scoring path's
cost and lookup direction, not its windows.

## Shipped and deployed (all live in prod, verified healthy, zero errors)

| # | change | predicted | **actually measured in prod** |
|---|---|---|---|
| **T16** | Seed gate looked back **2 days** while the scorer reads **6**. Now matches, via new pipelined `exists_any`; window exposed as `USER_DATA_CHECK_DAYS`. | +14% of DAU | **Live and correct** (7.6 EXISTS/request confirms), but `no_user_data` moved 50.0% → 49.3% — i.e. **~8% of requesters / ~5% of requests**, not 14%. Its target segment requests infrequently. |
| **T15** | `pl:` shards older than a post's intern date are provably empty but all 6 were probed. Added `post_likers_retention_bounded`; wired into scorer (1 site) + candidate-sync (6 sites). | −59% scoring read ops | **Unverified.** zcard went 2,410 → 2,655/sec (+10%), confounded by the simultaneous T16 deploy, sync cold-start, and diurnal traffic. Provably a strict subset of the old key set, so it cannot be harmful. Needs a clean isolated A/B. |
| **T6** | Profile arm gets `min_post_likes=5`, `max_likers_per_post=100` via its own `Scorer` **and its own `LikerCache`**. | ~1.5× profile-arm reach | Deployed; profile arm sees little organic traffic yet (0 shadow firings in 40 min). |
| Phase B | Durable-profile shadow arm + `overlap_count` observability on both arms. | — | Deployed, serves nothing. |

**Deployed images:** `personalization-api` and `personalization-candidate-sync` both on
`dgaff/personalization:ucl-b2-shards-gate6`. 3/3 api pods healthy, 0 restarts, 0 error lines.

## What I got wrong tonight, corrected in place
Four separate overclaims, all caught by measurement — worth reading because the *pattern* matters:

1. **"+61% addressable population" for durable profiles → actually ~1.6% of DAU.** I inferred "who
   has recent likes" from ClickHouse when the live path reads firehose-fed `ul:`. Same class of
   error as the earlier `feed_interactions` trap, one level up. **Ground truth for live capability
   is `ZCARD ul:{hash}:{date}` — never ClickHouse.**
2. **"`min_post_likes` is the funnel bottleneck" → it isn't.** Relaxing 10→1 buys 0–18%; the filter
   fires before the overlap check, so it was discarding candidates that had no overlap anyway.
3. **"T16 gains 14% of users" → ~5% of requests.** I measured user-weighted and predicted a
   request-weighted outcome. Seedless users request 3.4× more often, so per-request reach is much
   smaller. **Always check whether a metric is per-user or per-request before predicting from it.**
4. **My probes were off by one day** — hardcoded dates ending `20260811` while UTC had rolled to
   `20260812`, so every probe skipped the highest-volume shard. Corrected; conclusions held, but
   T15's headline went 72% → 59%.

I also nearly reported a 37-response sample showing `no_user_data` at 14% as a win; the real figure
at 706 responses was 49.3%. Small samples on a 28%-random-holdout system are worthless.

## T7 (invert the lookup): BUILT, TESTED, REFUTED

I predicted 1.88× coverage and 33–145× fewer Redis ops from offline measurement. **Built it,
A/B'd it on live traffic, and it lost: 0.94× coverage at ~5× the latency** (26× slower when
co-liker sets exceed 256). Root cause: my harness capped co-liker sets at 128, but
`max_total_sources = 10000` and production sets run to 5,157 — the entire cost argument depended
on `|co-likers| << |candidates|`, which is false. Full post-mortem in the T7 section below.

It survives only at **K ≤ 256, where it gives 1.22× coverage** — which is where my offline number
came from. That points at the real next experiment: **lower `max_total_sources` from 10,000 to
~256**, a pure config change that would cut cost on the *existing* post-first path too, since T8
showed coverage saturating well before 256.

## Three independent measurements now say seed depth is not the lever
- 23× more seed → **1.1×** coverage (T1)
- 13,623 co-likers → 44 covered, while 455 co-likers → **95** covered (T8)
- 117 seed → 4 covered, while 9 seed → **55** covered (T16 validation)

What matters is *which* co-likers you get and whether they're active in *this feed's* pool. And the
signal the live path ranks co-likers on — raw overlap count — has **r = 0.07–0.20** with actual
coverage contribution (T17). That's close to noise.

## Recommended order of work

1. **Characterise the zero-seed 50%.** They are half of all eligible traffic and CF can never serve
   them. Are they polling clients, scrapers, or genuine heavy lurkers? 5.5 requests/user vs 1.6 is
   suspicious. If a large share is automated, the underserved *human* population is much smaller
   than 50% and priorities shift; if it's real lurkers, they need content-based ranking. **This is
   the highest-information question left and it is cheap to answer** (user-agent / auth pattern /
   request cadence analysis on existing logs).
2. **Build a user-hash randomized treatment override**, then use it to settle the co-liker cap
   question (and any of the other eight Thompson dimensions). Tried to answer it retrospectively
   from 14 days of `feedContext` engagement data; the signal strongly favoured K=250 and survived a
   within-(feed,user) design, but **failed a fallback control** — see T19. Adaptive assignment makes
   all retrospective arm comparisons unusable, and this override is the fix.
3. **Persist Thompson arms to Redis.** They are in-memory per-pod today, so every deploy wipes all
   learning across 3 replicas. The search space has almost certainly never converged.
3. **Isolate T15** with a clean A/B (swap only the candidate-sync image, hold everything else) to
   find out whether the 59% arithmetic translates to real load reduction.
4. **T14** — move `apc:` liker counts off 6-shard ZCARD recomputation onto ClickHouse
   `uri_action_counts_v2` (already exists) or incremental streamer updates. `zcard` is 26.6% of all
   Valkey commands.
5. **T17/T8** — with inversion making co-likers cheap, raise K to 256–512 to absorb the ~45%
   per-feed attrition. Do **not** switch co-liker ranking to activity without an engagement test:
   it gives 2.3× coverage but trades away exactly the specificity the normalization protects.

## Things deliberately NOT done, and why
- **Did not raise `INVERTED_MAX_LIKERS_PER_POST` globally.** It would buy 1.46× coverage for the
  live arm, but by multiplying the already-dominant per-candidate fetch cost 3.3× — spending
  precisely the resource T7 saves. Wrong trade to make blind.
- **Did not change co-liker ranking to activity-weighted.** Coverage/specificity trade with no
  engagement measurement available overnight.
- **Did not touch `DEFAULT_RETENTION_DAYS`, pool age windows, or `ALGO_POSTS_LIMIT`.** Evidence says
  pools are supply-limited rather than age-limited (49 of 66 are under 10k, only 3 near the 40k cap),
  and the retention window is already wider than the pool age cap makes useful.
- **Did not enable `DURABLE_PROFILE_ENABLED`.** Phase C should follow an engagement test, and the
  segment turned out to be ~1.6% of DAU.

---

## Confirmed / refuted so far (carried in from earlier work)

### 🔴 T1 — "Widening the seed window helps users who already have recent likes"
**REFUTED.** 23× more seed and 13× more co-likers bought only 1.04–1.10× more scoreable
candidates. Co-liker coverage saturates at ~100–200 co-likers because co-likers overlap heavily
on popular posts. *Do not* pursue seed-window widening for active users.

### 🔴 T2 — "Users with no recent likes are a large, addressable, entirely unserved segment"
**SUPERSEDED — see T2-REVISED below. The +61% figure is wrong; the real number is ~1.6% of DAU.**
The original reasoning: 13,196 of 138,745 3-day requesters have zero likes in the live 6-day
window but ≥20 in history, against 21,597 currently personalizable. That used
`user_action_logs` to infer who has recent likes, which understates it badly versus the
firehose-fed `ul:` the serving path actually reads.

Still valid from this entry: the historical like graph *is* `user_action_logs`
(`app.bsky.feed.like`, 141M rows, 986k users, years deep), and `feed_interactions` is a
serving-attribution table that must never be used as like history (13× fewer likes).

### 🟡 T3 — "A durable stored co-liker set makes those users servable"
**MECHANICALLY CONFIRMED, but the population it serves is ~1.6% of DAU, not 61%.** 62,905
profiles built (mean 124.2 co-likers, ~116 MB Valkey, 23.4 min for the full population). Four
sampled lurkers went 0 → 12/500/2/24 scored posts in production shadow mode, so the machinery
works end to end. It is simply aimed at a much smaller segment than I claimed — and **T16 reaches
~9× more users with no new infrastructure at all.**

### 🔴 T4 — "A ~128-member profile will distort `paths_boost` via inflated `overlap_count`"
**REFUTED.** Profile arm `overlap_max` 1–3 vs live arm 1–8; `overlap_mean ≈ 1.0`, so
`paths_boost = overlap_count^power ≈ 1`. `pl:` is too sparse (p50 = 1 liker/post) for two
co-likers to collide on one candidate. This was my top-rated risk; it is a non-issue.

### 🔴 T5 — "`min_post_likes=10` is the dominant loss in the funnel"
**REFUTED.** It *looked* dominant (5,048 of 10,703 candidates dropped) but `few_likers` fires
*before* the overlap check, so it was discarding candidates that had no profile overlap anyway.
Relaxing 10 → 1 buys only +5%, +18%, 0%, 0% across four lurkers.

### 🟢 T6 — "Liker-list truncation is the dominant reach constraint"
**CONFIRMED, and it is a systematic bias, not just a volume cap.** The scorer fetches the N *most
recent* likers per candidate (`max_likers_per_post=30`), so a co-liker who liked the post earlier
is **invisible**. Detection probability is `min(1, N/L)` for a post with L likers, and **72–100%
of eligible overlapping candidates have L > 30**. Modelling only this reproduced production
almost exactly:

| user | predicted at N=30 | observed |
|---|---|---|
| 2557e27f | 16 | 12 |
| 4ca71711 | 589 | 500 (capped) |
| abcd3a4e | 1 | 2 |
| 260c3db6 | 21 | 24 |

Raising N 30 → 100 is expected to gain **+39% / +44% / +100% / +76%**, flattening past 100–200.
Shipped for the profile arm only, via a separate `Scorer` + separate `LikerCache` (the shared
cache would have leaked longer liker lists into the live arm — `scorer.rs:520` writes back).

---

## Active theories

### 🔵 T7 — "Inverting the lookup (co-liker→posts instead of post→likers) is both cheaper AND strictly higher-coverage"
**The big one.** Current scoring iterates candidates and fetches `pl:{post}` per candidate,
which is where truncation bias enters. The inverse — iterate the user's ~128 co-likers, read
`ul:{coliker}` (their recent likes), and intersect with the pool — has **no truncation at all**
and touches far fewer keys.

Cost comparison (order of magnitude):
- current: ~3,800 candidates × 6 date shards ≈ **22,800** ZRANGEBYSCORE ops per scoring run
- inverted: 128 co-likers × 6 date shards = **768** ZRANGE ops, then in-memory intersect

Evidence already in hand: my offline harness *is* the inverted method, and it found 41 / 1,236 /
7 / 67 scoreable where production (pl:-based) found 12 / 500 / 2 / 24 — **2–3.4× more coverage**.

If this holds it satisfies both halves of the goal at once: ~30× fewer Redis ops *and* 2–3×
coverage. Needs: validation at scale (many users × many feeds), memory bound on heavy co-likers,
and a decision on whether to apply it to the live arm too.

### ⬜ T8 — "Profile size K matters more than the earlier curve suggested, now that truncation is understood"
The K-coverage curve was measured *through* the truncating pl: path, which suppresses the value
of extra co-likers (each additional co-liker has only `min(1,N/L)` chance of being seen). Under
the inverted lookup every co-liker is fully visible, so coverage may keep climbing well past
K=128. Re-measure K ∈ {32, 64, 128, 256, 512} under the inverted method, against memory
(listpack cliff at 128 applies only to ZSETs; the packed string is linear at 12 B/entry).

### ⬜ T9 — "Candidate pool age window is a cheap coverage lever"
`SYNC_PREFERRED_MAX_AGE_HOURS=72`, `SYNC_FALLBACK_MAX_AGE_HOURS=336`. A larger pool means more
chances to intersect, but staler posts. Measure scoreable-candidate count and post age
distribution as a function of pool age, and check the interaction with `ap:` set size (pool size
was the dominant coverage predictor at r=0.604).

### ⬜ T10 — "The 6-day `pl:`/`ul:` retention is the ceiling on the inverted method"
Under the inverted lookup, coverage is bounded by how much of each co-liker's like history is
retained in `ul:`. At 6 days retention we see only 6 days of co-liker activity. Since candidates
themselves are ≤72–336h old, this may already be sufficient — but worth quantifying: what
fraction of pool-post likes fall inside 6 days?

### ⬜ T11 — "Most feeds are too small to personalize at all, and that dominates population reach"
`MIN_CANDIDATE_POOL_FOR_PERSONALIZATION=500` gates out small pools. Quantify: distribution of
`ap:` sizes across all live feeds, how many users request only small-pool feeds, and whether the
inverted method (being cheap) makes personalizing small pools viable after all.

### ⬜ T12 — "Only ~6% of feed requests reach personalization; cache hit rate caps any gain"
Observed 158 feed requests → 9 personalization computations. Whatever we improve applies to a
small slice unless cache TTL/invalidation is also considered. Quantify the real ratio over a
diurnal cycle before over-investing in per-run quality.

---

---

## ⚠️ MAJOR CORRECTION — T2/T3 reach was overstated ~36×

### 🔴 T2-REVISED — "durable profiles unlock +61% of the population"
**REFUTED against ground truth.** Measured 2,500 sampled **daily-active** users directly against
the live `ul:` graph (the exact keys the serving path reads, `ul:{hash}:{YYYYMMDD}` × 6 days):

| among 2,500 DAU | share |
|---|---|
| **already personalizable today** (has live 6d likes) | **63.2%** |
| has a durable profile (`ucl:`) | 24.4% |
| both — profile adds nothing for them | 22.7% |
| **profile-only → genuinely newly reachable** | **1.6%** |
| neither → unreachable by collaborative filtering | 35.2% |

**Durable profiles add ~1.6% of DAU, not 61%.**

**Root cause of my error — the same trap twice, one level up.** I corrected from
`feed_interactions` → `user_action_logs` and assumed the latter was "the" like graph. It is not
the firehose either. `ul:` in Valkey is Jetstream-fed and has far broader like coverage than
`user_action_logs`. Estimating "who has recent likes" from *any* ClickHouse table understates it.

**Rule going forward: the only ground truth for "can this user be personalized today" is
`ZCARD ul:{hash}:{date}` over the retention window.** ClickHouse is for history and analysis,
never for live-capability estimates.

### 🟢 What this reframes
63.2% of DAU already have seed, yet only ~6% of feed requests reach personalization and 81% of
responses contain zero personalized posts. **Seed availability is not the bottleneck — the
scoring path is.** So T6/T7 (truncation, lookup direction) matter far more than anything about
like windows, and they should be aimed at the **live arm serving 63.2% of DAU**, not the profile
arm serving 1.7%.

Durable profiles are still worth keeping (they're built, cost ~116 MB, and serve a real if small
segment) but they are not the lever.

---

## Feed-level reach findings

### 🟢 T11-PARTIAL — "Personalization is structurally limited to ~65 feeds"
**CONFIRMED, but it is mostly correct behaviour, not a bug.** Only **66 of 10,455 probed feed ids
have a live `ap:` pool**, and `HLEN feed:access = 65` — independently confirmed by
candidate-sync issuing exactly 65 ClickHouse queries per cycle. Pools are demand-driven: sync
covers only feeds present in the `feed:access` HSET within `FEED_ACCESS_WINDOW_SECONDS=86400`.

Context: 1,873 distinct feeds were requested in 24h (1.69M impressions, 88,500 users), but
personalization_layer only *supports* ~175–190 (`supported_feeds`=190,
`algo_id_to_feed_uri`=175); the rest are served elsewhere. Of the 175 supported, **161 have no
pool** — but those 161 account for only **14,441 impressions (0.85%) and 859 users (1.0%)**, and
121 of them have zero traffic. So the demand-driven design is working; the tail is genuinely dead.

### 🟡 T13 — "Empty-response feeds are locked out of syncing permanently"
**PLAUSIBLE, low impact.** `record_feed_access` is gated on `!response.feed.is_empty()`
(`api/feed.rs:1164`). An unsynced feed has neither a candidate pool nor fallback tranches
(`trending:`/`popular:`/`velocity:`/`discovery:` are all synced per-algo too), so it serves empty
→ access never recorded → never synced. Registration seeds `feed:access` once
(`api/admin.rs:154`), so a feed that goes quiet for 24h falls out and cannot self-recover.
Bounded by the 1% traffic figure above, so it is a correctness wart rather than a reach lever.

---

## Truncation: two results that look contradictory, both true

### 🟢 T6 stands — but for a subtler reason than "pools are popular"
Pool-wide, truncation looks almost free. Per-feed, weighted over eligible posts (`L≥5`), the
fraction of co-liker signal visible is:

| | E(N=30) | E(N=100) | E(N=200) |
|---|---|---|---|
| weighted across 12 real pools | **90%** | 97% | 99% |

Only 8–29% of pool posts have >30 likers (mean L = 15–103). So on a *random* pool post,
`max_likers_per_post=30` costs ~10%.

**But the posts that matter are not random.** A co-liker is far likelier to have liked a *popular*
post, so the posts where overlap actually occurs skew high-L: **72–100% of the four lurkers'
overlapping candidates had L > 30.** The realized effect is what I measured empirically, both
sides observed, no modelling: production found **12 / 500 / 2 / 24** scoreable where the
untruncated inverted method found **41 / 1,236 / 7 / 67** — a **2.4–3.4× coverage gap.**

Lesson: pool-average statistics understate truncation damage because overlap is
popularity-biased. Judge truncation on the overlap-conditioned distribution, not the pool.

---

---

## 🟢 T7 CONFIRMED — inverting the lookup wins on both axes

Measured the full live chain server-side for 10 real live-seeded users against algo 2323
(pool 19,667): their `ul:` → co-likers via `pl:` top-30 (exactly what production sees) → top-128
co-likers → their `ul:` ∩ pool. Production-equivalent coverage modelled as `Σ min(1,30/L)` from
`apc:` (validated to ~15% against observed production earlier).

| user likes | co-likers found | inverted coverage | prod @N=30 | ratio | Redis ops |
|---|---|---|---|---|---|
| 467 | 13,623 | 44 | 27 | 1.6× | 3,570 |
| 245 | 5,657 | 30 | 19 | 1.6× | 2,238 |
| 127 | 3,631 | 11 | 8 | 1.4× | 1,530 |
| 83 | 455 | 95 | 55 | 1.7× | 1,266 |
| 34 | 1,470 | 104 | 52 | 2.0× | 972 |
| 16 | 765 | 128 | 57 | 2.2× | 864 |
| 10 | 717 | 8 | 3 | 2.7× | 828 |

**Totals: inverted 441 vs 234 → 1.88× coverage.** And `INVERTED_MAX_POSTS_TO_SCORE = 0`
(**unlimited**), so production fetches likers for *every* pool candidate: algo 2323 costs
~19,667 × 6 = **118,000 ops per scoring run** versus the inverted method's **810–3,570** —
**33–145× fewer**.

**Raising `max_likers_per_post` 30 → 100 alone captures 1.46× (78% of full inversion)** as a pure
config change — but it *multiplies* the already-dominant per-candidate fetch cost by 3.3×, so it
buys coverage by spending exactly the resource inversion saves. Inversion is the better trade.

### 🟡 T8-PARTIAL — co-liker count does not predict coverage; K=128 is not clearly enough
Counterintuitive: **13,623 co-likers → 44 covered, while 455 co-likers → 95 covered.** Selecting
top-K by raw overlap count favours similar-but-inactive neighbours. Contribution by rank quartile
(new in-pool posts; note later quartiles only count *new* posts, so the decline is partly a
dedup-order artifact):

| user | q1 (top 32) | q2 | q3 | q4 | overlap sum q1/q4 |
|---|---|---|---|---|---|
| e1c6f180 | 22 | 13 | 5 | 4 | 725/316 |
| c4911553 | 45 | 8 | 25 | 17 | 135/32 |
| 698a8ab7 | 46 | 16 | 34 | 8 | 95/32 |
| de496c17 | 78 | 28 | 12 | 10 | 69/32 |
| fb72683c | **0** | 15 | 3 | 12 | 482/145 |

Ranks 33–128 roughly **double** what the top 32 contribute, so coverage is still climbing at
K=128. And fb72683c's top-32 contributed *nothing*. Ranking by overlap is weakly informative at
best. Worth testing selection by co-liker *recent activity* (maximises coverage) versus overlap
(maximises specificity) — an explicit quality/coverage trade, currently made implicitly.

---

## 🟢 T14 — The dominant Valkey cost is recomputing liker counts, not personalization

`INFO commandstats` on prod Valkey (≈15 days uptime, 43.08/87.81 GB used, `noeviction`,
6,651 ops/sec instantaneous at low traffic):

| command | calls | share |
|---|---|---|
| **zcard** | 2.27B | **26.6%** |
| ping | 1.19B | 14.0% |
| unwatch | 1.19B | 14.0% |
| zrevrange | 945M | 11.1% |
| hget | 640M | 7.5% |

Three things fall out:

1. **`zcard` at 26.6% is `candidate-sync` recomputing `apc:` from scratch.** `sync.rs:334` maps
   *every* post in a pool to 6 date-sharded `pl:` keys and sums ZCARD — 118,000 ZCARDs for algo
   2323 alone, ~3.4M across all 574,053 pooled candidates per pass, and `sync.rs:454/558/780`
   repeat the pattern for the trending/popular/velocity/discovery tranches.
   **Fix:** these counts already exist in ClickHouse `uri_action_counts_v2` (59.7B rows), and the
   candidate query already reads `algorithm_posts_v2` — join the count there and ship it with the
   URIs, or maintain `apc:` incrementally in the like-streamer. Either removes ~26% of all Valkey
   commands. Biggest single CPU win found tonight, and it is pure infrastructure — no ranking risk.

2. **ping + unwatch = 28% of calls is pure connection overhead**, ~1:1 with checkouts
   (deadpool-redis recycling). Worth a look at pool config; cheap per call but a quarter of all
   traffic.

3. **keyspace_misses 3.53B vs hits 1.13B — a 76% miss rate**, because every logical read probes
   **6 date shards** and most don't exist (a user active 2 of 6 days yields 4 misses). Date
   sharding is a **6× op multiplier on every read path**. A per-user "active days" bitmap read
   once, or a single flat key with score-encoded dates (the `ula:` pattern), would cut read ops
   ~4–5×. This compounds with everything else: the inverted method's 768 ops would become ~150.

---

## Log

- **2026-08-11 ~19:00 PDT** — Ledger created. T1–T6 carried in. Rebuild in flight
  (`phaseb-ucl-mpl5-mlp100`) with T6's fix. Disk at 94% (12 GB free); heavy docker prune deferred
  until the build lands so its cache isn't pulled out from under it.
- **~19:40** — Feed-level reach mapped (T11/T13): 66 pooled feeds, 65 in the access window,
  demand-driven and mostly correct; unpooled supported feeds are ~1% of traffic.
- **~20:10** — **Reach correction.** DAU measurement against live `ul:` shows 62.6% already
  personalizable and durable profiles adding only 1.7%. T2/T3 reach claims revised down ~36×.
  Focus shifts to the live arm's scoring path.
- **~20:25** — Truncation reconciled: 90% of pool-wide signal visible at N=30, but 2.4–3.4×
  realized coverage gap because overlap is popularity-biased.
- **~20:50** — T7 CONFIRMED: inversion = 1.88× coverage AND 33–145× fewer Redis ops.
  `INVERTED_MAX_POSTS_TO_SCORE=0` (unlimited) discovered as the reason per-run cost is so high.
- **~21:10** — T14: `zcard` is 26.6% of all Valkey commands, all of it candidate-sync recomputing
  `apc:` liker counts over 6 date shards. Counts already exist in ClickHouse
  `uri_action_counts_v2`. Largest clean CPU win available. Also found 76% keyspace miss rate from
  6-shard probing (a 6× op multiplier) and 28% of calls being ping/unwatch connection overhead.

---

## 🟢 T15 CONFIRMED & SHIPPED — 72% of scoring-path read ops are provably wasted

Post IDs are `{YYYYMMDD}{seq:010}` where the prefix is the **intern** date. A like is recorded
under the date it happened, and a post cannot be liked before it has an ID — so **every `pl:`
date shard older than the post's intern date is guaranteed empty.** But
`Keys::post_likers_retention` returned all 6 retention dates regardless.

This compounds with the pool age cap: `SYNC_PREFERRED_MAX_AGE_HOURS=72` means candidates are ≤3
days old while retention is 6 days. Direct evidence — 400 sampled pool posts × 4 feeds, likes by
shard age:

| algo | d0 | d1 | d2 | d3 | d4 | d5 |
|---|---|---|---|---|---|---|
| 2323 | 1,108 | 1,046 | 514 | **0** | **0** | **0** |
| 396 | 4,524 | 3,660 | 955 | **0** | **0** | **0** |
| 3153 | 2,739 | 2,844 | 1,136 | **0** | **0** | **0** |
| 1988 | 2,319 | 3,188 | 1,318 | **0** | **0** | **0** |

Half the shards structurally cannot hold data. Sampling 3,000 members from each of 5 live pools,
shards needed vs probed: **90,000 → 37,089, a 59% reduction.**

**Shipped** as `Keys::post_likers_retention_bounded`, wired into the scorer's per-candidate liker
fetch (1 site) and candidate-sync's `zcard_summed_multi` (6 sites). Since `zcard` alone is 26.6%
of all Valkey commands and is dominated by those sync sites, this is expected to remove roughly
**19% of total Valkey command volume** on its own, with **zero behaviour change** — the bounded
key set is a strict subset of the unbounded one, which a test asserts directly. Legacy non-dated
IDs fall back to the full window; a clock-skewed/future ID still probes today's shard.

This is the cheapest large win of the session: pure infrastructure, no ranking risk, no config to
tune.


---

## 🔧 Measurement-harness bug found and corrected (my error, not the system's)

All my ad-hoc Valkey probes hardcoded a 6-date list ending at `20260811`. It was already past
midnight UTC, so **today is `20260812`** and every probe silently skipped today's shard — the
highest-volume one. Production is unaffected (`retention_dates()` is computed from the clock);
only my measurements were wrong.

Corrected figures:
- T15 shard saving: **72% → 59%** (still the largest clean CPU win).
- DAU personalizable: 62.6% → **63.2%**; profile-only 1.7% → **1.6%**; neither 35.7% → **35.2%**.
  Conclusions unchanged and slightly strengthened.
- Seed depth among seeded users: **median 16** likes in the window, p90 219, max 9,149.

Also verified this does *not* expose a bug in `post_likers_retention_bounded`: in production the
first retention date equals today, so a post interned today satisfies `d >= intern_date` and gets
exactly one shard. The "future-dated" fallback only triggers on genuine clock skew, and a test
covers it.

**Lesson for future probes: never hardcode dates — derive them, and remember the service works in
UTC while the host is PDT.**


---

## 🟢🟢 T16 — THE FINDING OF THE SESSION: the seed gate looks back 2 days, the scorer looks back 6

`api/feed.rs` decided whether to even attempt personalization with:

```rust
let user_has_data = exists_today.unwrap_or(false) || exists_yesterday.unwrap_or(false);
```

**Two days.** But the scorer reads `Keys::user_likes_retention(user_hash, DEFAULT_RETENTION_DAYS)`
— **six days**. So any user whose most recent like was 3–6 days ago was rejected with
`fallback_reason=no_user_data` and served unfiltered fallback, even though the scorer would have
found their seed and ranked with it.

Measured on 2,500 sampled daily-active users against live `ul:`:

| | share of DAU |
|---|---|
| pass the 2-day gate | 49.2% |
| have seed in the 6-day window | **63.2%** |
| **turned away despite usable seed** | **14.0%** |

That is **22.1% of every seeded user**, and it independently matches the observed
`fallback_reason` distribution exactly: `no_user_data` was **50%** of 685 sampled responses, right
at the 49.2% gate-pass rate.

**Reach comparison — this is the punchline of the whole session:**

| intervention | newly personalizable DAU | infrastructure required |
|---|---|---|
| durable co-liker profiles (Phases A+B, ~116 MB, 2 builds, new binary + codec + shadow arm) | **1.6%** | substantial |
| **fixing this one gate** | **14.0%** | **none — the seed is already in `ul:`** |

~9× the reach of everything else built this session, from making two numbers agree.

Their seed is thin (median **2** likes), so quality will be modest and should be watched — but the
alternative for these users is 100% unfiltered fallback, and the +13% within-user engagement lift
for personalized posts was measured against exactly that baseline.

**Shipped:** gate now checks the full retention window via a new pipelined
`RedisClient::exists_any` (one round trip regardless of window width), with the window exposed as
`USER_DATA_CHECK_DAYS` (default `DEFAULT_RETENTION_DAYS`) so it can be reverted to 2 without a
rebuild. A config test asserts the gate window equals the scorer's.

**Cost:** ~22% more scoring runs. Affordable given Valkey is at 43/88 GB with headroom, and T15
simultaneously removes ~59% of the scoring path's read ops.

**Why this went unnoticed:** the 2-day check has a comment explaining it covers users who liked
just before UTC midnight — the intent was a *lower bound* fix for a timezone edge, and it silently
became an *upper bound* on how far back seed counts. The two windows live in different files with
no test tying them together. There is now a test.

- **~21:40** — T15 shipped (bounded date shards, 59% fewer scoring read ops, 7 call sites).
- **~22:00** — Found and corrected my own date-off-by-one measurement bug (UTC vs PDT).
- **~22:20** — Funnel measured over 685 real responses: 21.8% personalized, `no_user_data` 50%,
  `personalization_holdout` 28%, cache hit only 6.1%.
- **~22:40** — **T16 found and shipped**: 2-day seed gate vs 6-day scorer window costs 14.0% of
  DAU. Highest-reach finding of the session, ~9× the durable-profile work, zero new infra.

### 🟢 T16 quality validated — thin seed is not useless

The obvious objection to T16 is that median-2-like users will yield nothing, so admitting them
just burns scoring runs. Measured directly against algo 2323 (pool 19,667), `min_post_likes=10`:

| group | seed likes | co-likers | inverted coverage | production @N=30 |
|---|---|---|---|---|
| **newly admitted** | 117 | 3,689 | 14 | 4 |
| | 9 | 522 | 117 | **55** |
| | 5 | 444 | 4 | 2 |
| | 3 | 220 | 8 | 2 |
| | 2 | 132 | 15 | 3 |
| | 1 | 128 | 40 | **14** |
| | 1 | 22 | 2 | 2 |
| | 1 | 11 | 0 | 0 |
| already served | 500 | 3,917 | 409 | 347 |
| | 169 | 3,675 | 45 | 16 |
| | 68 | 1,110 | 58 | 28 |
| | 28 | 182 | 460 | **393** |
| | 13 | 586 | 124 | 73 |
| | 4 | 90 | 62 | 28 |

**7 of 8 newly-admitted users get non-zero coverage**; only one (11 co-likers) gets zero, so ~12%
of the new scoring runs are wasted — acceptable. A user with a **single** like reached 128
co-likers and 14 scoreable candidates. Median for the newly admitted is ~3 production-detectable
candidates, versus ~50 for already-served users: thin, but the comparison baseline is zero.

**Third independent confirmation that seed depth does not predict coverage:** 117 seed → 4
covered, while 9 seed → 55; 500 seed → 347, while 28 seed → 393. What matters is *which*
co-likers you get and how active they are, not how much seed you had. This is the same signal as
T8 (13,623 co-likers → 44 covered vs 455 → 95) and T1 (23× seed → 1.1× coverage). **Three
separate measurements now say the seed side is not the lever** — which is worth stating plainly
given the session started out trying to widen seed windows.


---

## 🟢 T17 — Co-liker selection is feed-agnostic but coverage is feed-specific; ~45% of slots are dead per feed

Profiling all 128 top co-likers for three users against algo 2323, recording each one's overlap
count, recent activity, and **total** in-pool coverage (order-independent, so it measures
individual value):

| user | r(overlap, coverage) | r(activity, coverage) | top-32 coverage by overlap / by activity |
|---|---|---|---|
| d254450b | 0.07 | **0.22** | 553 / **1,297** |
| e97ce75c | 0.12 | **0.36** | 571 / **1,308** |
| 049db60c | 0.20 | 0.28 | 118 / 118 |

**Raw overlap count — the signal the live path ranks on — barely predicts a co-liker's value
(r = 0.07–0.20).** Recent activity predicts 2–3× better and yields ~2.3× the coverage.

But the *reason* is a trade-off, not a free win: activity-ranked neighbours are promiscuous likers,
precisely the low-specificity signal that `1/source_total_likes` and `Σ 1/L_j` exist to suppress.
**Coverage wants active neighbours; relevance wants discriminating ones.** That tension is
currently resolved implicitly and badly — by a signal with r≈0.13. Do **not** flip the ranking to
activity without an engagement experiment; the principled form is a product of specificity and
activity, and the durable profile already stores the specificity half (`Σ 1/L_j`) rather than raw
counts.

### The actionable part
Slot waste is **not** inactivity — **0% of top-128 co-likers had zero recent activity** — but
**34%, 47%, and 56% contributed zero coverage for this feed.** They are active; they just like
different content than this feed carries.

Root cause: `colikes:{hash}` (and `ucl:{hash}`) are computed **per user**, then reused for **every
feed** that user requests. Coverage, however, is per-feed. So roughly **45% of every user's
co-liker budget is dead weight on any given feed**, and which 45% changes per feed.

Two ways out, and they compose:
1. **Raise K.** With ~45% per-feed attrition, K=128 yields only ~70 useful neighbours. K=256–512
   would restore the intended budget. Affordable *only* because inversion (T7) makes each
   additional co-liker cost ~6 ops instead of a share of a 118,000-op pool scan — K=512 inverted
   is still ~20× cheaper than today's K=128 pl:-based scan.
2. **Per-feed selection.** Ideal but needs a per-(user, feed) cache, multiplying key count by the
   number of feeds a user reads. Not worth it before (1) is tried.

- **~23:10** — T16 quality validated: 7/8 newly-admitted users get non-zero coverage; one-like
  user reached 14 candidates. Third independent confirmation that seed depth doesn't predict
  coverage.
- **~23:35** — T17: overlap-count ranking is near-useless (r=0.07–0.20); activity predicts 2–3×
  better but trades away specificity. 34–56% of top-128 slots contribute nothing on a given feed
  because selection is per-user while coverage is per-feed.

---

## 🔧 Build/infra notes from the session (relevant to iteration speed)

**The image build is the bottleneck on everything.** The Dockerfile pins
`--platform=linux/amd64` but the Mac Mini is **arm64**, so the whole Rust build runs under QEMU:
**~76 minutes** per image, with ~35 of those minutes in silent LTO linking
(`lto = true`, `codegen-units = 1`, 5 crates).

Two failures came out of that:
1. Iterating code while a build was in flight meant killing and restarting it — three times.
   **Batch all changes, then build once.**
2. The 4th attempt died with **"No space left on device"** inside the Docker VM (40 GB
   `/var/lib/docker` hit 100%) while compiling `sqlx-postgres` — a dependency of
   `graze-feed-stats`, which is unrelated in-progress work. `docker builder prune -af` reclaimed
   28 GB. Note `dgaff/bsky_feeder` alone occupies 19.3 GB of that volume.

**Mitigation added: `Dockerfile.iter`**, which builds only `graze-api` + `graze-candidate-sync`
(the two crates that changed). Much faster and far less disk. **It produces an image valid only
for the `personalization-api` and `personalization-candidate-sync` deployments** — it omits
`graze-like-streamer`, `graze-frontdoor`, and `graze-feed-stats` binaries, so `like-streamer` must
keep using an image from the main Dockerfile. Use the main Dockerfile for anything shipping to all
three.

Worth considering later: a `--mount=type=cache` for `/usr/local/cargo/registry` and the target dir
would make repeat builds dramatically cheaper (needs binaries copied out inside the same `RUN`,
since cache mounts don't persist into layers).


---

## ⚠️ T16 POST-DEPLOY REALITY CHECK — shipped and live, but ~3× less effective than predicted

**The code is live and correct.** Verified independently: **7.6 `EXISTS` per gated request**
(old 2-day code would be 2.0, new 6-day code 6.0, plus noise from other callers). No errors, no
restarts.

**But `no_user_data` did not move**: 50.0% pre-deploy → **49.3%** post-deploy over 706 responses.
(An early 37-response sample showed 14% — that was small-sample noise, and I nearly reported it as
a win.)

### Why: user-weighted ≠ request-weighted
My +14% prediction came from a **user-weighted** sample (2,500 DAU sampled uniformly). The funnel is
**request-weighted**. Pulling requester DIDs straight out of 45 minutes of production logs
(423 distinct requesters, 1,065 requests):

| seed class | % of USERS | % of REQUESTS | requests/user |
|---|---|---|---|
| has 2-day seed | 68.8% | 44.4% | 1.6 |
| **3–6 day only — T16's target** | **8.0%** | **5.2%** | 1.6 |
| **no seed at all** | **23.2%** | **50.4%** | **5.5** |

**Users with no seed at all make 5.5 requests each — 3.4× more than seeded users.** They are 23% of
people but **over half of all traffic**. My DAU sample drew from `interactionSeen` (users who *saw*
posts), which skews engaged and under-represents these high-frequency zero-seed requesters.

**Corrected T16 value: ~8% of requesters / ~5% of requests, not 14%.** Still a real gain, still
nearly free, still worth keeping — but it is not the headline I wrote earlier tonight.

### The finding that replaces it
**~50% of all personalization-eligible traffic comes from users with zero likes in the retention
window** — unreachable by collaborative filtering under *any* window, profile, or lookup scheme.
That, not window width, is the ceiling on personalization coverage. Their options are
content-based ranking (embeddings), the follow/author graph, or fallback (what they get today).

Worth investigating next: *why* zero-seed users request 3.4× more often. Polling clients, scrapers,
or genuinely heavy lurkers all imply different responses, and if a meaningful share are automated,
the "50% of traffic" figure overstates the human population being underserved.

### Also unverified: T15's production effect
zcard rate went **2,410 → 2,655/sec (+10%)** across the deploy, and zcards-per-main-pool-candidate
measured **5.36** (unbounded ≈ 6.0, bounded ≈ 2.4). Neither shows the predicted drop, but neither
is a clean measurement: the T16 deploy landed simultaneously and adds load, candidate-sync
cold-started and re-synced everything, traffic was rising with the diurnal curve, and the
per-candidate denominator ignores the four fallback-tranche syncs that zcard their own post lists.
**The change is provably a strict subset of the old key set (tested), so it cannot be harmful — but
its production saving is unquantified.** To measure it cleanly: hold everything else still and
compare zcard/sec across a single candidate-sync image swap.


---

# 📋 Deploy log

| what | detail |
|---|---|
| image | `dgaff/personalization:ucl-b2-shards-gate6` (digest `sha256:fe262e44…`), linux/amd64, 18.7 MB |
| built with | **`Dockerfile.iter`** — only `graze-api` + `graze-candidate-sync` (~20 min vs ~76 min for the full 5-crate LTO build under QEMU) |
| `personalization-api` | deployed, 3/3 healthy, 0 restarts, 0 error lines |
| `personalization-candidate-sync` | deployed, 1/1 healthy, syncing normally, 0 error lines |
| `personalization-like-streamer` | **untouched** — still on its previous image (the iter image lacks its binary) |
| env unchanged | `DURABLE_PROFILE_SHADOW_MODE=1`, `DURABLE_PROFILE_ENABLED` unset, `USER_DATA_CHECK_DAYS` unset (defaults to 6) |

### Rollback
```bash
kubectl --context do-sfo3-k8s-1-31-1-do-4-sfo3-1731769323576 \
  set env deploy/personalization-api USER_DATA_CHECK_DAYS=2   # revert T16 only, no rebuild
```
Full rollback: `set image` both deployments back to `dgaff/personalization:83fe4bc` (api) and
`dgaff/personalization:latestrs` (candidate-sync).

### Also done overnight
- Full Phase A run: **62,905 durable profiles** (mean 124.2 co-likers, ~116 MB Valkey, 23.4 min,
  0 chunks failed).
- Docker VM disk recovered from **100% full** (a build died on it) — `docker builder prune -af`
  reclaimed 28 GB. Note `dgaff/bsky_feeder` occupies 19.3 GB of the 40 GB VM volume; that is the
  main thing to delete if space runs short again.

### ClickHouse load
Kept deliberately light — roughly a dozen queries all night, each narrow or sampled. Two OOMed
(18 GiB limit) and were redesigned rather than retried: the 141M-row co-liker self-join needs the
seed cap and viral-post filter to fit, and a `LEFT JOIN` over `user_action_logs` should be replaced
by two aggregations.

---

## 🟢 T18 — The zero-seed "50% of traffic" is mostly pagination, not 50% underserved humans

Answering the top open question directly, from an hour of production logs (1,136 served responses
carrying a `user_did`, joined against live `ul:` seed state):

| class | users | requests | req/user | feeds/user | **median gap between a user's requests** |
|---|---|---|---|---|---|
| seeded | 325 | 540 | 1.7 | 1.0 | **60 s** |
| zero-seed | 98 | 544 | **5.6** | 1.0 | **1 s** |

**A 1-second median gap, on a single feed, is pagination or polling — not human browsing.** Seeded
users show a 60-second cadence, which looks like reading.

So the earlier alarming framing ("half of all eligible traffic is unreachable by collaborative
filtering") is an artifact of the denominator. Corrected reading:

- **~23% of *people* have no usable like-seed** — that is the real CF reach ceiling.
- They generate ~50% of *requests* because they issue ~3.4× more per user at pagination cadence.
- Therefore **`fallback_reason=no_user_data` at ~49% of requests substantially overstates the human
  problem**, and any per-request personalization rate is depressed by paginating clients that never
  re-run personalization (it only runs on the first page).

**Methodological rule this establishes: report personalization coverage per *user* or per *first
page*, never per request.** Every per-request percentage in this ledger — including the 21.8%
personalized and 49.3% `no_user_data` — is biased downward by pagination. This also partly explains
why T16's user-level gain didn't show up per request: its beneficiaries are low-cadence readers,
diluted by high-cadence paginators.

Still worth a follow-up: whether zero-seed paginators are new users, clients pre-fetching pages, or
automation. `feeds/user = 1.0` for both groups rules out feed-hopping scrapers, which points at
ordinary client pagination.

- **~01:30** — Deployed `ucl-b2-shards-gate6` to api + candidate-sync; both healthy, 0 errors.
  T16 verified live (7.6 EXISTS/request) but request-level effect ~5%, not 14%.
- **~02:10** — T18: zero-seed users paginate at a 1 s cadence (5.6 req/user) vs seeded users at
  60 s (1.7 req/user). The "50% of traffic" ceiling is really ~23% of people. **All per-request
  metrics in this ledger are pagination-biased.**

---

# 🔵 LinkLonk framing — corrects T17 and reframes T7

Re-reading the LinkLonk algorithm description changes how two findings should be read. The walk is:

> **Step 1.** into one of the items you upvoted · **Step 2.** into one of the sources that upvoted
> it **before you** · **Step 3.** into one of the other items that source upvoted.
>
> Score = sum of path probabilities. Branching factor divides at each step: `1/|your upvotes|` ×
> `1/|sources who upvoted before you|` × `1/|items that source upvoted|`.

Those three factors are **exactly** the three terms in `aggregate_coliker_weights_normalized`
(`1/user_likes_count` × `1/likers.len()` × `1/source_total_likes`), and the "before you" proof-of-work
filter is faithfully implemented at `coliker.rs:223-232`. The implementation is a faithful faceted
LinkLonk.

## 🔴 T17 CORRECTED — production does NOT rank co-likers by overlap count

I wrote that "the signal the live path ranks co-likers on — raw overlap count — has r = 0.07–0.20
with coverage; that's close to noise." **That was wrong.** `scoring_core.rs:339` sorts by
**weight** — the LinkLonk path probability — before `truncate(max_total_sources)`. Raw overlap
count was *my harness's* ranking, not production's.

What survives, and what it actually means:

- **34–56% of top-128 co-likers contribute zero coverage on a given feed.** Still true, and it is a
  property of *faceting*, independent of ranking criterion.
- **Ranking by activity yields ~2.3× the coverage.** Still true — but the correct reading is that
  activity-ranking **deliberately violates LinkLonk's fairness principle**. Step 3's `1/|items
  source upvoted|` exists precisely to stop prolific upvoters from dominating attention ("the more
  items someone upvotes, the less attention each of them gets"). Coverage and LinkLonk weight are
  **designed** to be in tension.

So the earlier recommendation stands but for a better reason: **do not switch co-liker ranking to
activity.** It is not an untested trade-off — it is a known, intentional rejection in the algorithm
we are copying, and LinkLonk works in production partly *because* of it.

## 🟢 T7 reframed — the inverted lookup IS LinkLonk's Step 3, not an optimization

"Step 3: into one of the **other items that source upvoted**" is literally *enumerate the source's
items*. That is the co-liker-first traversal. **The post-first traversal is the deviation**, adopted
as an implementation convenience, and `max_likers_per_post = 30` truncating Step 2's liker list is a
lossy approximation of the specified walk — it silently drops paths, keeping only those whose Step-2
source liked the item most recently.

This is a stronger argument than "1.88× coverage for 33–145× fewer ops": the inverted path is
**closer to the algorithm that is known to work in production on Bluesky**. The measured coverage
gain is the recovered paths.

Two details the docs confirm the inverted path handles correctly:
- *"When you filter recommendations by time, we discard paths that were upvoted before the time
  limit in Step 3"* → `min_time` on the `ZREVRANGEBYSCORE` of each co-liker's likes. Implemented.
- Step 3's branching factor `1/|items source upvoted|` is directly observable when you enumerate the
  source's items, rather than needing a separate `ulc` counter lookup.

## The faceting departure is the real design problem

LinkLonk has **no facet** — every item any source upvoted is a candidate. Graze restricts Step 3 to
a single feed's pool. That single change is the origin of the 34–56% dead-slot rate: a user's
co-liker set is computed once per user (`colikes:{hash}`) and reused across every feed, but
usefulness is per-feed.

LinkLonk's own answer to this is **channels**: *"If you filter your recommendations to a channel,
then Step 1 goes only into items that you put into that channel."* — i.e. LinkLonk facets at
**Step 1** (restrict the seed), not at Step 3 (restrict the output). Graze does the opposite.

That is worth considering seriously: seeding from the user's likes *that are topically relevant to
this feed* would produce co-likers who are relevant to this feed by construction, instead of
producing a generic co-liker set and discarding half of it at the end. It costs a per-(user, feed)
co-liker set, which is why it was presumably avoided — but it is the faithful faceting point, and it
would fix the dead-slot problem at its source rather than by raising K.


---

# 🔴 T7 REFUTED IN PRODUCTION — the inverted lookup loses at real parameters

Built, deployed, and A/B'd on live traffic (`INVERTED_LOOKUP_SHADOW_MODE=1`, both arms computed per
request, post-first served, `lookup_arm_comparison` logged). Shadow mode has since been **turned
off** — it doubles scoring latency and the inverted arm hit 3,760 ms on one request.

## Result: 0.94× coverage at ~5× the latency

| co-liker window | n | post scored | inv scored | ratio | post ms (med) | inv ms (med) |
|---|---|---|---|---|---|---|
| 4-day (my defect, see below) | 22 | 1,658 | 1,630 | 0.98× | 71 | 126 |
| **6-day (corrected)** | 16 | **1,707** | **1,609** | **0.94×** | **43** | **227** |

Split by co-liker-set size — the whole story is here:

| corrected run | n | post | inv | ratio | post ms | inv ms |
|---|---|---|---|---|---|---|
| **K ≤ 256** | 9 | 215 | **262** | **1.22×** | 48 | 101 |
| **K > 256** | 7 | 1,492 | 1,347 | **0.90×** | 39 | **1,003** |

At large co-liker sets the inverted arm is **26× slower and slightly worse.**

## Why my offline prediction (1.88× / 33–145× fewer ops) was wrong

Three methodological errors, all pointing the same way:

1. **`max_total_sources = 10000`, not 128.** My harness capped co-likers at 128 — my own arbitrary
   choice. Production sets ran to **5,157** (median 232, p90 1,500) in the sample. The entire cost
   argument was `|co-likers| << |candidates|`; at K=5,157 against ~10,000 candidates with bounded
   shards, the inverted arm touches **more** keys than post-first.
2. **My harness read up to ~1,194 likes per co-liker** (199 per day-shard × 6) versus production's
   merged limit of 500 total. I gave the inverted arm a more generous window than I then shipped.
3. **I compared against a *model* of post-first** (`Σ min(1, 30/L)`), not the real thing. The model
   was validated on four users and evidently does not generalise.

## The reasoning error underneath

I argued the co-liker-side cap "rarely binds because the median seeded user has only ~16 likes in
the window." **That is true of users and false of co-likers** — co-likers are selected by a weight
whose Step-2 term rewards having liked things, so the co-liker population is systematically far more
prolific than the median user. Moving truncation from the post side to the co-liker side moved it
onto a heavier-tailed distribution, which is why coverage got slightly *worse* at large K.

## My own implementation defect (found and fixed mid-test)

I set `inverted_coliker_like_days = 4`, reasoning that pool posts are ≤72 h old so older shards
cannot contain a pool hit. But `params.time_window_hours = 144.0` — **the scoring window is 6 days** —
and `SYNC_FALLBACK_MAX_AGE_HOURS=336` lets pools carry older posts. The 4-day read silently dropped
two days of co-liker likes. Fixed live via `INVERTED_COLIKER_LIKE_DAYS=6` (the knob paid for itself),
and the corrected numbers are the ones above. Note the fix made coverage *worse* (0.98× → 0.94×)
because the extra days added latency without adding hits — itself evidence the window was not the
binding constraint.

## What this means

**Post-first is the better architecture at production parameters.** Candidate-side truncation is
cheaper than co-liker-side enumeration once co-liker sets are large, and the truncation it applies
turns out to cost less coverage than the co-liker-side cap that replaces it.

The one regime where inversion wins is **K ≤ 256 (1.22× coverage)** — which is also where my offline
result came from. That points at a *different* experiment worth running, and it is a pure config
change on the existing code:

- **Lower `max_total_sources` from 10,000 to ~256.** T8 already showed coverage saturating well
  before that, and it would cut cost on the **post-first** path too (fewer weights to carry, smaller
  `source_weights` map in the hot loop). If coverage holds at K=256, that is a real win with no new
  code — and it would make inversion worth re-testing afterwards.

The inverted code stays in the tree behind two default-off flags. It is not wasted: it is the
faithful expression of LinkLonk's Step 3, it is instrumented, and it becomes viable if
`max_total_sources` comes down.

- **~04:50** — Built + deployed the inverted lookup with per-request A/B. **T7 REFUTED at production
  parameters**: 0.94× coverage, 5× latency (26× at K>256). Root cause: `max_total_sources=10000`,
  not the 128 my harness assumed. Shadow mode disabled. New lead: lower `max_total_sources` to ~256.

---

# 🟡 T19 — Capping co-likers: NOT VALIDATED, and retrospective Thompson analysis is unusable

The proposed next step was "lower `max_total_sources` from 10,000 to ~256." Two discoveries changed
the approach, and the result is inconclusive for an instructive reason.

## It is already being A/B tested

`max_total_sources` is **not** env-configurable — it comes from presets — but it **is one of nine
Thompson-sampling dimensions**, explored over `[250, 500, 750, 1000, 1500, 2000, 5000, 10000,
20000]` (`thompson.rs:70`). And the selected value is written into the base64 `feedContext`
provenance (`ProvenanceParams`, field `params.max_total_sources`), which ClickHouse stores on every
impression and like. So 14 days of engagement data by cap value already existed.

Note also: **the bandit arms are in-memory only** (`self.bandits`), with no Redis persistence. They
reset on every pod restart, across 3 independent replicas. The learner is mostly exploring, never
accumulating.

## The raw signal looked great, then died under a control

Raw like rate by cap (14 days): **250 → 4.30%**, 500 → 3.44%, 1000 → 3.40%, 5000 → 1.96%,
20000 → 3.21%. K=250 best, z≈4.3 vs all others pooled.

Restricting to the same **(feed, user) pairs that saw both treatments** — 314 users, 38 feeds, which
cancels feed-mix and user-engagement confounds — still looked strong: **5.36% (≤500) vs 4.02%
(>500), a 33% relative lift, z≈3.3, p≈0.001.**

Then the control:

| source | low cap (≤500) | high cap (>500) | ratio |
|---|---|---|---|
| **fallback** — cap has no causal path | **2.28%** | 0.82% | **2.80×** |
| personalized | 5.36% | 4.02% | 1.33× |

**Fallback posts are selected by popularity/velocity; the co-liker cap cannot touch them.** Yet
low-cap requests show a *larger* relative lift on fallback than on personalized. Whatever makes
low-cap requests engage better is a property of the **requests**, not the cap — and it is strong
enough to fully account for the personalized-side effect.

Most likely mechanism: Thompson arm selection is not independent of context. Arms are per-pod,
in-memory, reset on restart, so which arm is chosen correlates with **time**, and like rates vary
strongly by time of day. The low-cap group is 5× smaller and therefore more time-clustered.

## Two conclusions

1. **Capping co-likers is neither supported nor refuted.** Do not ship it on this evidence.
2. **Any retrospective comparison of Thompson parameters from `feedContext` is unusable for causal
   claims** — assignment is adaptive, not random. This invalidates the obvious way anyone would try
   to answer "which parameter value is best" from the existing data. **Always include a control
   surface that the parameter cannot affect** (fallback posts are the ideal one here); if the control
   moves, the result is confounded.

## What would actually settle it

A randomization independent of the bandit: assign the cap by **hashing the user DID** (e.g. `hash %
2` → 250 vs 10000), so treatment is orthogonal to time and to Thompson state, and stable per user.
The plumbing already exists — `select_params_with_holdout_and_search_space` takes a
`treatment_override`. That is a small, contained change, and it is the only way to get a clean
answer. It also fixes the general problem: with a hash-based override we could cleanly A/B *any* of
the nine dimensions.

Worth pairing with: **persist the bandit arms to Redis.** Right now every deploy discards all
learning across 3 replicas, which is likely why the search has not converged on anything.

- **~05:40** — T19: found `max_total_sources` is already a Thompson dimension with 14d of engagement
  data in `feedContext`. Raw signal strongly favoured K=250 (4.30% vs 2.80%), survived a
  within-(feed,user) design (5.36% vs 4.02%, z≈3.3) — then **failed the fallback control** (2.28% vs
  0.82%, a bigger lift on posts the cap cannot affect). Inconclusive. Retrospective Thompson-arm
  analysis is confounded by adaptive assignment. Need a user-hash randomized override.

---

# 🟢 T20 — Randomized experiment harness + Thompson persistence: BUILT, DEPLOYED, VERIFIED

Both changes from T19's "what would actually settle it" are live on `personalization-api`
(`dgaff/personalization:ab-thompson-persist`). 149 tests, clippy and fmt clean, zero errors in prod.

## 1. Thompson arm persistence — working

Arms were in-memory per pod, wiped on every deploy across 3 replicas. Now each pod stages its
evidence as **deltas** and pushes them with `HINCRBYFLOAT` to `thompson:arms:{algo_id}`
(fields `{dimension}:{value}:a|b`), then reloads the merged totals. Deltas rather than absolute
alpha/beta so replicas **accumulate instead of clobbering** each other. Priors are re-added locally,
so Redis holds only observed counts and priors are never double-counted. Pending evidence is only
dropped once the write succeeds, so a Redis blip defers rather than loses it.

Verified in production: `arms_loaded` climbed **9 → 18 → 26 → 35** across successive cycles, which
means pods are picking up *each other's* observations. 10 algos had persisted arms within minutes;
e.g. algo 2323 held `max_sources:2000:a=2`, `max_sources:5000:a=1`, `max_sources:10000:a=1`,
`max_sources:500:b=1`.

Flags: `THOMPSON_PERSIST_ENABLED=1`, `THOMPSON_PERSIST_INTERVAL_SECONDS=30`.

## 2. User-hash randomized experiment — working

`HashExperiment` forces one bandit dimension to a value chosen by hashing `salt|user_did`.
Assignment is orthogonal to time and to bandit state, and stable per user — a genuine randomized
experiment, which is exactly what the retrospective analysis could not be.

Design details that matter:
- **Membership and arm are drawn from different parts of the hash**, so ramping `traffic_pct` up or
  down does not reshuffle which arm existing users are in. A test asserts this.
- **Enrolled requests are excluded from bandit learning** (`is_hash_experiment`) — the bandit did not
  choose the value, so crediting it would corrupt the arms we are evaluating. A test asserts the
  observation counter does not move.
- **Holdout requests are never enrolled**, so the pre-existing control surface stays interpretable.
- The forced value flows into the existing `feedContext` provenance, so **no new analysis plumbing**.

Live config: `AB_EXPERIMENT_ENABLED=1`, `AB_EXPERIMENT_DIMENSION=max_sources`,
`AB_EXPERIMENT_VALUES=250,10000`, `AB_EXPERIMENT_TRAFFIC_PCT=100`, `AB_EXPERIMENT_SALT=v1`.

Confirmed in ClickHouse within minutes: only **250** and **10000** appear among personalized
requests, plus `1000` for holdout traffic (the default, correctly excluded).

## ⏳ The result needs about a week, and here is why

Only impressions that carry `params` in the provenance are usable — i.e. requests where
personalization actually ran. That is **3,420 impressions/day** (measured over 14 days), against
~24,000 impressions in a 20-minute window that carry no params at all (cached responses and requests
that never attempted personalization).

At ~1,710 impressions/arm/day and a ~4% like rate, detecting a **20% relative** difference at 80%
power needs ≈10,300 impressions per arm → **~6 days**. A 33% effect (the size the confounded signal
suggested) would resolve in ~2–3 days.

**Analysis caveat for whoever reads it:** randomization is per *user*, and users differ enormously in
request volume — in the first 20 minutes arm A had 41 impressions from 2 users while arm B had 7 from
4 users. Pooling raw impressions lets one heavy paginator dominate. **Compute per-user like rates and
compare those, or use a user-clustered test.** This is the same pagination bias as T18.

### Ready-to-run readout

```sql
WITH d AS (
  SELECT did,
    JSONExtractInt(tryBase64Decode(interaction_feed_context),'params','max_total_sources') AS arm,
    JSONExtractString(tryBase64Decode(interaction_feed_context),'source') AS src,
    interaction_event AS ev
  FROM default.feed_interactions
  WHERE occurred >= now() - INTERVAL 7 DAY AND interaction_feed_context != ''
    AND interaction_event IN ('app.bsky.feed.defs#interactionSeen','app.bsky.feed.defs#interactionLike')
),
per_user AS (
  SELECT did, arm, src,
    countIf(ev='app.bsky.feed.defs#interactionSeen') AS seen,
    countIf(ev='app.bsky.feed.defs#interactionLike') AS likes
  FROM d WHERE arm IN (250, 10000) GROUP BY did, arm, src
)
SELECT src, arm, count() AS users, sum(seen) AS seen, sum(likes) AS likes,
       round(100*avg(likes/nullIf(seen,0)), 3) AS mean_per_user_like_rate_pct
FROM per_user WHERE seen > 0 GROUP BY src, arm ORDER BY src, arm
```

**Read the `fallback` rows first.** The cap cannot affect fallback posts, so if the arms differ
there, the result is confounded and must be discarded — that check is what caught T19, and it is now
built into the readout by default.

- **~07:30** — T20: built + deployed Thompson arm persistence (verified merging across replicas,
  `arms_loaded` 9→35) and a user-hash randomized experiment harness (verified only arms 250/10000
  appear). `max_sources` A/B now running at 100% traffic. Needs ~6 days for a 20% effect at 3,420
  usable impressions/day. Readout query recorded, with the fallback control built in.

---

# 🟡 T21 — Randomized `max_sources` A/B at 27h: not decisive, but direction is AGAINST capping

Experiment window `2026-08-12 09:11:56` → +27h. **3,681 impressions, 101 likes, 483 users**, arms
balanced (247 vs 239 users, 7.9 vs 7.2 impressions/user).

## Two analysis errors I made and corrected here

**1. Conditioning on a post-treatment variable.** My first readout compared like rates *among
personalized impressions*, using fallback as a control. But the cap **causally changes the feed
mix**: arm 250 served **fewer personalized (869 vs 1,068) and more fallback (1,017 vs 587)**
impressions — a smaller co-liker set finds fewer candidates, so more slots fall through to fallback.
`source` is therefore a *post-treatment* variable, and splitting on it biases the estimate. It also
invalidates fallback as a control, because the fallback impressions in the two arms are not
exchangeable (arm 250's fill deeper, lower-engagement slots).

The unbiased estimator is **intention-to-treat**: all impressions, no conditioning on what type of
post got served.

**2. Treating clustered impressions as independent.** ITT pooled looked emphatic —
**1.84% (36/1,961) vs 3.77% (65/1,724), z=−3.59, p<0.001** — but impressions cluster within users,
and a pooled z-test assumes independence. A **permutation test shuffling arm labels at the user
level** gives:

| test | result |
|---|---|
| pooled z-test (ignores clustering) | p < 0.001 |
| **permutation test, user-level (correct)** | **p = 0.151** |
| likes per user, permuted | p = 0.257 |

So **not significant.** The emphatic pooled number was an artifact of clustering, exactly the same
pagination/clustering bias as T18. I nearly reported it.

## What the data does say

- **Direction is consistently against capping.** Both readouts (14h and 27h) favour the large cap:
  1.84% vs 3.77% ITT, 0.146 vs 0.272 likes/user. Not significant, but never once favouring 250.
- **The retrospective signal is not reproducing.** That analysis said 250 was clearly better (4.30%
  vs 2.80%). Under proper randomization the direction is *reversed*. This is now strong
  corroboration that the retrospective result was an artifact of adaptive assignment (T19).
- **The cap does measurably reduce personalized coverage** — 869 vs 1,068 personalized impressions,
  ~19% fewer — which is a clean causal confirmation of the coverage curves from T8/T17.

## Recommendation

**Do not ship the 250 cap.** It was my own proposed next step, and the randomized evidence points
the other way: fewer co-likers means measurably less personalized coverage, and the engagement
trend, while not significant, is consistently worse. The cost saving is not worth pursuing against a
trend like that.

Leave the experiment running 2–3 more days for a definitive read. If it stays non-significant with
this direction, the conclusion is "the cap does not help and slightly hurts" — which still settles
the question, just not in the direction I expected.

- **2026-08-13 ~12:20** — T21: 27h randomized readout. Corrected two of my own analysis errors
  (conditioning on post-treatment `source`; ignoring user clustering). Permutation test p=0.151 —
  not significant, but direction consistently favours the LARGE cap. **Recommendation flipped: do
  not ship the 250 cap.** Retrospective signal confirmed as an artifact.

---

# 🔧 Stage 0 + Stage 1 implementation (2026-08-13)

Executing `~/.claude/plans/proceed-with-this-proposal-joyful-dream.md`. Serving-side code complete
and green (171 tests, clippy `-D warnings` clean, fmt clean); image `stage01-interleave` building.

## Stage 0 — three of five items were live bugs

1. **The `fsc:` cache was destroying provenance.** `api/feed.rs` relabelled *every* cached item
   `PostLevelPersonalization` and set `was_personalized = true` unconditionally. So on page 2+,
   fallback and author-affinity posts have always been recorded as personalized — **some historical
   `personalized=true` rates are inflated by this.** Cache entries now carry a source tag
   (`p|…`, `a|…`, `f:trending|…`), with untagged legacy entries still decoding so a deploy does not
   garble in-flight caches. `was_personalized` is now derived from the actual cached sources.
2. **A test caught a bug in my own fix.** I used `split_once` on the tag separator, which corrupts
   the URI if a tranche name ever contains `|`. `rsplit_once` is correct — the URI half (DID + rkey,
   both base32) can never contain the separator, so it is the trustworthy side to anchor on.
3. **`PERSONALIZATION_HOLDOUT_RATE` was 0.5** → **0.05**. Half of authenticated first-page requests
   were skipping personalization entirely, halving experiment throughput.
4. `inject_special_posts` ignored its `limit` and nothing truncated after it, so responses could
   exceed the requested size and tail positions were unstable — which matters because interleaving
   attribution is positional.
5. `diversify_posts` gained `preserve_order`: cap authors but keep input order and original scores.
   Required because it otherwise re-sorts by adjusted score and would shred a draft.

## Stage 1 — interleaving harness

`experiment/interleave.rs`: `Ranker` enum (all later stages plug in here rather than forking the
serving path), team draft with **competitive pairs**, and a per-user coin flip.

Design points that matter for correctness:
- **Items both rankers offer are added once and left untagged.** They carry no preference
  information; crediting them would inflate whichever ranker drafts first.
- **Unopposed items (one list exhausted) are also untagged** — they keep the feed full without
  becoming evidence.
- The coin flip is derived from a *different* hash input than enrolment, so draft order and
  enrolment are independent; and it is stable per user so a feed does not reshuffle between pages.
- `Ranker::parse` **rejects** unknown names rather than defaulting, so a typo fails loudly instead
  of quietly measuring the control against itself.

**The harness's own negative control is a test:** interleaving a ranker against itself must produce
**zero** competitive pairs. If that ever fails, any measured preference is an artifact.

Attribution plumbing: `ScoringResult.ranker_by_post` (a map, not a parallel vector — diversity
filters and reorders, and a map survives both) → `ScoredPost.ranker` → `ranker_by_uri` in the
handler → `feedContext.ranker`, **and** into the `fsc:` cache tag (`p/sampled_walk|…`) so
attribution survives pagination. Without that last part the credit would silently vanish after
page 1 and every experiment would read as "no effect".

Config, all default-off: `INTERLEAVE_ENABLED`, `INTERLEAVE_CONTROL`, `INTERLEAVE_TREATMENT`,
`INTERLEAVE_TRAFFIC_PCT`, `INTERLEAVE_SALT`, plus `DIVERSITY_PRESERVE_ORDER`. A warning fires if
interleaving runs while diversity would re-sort.


## Stage 2 — Python analysis layer (built, 27 tests passing)

`analysis/` — `graze_analysis/{spec,stats,data,runner}.py`, two experiment specs, a CronJob, and a
container that later also hosts the stage 4 and 6 training jobs (one image, not three).

**Switched from Rust to Python for this layer.** Every routine in `stats.py` replaces a hand-rolled
version that produced a wrong answer; `statsmodels` supplies cluster-robust covariance
(`cov_type='cluster'`) directly, which is exactly the 6.2x problem, and there is a reference
implementation to check against instead of my own arithmetic.

Design choices, each traceable to a specific error made during the investigation:

| default | error it prevents |
|---|---|
| `unit` is a **required** spec field | predicting a per-request outcome from a per-user measurement (overstated reach ~3x) |
| `negative_controls` **required**, with a written `reason` | two results that looked significant until a surface the treatment could not affect moved just as much |
| `post_treatment_fields` denylist, raises `SpecError` | slicing by `source`, which the treatment causally changes (869 vs 1,068 personalized; 1,017 vs 587 fallback) |
| cluster-robust SE by default, permutation cross-check | pooled z-test said p<0.001 where the cluster-correct answer was p=0.15 |
| `insufficient_data_gate` | a 37-response sample read as 14% when the truth at 706 was 49.3% |
| always-valid confidence sequences | peeking at one experiment three times in a day |
| timezone-aware timestamps **required** | probes hardcoded to PDT dates while the service runs UTC |
| `start` bound in every query | mixing in pre-experiment rows silently restores adaptive assignment |

**Gates run before the effect is printed.** Both historical false positives were believed because
the effect was seen first and the caveat second; `analyse_ab` returns `WITHHELD` with no effect size
at all when a gate trips, and a test asserts no `primary` line appears in that case.

**A test caught a bad test.** My first small-sample replay asserted that a 14%-vs-49% difference
should be undetectable at n=34 — but at 3.5x it legitimately is detectable, so the estimator was
right and the test was wrong. The real historical failure was a small sample being *wrong*, not
*undetected*, which is now guarded by an explicit observation floor rather than by a p-value.

## 🔴 The interleaving self-check FAILED on first run — and caught a real bug

Deployed `stage01-interleave-v2`, enabled `INTERLEAVE_SELF_CHECK=1` with control == treatment ==
`post_first` (which should be a perfect null), and measured:

| | observed | expected |
|---|---|---|
| competitive pairs | **123** | 0 |
| shared items | 1,904 | all of them |
| control vs treatment scored | **2,119 vs 2,140** | identical |
| co-liker derivations per draft | **~2.1** (32 across 15 drafts) | 1 |

**The same ranker, run twice in one request, produced different rankings.** Root cause: each arm
derived co-liker weights independently, and `seed_sample_pool > 0` shuffles the seed with
`thread_rng` on every derivation (`coliker.rs:283`, `scorer.rs:662`). So ~6% of drafted items
differed purely from run-to-run randomness.

**Why this mattered more than the self-check itself.** That noise is *indistinguishable from a real
treatment effect*. Every future interleaving experiment would have carried a ~6% spurious
disagreement floor, silently consuming the sensitivity the whole stage exists to buy. The self-check
found it before a single real comparison was run — which is exactly the job it was built for, and a
strong argument for never skipping this kind of gate.

**Fix: derive the co-liker weights ONCE and score both arms from them** (`score_with_ranker`), so the
arms differ only by traversal. This is strictly better on three axes:
- removes the dominant noise source from every experiment,
- halves the co-liker work per enrolled request (measured 27–106 ms per derivation),
- makes the null actually null, so the self-check becomes a meaningful gate.

**Also corrected: my PASS criterion was wrong.** I asserted "zero competitive pairs", but against a
*stochastic* ranker that is unachievable in principle — the honest criterion is that τ is
indistinguishable from zero, which is what the analysis runner's sign test measures. The unit test's
zero-pairs assertion remains correct because it operates on identical *lists*, not identical
*rankers*. Both checks are kept: pairs≈0 for the deterministic case, τ≈0 for production.

### The self-check now passes — and the fix was worth more than the check

Re-ran the identical-ranker null on `stage01-interleave-v3` (shared weights):

| | before fix | after fix |
|---|---:|---:|
| drafts observed | 15 | 47 |
| competitive pairs | 123 | **2** |
| shared items | 1,904 | 4,192 |
| disagreement rate | **6.1%** | **0.05%** |
| scored count delta | 21 | 1 |

**The residual 2 pairs are not a bug, and chasing them to zero would be wrong.** Both came from a
single draft with `control_scored=23, treatment_scored=24` — the arms are scored sequentially against
a *live* like stream, so a like landing in Valkey in the milliseconds between them makes one more post
scorable for the second arm. It is noise rather than bias, because draft order is coin-flipped per
user, so the extra item is equally likely to land in either arm.

So the criterion is a **tolerance**, codified in `analysis/graze_analysis/selfcheck.py`:
disagreement ≤ 0.5% (an order of magnitude below the ~1% smallest effect worth detecting, so a real
effect can never be mistaken for the floor), plus a *separate* check on scored-count imbalance ≤ 1%,
because one post differing is a race while a consistent gap means the arms aren't seeing the same
candidate set — a defect no amount of data fixes. Both real measurements above are pinned as test
fixtures so neither the bug nor the over-strict criterion can return unnoticed.

**Stage 1 is complete and the harness is trustworthy.** 0.05% floor against effects of ~1–5%.

### Installing statsmodels exposed a real defect in my own negative-control gate

The gate was silently running on the hand-rolled sandwich fallback locally. With `statsmodels`
actually present, a control whose two arms had a **literally identical rate** (`diff = +0.0000`) came
back `p = 0.0000` and **withheld a valid result**. Every residual had vanished, so the fit was
degenerate and the p-value was an artifact.

Significance alone was the wrong test in *both* directions — it fires on degenerate fits, and on a
large enough sample it fires for control movements far too small to explain anything away. Two fixes:

- `Estimate.significant` now requires a **finite, positive standard error**. A zero SE means the
  model is broken, not that the effect is certain.
- `control_moved()` requires the control to be significant **and** at least **25% of the primary
  effect**. The motivating confound is untouched (fallback +1.46pp vs treatment +1.34pp = a 109%
  share), but a control that could account for 2% of an effect no longer discards the other 98%.

This is a reminder that the gates need their own tests as much as the estimators do: the gate had
been passing its tests while running on a different code path than production would.

## Stage 2 shipped: the first trustworthy readout kills the co-liker cap

`dgaff/graze-analysis` runs hourly in-cluster (`personalization-analysis` CronJob, `pool-main`,
200m/512Mi — no new node pool needed). All 41 tests, including both false-positive replays, pass
inside the amd64 image. First production readout of the randomized `max_total_sources` experiment:

```
=== max_sources_250_vs_10000 — NOT SIGNIFICANT ===
  primary    : diff=-0.0175 (-48.6%) 95% CI [-0.0432, +0.0083] p=0.1845 units=541 obs=4329
  control    : diff=-0.0044 (-27.5%) 95% CI [-0.0202, +0.0113] p=0.5799  (flat — gate passed)
  permutation: diff=-0.0173 (-48.1%) p=0.1910
  CUPED variance reduction: 6.4%
```

**The retrospective analysis of this exact change claimed +33% (5.36% vs 4.02%, z≈3.3). The randomized
readout puts the point estimate at −48.6%.** Not merely smaller — the opposite sign. The negative
control is flat this time (p=0.58), so the gate reports rather than withholds, and both the
cluster-robust and permutation tests agree. The honest verdict is null: we cannot distinguish it from
zero, and we can rule out the claimed +33% direction. **`max_total_sources = 250` should not ship.**

That is five for five: every offline or retrospective estimate in this project has been killed or
reversed by a randomized test.

### CUPED is not going to rescue our power problem

Measured **6.4%** variance reduction, against ~21% (2025 marketplace study) to ~50% (Bing) in the
literature. The pre-period per-user like rate is a weak predictor of the in-period rate here — which
fits what we already know: most users' like behaviour is sparse and bursty, so a 14-day pre-period
gives a noisy covariate. CUPED stays in (it is unbiased for any covariate, so a weak one costs
nothing), but the 48-day-to-detect-20% problem is essentially untouched by it. **Interleaving, not
variance reduction, is what buys the sensitivity** — which is why it was sequenced first.

## Stage 3 (B) built: faceting the co-liker seed at step 1

**New Redis asset `apa:{algo_id}`** — the set of author DID hashes with at least one post in
`ap:{algo_id}`. Written by `graze-candidate-sync` inside `sync_author_success_and_discovery`, which
already builds an author→posts map over the same pool membership, so the set costs one pipeline rather
than a second interner sweep of 40k posts. Temp-key-then-`RENAME` like `store_posts`, because a
half-written author set would quietly shrink every seed derived from it. Includes *every* pool author,
deliberately not inheriting the `min_posts` threshold used for success scoring — the set answers
"does this feed carry this author at all".

**Serving:** `get_or_compute_colikes_per_feed` filters the seed to posts whose author is in `apa:`,
then hands off to the *same* `colikes_from_seed` the production path uses. Two round trips regardless
of seed size (one batched interner lookup, one new `SMISMEMBER` helper). Deliberate choices:
- **Fetches 2× the seed before filtering.** Otherwise the treatment's post-filter seed would be
  smaller than the control's and the comparison would be partly about seed *size* rather than seed
  *composition* — a confound built into the design.
- **Unresolvable posts are kept, not dropped.** An expired `id2uri:` shard is a lookup failure, not
  evidence that the feed lacks that author.
- **A missing `apa:` key passes the seed through untouched.** A sync that has not run must never
  silently turn personalization off.
- **`SeedFilterStats` is returned and carried on `ScoringResult.seed_keep_rate`.** This is the part
  that makes the experiment readable: if the keep rate collapses toward zero, the "treatment" is
  little-or-no personalization wearing a ranker's name, and a win would be a finding about fallback
  quality rather than about seeding. Read the keep rate *before* the preference.

### Fixed the nondeterminism class, not just the instance

Stage 1's shared-weights fix made two *traversal* rankers agree, but `PerFeedSeeded` legitimately
differs in the **weights**, so it must derive its own — which would have re-exposed the `thread_rng`
seed shuffle the moment anyone ran it against itself. Rather than patch that per ranker, **seed
sampling is now deterministic per (user, day)** at both sites (`coliker.rs`, `scorer.rs`).

The shuffle exists to vary *which* of a user's likes seed the walk — a between-user and between-day
property. Drawing it fresh per request bought nothing and cost the harness its null. Determinism is
also close to free behaviourally: the derived weights are cached with a TTL, so a user's seed sample
was *already* fixed for the life of that cache entry; rotating daily preserves the variety. Both
properties are pinned by tests (stable within a user-day, different across users, still a genuine
permutation).

Verification: 182 tests pass, clippy clean under `-D warnings`, fmt clean.

## 🔴 Stage 3 (B) must NOT run as designed — measured before exposing anyone

The spec said read the seed keep rate before the preference. Measured it read-only on 400 real users
with likes today, seed taken over the **full 6-day retention window** (the same window the scorer
uses), authors resolved through `id2uri:` and tested against live `apa:` sets:

| algo | pool posts | pool authors | users whose seed filters to **zero** | mean keep rate |
|---|---:|---:|---:|---:|
| 1988 | 27,458 | 8,310 | **87%** | 0.91% |
| 396 | 12,236 | 250 | **89%** | 0.77% |

**For ~88% of users, faceting the seed to the feed's authors leaves nothing at all.** The treatment
would therefore be "no personalization" for the overwhelming majority of enrolled users, and any
preference the harness measured would be a finding about *fallback quality*, not about seed
composition. Running it would have produced a real, significant, and completely misattributed result.
The experiment is not enabled. Production is back at baseline (no `INTERLEAVE_*`, no
`DIVERSITY_PRESERVE_ORDER`).

**I made the small-sample error again, one level down, and caught it here.** The first pass sampled
only today's `ul:` shard and reported a median resolvable seed of **3**, which made keep rates look
coarse and pessimistic for a measurement reason rather than a real one. Over the correct 6-day window
the median seed is **29** (mean 66) — a 10x correction — and the verdict barely moved (94% → 87%
zero). The lesson that generalises: **the unit of measurement must match the unit the code uses.**
Had the conclusion been favourable rather than unfavourable, the flawed version would have been much
easier to believe.

### Why the LinkLonk analogy broke, which is the actually useful finding

The plan reasoned "LinkLonk facets at step 1, we facet at step 3, so move our faceting to step 1".
That reasoning skipped a structural difference:

**LinkLonk users subscribe to channels, so their upvotes are in-channel *by construction*.** Step-1
faceting there is nearly free — it discards almost nothing. Our users like across all of Bluesky,
while a Graze feed is a narrow slice of it (algo 396 is a 250-author curated list). "Restrict the seed
to the feed" is therefore **not** the analogous operation; in our topology it is a near-total seed
wipe. The analogous operation is to make the seed *reflect* topical affinity without requiring
membership — i.e. **weight** the seed rather than **filter** it.

This also explains, retroactively, the measurement that motivated stage B: 34-56% of a user's top-128
co-likers contributing zero coverage on a feed is not evidence that faceting early would help. It is
evidence that **our users' like graph and any single feed's author set barely intersect** — which is a
statement about the sparsity of the overlap, not about where in the walk to apply a filter.

**What survives.** `apa:{algo}` is built, correct, and cheap (~170 KB/feed, 4h TTL); the serving path,
the packed telemetry, and the `SMISMEMBER` batch helper all work; and the keep-rate probe is now a
reusable pre-flight check. The determinism fix shipped independently and is a strict improvement.

### Correction: "soft weighting" was the wrong salvage for stage B

I had recommended keeping the whole seed and merely *upweighting* in-pool authors. That does not
survive the measurement above. **87-89% of users have zero in-pool seed posts** — for them there is
nothing to upweight, so soft weighting is inert at exactly the same rate hard filtering is
destructive. Any step-1 intervention keyed on *exact author membership* fails for the same structural
reason.

What the sparsity finding actually implies is that membership is the wrong predicate and
**similarity** is the right one: weight the seed by how close its authors are to the feed's authors,
which needs an author-author similarity model — i.e. stage C. So the finding is an argument for C, not
a repair for B. Stage B is closed rather than pivoted.

Stage C and D both need the `pool-jobs` node pool (offline jobs cannot schedule on `pool-main`), and
that is a spend decision. **Stage 5 (A, the sampled walk) needs no new infrastructure at all** — no new
keys, no ClickHouse load, no node pool — so it is the next thing built.

## Stage 5 (A) built: Pixie-style sampled walk

New `crates/graze-api/src/algorithm/walk.rs` (pure, unit-tested math) plus
`Scorer::score_sampled_walk` (Redis orchestration). No new keys, no ClickHouse load, no node pool.

**Why sampling, restated from the measurements.** Both traversals we have are enumerative and both hit
a wall. Post-first truncates each candidate's liker list to the 30 most recent, and 72-100% of
overlapping candidates have more than 30 likers — so the truncation is biased toward whoever liked most
*recently* rather than whoever is most *informative*. Co-liker-first enumerates everything and measured
0.94x coverage at 5x latency, because real co-liker sets reach 5,157. Sampling escapes the dilemma
rather than picking a side: cost is bounded by the walk budget instead of by graph degree.

**Two batched phases, not N round trips.** A literal 3-step walk needs a round trip per step, which is
unaffordable at p99. The same walk runs as: sample (seed, co-liker) pairs from liker lists already
fetched, then fetch the sampled co-likers' likes in one pipeline. Same distribution, different
scheduling.

**Design choices worth defending:**
- **Budget allocated by `1/(1 + ln(1+degree))`** — favours niche seeds, matching the reasoning behind
  LinkLonk's own `1/|likers|` step-2 term. Logarithmic rather than reciprocal so high-degree seeds
  still contribute instead of starving. Every seed gets at least one walk when the budget allows,
  because a seed with zero walks is indistinguishable from one that was never there — which is exactly
  how the enumerative paths acquired their bias.
- **Multi-hit booster `(Σ√Vₛ)²`** — rewards breadth over depth. Two seeds × one visit scores 4; one
  seed × two visits scores 2.
- **Popularity discount kept** (default power 0.5). Dropping it would quietly abandon LinkLonk's
  step-3 fairness term, which exists so prolific accounts cannot dominate.
- **Total ordering with a post-id tie-break.** Equal scores must not order by hash-map iteration; two
  runs in one process would disagree, which is precisely the defect the self-check caught.
- **Exhaustive fallback below 5 seed posts**, where sampling variance is worst and enumeration is
  cheap anyway.

### The number that decides whether this can work: `breadth_mean`

The booster only does anything if candidates are reached from *several* seeds. The enumerative paths
measure `overlap_mean ≈ 1.0` — they essentially never observe breadth. So `breadth_mean` and
`multi_seed_candidates` are logged on every walk, and **they should be read before any preference
readout**. If breadth comes back near 1.0, the booster is inert, the walk is not finding structure the
other paths miss, and a null result is *expected* rather than informative. That would itself be a
finding: it would mean the like graph is too sparse for multi-path corroboration at this scale, which is
the same sparsity conclusion stage B ran into.

Verification: 199 tests pass, clippy clean under `-D warnings`, fmt clean.

### Pre-flight: the walk's breadth, measured read-only on 150 real users

Same gate Stage 3 failed, applied before exposing anyone. The probe mirrors
`score_sampled_walk` exactly — same two phases, same `1/(1+ln(1+degree))` allocation, same
before-the-user liker filter, same pool faceting.

| | algo 1988 | algo 396 |
|---|---:|---:|
| users the walk ran for | **124 / 150 (83%)** | 124 / 150 (83%) |
| median seed size | 35 | 36 |
| co-likers sampled (median) | 256 (the cap) | 256 |
| pool visits (median) | 56 | 212 |
| **candidates reached (median)** | **44** | **134** |
| users reaching zero candidates | 6 / 124 | 6 / 124 |
| **breadth_mean** (median / mean / max) | **1.024 / 1.157 / 4.20** | **1.055 / 1.372 / 20.0** |
| multi-seed candidates (mean) | 28.7 | 24.0 |
| users with breadth > 1.05 | **38%** | **52%** |

**Verdict: runnable, unlike stage B.** Three things separate this from the faceting dead end:

1. **The treatment is active for 83% of users**, against 12% for author faceting. Whatever this
   measures, it will be measuring the ranker rather than the absence of one.
2. **Coverage is real** — a median of 44-134 candidates reached, with only 6 of 124 users at zero.
3. **The booster is weak but not inert.** Median breadth barely exceeds 1.0, so the median user gets
   little corroboration; but the *mean* is 1.16-1.37, the tail reaches 4.2-20, and 38-52% of users
   clear 1.05 with a mean of ~24-29 multi-seed candidates each. The enumerative paths sit at
   `overlap_mean ≈ 1.0` exactly, so sampling 256 co-likers does surface breadth that fetching 30
   likers per candidate cannot.

**Calibrated expectation, recorded before the readout:** a modest effect concentrated in the ~40-50%
of users with real breadth, diluted by a median user for whom the booster does almost nothing. If the
interleaved result is null, breadth is the first place to look — and the honest follow-up would be
raising `WALK_MAX_USERS` rather than declaring the approach dead.

## 🔴 The sampled walk is not viable — and three failures now point at ONE cause

Ran it interleaved against `post_first` at 10% then 50% (n=140 walks, n=496 control scorings), then
disabled it. Production is back at baseline.

| | control (`post_first`) | treatment (`sampled_walk`) | ratio |
|---|---:|---:|---:|
| scoring time, median | 117 ms | **466 ms** | **4.0x slower** |
| scoring time, p95 | 564 ms | 1,186 ms | 2.1x |
| scored candidates per draft | **108** | **37.6** | **0.35x coverage** |
| breadth_mean | ~1.0 (as `overlap_mean`) | 1.030 median / 1.112 mean | 41% of users > 1.05 |
| early stopping fired | — | **2 / 140** | effectively dead |

Worse coverage at four times the cost. That is the inverted lookup's result again, almost exactly
(0.94x coverage at 5x latency) — and this time the tuning knobs cannot save it: closing the coverage
gap means *more* walkers, and walkers are what costs the latency.

Two secondary findings worth keeping:
- **`WALK_EARLY_STOP_NP = 200` is unreachable.** The walk reaches a median of 30 candidates, so a rule
  requiring 200 of them to be visited three times can never fire. The early-stopping mechanism that
  was supposed to bound latency was inert in every run but two. A default chosen from the paper rather
  than from our own measured candidate counts.
- **Only 250 shared items across 158 drafts.** The two rankers almost entirely disagree, which means
  interleaving was comparing near-disjoint rankings rather than reordering a common set.

### The convergent cause: 99.9% of what a walk fetches is outside the pool

Phase 2 fetches 256 co-likers x up to 200 recent likes ≈ 51,200 post IDs, and keeps the ~30-56 that
land in `ap:{algo}`. **A 99.9% discard rate.** That is not a tuning problem, it is the topology:

| measurement | finding |
|---|---|
| Stage 3 (author faceting) | 87-89% of users have **zero** liked-authors in a feed's pool |
| Inverted lookup | 0.94x coverage at 5x latency enumerating co-likers' likes |
| Stage 5 (sampled walk) | **99.9%** of fetched co-liker likes fall outside the pool |

Three independent probes, one conclusion: **a feed pool is a tiny, recent slice of an enormous global
like graph, so almost any path from a user through co-likers lands outside it.** The bottleneck was
never the traversal strategy.

**This explains why `post_first` keeps winning, which I had been treating as a puzzle.** It starts
*from the pool* and works backwards, so every unit of work it spends is spent on a candidate that
could actually be served. Its "truncate to the 30 most recent likers" bias is the price of being
pool-anchored — and that price is evidently far lower than the cost of not being anchored. Every
alternative that starts at the user and walks outward pays to rediscover that the graph mostly points
somewhere else.

### What this implies for the remaining plan

**Stages A and B are both closed, for the same reason.** Neither should be revisited without new
evidence that the pool/like-graph overlap has changed.

**Stages C (item-item) and D (two-tower) are unaffected by this failure mode**, and that is not
special pleading — it is structural. Both are **pool-anchored by construction**: they enumerate the
feed's candidates and score each one by similarity to the user, with no traversal out into the graph.
C scores pool posts by author-author similarity; D scores pool posts by embedding dot product. The
99.9% discard rate cannot arise, because nothing off-pool is ever fetched.

So the plan's ordering — which put the graph-traversal approaches (A) before the model-based ones
(C, D) — turns out to have been backwards for our topology. The evidence now says go to C.

# 2026-08-14: 24-hour measurement — the project has been optimising the wrong thing

Production stable: 3/3 pods, **zero restarts in 23h**, analysis CronJob ran hourly all day, no
experiment flags set.

## Correction: `max_total_sources = 250` is not "ruled out negative", it is unmeasured

The estimate decayed as data accrued, which is what a noisy early estimate does:

| | 8/13 (obs=4,329) | 8/14 (obs=10,977) |
|---|---:|---:|
| diff | −0.0175 (**−48.6%**) | −0.0029 (**−11.1%**) |
| 95% CI | [−0.0432, +0.0083] | [−0.0217, +0.0159] |
| p | 0.18 | 0.76 |

Yesterday I wrote that the randomized readout "rules out the claimed +33% direction." **With 2.5x the
data that no longer holds** — +33% now sits inside the interval. The defensible statement is narrower:
the retrospective's z≈3.3 confidence is not reproduced, and the effect is indistinguishable from zero
in both directions. The anytime-valid interval is what made watching this decay legitimate rather than
p-hacking.

## The structural finding: we serve ~1% of Graze's traffic, and personalization touches 0.3% of it

`feed_interactions` is shared with other Graze services. Their provenance is only
`{"feed_uri": ...}` — **457,269 such rows in 6 hours, with zero of them carrying `depth`, `total`,
`personalized`, or `algo_id`** — against roughly 4,500 rows from personalization-api in the same window.

Within our own slice (7 days, by `source`):

| source | seen | likes | like rate | users |
|---|---:|---:|---:|---:|
| fallback | 76,145 | 1,699 | **2.23%** | 3,175 |
| **personalized** | 34,312 | 1,680 | **4.90%** | 1,312 |
| pinned | 2,790 | 1 | 0.04% | 534 |
| rotating | 1,529 | 4 | 0.26% | 412 |

And across *all* interactions in 24h, `source = 'personalized'` is **0.33%** of impressions.

So the leverage stack is: personalized items are ~30% of impressions **in our own feeds**, our feeds
are ~1% of Graze's interaction volume, and a ranking improvement only moves the personalized slice. A
10% better ranker would touch 10% x 30% x 1% ≈ **0.03%** of Graze engagement. Stages C and D are aimed
at that 0.03%.

## The randomized answer: personalization's total effect is ~zero so far

The 4.90%-vs-2.23% comparison above is **observational and badly confounded by position** —
personalized items rank higher, so they are more visible, and the gap would appear even if the
selection were worthless. The 5% holdout is a genuine randomized arm, so it answers the question
properly:

```
=== personalization_holdout — NOT SIGNIFICANT ===
  primary    : diff=+0.0009 (+5.4%) 95% CI [-0.0148, +0.0166] p=0.9071 units=1501 obs=25813
  control    : pinned_and_rotating diff=+0.0012 p=0.3146  (flat — gate passed)
  permutation: diff=+0.0076 (+43.3%) p=0.3020
  CUPED variance reduction: 9.5%
```

**Point estimate essentially zero, negative control clean.** The interval is wide (roughly −87% to
+98% relative), so this does not yet *establish* that personalization does nothing — it is
underpowered, by construction, because the holdout was cut to 5%. Note also the two estimators
disagree in magnitude (+0.0009 impression-weighted versus +0.0076 unweighted), meaning the apparent
effect lives in light users; neither is significant.

**This single number gates everything downstream.** If the engine's total causal effect is near zero,
improving the ranking of the 30% of impressions it touches cannot matter, no matter how good stage C
or D turns out to be.

## Three more analysis-layer bugs, all found before they produced a number

1. **No population restriction.** An arm identified by a field being *false* matches every foreign-service
   row, because an absent JSON field extracts as false. The holdout spec is exactly that shape, so it
   would have put 457k rows of another service's traffic into the treatment arm. Added a mandatory-when-needed
   `population` clause, applied *inside* the CTE so it filters before arm assignment.
2. **Booleans rendered as Python reprs.** `True`/`False` reached the SQL and matched nothing.
3. **`JSONExtractInt` on a JSON boolean returns 0** — identical to an absent field — so both arms
   collapsed into one and the estimator reported *zero observations*. The gate then said "insufficient
   data", which reads exactly like an experiment that has not accrued yet rather than one that can
   never accrue. **A silent failure wearing the costume of a safe one**, and the most dangerous of the
   three. Extractor is now inferred from the arm's value type.

50 tests. Every one of these was caught by asking "why is this number zero" rather than accepting a
withheld verdict at face value.

## Two fixes and a corrected number (8/14, continued)

### 🔴 The holdout was a per-REQUEST coin flip, so yesterday's readout was invalid

`feed.rs:700` was `rand::random::<f64>() < personalization_holdout_rate` — an independent draw **per
request**. That breaks the experiment two ways at once:

1. **Within-user contamination.** The same user was held out on some requests and personalized on
   others, so every active user sat in *both* arms. That attenuates any real effect toward zero. The
   readout I reported (+0.0009, +5.4%, p=0.91) is exactly what a diluted design produces **whether or
   not personalization works** — so it was not evidence of a null, it was an artifact.
2. **Cache carryover.** Pages 2+ come from `fsc:`, so a user whose first page was personalized kept
   receiving personalized content afterwards regardless of later draws, leaking treatment into control.

Replaced with a stable per-user assignment: `hash_did("{salt}|{did}")` mapped to `[0,1)` and compared
against the rate, with `PERSONALIZATION_HOLDOUT_SALT` deliberately distinct from the Thompson and
interleaving salts — sharing one would correlate holdout membership with bandit arms, so the holdout
readout would partly be measuring the bandit. Five tests pin the properties the readout depends on:
stable per user across calls, observed rate within 3% of configured at 0.05/0.2/0.5, degenerate rates,
salt changes reshuffle (so a salt change starts a new experiment), and near-independence from the
interleaving assignment (joint/expected within 0.85-1.15).

**The holdout spec needs a fresh `start` once this deploys.** Data before the fix came from a different
assignment mechanism and must not be pooled with data after it.

### Correction: "87% of candidates have no likers" was one feed, not the system

I read `posts_skipped_no_likers` median 1,010 / 1,166 checked from the logs and was about to report 87%
as a system-wide fact. Direct measurement of `pl:` and `apc:` on 400 sampled pool posts per feed
disagreed — and both were right. **Algo 5395 alone is 385 of 466 scorings (83% of all traffic)** and has
an 86.6% no-liker rate; the per-request median was simply that one feed.

The real picture is that zero-liker share **varies enormously by feed**, and the probe matches each
feed's logged value exactly:

| algo | pool posts with zero likers | median likers among those that have any |
|---|---:|---:|
| 1988 | 16% | 5 |
| 396 | 19% | 6 |
| 33024 | 23% | — |
| 2323 | 34% | 3 |
| 2243 | 45% | 2 |
| 2304 | 50% | 2 |
| 2567 | 71% | 4 |
| 4051 | 72% | 1 |
| **5395** | **~87%** | ~1 |

No probing bug: shard-bounded and full-window counts agreed on **0 of 1,600** sampled posts, and `apc:`
matched the live `pl:` count exactly in every case. `Keys::post_likers_retention_bounded` is sound.

**This reframes the coverage question from global to per-feed.** A feed whose pool is mostly unliked
posts cannot be personalized by *any* co-liker method — there is no like signal to work with, so this
is not a ranking problem and no model in stages C or D would fix it. And because one such feed
dominates the traffic, the system-wide "personalization barely covers anything" symptom is largely
one feed's pool composition rather than an engine defect.

The lever this exposes is cheap and specific: **decide per feed whether personalization is viable, from
its own pool's like density**, and spend the scoring budget where there is signal. That is a config and
routing question, not a modelling one.

### Deployed: per-user holdout at 20%, and the old bug's severity measured

`holdout-peruser-v1` live at 18:43:54Z, rate raised to 0.20 at 18:45:31Z, 3/3 pods, zero errors.
Spec `start` stamped **18:47:00Z** — moved *forward* from the 18:30 I had staged, because the deploy
landed later and an earlier start would have silently pooled per-request-assigned rows into the clean
experiment, which is the exact contamination the reset exists to prevent.

Verified against the same ClickHouse rows the readout consumes, counting users who appear under both
values of `is_personalization_holdout`:

| window | users in **both** arms | users in one arm |
|---|---:|---:|
| pre-fix (per-request coin flip) | **238** | 1,085 |
| post-fix (per-user hash) | **0** | 10 |

**18% of users were in both arms simultaneously before the fix** — and those are disproportionately the
*active* users who generate most impressions, so the contamination was concentrated exactly where the
signal lives. That is the mechanism behind the spurious +5.4% (p=0.91) null, now measured rather than
inferred.

A first log-based version of this check reported "0 users in both arms" but the parser had extracted no
user hashes at all, so the 0 was vacuous — it proved the query returned nothing, not that the property
held. Re-run against ClickHouse, where the pre/post contrast makes a passing result distinguishable
from an empty one.

Accrual so far is ~10 users / 31 rows in 13 minutes, i.e. roughly 3,400 observations/day, so the spec's
2,000-observation floor should clear in about 14 hours. **The current 5-vs-5 arm split says nothing
about whether the rate is 20%** — n=10 users cannot distinguish 20% from 50%, and the rate should be
re-checked once a few hundred users have accrued.

# 2026-08-17: three days of a VALID holdout — positive, large, and not yet significant

Production stable: 3/3 pods, **zero restarts in 2d18h**, holdout rate 0.20, realised rate **20.1%**
(399 of 1,981 users) — exactly as configured.

```
=== personalization_holdout — NOT SIGNIFICANT ===
  primary    : diff=+0.0207 (+140.5%) 95% CI [-0.0056, +0.0470] p=0.1233 units=2626 obs=74194
  control    : pinned_and_rotating diff=+0.0020 p=0.0555
  permutation: diff=+0.0199 (+135.0%) p=0.4202
  CUPED variance reduction: 18.3%
```

Raw rates: **treated 4.47%, holdout 1.29%**. The point estimate flipped from the contaminated
design's +5.4% (p=0.91) to **+140%**, which is what fixing an attenuating design does — but it is not
significant, and three things below say to hold the conclusion loosely.

**The mechanism verifiably works.** Holdout users received `fallback`, `pinned`, `rotating` and
**zero `personalized` impressions**; treated users got 15,726 personalized against 21,018 fallback.
The suppression does what it claims.

## The weighted estimate rests on twelve users

| impressions/user | treated rate | holdout rate | holdout users |
|---|---:|---:|---:|
| 1-5 | 0.53% | 0.42% | 167 |
| 6-20 | 0.87% | 0.89% | 126 |
| 21-100 | 1.75% | 0.70% | 94 |
| **100+** | **9.30%** | **2.88%** | **12** |

The 100+ bucket supplies 1,368 of 1,710 treated likes — **80% of the signal** — and its holdout cell
has **12 users and 66 likes**. That is why the two estimators diverge so far: cluster-robust WLS
weights by impressions and reports p=0.12, while the unit-level permutation test reports p=0.42. The
unweighted per-user mean is the more robust summary: **1.098% treated versus 0.590% holdout**, i.e.
**+0.51pp / +86%** rather than +140%, and still not significant.

**Important caveat on that table: impression count is post-treatment.** If personalization makes people
engage more, it *moves them into* the heavy bucket, so bucketing by impressions conditions on a
consequence of the treatment — the same error as slicing by `source`. The table is therefore evidence
about the estimate's **fragility**, not a valid subgroup finding, and must not be reported as "the
effect is only in heavy users."

## The negative control is uninformative, not marginal

The readout shows the control at p=0.0555, which reads as "nearly moved". The underlying counts are
**3 likes in total** (pinned 1 vs 0, rotating 2 vs 0). At that size the control cannot discriminate
anything; the right description is *no power*, not *nearly significant*. My magnitude gate is what kept
this from withholding a valid result — the control's +0.0020 is 9.7% of the primary effect, below the
25% share threshold — and that is the gate working as designed.

## A real labelling leak, worth fixing: only first pages carry the holdout flag

Five users still appear in **both** arms (against 238 pre-fix), and their rows span the whole three days
rather than clustering at the deploy boundary, so this is not deploy residue. The likely cause is
structural: the holdout branch is gated on `is_first_page`, and `is_personalization_holdout_for_provenance`
is only set there. A holdout user's **pages 2+** are served from `fsc:` (fallback content, correctly)
but recorded **without** the flag — so those impressions land in the *treated* arm.

Direction matters: this puts fallback-served impressions into the treated arm, which **dilutes** the
measured effect. The true effect is therefore likely a little larger than measured, not smaller. The fix
is to carry the holdout assignment in the cursor so every page of a session is labelled consistently.

## Where this leaves the decision

- **Positive and possibly large, but not established.** +86% (robust) to +140% (weighted), CI includes zero.
- **Needs roughly another week** at the current rate for the unweighted estimate to resolve.
- **I would not raise the holdout further.** Going 20% -> 50% buys 1.56x efficiency, but if the engine
  really is worth +86% then suppressing it for half of users has a real cost. That trade was cheap when
  the point estimate looked like zero; it is not cheap now.
- **Stage C and D stay on hold.** They improve ranking within the personalized slice; that slice is worth
  investing in only once its value is established, and the `pool-jobs` spend is unjustified until then.
- **The per-feed coverage lever is the right parallel work**: no new infrastructure, and if personalization
  is genuinely worth +86%, moving impressions from the 2.2% fallback bucket into the 4.5% personalized
  bucket multiplies whatever the true effect turns out to be.

## 2026-08-17: labelling leak fixed; per-feed coverage work started

### The leak, located precisely and closed structurally

The dual-arm users were each on a **single feed**, so it was not a multi-feed effect. Their
treated-labelled rows sat at **depth 3-6, never page 1**, and some carried `source: personalized`:

| arm | source | algo | rows | depths |
|---|---|---|---:|---|
| holdout | fallback | 2323 | 337 | 0-29 |
| **treated** | **personalized** | 2304 | **17** | **5-6** |
| treated | fallback | 2323 | 6 | 3 |

Cause: the holdout branch was gated on `is_first_page`. A paginated request whose cursor did not happen
to carry `fallback_only` skipped the holdout entirely, read `fsc:`, and served personalized content
*labelled as treated*. **Two-way contamination** — a holdout user both receiving treatment and being
counted as treated. My earlier guess that the cursor never carried the flag was wrong; it usually does,
which is exactly why the failure was rare and survived three days.

Fix: derive the arm from the DID on **every** request and route holdout users through the existing
fallback-only path on **every** page. That path already handles pagination offsets and exclusions, so
this **deleted 44 lines** of duplicated first-page-only handling rather than adding a second path to keep
in sync. The invariant now holds by construction: a holdout user can neither be served personalized
content nor be labelled treated, on any page, with or without a cursor.

Two new tests state the contract directly — the arm may not depend on page or cursor, and membership is
**monotone in the rate** (raising 0.05 -> 0.20 only ever *adds* users, never swaps them out, so a rate
increase does not invalidate accrued members).

### Per-feed viability, measured across all 64 live pools

| scoreable share (at `min_post_likes = 10`) | feeds |
|---|---:|
| viable, >=20% | **22** |
| marginal, 5-20% | **27** |
| dead, <5% | **15** |

**Pool size does not predict viability**, which is why the existing `min_candidate_pool_for_personalization`
gate cannot separate these: algo 8352 has **596 posts and 22% scoreable**, while algo 5395 has **1,000
posts and 2%**. Density is the measure that separates them — and algo 5395 is **~83% of all scoring
traffic**, so most of the engine's compute goes to a feed that structurally cannot produce a ranking.

Implemented as two pieces:
- **`graze-candidate-sync`** publishes a like-density histogram (`scoreable_1/2/3/5/10`) into `am:{algo}`
  during `store_posts`. Free — `liker_counts` is already in hand for every post. Stored as counts at
  several thresholds rather than one share, so the serving threshold can change without a sync redeploy.
- **`graze-api`** gates on it with one `HGET` before the co-liker walk. **Disabled by default (0.0)**, and
  **fails open**: a missing field means "sync has not published yet", and treating absence as "not viable"
  would disable personalization everywhere the moment the API deploys ahead of candidate-sync.

### The second lever this exposed: `min_post_likes` is set high

Scoreable share by threshold, busiest feeds:

| algo | t>=1 | t>=2 | t>=3 | t>=5 | **t>=10 (current)** |
|---|---:|---:|---:|---:|---:|
| 1988 | 83% | 68% | 55% | 41% | **29%** |
| 396 | 83% | 68% | 57% | 46% | **36%** |
| 33024 | 78% | 63% | 53% | 41% | **26%** |
| 2323 | 64% | 53% | 43% | 33% | **16%** |
| 2304 | 60% | 36% | 27% | 12% | **5%** |
| 2243 | 57% | 33% | 27% | 16% | **8%** |
| 5395 | 14% | 8% | 4% | 3% | **2%** |

Dropping the floor from 10 to 3 would roughly **2-5x the admissible candidate pool** on most feeds.
**But admissible is not the same as scored**, and this is precisely where an earlier `min_post_likes`
experiment was already **refuted**: a post with 3 likers has at most 3 chances to overlap the user's
co-likers, so a larger admissible pool need not yield more *scored* candidates — and lower-liker posts
are also lower quality. This is a candidate for a real experiment through the harness, not a config
change to make on the strength of the table above.

### Deployed 8/17: labelling fix + per-feed density histogram

`holdout-density-v1` — candidate-sync at 13:29:41Z, api at 13:36:54Z (sync first, deliberately, since
the API's gate reads a field sync writes). 3/3 pods, **zero errors**, gate confirmed inert
(`MIN_POOL_SCOREABLE_SHARE` unset = 0.0, zero density-gate early exits in the logs), holdout still
firing (85 `personalization_holdout` responses in 3 minutes).

**The histogram works and validates against an independent measurement.** Only 3 syncs ran in the first
8 minutes — the rolling sync covers ~63 pools at that cadence, so my first check of 0/63 was *premature,
not a bug signal*. Checking only the feeds that had actually synced:

| algo | post_count | s1 | s2 | s3 | s5 | s10 | share@10 | earlier probe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 396 | 7,277 | 6,094 | 5,162 | 4,546 | 3,736 | 2,587 | **35.6%** | **36%** ✓ |
| 1753 | 378 | 303 | 270 | 253 | 232 | 203 | 53.7% | — |
| 1937 | 691 | 381 | 313 | 287 | 267 | 237 | 34.3% | — |
| 1660 | 297 | 106 | 87 | 73 | 59 | 33 | 11.1% | — |

Algo 396 landing on 35.6% against the 36% measured by a completely separate sampling probe is the check
that matters. Algo 1988 shows `post_count` without the histogram — it synced on the old pod mid-rollout
and will pick it up next cycle; full coverage of all 63 pools takes roughly three hours at this cadence.

**Labelling fix confirmed:** users appearing under both arms went **5 -> 0**.

| window | users in both arms | users in one arm |
|---|---:|---:|
| pre-fix (8/14 18:47 -> 8/17 13:37) | **5** | 1,986 |
| post-fix (since 8/17 13:37) | **0** | 12 |

**Two honest caveats on that verification.** First, I also queried "holdout-arm rows with
`source: personalized`" and got 0 in *both* windows — that result is **tautological, not evidence**: the
mislabelled rows carried `holdout = false` by definition, so they could never appear in a query
restricted to the holdout arm. It tests nothing. Second, 12 users over ten minutes is weak evidence
against a contamination that took three days to accumulate five cases. The stronger argument is
structural: the arm is now a function of the DID alone, holdout users route through the fallback-only
path on every page, and two tests pin that the arm cannot depend on page or cursor.

**The holdout experiment's `start` stays at 2026-08-14T18:47:00Z.** The fix changes measurement accuracy,
not the assignment mechanism — the arm for any given DID is unchanged and membership is monotone in the
rate — so the three days of accrued data remain poolable. Resetting again would discard them for no
methodological reason.

### Density-gate pre-flight: halve the scoring work for ~2% of the output

Histogram reached **59/63 pools (94%)**. Traffic-weighted over a 90-minute window (234 scorings, 4,861
scored posts) — because feed *count* is the wrong denominator; what matters is scorings skipped versus
output lost:

| gate | feeds skipped | **% of scorings skipped** | **% of scored output lost** |
|---:|---:|---:|---:|
| 0.02 | 1 | **53.0%** | **2.0%** |
| 0.05 | 2 | **53.4%** | **2.1%** |
| 0.10 | 8 | 89.7% | 37.2% |
| 0.20 | 10 | 95.7% | 74.0% |

**A gate anywhere in 0.02-0.05 removes over half the engine's scoring work and costs about 2% of its
output.** Above that it falls off a cliff: 0.10 loses 37% and 0.20 loses 74%, because feed 8386 is 28.6%
of scorings and 25.1% of all output at only 8.4% density — productive despite being sparse.

The per-feed picture behind it:

| algo | % of scorings | % of output | zero-output scorings | density@10 |
|---|---:|---:|---:|---:|
| 4051 | **53.0%** | 2.0% | **64%** | **1.6%** |
| 8386 | 28.6% | 25.1% | 39% | 8.4% |
| 2323 | 5.6% | 27.3% | 8% | 13.7% |
| 2304 | 3.4% | 5.4% | 25% | 6.4% |

**Correction to something I said earlier.** I reported algo 5395 as "~83% of all scoring traffic". That
was true of the window I sampled then; in this window 5395 does not appear in the top twelve and **4051**
is 53%. The *pattern* holds — a very low-density feed absorbs most of the scoring work — but the identity
of that feed shifts between windows, so the gate should be justified by density rather than by naming a
particular feed.

**Recommended value: 0.04.** It catches 4051 (1.6%), 5395 (2.9%), 4058 (3.2%) and 2340 (3.3%) while
leaving the lowest-density *productive* feed, 2304 at 6.4%, with a 1.6x margin. That margin is the real
risk to weigh: densities drift as pools turn over, and a feed drifting below the gate silently stops
being personalized. 0.10 is emphatically wrong despite skipping more work — it would take 8386 with it.

### Density gate enabled at 0.04 — live and verified against its prediction

`MIN_POOL_SCOREABLE_SHARE=0.04` live at 2026-08-17T22:15:20Z. 3/3 pods, **zero errors**.

Over the following 8 minutes (80 personalization attempts):

| | |
|---|---:|
| gated out by density | **36** |
| reached the scorer | 44 |
| **skip rate** | **45.0%** |
| pre-flight prediction | 53% |

**Exactly the two intended feeds were gated**, at the densities the histogram published:

| algo | skipped | published share@10 |
|---|---:|---:|
| 5395 | 34 | **3.02%** |
| 4051 | 2 | **1.61%** |

And every productive feed still scores: 2323 (2,131 posts scored in 9 runs), 1988 (1,136), 2834 (645),
2243 (220). Nothing with real output was caught.

**45% versus the predicted 53% is traffic-mix drift, not a modelling error** — the same effect already
noted, where the dominant low-density feed differs between windows. Here 5395 supplied 34 of 36 skips;
in the pre-flight window 4051 supplied nearly all of them. The prediction was made on one window and
verified on another, and landing within 8 points across a shifting mix is about as good as that
comparison can be.

**The gate sits in a narrower corridor than I would like, and this is the thing to watch.** Measured
margins on both sides:

- **5395 at 3.02%** would escape gating if its density rose ~32%.
- **2304 at 6.4%** — the lowest-density *productive* feed — would start being gated if its density fell
  ~37%.

Densities move as pools turn over, so a feed drifting under the gate would **silently stop being
personalized**, which is precisely the class of failure this project keeps finding. The natural follow-up
is a cheap guard: alert when any feed with meaningful scored output has a published density within, say,
1.5x of the gate. That converts a silent failure into a visible one.

### Drift guard built — and it found real drift on its first run

`analysis/graze_analysis/drift.py`, wired into the hourly CronJob. 58 tests (8 new), fixtures taken from
the real measured densities so the guard is pinned to production values rather than invented ones.

It reports two things, and the distinction is the point:
- **AT RISK** — currently productive, density within `warn_factor` (1.5x) of the gate. A *prediction*.
- **NEWLY GATED** — produced personalized impressions in the previous window, produces none now, density
  below the gate. A *detection*: the failure has already happened. Only this exits non-zero, so a real
  regression fails the Job while a proximity warning does not page anyone.

**First run output:**

```
=== density drift guard — 1 FINDING(S) ===
  [AT_RISK] algo 2304: productive (103 personalized impressions) but density 5.78% is within
            1.5x of the 4.00% gate — a 31% fall would silence it
```

Algo 2304 was **6.40%** when I measured it hours earlier and is **5.78%** now — a 10% fall in a few
hours, exactly the feed I had identified as the closest productive one. The narrow-corridor worry was
not hypothetical.

**Removed a second source of truth while wiring it.** The gate had been an explicit env var on the API
deployment, which would have meant setting it *again* on the CronJob and keeping two values in sync — a
new drift hazard introduced by the drift guard. Moved `MIN_POOL_SCOREABLE_SHARE` and
`INVERTED_MIN_POST_LIKES` into the `personalization-env` ConfigMap, which both the API and the guard read
via `envFrom`. Verified the gate survived the move: **218 early exits**, same two feeds (shares 0.0302
and 0.0169), zero errors. The guard's `envFrom` also repeats the API's exact ordering — both
`app-secrets` and `personalization-secrets` define `REDIS_URL` and the later wins — so the guard reads
the same Valkey the service reads.

### `feed_interactions` has no TTL, so the holdout can accumulate indefinitely

655M rows back to 2025-09-03, stable ~1.7M/day, `SharedMergeTree` partitioned by month with no TTL
clause. **Waiting will keep growing the sample**, which is what the "give it another week" plan depends
on. Verified the experiment's sample is monotone in the window: obs 47,119 -> 53,675 -> 55,510 for
end times 13:00 -> 22:00 -> now, the last matching the runner's reported 55,490.

Latest holdout readout: **+0.0281 (+226.2%), 95% CI [-0.0068, +0.0630], p=0.1141**, units 2,217. The
estimate has grown from +140% as the labelling leak's dilution was removed, and p sits stubbornly around
0.11-0.12 — consistent with a real effect that the 20/80 split cannot yet resolve.

**One unreconciled number, flagged rather than explained away.** An earlier run today reported
`obs=74194` for this spec, where the verified value for that same window is **47,119**. I could not
account for the difference from the query alone, and I am not going to invent a reason. What I can say
is that the current figures are internally consistent and monotone, and that the effect's sign and rough
magnitude have been stable across both runs. If it recurs, the first thing to check is whether two
CronJob images with different `data.py` were in flight at once.

---

# 2026-08-25: the holdout is not converging, and the bandit has been dead for 13 days

Production stable: 3/3 pods, **zero restarts in 7d23h**, no deploy since 8/17. Density gate holding at
0.04; drift guard clean across 62 feeds — algo 2304, the AT_RISK feed from 8/17, has recovered.

## The holdout: eight more days bought almost no power

| | 8/17 | **8/25** |
|---|---:|---:|
| units / obs | 2,217 / 55,510 | **4,321 / 180,910** |
| weighted diff | +0.0281 (+226.2%) | **+0.0281 (+262.7%)** |
| 95% CI | [-0.0068, +0.0630] | **[-0.0047, +0.0610]** |
| p (WLS) | 0.1141 | **0.0935** |
| p (permutation) | 0.4202 | **0.2487** |

Unweighted per-user, the robust summary: **1.232% treated vs 0.638% holdout = +0.59pp / +93%**
(was +0.51pp / +86%). Negative control clean (p=0.53). Realised holdout rate 19-20% every week.

**The 8/17 fragility is resolved.** The 100+ impressions holdout cell went **12 users / 66 likes ->
69 / 167**, and every stratum now points the same way:

| impressions/user | treated | holdout | holdout users |
|---|---:|---:|---:|
| 1-5 | 0.632% | 0.376% | 283 |
| 6-20 | 1.061% | 0.653% | 285 |
| 21-100 | 1.506% | 0.983% | 233 |
| 100+ | 5.501% | 1.293% | **69** |

Still post-treatment bucketing — fragility evidence, never a subgroup claim. The 8/17 labelling fix also
held: **7 of 4,320** users appear in both arms, against 238 pre-fix.

**🔴 The "needs roughly another week" call was wrong, and this is the finding.** The week elapsed. The
sample **nearly doubled** and the interval narrowed **~5%** (half-width 0.0349 -> 0.0330). Under 1/sqrt(n)
it should have narrowed ~28%. Per-unit variance is growing about as fast as units accrue, because the
heavy users carrying the signal are also carrying the variance. **This design does not reach p<0.05 by
waiting.** Either decide on what is now an estimate stable across 11 days and 3.3x the data with
consistent strata and a clean control, or change the design — but do not schedule another week.

I would still not raise the holdout to 50%. It buys 1.56x efficiency, and the case against it is
*stronger* now than on 8/17, not weaker: the more the effect looks real, the more suppressing it for half
of users costs.

## `max_sources` ENDED — and it had silently disabled the Thompson bandit since 8/12

Final (obs=74,654, units=2,497): **diff=-0.0075 (-25.4%), 95% CI [-0.0194, +0.0043], p=0.2128**;
permutation -0.0073, p=0.2300; negative control clean (+0.0009, p=0.7935). Capping co-likers at 250 is
not an improvement and leans negative across 13 days of randomized traffic. With T21 this **closes the
co-liker-cap question**: the retrospective +33% never reproduced under randomization, in either direction.

**The side effect nobody was watching.** `/v1/thompson/stats` reported `observations_recorded: 0` against
`total_requests: 42,182`. `AB_EXPERIMENT_TRAFFIC_PCT=100` force-enrols every non-holdout user, and
`record_observation` (`thompson.rs:593`) early-returns on `is_hash_experiment`. That exclusion is correct
— forced arms must not train the bandit — but at **100% traffic it means the bandit learns from nothing**.
It had recorded zero observations for 13 days across all nine dimensions, which also made the T20 arm-
persistence work moot for its entire life. **A hash experiment at 100% traffic is a bandit kill switch;
that coupling is not obvious from either flag and deserves a guard.**

`AB_EXPERIMENT_ENABLED=0` set at **2026-08-25T23:21:13Z**. Verified after rollout: **7 observations
recorded out of 7 requests**, exploration firing again.

## The `end:` clause this required, and why it is not cosmetic

With the experiment off, `max_total_sources` reverts to *adaptive* Thompson selection, whose arms still
include 250 and 10000 — so leaving the spec open-ended would pool randomized rows with adaptively-assigned
ones and **silently rebuild the exact confound `start` exists to exclude**. Set to the rollout *start*
(23:21:13Z), not its completion: during the ~90s rollout old pods were still enrolling, so the earlier
timestamp is the last moment every serving pod was provably randomizing.

Specs are baked into the analysis image, so the clause is applied by overlaying that one file from
ConfigMap `personalization-analysis-specs` via a `subPath` mount — image stays immutable, and `subPath`
rather than a directory mount so the other three specs are not shadowed. `analysis/kube/analysis-cronjob.yaml`
updated to match the cluster.

**Verified in a single run:** `max_sources` froze at 74,654 obs while `personalization_holdout` kept
accruing (180,893 -> 180,910). One spec frozen, the other live, same job — which is what proves the
clause is scoped rather than global.
