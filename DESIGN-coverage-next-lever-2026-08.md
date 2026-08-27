# Coverage: what the next lever actually is

**Date:** 2026-08-27
**Status:** decision document — recommends *against* the plan it was commissioned to design
**Supersedes:** the "turn on durable co-liker profiles as a randomized experiment" recommendation
of 2026-08-26

---

## TL;DR

I was asked to design the durable-co-liker-profile experiment plus the coupled holdout reset.
**Two pre-flight measurements say do not run it.**

1. The profiles **do not exist** — `ucl:` key count is **0**. They were built 2026-08-11 with a 7-day
   TTL and expired around 2026-08-18. Flipping `DURABLE_PROFILE_ENABLED=1` today would change
   nothing and return a null, which we would have been at real risk of reading as "durable profiles
   don't help."
2. Even rebuilt, the ceiling is **3.7%**. Of 352 users turned away with `no_user_data` over 7 days,
   only **13** have ≥20 likes in 365 days — the minimum a profile can be built from.

The finding underneath that is the important one, and it is structural:

> **73.6% of the users the engine turns away have no like history at all.**
> Co-liker personalization is seeded from likes. These users have none. No amount of
> history-mining, window-widening, or profile-building reaches them, because there is nothing
> to mine.

Coverage is still the right problem. Durable profiles are not the right instrument for it.

---

## What the full cycle confirmed

The daily coverage job's first complete run (24h, provenance-based, PR #17/#18):

| | |
|---|---|
| `no_user_data` share of **addressable** non-personalization | **98.1%** |
| hours (of 24) where that share is 100.0% | **22** |
| treated arm never receiving personalized content | **39.4%** |

The earlier "73%" figure and this 98.1% are the same finding on different denominators — 73%
counted the intentional holdout in the denominator. Put on the same basis, the 6-hour log scrape
gave 96.6%. They agree, and the cause is now stable across a full diurnal cycle rather than
inferred from one window.

Two corrections to numbers previously quoted:

- **"52% of the treatment arm gets nothing" was overstated → 39.4%.** The 52% counted users who
  never *saw a personalized item*; 39.4% counts users who never *received a response containing*
  personalized content. Both true, different questions.
- **The interaction-asymmetry "corroboration" is withdrawn.** Personalized users generate ~19.5
  impressions each against ~8.0 for turned-away users, which looked like independent support for
  the holdout's positive signal. It is confounded: eligibility *is* having recent likes, and users
  who like more also interact more. The correlation is expected under the null.

---

## Why durable profiles cannot move this

The ceiling is not a tuning question, it is arithmetic. Of the 352 turned-away users:

| population | users | share |
|---|---:|---:|
| ≥20 likes in 365d — **profile buildable** | **13** | **3.7%** |
| 1–19 likes — too thin to seed | 80 | 22.7% |
| **no like history whatsoever** | **259** | **73.6%** |

This ceiling is airtight for the implementation as built, and that is worth stating precisely,
because the obvious objection is the one this project has hit before. Memory's standing rule is
that ClickHouse *understates* live capability — `ul:` in Valkey has broader like coverage than
`user_action_logs`, and reasoning from ClickHouse once produced a 36×-overstated reach claim in the
other direction. So could the 3.7% be too low?

**No, for this decision.** `graze-build-coliker-profiles` builds profiles *by scanning
`user_action_logs`*. A user absent from that table cannot receive a profile regardless of what they
"really" liked. The table's coverage is not a measurement artifact here — it is the build's actual
input, so 3.7% is the true ceiling on what this mechanism can rescue.

It also reconciles cleanly with the corrected historical estimate: durable profiles were measured
to add ~1.6% of DAU against ~63% already personalizable. 3.7% of the turned-away slice is the same
statement viewed from the other end.

---

## What I do *not* know, stated as such

**The follow graph is untested, not refuted.** A user with zero likes may still follow accounts, and
follows would be a seed that does not require like history. I could not size it:
`user_action_logs` contains **no `graph.follow` rows at all** — its action types are
`interactionSeen`, `feed.like`, `feed.repost`, `feed.reply`, and the `interaction*`/`request*`
variants. The 0.0% my query returned is a **table limitation, not a property of the users**, and
reporting it as a finding would repeat exactly the error that once produced a confidently wrong
"no signal here" verdict from the wrong table.

Sizing it requires the follow graph where it actually lives — `network-cache`, or the AppView. That
is the first thing to measure, not the first thing to build.

---

## The other thing those measurements exposed

`user_action_logs` carries **~100× more engagement signal** than the table every experiment reads:

| signal | in `feed_interactions` (experiment window) | in `user_action_logs` (90d) |
|---|---:|---:|
| `requestLess` | **221** | **25,495** |
| repost / reply | 557 combined | 3.49M / 784k |

This matters because **power, not patience, is the binding constraint.** The holdout is at
p=0.1015 after 197,612 impressions, and p has *risen* from 0.0952 while the sample grew by ~14,000 —
more data is now actively failing to help. The relevance guardrail is worse: stuck at n=221,
unchanged in 24h, effectively inert.

If actions in `user_action_logs` can be joined to impressions in `feed_interactions` on
`(did, post_uri)`, the arm assignment from the provenance blob carries over and every experiment
gains a much larger outcome set — including a relevance guardrail with real power. **Unverified:** I
have not confirmed the join keys align or that coverage is high enough to be worth it. That is a
measurement, and it is cheap.

---

## Recommended sequence

Ordered by evidence-per-unit-effort, not by what was planned.

1. **Size the follow graph** against the turned-away population, using `network-cache`/AppView
   rather than `user_action_logs`. This decides whether ~74% of the unserved population is
   reachable at all. Read-only. Nothing should be built before this number exists.
2. **Test the `user_action_logs` → `feed_interactions` join.** If it holds, it is the cheapest
   available fix for the power problem that waiting demonstrably cannot solve, and it fixes the
   inert guardrail as a side effect. Read-only.
3. **Then** design the actual coverage experiment, aimed at whatever (1) says is reachable.
4. **Durable profiles: shelve, do not delete.** The code is built and validated; the ceiling is
   3.7%. Worth revisiting only as a small additive gain after a larger lever lands, and only with
   the TTL problem fixed first (see below).

### If durable profiles are ever switched on

- **Rebuild first.** `graze-build-coliker-profiles`, `PROFILE_DRY_RUN=1` to rehearse, then
  `PROFILE_CHUNK_COUNT=8` (~39 min for ~64k users, validated 2026-08-11). The naive single-query
  version OOMs at 18 GiB; the 128-recent-post seed cap and the `L<=500` viral-post filter are
  load-bearing, not tuning.
- **Fix the 7-day TTL, or the experiment dies mid-flight.** That is not hypothetical — it is
  precisely what already happened, silently, between 8/18 and 8/27. Either extend the TTL beyond
  the experiment horizon or schedule the batch job as a CronJob.
- **Verify before serving:** profile count, mean size ~124/128, `STRLEN` ~1,536 B, ~118 MB total.

### The holdout coupling (unchanged, and still a single decision)

Any change to what the treatment arm receives changes the estimand, so by this repo's own
convention — the holdout spec has already reset `start` twice, once when the rate went 0.05→0.20 —
the holdout window resets with it. Because the current window cannot converge, that reset costs
nothing real. **It must be designed together with whichever treatment change ships, not after it.**

---

## The pattern worth naming

Three of the last four things I recommended here were revised or killed by the measurement taken
immediately before acting: the co-liker cap reversed under randomization, widening the engagement
metric died on zero clickthroughs, and durable profiles are capped at 3.7%. The coverage job itself
shipped with two defects that only its first real output exposed.

The through-line is that the cheap read-only check *before* the build keeps being the highest-value
step, and that this system's plausible-sounding priors have a poor track record. Steps 1 and 2 above
are deliberately both measurements.

---

# Measurement results (2026-08-27, same day)

Both recommended measurements were run. **One is strongly positive, one is negative**, and together
they point at a single conclusion.

## Measurement 1 — the follow graph: POSITIVE

Sampled 60 DIDs from the cohort that is unreachable by likes (turned away with `no_user_data` *and*
zero like history in 365 days), and read `followsCount` from the public AT Protocol AppView.
`network-cache` could not answer this: it is a demand-fetched cache, not a store, so there is no
local follow graph to query.

59 of 60 profiles resolved:

| followsCount | users | share |
|---|---:|---:|
| zero | 1 | 2% |
| 1–9 | 7 | 12% |
| **≥10 — seedable** | **51** | **86%** |
| ≥50 | 30 | 51% |

Median follows: **50**. Median `postsCount`: **1**, with 24 of 59 having posted nothing at all.

**86% of the users the engine cannot reach have a usable follow graph.** These are consumers: they
follow, they read, they do not post and do not like. Likes are the one signal they do not generate,
and likes are the only signal the engine currently seeds from.

### The implementation path already exists

`author_affinity.rs` step 1 reads the authors a user has *liked* from `ula:{hash}`, then fans out
through `authl:{author}:{date}` to other users who liked those authors, aggregates weights, and
hands off to the existing scorer. **A follow-seeded variant substitutes the seed set — authors
followed instead of authors liked — and leaves steps 2, 3 and the whole scoring path unchanged.**

This is the smallest available change with the largest addressable population, which is the opposite
of the durable-profile trade.

## Measurement 2 — richer outcome tables: NEGATIVE

The "~100× more signal" claim does not survive. Two ways of testing it:

- **Per-post join** (`feed_interactions.interaction_item` = `user_action_logs.action_identifier`):
  key formats are identical AT-URIs and the join works mechanically, but yields **0.27×** the
  outcome events `feed_interactions` already supplies.
- **Per-user, per-algo attribution** (correct approach — arm assignment is a stable per-user hash and
  needs no per-impression join): **82.84×** more outcome events do exist on our algos, 691,289
  against 8,345. But **only 0.9% of those users — 680 of 75,883 — appear in our provenance at all**,
  so only they can be assigned an arm.

The events are real and they are on our feeds. They belong to Graze's *other* serving paths. This is
the 2026-08-14 structural finding arriving from a new direction: personalization-api serves ~1% of
Graze's interaction volume, so 99% of the engagement on these algos is generated by users our
randomization never touched.

**Conclusion: experiment power cannot be bought from a richer outcome table.** The binding constraint
is the size of the randomized population, not the resolution of the measurement. That also means the
`requestLess` guardrail cannot be rescued this way — 1,069 events exist on our algos against 231 in
our slice, but arm-mappability caps what is usable.

## What the two results mean together

Power has exactly three sources and measurement is not one of them:

1. **More randomized users** — personalization-api serving a larger share of traffic. Not a
   near-term lever.
2. **Within-user designs** — interleaving needs no additional users, and the Stage 1 harness is
   already built (sitting at 7 tagged impressions). This is the right tool for *ranker* comparisons.
3. **A larger effect** — a bigger treatment dose.

**Follow-graph seeding is route 3, and that is why it is the answer to both problems.** Today ~39%
of the treatment arm receives no personalized content, so the holdout is measuring a treatment that
is undelivered for two users in five. Raising coverage raises the dose, which raises the effect,
which is the only path to a holdout that resolves. Coverage was never a parallel workstream — it is
the numerator of the value and the denominator of the noise, now confirmed from both ends.

## Revised recommendation

1. **Build follow-graph seeding** as a variant of `author_affinity`'s seed step. Largest addressable
   population (86% of the unreachable cohort), smallest code change, reuses the entire scoring path.
2. **Reset the holdout window when it ships** — same coupling as before, same reasoning, and now
   with an actual treatment change worth coupling to.
3. **Fetch-and-cache follows with a TTL that outlives the experiment.** The durable-profile
   post-mortem is the specification for this failure mode: a 7-day TTL expired silently and would
   have produced a null read as "the idea doesn't work." Do not repeat it.
4. **Interleaving for ranker iteration**, once there is personalized content to interleave.
5. **Durable profiles stay shelved** at a 3.7% ceiling.

---

# Fan-out cost, measured (2026-08-27)

Measured against prod Valkey for 24 real users from the unreachable cohort, using their actual
follow lists from the AppView, hashed with the same SHA256-first-16-hex `hash_did`. Cost model taken
from `author_affinity.rs::compute_author_colikes`: one `ZREVRANGE` on the seed, then
`6 x ZREVRANGEBYSCORE` per surviving author (`DEFAULT_RETENTION_DAYS=6`), each limited to
`AUTHOR_AFFINITY_MAX_LIKERS_PER_AUTHOR=100`, authors capped at `AUTHOR_AFFINITY_MAX_AUTHORS=100`.

| | follow-seeded | like-seeded (current, users *with* a seed) | ratio |
|---|---:|---:|---:|
| median seed authors | **59** | 6 | 9.8× |
| median Redis ops / user | **354** | 40 | 8.9× |
| median liker members read | **2,864** | ~410 | 7.0× |
| median **unique sources** | **2,530** | 410 | **6.2×** |
| max unique sources | 6,016 | — | |
| wall clock | **90 ms/user** | — | |

Baseline note: of 24 sampled `ula:` users only **18 had a usable seed** after the
`min_author_likes >= 2` filter, and the all-users median seed is **2 authors**. The current path is
thin even for users it does serve.

## The verdict on cost: affordable, because the fan-out is cached hourly

`AUTHOR_AFFINITY_TTL_SECONDS=3600` with a 600 s refresh threshold, so this runs about **once per user
per hour, not per request**. 354 ops at 90 ms, amortised over an hour, is not a per-request cost.

Rough scale check: provenance shows on the order of 900 distinct users per 24h, so ~100–200
author-affinity recomputations per hour → **roughly 20–70 ops/sec**. Against the measured Valkey
baseline where `zcard` alone runs ~2,655/sec and is 26.6% of all commands, this is under 1%. The
fan-out is not the problem.

**2,530 median unique sources also sits comfortably under `max_total_sources = 10000`**, and T21
found capping at 250 was not an improvement and leaned negative — so more sources is not a known
harm. The number to watch is end-to-end latency against the Thompson speed gate
(`max_response_time_ms = 500`), since 6.2× the sources means 6.2× the scoring work, and scoring is
the CPU-heavy stage rather than the fan-out.

## 🔴 Correction: "substitutes the seed set and leaves the rest unchanged" was wrong

The claim earlier in this document that follow-seeding is a pure seed swap does not survive reading
the aggregation loop. Two things in `compute_author_colikes` are **like-count-dependent**:

```rust
.filter(|(_, like_count)| *like_count >= min_author_likes as f64)   // MIN_AUTHOR_LIKES = 2
let weight = user_like_count.sqrt();                                // the aggregation weight
```

A follow carries no like count. So a naive seed swap yields **every followed author filtered out**
(score absent or 0, below 2), and weight `sqrt(0) = 0` for anything that slipped through. The fan-out
and the scorer are genuinely reusable; the **filter and the weight are not**, and they need to be
designed rather than inherited.

### Recommended weighting, and why

Uniform weight 1.0 is the obvious placeholder but throws away the only affinity signal available.
The principled option is **inverse author popularity** — a follow of a niche account says far more
about a user than a follow of a huge one, which is the same intuition as LinkLonk's fairness term
`1/|items source upvoted|` that production already relies on to stop prolific upvoters dominating.

Conveniently the fan-out already reads each author's liker-set size, so `1 / sqrt(|authl|)` costs
nothing extra. **This is a design decision that needs its own validation, not an assumption** — it is
exactly the kind of unvalidated modelling choice that made the T17 "ranking signal is noise" claim
wrong (it was measuring the harness, not production) and the T7 inverted-lookup cost argument
collapse.

## Revised build order

1. **Follow fetch + cache**, TTL well beyond the experiment horizon. Follows are far more stable than
   likes, so a long TTL is both safe and cheap — and the durable-profile silent expiry is the
   specification for getting this wrong.
2. **A follow-specific filter and weight** in the author-affinity seed step. Start with uniform, ship
   inverse-popularity behind a flag, and let the randomized experiment choose.
3. **Latency pre-flight before exposing anyone:** score a follow-seeded user end-to-end and check the
   500 ms Thompson gate at 2,530 sources. Cheap, and it is the one number this measurement did not
   settle.
4. **Then** the randomized experiment, with the holdout window reset in the same change.
