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
