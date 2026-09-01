# Handoff: personalization coverage work — 2026-09-01

Written for whoever picks this up on the new Mac Mini. You have the same repo; this explains the
live state, what I was in the middle of, what to do first, and the traps that cost me six bugs.

**Read `~/.claude/projects/-Users-dgaff-Code-graze/memory/personalization-holdout-experiment.md`
first.** It is the running ledger and is more current than `THEORIES-personalization.md` (which
stops at 2026-08-17). Also useful: `DESIGN-coverage-next-lever-2026-08.md` for how we got here, and
`memory/pool-cache-and-gate-breach.md` for the latency work.

---

## 1. One-paragraph summary

The personalization engine seeds exclusively from a user's recent *likes*. Measured over a full
diurnal cycle, `no_user_data` is **98.1% of addressable non-personalization**, and **73.6% of the
users it turns away have zero likes in 365 days** — so nothing built from like history can reach
them. But **86% of them follow 10+ accounts (median 50)**. We built follow-graph seeding, shipped it
behind a randomized experiment, and it is now **delivering**: `no_user_data` drops from ~12.3% to
~7.5% and personalized coverage rises ~10pp in the treated arm. The *value* question (does it raise
engagement?) is still unresolved and needs weeks. As of 2026-08-31 16:49:48Z the experiment
enrolment is **eligibility-gated**, which should roughly double its statistical resolution.

---

## 2. Machine setup on the new Mini

Everything that matters runs **in the DigitalOcean cluster**, not locally. The Mac is only a control
plane. You need:

| tool | why | check |
|---|---|---|
| `kubectl` + DO context | all jobs run in-cluster | `kubectl config get-contexts` should list `do-sfo3-k8s-1-31-1-do-4-sfo3-1731769323576` and it must be **current** |
| `doctl` (authenticated) | registry tags/digests, DOCR pull secret | `doctl account get` |
| `gh` (authenticated) | PRs, and triggering image builds | `gh auth status` |
| `docker` | **only** for the small Python analysis image | see §7 warning |

**Do NOT build the Rust image locally.** Neither Dockerfile has cargo cache mounts, so every build is
a full cold compile under amd64 emulation on an arm64 Mac: ~15 min and load average ~9. That is what
made the old Mini run hot. Use GitHub Actions instead:

```bash
gh workflow run docker-release.yml --ref main
```

Native amd64 runners, no emulation, pushes to DOCR. Requires repo secret
`DIGITALOCEAN_ACCESS_TOKEN` (already set). **Deploy by digest** — it publishes `latest` and a short
SHA, both mutable.

The cluster needs no setup; it already has `dockerhub-secret`, `docr-graze-social-labs`,
`app-secrets`, `personalization-secrets` and the `personalization-env` ConfigMap.

---

## 3. What is running right now

### Service
```
personalization-api  registry.digitalocean.com/graze-social-labs/personalization
                     @sha256:11b4082b36119a155a771b70c0ff0f9bb6142738120c6833543f605af1a79792
                     (main 44e4673)  3 replicas
```
Also deployed, **not** on the current image and deliberately untouched:
`personalization-candidate-sync` → `dgaff/personalization@sha256:2fc398a9…`,
`personalization-like-streamer` → `dgaff/personalization:latestrs`.

### Probes (added 2026-08-27, and read §8 before touching them)
- `startupProbe` → `/internal/ready` (Redis-dependent), 3s × 40
- `readinessProbe` → `/internal/alive` (static 200), 10s, 3 failures
- **Never** put `/internal/ready` on the readinessProbe. All replicas share one Valkey, so a blip
  fails readiness everywhere at once and empties the Service.

### Env flags that matter
```
FOLLOW_SEED_EXPERIMENT_ENABLED=1      FOLLOW_SEED_EXPERIMENT_TRAFFIC_PCT=100
FOLLOW_SEED_EXPERIMENT_SALT=v1        FOLLOW_SEED_WEIGHT_MODE=uniform
PERSONALIZATION_HOLDOUT_RATE=0.20     AB_EXPERIMENT_ENABLED=0        (max_sources ended)
THOMPSON_PERSIST_ENABLED=1            AUTHOR_AFFINITY_ENABLED=false  (see §8, trap 3)
DURABLE_PROFILE_SHADOW_MODE=1         (durable profiles: built, dormant, ucl: keys EXPIRED)
```
`POOL_CACHE_TTL_SECONDS` is unset → default 30s. `=0` disables the pool cache with no rebuild.

### CronJobs (all in `default`, manifests in `analysis/kube/`)
| name | schedule (UTC) | what it does |
|---|---|---|
| `personalization-analysis` | `17 * * * *` | contract check, then the three experiment readouts, then the density drift guard |
| `personalization-coverage` | `15 1 * * *` | why personalization did not happen, per cause, rolling 24h |
| `personalization-dose-check` | `7 15 * * *` | **is the follow-seed treatment being delivered** |
| `personalization-follow-seeds` | `23 4 * * *` | writes `uf:{hash}` seeds for newly-unreachable users |

Analysis image: `dgaff/graze-analysis@sha256:573861b44a0456467695ff0842c1bf2606d2889e6380410d4aaa400e39c58264`

---

## 4. The experiment

Three specs in `analysis/experiments/` are scheduled. Arms come from the base64 provenance blob on
every impression (`interaction_feed_context` in `default.feed_interactions`).

| spec | arm field | window start | status |
|---|---|---|---|
| `personalization_holdout` | `is_personalization_holdout` | 2026-08-28T13:41:26Z | +328% but **INCONCLUSIVE** (WLS p≈0.09, permutation p≈0.25) |
| `follow_seed` | `follow_seed` (top level) | **2026-08-31T16:49:48Z** | just restarted, eligibility-gated |
| `max_sources_250_vs_10000` | `params.max_total_sources` | 2026-08-12 (**ended**) | frozen by an `end:` clause; −25.4%, p=0.21 |

Two dormant specs (`interleave_*`) reference a `ranker` field nothing writes. The contract check
correctly flags them; they are **not scheduled**, so that is expected, not a regression.

### Secondary outcomes (both added because `like_rate` is starved — ~1,065 likes vs ~31,000 impressions)
- **winsorized scroll depth** at p90. Raw impressions/user has an MDE of 31% of the mean; p90 takes
  it to ~14%. The quantile is declared in the spec and **must not be tuned after seeing a result**.
- **`page_id`** in the provenance (added 2026-08-30 ~11:55 UTC) makes pages countable:
  `uniqExact(page_id)` per user. It replaced `depth = 0`, which missed **54.2% of users** because
  clients report only what they *saw*. Any page query needs `occurred >= '2026-08-30 11:55:00'`.

---

## 5. DO THIS FIRST

```bash
kubectl logs job/$(kubectl get jobs -o name | grep personalization-dose-check | tail -1)
```

The dose check gates everything. It prints `no_user_data` and personalized share by arm and
self-labels `UNDERPOWERED` below 500 impressions in the smaller arm.

- **`DELIVERED`** → the treatment is reaching people; the primary readouts are worth reading.
- **`NOT DELIVERED`** → *stop*. A null `like_rate` under this condition means **undelivered**, not
  "follows do not help". Check: (a) `personalization-follow-seeds` ran at 04:23Z, (b) `uf:` keys
  exist and have not hit their 30d TTL, (c) `FOLLOW_SEED_EXPERIMENT_ENABLED` is still `1`.
- **`UNDERPOWERED`** → say so and read nothing into the direction.

Then the hourly readout:
```bash
kubectl logs $(kubectl get pods --sort-by=.metadata.creationTimestamp -o name \
  | grep personalization-analysis | tail -1)
```

---

## 6. What I was doing, and what is next

I had just shipped **eligibility-gated enrolment** (PR #52) and reset the `follow_seed` window
(PR #53) about three hours before handoff. Nothing was mid-flight; the repo is clean on `main` and
everything is deployed and verified.

**Why:** enrolment was `traffic_pct=100` over all non-holdout users, but only **276 of 1,031 treated
users had a `uf:` seed**. The other 755 already had like seeds, so the follow hook never ran for them
— 73% of the treatment arm was untreatable, diluting the ITT ~3.7×. Restricting enrolment trades N
(~2,062 → ~749 units) for effect size and should net **~2.2× on z**.

**That prediction is falsifiable and is the first thing worth testing.** If the concentrated window
does not move faster than the diluted one did, my dilution analysis was wrong.

### Next steps, in order

1. **Wait ~3–5 days**, then read the dose check and the `follow_seed` readout. Do not read it sooner;
   this experiment has a history of premature conclusions.
2. **Watch the negative secondary trend.** Before the reset, all three dense secondaries leaned
   negative for `follow_seed` — scroll depth −6.3% (p=0.18), pages −0.20 (p=0.40), raw impressions
   lower — while the **holdout's** scroll depth was **+11.3% (p=0.069)**, i.e. positive. If that
   persists on the concentrated population it is the real finding: we may be reaching more users with
   content they engage with *less*. Fewer pages could be "satisfied sooner" or "content is worse" and
   scroll depth cannot distinguish them. **Do not resolve this by picking the flattering reading.**
3. **If the holdout still has not resolved in a week**, consider `PERSONALIZATION_HOLDOUT_RATE`
   0.20 → 0.50. Buys a 21% narrower interval at the same elapsed time; costs suppressing
   personalization for half of users. It is an env edit; it **resets the holdout window**.
4. **Only after the value question resolves**, revisit ranking quality (Stages C/D). The 2026-08-14
   leverage math still stands: personalized items are ~30% of impressions in our feeds, our feeds are
   ~1% of Graze interaction volume, so a 10% better *ranker* moves ~0.03% of Graze engagement. That
   is arithmetic, not a p-value, and it is why ranking work has stayed shelved.
5. **`FOLLOW_SEED_WEIGHT_MODE=inverse_popularity`** is implemented and untested in production. Read
   the caveat in the enum first: the popularity proxy is the fan-out's liker list, capped at
   `AUTHOR_AFFINITY_MAX_LIKERS_PER_AUTHOR=100`, so every author at or above the cap gets an identical
   weight and the mode only discriminates below it. **A null on that mode means the signal was
   compressed, not that the idea is wrong.**

---

## 7. Standing rules, earned the hard way

Six bugs in this feature. **Every single one had the same shape: a producer changed and a consumer
did not.** And each was found by asking *why a log line was absent* — never by reading the call path,
which convinced me four separate times and was wrong four separate times.

1. **Prove a flag engages with a log line before believing it works.** Not by tracing the code.
2. **Test that a value reaches its consumer**, not merely that it is computed correctly. My arm
   assignment had six passing tests — determinism, split balance, salt independence, ramp stability —
   and not one checked that the assignment reached the code it was supposed to affect.
3. **When a provenance field moves, grep every consumer**: spec YAMLs, inline CronJob scripts, ad-hoc
   queries. `analysis/graze_analysis/contract.py` now runs first in the hourly job and fails loudly
   if a scheduled spec reads a field nothing writes — because that failure is otherwise *silent*
   (zero rows → `WITHHELD`, indistinguishable from "not enough data yet"). One such bug hid for 24
   hours while the readout confidently said `NOT DELIVERED`.
4. **An absent JSON field extracts as `false`.** Any spec with a boolean arm needs a `population`
   clause requiring the key to *exist*, or every unenrolled and pre-change row lands in the control
   arm.
5. **Assert your edits landed.** Five consecutive memory updates silently no-op'd because `.replace()`
   targets had drifted while the script printed success unconditionally. Prefer append; verify with
   grep.
6. **Confirm a PR says `MERGED` before deleting its branch.** `git push origin --delete <branch>`
   *closes* an open PR. That put a fix in the cluster but not on `main`, so anyone applying the repo
   manifest would have reverted it.

### The four blockers that made the first flip measure nothing
Worth understanding, because the same shapes will recur:
1. The arm was threaded through `apply_thompson_params` — exported and **never called**. The serving
   path uses `merge_params`.
2. The arm was nested under `params`, written only on the personalization-*success* path, so all
   2,129 `no_user_data` rows carried no arm — excluding exactly the rows the treatment targets.
3. The read path hung off the author-affinity branch, and `AUTHOR_AFFINITY_ENABLED=false`. It is
   now attached at the `coliker_weights.is_empty()` hook, which is purpose-built for additive
   fallbacks and says so in its own comment.
4. **The pre-personalization seed gate turned these users away before `compute_personalization` ran.**
   This is also why **durable co-liker profiles have served nothing since 2026-08-11** — recorded as
   "0 shadow firings" and misread as low traffic. Any mechanism at that hook is dead until the gate
   admits the user.

---

## 8. Dead ends — do not retry these

- **Widening `FOLLOW_SEED_LOOKBACK_DAYS`.** 7d, 14d, 30d and 60d all return the same **931**
  targetable users, because `fallback_reason` only exists since 2026-08-26 and the lookback cannot
  reach past its own instrumentation. 749 are already seeded. This improves on its own as
  instrumented history accumulates.
- **Richer outcome tables for power.** `user_action_logs` has **82× more events** on our algos
  (691,289 vs 8,345) but **only 0.9% of those users appear in our provenance**, so only they can be
  assigned an arm. We serve ~1% of Graze's interaction volume; the events belong to other serving
  paths. Power cannot be bought this way.
- **Widening the engagement metric with clickthroughs.** There are **zero** clickthrough events in
  this dataset. A composite buys 1.09×.
- **Durable co-liker profiles as the coverage lever.** Ceiling is **3.7%** — of 352 users turned
  away, only 13 have the ≥20 likes/365d a profile needs, and 73.6% have none at all. Also the `ucl:`
  keys **expired** (7d TTL, built 8/11). Shelved, not deleted.
- **`interaction_request_id` / `impression_id`** as page counters. Both are degenerate: exactly one
  distinct value across 31,188 rows. `feed_interactions` is shared and nothing in this service writes
  them. That is why `page_id` went in the provenance blob instead.

---

## 9. Rollbacks

| what | how |
|---|---|
| follow-seed experiment off | `kubectl set env deploy/personalization-api FOLLOW_SEED_EXPERIMENT_ENABLED=0` — env only, no rebuild |
| follow-seed read path off | `FOLLOW_SEED_READ_ENABLED=0` (already the default; the arm overrides it) |
| pool cache off | `POOL_CACHE_TTL_SECONDS=0` — restores per-request `SMEMBERS`, no rebuild |
| previous API image | `sha256:e66e98172cbc95447137bf08fb6efa92c55376d5e2670a903b5db34488ba23a6` (pre-eligibility-gating) |
| earlier known-good | `sha256:d0ee3323fe8951bf7d8ebc325eddfada8f5800ac6ac92766721e5a541a690016` (gate fix), `sha256:2fc398a91fd0…` (Docker Hub, pre-DOCR) |

Standing rollout rule for this service: **surge-first**, `maxSurge=1 / maxUnavailable=0`, deploy by
digest, verify with `kubectl rollout status` and then a log check.

---

## 10. Judgment calls I made that you may want to revisit

- **I did not reset the holdout window** for the eligibility-gating change, though a strict reading
  argues for it. It touches only the ~749 eligible users, a small slice of the holdout's treated arm,
  and that window is finally accruing signal with p *falling* as units grow rather than rising. A
  fourth reset would cost more than the contamination it removes. The reasoning is written into
  `personalization_holdout.yaml` itself — disagree with it on the record if you like.
- **The `follow_seed` estimand changed** with the reset, from "does this help everyone" to "does this
  help the users it can reach". I think the second is the better question; it is nonetheless a
  different one, and it is the fifth start for that window.
- **`interleave_self_check` was dropped from the hourly readout** (7 tagged impressions against a 100
  minimum — it only ever printed `WITHHELD`). The harness remains for when interleaving is actually
  exposed. **Interleaving is the untapped lever for ranker comparisons**: within-user designs need no
  additional users, which is the binding constraint here.

---

## 11. Housekeeping

- `personalization-follow-seeds` is pinned to `sha256:c7b9af9c…`, an older image. The
  `graze-build-follow-seeds` binary has not changed so it works correctly, but it is drift — bump it
  next time you deploy.
- `kube/api-deployment.yaml` still declares `image: dgaff/graze-personalization:latest` while the live
  deployment runs a DOCR digest. **Patch the live deployment; do not apply that file.**
- The `requestLess` relevance guardrail is at **n=26** events. Effectively inert. A quiet guardrail
  there proves nothing about relevance, and it should not be read as reassurance.
- `8080/metrics` returns **401** (it is behind the admin key). The unauthenticated metrics server is
  on **8081**, and the live deployment carries no Prometheus scrape annotation at all — consistent
  with `/metrics` having been recorded as dead.

---

## 12. Useful one-liners

```bash
# dose check (run this first)
kubectl logs job/$(kubectl get jobs -o name | grep personalization-dose-check | tail -1)

# hourly readout: contract check, three experiments, drift guard
kubectl logs $(kubectl get pods --sort-by=.metadata.creationTimestamp -o name \
  | grep personalization-analysis | tail -1)

# coverage decomposition (why personalization did not happen)
kubectl logs $(kubectl get pods --sort-by=.metadata.creationTimestamp -o name \
  | grep personalization-coverage | tail -1)

# force any CronJob to run now
kubectl create job adhoc-$RANDOM --from=cronjob/personalization-dose-check

# is the follow-seed path engaging? (behavioural proof, not code reading)
for p in $(kubectl get pods -l app=personalization-api -o name); do
  kubectl logs $p --since=30m | grep -c follow_seed_fallback_engaged; done

# build + deploy an API change (never build Rust locally)
gh workflow run docker-release.yml --ref main
doctl registry repository list-tags personalization | head -3
kubectl set image deploy/personalization-api personalization-api=<DOCR>@sha256:<digest>
kubectl rollout status deploy/personalization-api

# ad-hoc ClickHouse/Valkey query: run it IN-CLUSTER, creds are k8s secrets only.
# Pattern: ConfigMap with a .py file + a Job on dgaff/graze-analysis, envFrom
# personalization-env, app-secrets, personalization-secrets (in that order — the
# later secret's REDIS_URL wins, and it is the one the service reads).
```
