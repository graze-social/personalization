# Personalization: SOTA review and four competing directions

Research synthesis, 2026-08-13. Written against the measured particularities of Graze's engine
documented in `THEORIES-personalization.md`.

---

## Part 0 — The constraint that determines everything

From the live A/B: within-user clustering inflates variance **6.2×** (naive pooled z=3.59 vs
cluster-correct permutation z≈1.44). Combined with ~3,600 usable impressions/day, capacity is:

| effect size | time to detect (80% power) |
|---|---|
| 50% | 9 days |
| 30% | 22 days |
| 20% | **48 days** |
| 10% | **6 months** |

**~4–8 well-powered live experiments per year.** Any plan that requires testing many algorithmic
designs is dead on arrival unless measurement sensitivity improves first. This inverts the intuitive
priority: **the highest-leverage interventions are not algorithmic.**

---

## Part 1 — Measurement interventions (do these first; they are prerequisites)

### 1.1 Interleaving — the single biggest lever (50–100×)

Instead of splitting users between rankers, blend both rankers' outputs into **one** feed and measure
which ranker's items win clicks/likes. Each user becomes their own control, which removes
between-user variance — the exact 6.2× penalty we measured.

Reported results:
- [Netflix](https://netflixtechblog.com/interleaving-in-online-experiments-at-netflix-a04ee392ec55):
  sensitivity up to **100×** vs A/B.
- [Airbnb](https://arxiv.org/abs/2508.00751): **~50× speedup**; reached the same conclusion as the
  corresponding A/B using **0.5% of the running time and 4% of the traffic**. Interleaving and A/B
  agreed directionally **82%** of the time (correlation ~0.60).
- Airbnb's design: **team-draft with "competitive pairs"** — one coin flip at the start, then draft
  the next item from each ranker; if the two differ, they form a competitive pair, if identical the
  item is added unassigned. Per-user statistic `τᵢ = wins(treatment) − wins(control)`, aggregated as
  a proportion test over users.

Applied to our 48-day test → **roughly a day.** This alone converts the meta-engine question from
"impossible" to "tractable."

**Three caveats, and one bites us specifically.** Airbnb documents that interleaving fails for
(a) **set-level objectives such as diversity**, (b) results reused by other surfaces, (c) continuous
metrics. **Graze applies a diversity re-rank** (`diversify_posts`), so interleaving must operate
*before* diversification, and any diversity-affecting change must still go to a full A/B. Their
observed failure mode is worth remembering: a treatment looked *worse* under interleaving than in the
subsequent A/B because of comparative advantage from control's higher base rate.

### 1.2 Counterfactual evaluation from controlled randomization (up to 100×)

Airbnb's second technique, and arguably a better fit for us than interleaving because it does not
touch what the user sees: generate **both** rankings for each request, randomly serve one, and use
the unserved one to construct an estimator. Notably they **do not use inverse propensity weighting** —
the randomization itself provides unbiasedness. Two estimators, averaged into one OEC:
- **Direct decomposition** — split outcomes into items ranked similarly in both (within α positions)
  vs differently, and reweight: `τ_decomp = τ_diff + θ·τ_sim`.
- **Position-decay reward** — `f(r) = γ^(−r)`, gain `g_i = 1 − γ^max(|r_diff| − α, 0)`.

Reported **15×–100×** speedup depending on estimator (the reward-based one hitting ~100×).

For us this is attractive because it is *offline after the fact*: we already log full provenance in
`feedContext`. We would only need to log the alternative ranking.

### 1.3 CUPED (~50% variance reduction)

Regress out each user's pre-experiment behaviour.
[Deng et al.](https://dl.acm.org/doi/abs/10.1145/2433396.2433413) report **~50% variance reduction**
at Bing — same power with half the users or half the duration. Best covariate is the same metric
measured pre-period; adding more covariates gains only 2–3% more. A
[KDD 2025 marketplace study](https://kdd.org/kdd2025/wp-content/uploads/2025/07/CameraReady-05.pdf)
reports a more modest 21% CI reduction, so treat 50% as an upper bound.

### 1.4 Always-valid inference (fixes a mistake I already made)

I peeked at the running experiment three times in one day. Under fixed-horizon tests that inflates
Type-I error. [Johari et al.](https://arxiv.org/abs/1512.04922) define always-valid p-values and
confidence sequences that stay valid under continuous monitoring — now standard in commercial
platforms. Any automated engine peeks constantly, so this is not optional for it.

### 1.5 Raise the denominator

Only ~15% of requests attempt personalization; the rest are cached or gated, and carry no provenance
params. Every percentage point of coverage here is a direct multiplier on experiment throughput.

---

## Part 2 — Four competing algorithmic directions

Each is a genuinely different bet. Ordered by expected value per unit of effort, not by ambition.

### Approach A — Sampled random walk (Pixie-style), replacing exhaustive enumeration

**The bet:** our cost/bias dilemma is an artifact of *enumerating* the graph, and disappears if we
*sample* it.

We measured both enumeration directions and both are bad: post-first truncates each candidate's liker
list to the 30 most recent (systematic bias — 72–100% of overlapping candidates have >30 likers),
while co-liker-first collapses when co-liker sets reach 5,157 (26× slower at K>256, and the shipped
version measured 0.94× coverage).

[Pixie](https://arxiv.org/abs/1711.07601) resolves exactly this. It estimates Personalized PageRank
with many short biased random walks — order 100,000 steps — over a 3B-node/17B-edge graph at **60 ms
p99**, and reports **up to 50% higher engagement** than the prior system. Properties that map onto our
measured failures:

| our problem | Pixie mechanism |
|---|---|
| truncation bias from `max_likers_per_post` | walk visits nodes in proportion to true transition probability; no per-node cap |
| cost explodes with co-liker count | fixed step budget regardless of graph size |
| `paths_boost` is ad-hoc, `overlap_count` ≈ 1.0 | **multi-hit booster** boosts items reached from multiple seeds *while discounting popular items* |
| 3.8 s latency outliers observed | **early stopping** once enough candidates are visited enough times |
| heavy seeds dominate | walk budget allocated per seed as a function of degree |

**Fit to our facet:** count only visits that land in `ap:{algo}`, or bias edges toward pool members.
Our `ul:`/`pl:` keys already *are* the bipartite user↔post graph, so no new data structure is needed —
this is a rewrite of the traversal, not of the storage.

**Cost:** medium. New traversal in `graze-api`, tunable step budget, needs the early-stopping
criterion to hold latency.
**Risk:** sampling variance for users with 1–2 likes (our newly-admitted segment). Mitigate by falling
back to exhaustive for tiny seeds, where it is cheap anyway.
**Why promising:** it is the production-hardened form of the algorithm we already run, and it fixes
the two specific defects we measured rather than trading one for the other.

### Approach B — Facet at Step 1, not Step 3 (LinkLonk's own answer)

**The bet:** we are faceting in the wrong place, and that single choice causes the 34–56% dead-slot
rate.

LinkLonk restricts **Step 1** — "if you filter to a channel, Step 1 goes only into items you put into
that channel." Graze instead computes one generic co-liker set per user and restricts the **output**
to a feed's pool. We measured the consequence: **34–56% of a user's top-128 co-likers contribute zero
coverage on any given feed**, and 0% of them were inactive — they are active people who like
different things.

**Implementation:** seed only from the user's likes that are relevant to *this* feed — e.g. likes on
posts whose authors appear in the feed's pool, or that match the feed's topical filter. Co-likers then
arrive feed-relevant by construction.

**Cost:** low-medium algorithmically, but it changes the cache unit from per-user to per-(user, feed),
multiplying key count by feeds-read-per-user (measured `feeds/user = 1.0` in a one-hour window, which
suggests the multiplier is small in practice — worth confirming over a longer window).
**Risk:** users with few in-topic likes get a smaller seed, and we know coverage is not seed-limited,
so this may be fine — but it is the direct test of that.
**Why promising:** cheapest fix for the largest measured inefficiency, and faithful to the algorithm
that demonstrably works in production.

### Approach C — Tuned classical item-item model (EASE / item-kNN) as a second candidate source

**The bet:** before building anything neural, the strongest realistic competitor is a well-tuned
classical model — and it may simply win.

[Dacrema et al.](https://dl.acm.org/doi/10.1145/3298689.3347058) reproduced 12 neural recommenders and
found **11 of 12 were outperformed by conceptually simple methods** once baselines were properly
tuned; with linear models included, **only 1 of 12** was clearly better. Their
[follow-up](https://arxiv.org/abs/1911.07698) found only 12 of 26 algorithms reproducible at all, and
named the pattern **"phantom progress."** A
[2025 study of diffusion recommenders](https://arxiv.org/html/2505.09364v3) reports the same thing
again.

Given we can run ~4–8 experiments a year, spending one on a neural model that the literature says
probably loses to a tuned baseline is a poor bet. **Test the baseline first.**

**Fit:** item-item similarity precomputed offline from 141M like edges, served as O(1) lookups;
restrict to the pool trivially; no per-request graph traversal at all. Directly attacks the sparsity
wall from the other side — item-item co-occurrence pools evidence across users, so a post with 1 like
still inherits similarity from that liker's other items.
**Cost:** low at serve time, moderate offline (a nightly job, like the durable-profile builder).
**Risk:** item-item recency — our candidates are ≤72h old, so freshly-created posts have no
co-occurrence yet. Likely needs an author-level or embedding-level backoff for new posts.
**Why promising:** best evidence-to-effort ratio in the entire literature, and it composes with A and
B rather than competing operationally.

### Approach D — Two-tower / graph-embedding retrieval, as an additional retrieval source

**The bet:** the only way to escape the sparsity wall entirely is dense scoring where *every*
candidate gets a score.

This remains the structurally correct answer to our hardest measured fact: **87% of candidates have
zero likers** and the median pool post has **1 like**. No traversal of a sparse graph fixes that; a
dense representation does, because similarity is defined for every pair.

Ingredients we already have: 141M like edges for training, plus content embeddings already computed
and archived by the enrichment pipeline. Two-tower is the standard architecture for exactly this
retrieval role
([survey](https://dl.acm.org/doi/pdf/10.1145/3771925),
[Pinterest multi-embedding retrieval](https://arxiv.org/pdf/2506.23060)).

**Cost:** high — training pipeline, ANN index, embedding refresh, plus much harder debugging and
auditability (which matters here; the Debug/audit surface exists and users can inspect provenance).
**Risk:** the Dacrema critique applies directly; and per-feed faceting requires either per-feed ANN
indexes or post-filtering a global index, which can starve narrow feeds.
**Why still worth doing eventually:** it is the only candidate that raises the ceiling rather than
recovering losses, and it composes as an *additional* retrieval source rather than a replacement.

### Honourable mention — use the negative signal we already collect

LinkLonk explicitly uses downvotes: "items you downvoted create a path that detracts from the paths in
the final sum." We collect `app.bsky.feed.defs#requestLess` (121,419 events) and **do not use it**.
Adding it as a negative path is faithful to the source algorithm, cheap, and untested.

---

## Part 3 — Suggested sequence

1. **Interleaving harness** (pre-diversity), validated against the running A/B. Converts 48-day tests
   into ~1-day tests. Everything else depends on this.
2. **CUPED + always-valid inference** in the analysis layer. Cheap, compounding, and fixes the peeking
   error I already made.
3. **Approach B** (facet at Step 1) — smallest change addressing the largest measured waste.
4. **Approach C** (tuned item-item) — highest evidence-to-effort, and becomes the honest baseline that
   any future neural work must beat.
5. **Approach A** (sampled walk) — once measurement is fast, this is the principled fix to the
   cost/bias dilemma and the path to a latency guarantee.
6. **Approach D** (two-tower) — only after 1–2 are in place, and only measured against a *tuned* C.

The through-line: we have spent this session discovering that our offline estimates were
systematically optimistic and our online tests are power-starved. Fixing the second makes the first
irrelevant, because we can simply try things.

---

## Sources

- [Netflix — Interleaving in Online Experiments](https://netflixtechblog.com/interleaving-in-online-experiments-at-netflix-a04ee392ec55)
- [Airbnb — Harnessing Interleaving and Counterfactual Evaluation for Search Ranking (2025)](https://arxiv.org/abs/2508.00751)
- [Airbnb Engineering — Beyond A/B Test](https://medium.com/airbnb-engineering/beyond-a-b-test-speeding-up-airbnb-search-ranking-experimentation-through-interleaving-7087afa09c8e)
- [Chapelle et al. — Large-Scale Validation and Analysis of Interleaved Search Evaluation](https://www.cs.cornell.edu/people/tj/publications/chapelle_etal_12a.pdf)
- [Deng et al. — CUPED](https://dl.acm.org/doi/abs/10.1145/2433396.2433413)
- [KDD 2025 — Variance Reduction in Online Marketplace A/B Testing](https://kdd.org/kdd2025/wp-content/uploads/2025/07/CameraReady-05.pdf)
- [Johari et al. — Always Valid Inference](https://arxiv.org/abs/1512.04922)
- [Anytime-Valid Confidence Sequences in an Enterprise A/B Testing Platform](https://arxiv.org/pdf/2302.10108)
- [Eksombatchai et al. — Pixie (WWW'18)](https://arxiv.org/abs/1711.07601)
- [Pinterest Engineering — Introducing Pixie](https://medium.com/pinterest-engineering/introducing-pixie-an-advanced-graph-based-recommendation-system-e7b4229b664b)
- [Dacrema et al. — Are We Really Making Much Progress? (RecSys'19)](https://dl.acm.org/doi/10.1145/3298689.3347058)
- [Dacrema et al. — A Troubling Analysis of Reproducibility and Progress](https://arxiv.org/abs/1911.07698)
- [Diffusion Recommender Models and the Illusion of Progress (2025)](https://arxiv.org/html/2505.09364v3)
- [A Comprehensive Survey on Retrieval Methods in Recommender Systems](https://dl.acm.org/doi/pdf/10.1145/3771925)
- [Pinterest — Multi-Embedding Retrieval Framework (KDD'25)](https://arxiv.org/pdf/2506.23060)
