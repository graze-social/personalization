"""Inference that is correct by default.

Every routine here replaces a hand-rolled version that produced a wrong answer during the
motivating investigation:

- :func:`cluster_robust_rate_diff` — a naive pooled z-test on clustered impressions reported
  ``p < 0.001`` where the cluster-correct answer was ``p = 0.15``. Within-user clustering inflates
  variance ~6.2x here, so treating impressions as independent is not a rounding error.
- :func:`permutation_rate_diff` — distribution-free cross-check, shuffling at the unit of
  randomization.
- :func:`cuped_adjust` — variance reduction from pre-period data.
- :func:`always_valid_ci` — the analysis is run on a schedule and looked at whenever, which
  invalidates fixed-horizon p-values. I peeked at one experiment three times in a day.
- :func:`always_valid_diff_ci` — the same protection for the ARM DIFFERENCE, which is the
  quantity actually decided on. The one-sample version had, since the harness was written,
  been applied to a sample pooled across arms — an interval on a level, which for a
  non-negative rate cannot contain zero.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Estimate:
    """A difference estimate with its uncertainty and provenance."""

    diff: float
    #: Relative difference vs the control rate, or ``nan`` when the control rate is zero.
    rel: float
    se: float
    ci_low: float
    ci_high: float
    p_value: float
    method: str
    n_units: int
    n_obs: int

    @property
    def significant(self) -> bool:
        """Significant *and* well-identified.

        A zero or non-finite standard error means the fit is degenerate — every residual vanished,
        or the design matrix was singular — and the p-value that comes back is an artifact, not
        certainty. Observed live: a negative control with a literally identical rate in both arms
        (``diff = 0.0000``) came back ``p = 0.0000`` from statsmodels and withheld a valid result.
        """
        if not math.isfinite(self.se) or self.se <= 0.0:
            return False
        return self.p_value < 0.05

    def describe(self) -> str:
        rel = "n/a" if math.isnan(self.rel) else f"{self.rel:+.1%}"
        return (
            f"{self.method}: diff={self.diff:+.4f} ({rel}) "
            f"95% CI [{self.ci_low:+.4f}, {self.ci_high:+.4f}] "
            f"p={self.p_value:.4f} units={self.n_units} obs={self.n_obs} "
            f"{'SIGNIFICANT' if self.significant else 'not significant'}"
        )


def _rate(num: np.ndarray, den: np.ndarray) -> float:
    d = float(den.sum())
    return float(num.sum()) / d if d else float("nan")


def cluster_robust_rate_diff(
    unit_ids: np.ndarray,
    arm: np.ndarray,
    numerator: np.ndarray,
    denominator: np.ndarray,
) -> Estimate:
    """Difference in rates with standard errors clustered by unit.

    ``arm`` is 0 for control and 1 for treatment. Rows are per-unit aggregates (one row per user)
    or per-observation; either way the SE is clustered on ``unit_ids``, which is what respects the
    fact that one user's impressions are not independent draws.

    Uses statsmodels' cluster-robust covariance when available and falls back to a hand-computed
    sandwich estimator otherwise, so the container can run without statsmodels in a pinch.
    """
    arm = np.asarray(arm, dtype=float)
    num = np.asarray(numerator, dtype=float)
    den = np.asarray(denominator, dtype=float)

    keep = den > 0
    unit_ids, arm, num, den = unit_ids[keep], arm[keep], num[keep], den[keep]
    if arm.size == 0 or len(np.unique(arm)) < 2:
        return Estimate(
            float("nan"), float("nan"), float("nan"), float("nan"), float("nan"),
            1.0, "cluster-robust (insufficient data)", 0, 0,
        )

    r_ctl = _rate(num[arm == 0], den[arm == 0])
    r_trt = _rate(num[arm == 1], den[arm == 1])
    diff = r_trt - r_ctl
    rel = diff / r_ctl if r_ctl else float("nan")

    # Weighted least squares of per-row rate on the arm indicator, weights = denominator, so the
    # point estimate equals the pooled rate difference while the SE accounts for clustering.
    y = num / den
    w = den
    X = np.column_stack([np.ones_like(arm), arm])

    se = float("nan")
    method = "cluster-robust (WLS, clustered SE)"
    try:  # pragma: no cover - exercised in the container image
        import statsmodels.api as sm

        model = sm.WLS(y, X, weights=w).fit(
            cov_type="cluster", cov_kwds={"groups": np.asarray(unit_ids)}
        )
        se = float(model.bse[1])
        p = float(model.pvalues[1])
    except Exception:
        se = _sandwich_se(unit_ids, X, y, w)
        p = _two_sided_p(diff / se) if se and not math.isnan(se) else 1.0
        method = "cluster-robust (WLS, sandwich SE fallback)"

    z = 1.959963984540054
    return Estimate(
        diff=diff,
        rel=rel,
        se=se,
        ci_low=diff - z * se,
        ci_high=diff + z * se,
        p_value=p,
        method=method,
        n_units=int(len(np.unique(unit_ids))),
        n_obs=int(den.sum()),
    )


def _sandwich_se(unit_ids: np.ndarray, X: np.ndarray, y: np.ndarray, w: np.ndarray) -> float:
    """Cluster-robust SE for the slope of a weighted linear fit, computed directly."""
    W = np.diag(w)
    XtWX = X.T @ W @ X
    try:
        bread = np.linalg.inv(XtWX)
    except np.linalg.LinAlgError:  # pragma: no cover
        return float("nan")
    beta = bread @ (X.T @ W @ y)
    resid = y - X @ beta

    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(unit_ids):
        m = unit_ids == g
        u = (X[m] * (w[m] * resid[m])[:, None]).sum(axis=0)
        meat += np.outer(u, u)

    n_g = len(np.unique(unit_ids))
    if n_g < 2:
        return float("nan")
    # Standard small-sample correction for the number of clusters.
    scale = n_g / (n_g - 1)
    cov = bread @ (scale * meat) @ bread
    return float(math.sqrt(max(cov[1, 1], 0.0)))


def _two_sided_p(z: float) -> float:
    if math.isnan(z):
        return 1.0
    return 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2.0))))


def permutation_rate_diff(
    unit_ids: np.ndarray,
    arm: np.ndarray,
    numerator: np.ndarray,
    denominator: np.ndarray,
    n_resamples: int = 20000,
    seed: int = 11,
) -> Estimate:
    """Distribution-free cross-check: shuffle arm labels **at the unit level**.

    Shuffling per observation would destroy the clustering that is the whole problem, so labels are
    permuted per unit and every row belonging to that unit moves with it.
    """
    unit_ids = np.asarray(unit_ids)
    arm = np.asarray(arm, dtype=float)
    num = np.asarray(numerator, dtype=float)
    den = np.asarray(denominator, dtype=float)

    keep = den > 0
    unit_ids, arm, num, den = unit_ids[keep], arm[keep], num[keep], den[keep]

    units, inverse = np.unique(unit_ids, return_inverse=True)
    if units.size < 4 or len(np.unique(arm)) < 2:
        return Estimate(
            float("nan"), float("nan"), float("nan"), float("nan"), float("nan"),
            1.0, "permutation (insufficient units)", int(units.size), int(den.sum()),
        )

    # One label per unit (assignment is per unit by construction).
    unit_arm = np.zeros(units.size)
    for i in range(units.size):
        unit_arm[i] = arm[inverse == i][0]

    def diff_for(labels_by_unit: np.ndarray) -> float:
        row = labels_by_unit[inverse]
        d0, d1 = den[row == 0].sum(), den[row == 1].sum()
        if not d0 or not d1:
            return 0.0
        return num[row == 1].sum() / d1 - num[row == 0].sum() / d0

    observed = diff_for(unit_arm)
    rng = np.random.default_rng(seed)
    shuffled = unit_arm.copy()
    hits = 0
    for _ in range(n_resamples):
        rng.shuffle(shuffled)
        if abs(diff_for(shuffled)) >= abs(observed):
            hits += 1
    p = (hits + 1) / (n_resamples + 1)

    r_ctl = _rate(num[arm == 0], den[arm == 0])
    return Estimate(
        diff=observed,
        rel=observed / r_ctl if r_ctl else float("nan"),
        se=float("nan"),
        ci_low=float("nan"),
        ci_high=float("nan"),
        p_value=p,
        method=f"permutation (unit-level, {n_resamples} resamples)",
        n_units=int(units.size),
        n_obs=int(den.sum()),
    )


def cuped_adjust(y: np.ndarray, covariate: np.ndarray) -> tuple[np.ndarray, float]:
    """Return CUPED-adjusted outcomes and the achieved variance reduction.

    ``y_adj = y - theta * (x - mean(x))`` with ``theta = cov(y, x) / var(x)``. The adjustment is
    unbiased for any theta, so a poorly-correlated covariate costs nothing but buys nothing.
    Reported reductions run ~50% (Bing) down to ~21% (a 2025 marketplace study).
    """
    y = np.asarray(y, dtype=float)
    x = np.asarray(covariate, dtype=float)
    if y.size < 3 or x.size != y.size:
        return y, 0.0
    var_x = float(np.var(x))
    if var_x <= 0:
        return y, 0.0
    theta = float(np.cov(y, x, bias=True)[0, 1] / var_x)
    y_adj = y - theta * (x - float(np.mean(x)))
    v0, v1 = float(np.var(y)), float(np.var(y_adj))
    reduction = (v0 - v1) / v0 if v0 > 0 else 0.0
    return y_adj, max(reduction, 0.0)


def _normal_mixture_radius(var_estimator: float, t: int, alpha: float, rho: float) -> float:
    """Half-width of a normal-mixture confidence sequence for an estimator of known variance.

    Factored out so the one-sample and two-sample sequences cannot drift apart: both boundaries
    are this function, differing only in the variance they hand it. Standard normal-mixture form
    (Howard et al., *Time-uniform Chernoff bounds*), with ``t`` observations accrued:

        radius = sqrt( V * 2 * (t*rho + 1) / (t*rho) * ln( sqrt(t*rho + 1) / alpha ) )

    For a single mean, ``V = s2/t``, which recovers the original
    ``s2 * 2 * (t*rho + 1) / (t^2 * rho) * ln(...)`` exactly.
    """
    inner = math.sqrt(t * rho + 1.0) / alpha
    return math.sqrt(var_estimator * 2.0 * (t * rho + 1.0) / (t * rho) * math.log(inner))


def always_valid_ci(
    values: np.ndarray, alpha: float = 0.05, rho: float = 1.0
) -> tuple[float, float, float]:
    """Normal-mixture confidence sequence for a mean — valid under continuous monitoring.

    Returns ``(estimate, low, high)``. Unlike a fixed-horizon interval this may be inspected at any
    time, any number of times, without inflating the error rate; the price is a wider interval.

    ⚠️ This is a sequence for ONE mean. Handing it a sample pooled across both arms yields an
    interval on the *level*, which for a non-negative rate excludes zero no matter what the
    treatment did — it is not evidence of an effect. For that, use :func:`always_valid_diff_ci`.
    """
    v = np.asarray(values, dtype=float)
    t = v.size
    if t < 2:
        return (float(v.mean()) if t else float("nan"), float("-inf"), float("inf"))
    mean = float(v.mean())
    s2 = float(np.var(v, ddof=1))
    if s2 <= 0:
        return mean, mean, mean
    radius = _normal_mixture_radius(s2 / t, t, alpha, rho)
    return mean, mean - radius, mean + radius


@dataclass(frozen=True)
class SequentialEstimate:
    """An arm difference bounded by a confidence sequence, safe to read at any time."""

    diff: float
    rel: float
    low: float
    high: float
    n_control: int
    n_treatment: int
    variance_reduction: float = 0.0
    #: What the difference is *of*. Named because this class is used for a like RATE and for a
    #: winsorized impression COUNT, and printing "per-unit rate" over a count would misdescribe it.
    estimand: str = "unweighted per-unit rate"

    @property
    def conclusive(self) -> bool:
        """True when the sequence has separated from zero and the sign is settled."""
        if not (math.isfinite(self.low) and math.isfinite(self.high)):
            return False
        return self.low > 0.0 or self.high < 0.0

    def describe(self) -> str:
        rel = "n/a" if math.isnan(self.rel) else f"{self.rel:+.1%}"
        cuped = (
            f" cuped={self.variance_reduction:.1%}" if self.variance_reduction > 0 else ""
        )
        return (
            f"anytime-valid on the arm DIFFERENCE ({self.estimand}): "
            f"diff={self.diff:+.4f} ({rel}) CI [{self.low:+.4f}, {self.high:+.4f}] "
            f"units={self.n_control + self.n_treatment}{cuped} "
            f"{'SEPARATED FROM ZERO' if self.conclusive else 'includes zero'}"
        )


def always_valid_diff_ci(
    control: np.ndarray,
    treatment: np.ndarray,
    alpha: float = 0.05,
    rho: float = 1.0,
    variance_reduction: float = 0.0,
    estimand: str = "unweighted per-unit rate",
) -> SequentialEstimate:
    """Confidence sequence for the DIFFERENCE of two arm means (treatment minus control).

    This is the quantity an A/B readout actually decides on, and the only one whose error rate
    survives being looked at hourly. It exists because the harness previously computed a sequence
    on the per-unit rate *pooled across arms* and printed it directly under the primary estimate:
    an interval on a level, which for a like rate cannot contain zero, sitting where a reader
    would look for sequential evidence of an effect. Meanwhile the fixed-horizon WLS p-value —
    read roughly 96 times over the 2026-08-28 holdout window — crossed 0.05 on 2026-09-01
    (p=0.0466) while the distribution-free permutation test sat flat at ~0.238 across every
    window, which is the signature of alpha inflation rather than an emerging effect.

    The estimand is the UNWEIGHTED mean of per-unit rates, deliberately, matching
    :func:`permutation_rate_diff` rather than the impression-weighted WLS. 3.3% of users carry 36%
    of impressions here, so impression weighting is precisely what makes the parametric fit
    fragile. The two numbers are therefore not expected to match.

    Variance of the difference is ``s2_c/n_c + s2_t/n_t`` (arms are independent by randomization);
    the sequence is indexed by total units accrued, which is marginally the more conservative
    choice since the boundary's log term grows in ``t``.
    """
    c = np.asarray(control, dtype=float)
    t_ = np.asarray(treatment, dtype=float)
    nc, nt = c.size, t_.size
    if nc < 2 or nt < 2:
        return SequentialEstimate(
            diff=float("nan"), rel=float("nan"), low=float("-inf"), high=float("inf"),
            n_control=nc, n_treatment=nt, variance_reduction=variance_reduction,
            estimand=estimand,
        )
    mc, mt = float(c.mean()), float(t_.mean())
    diff = mt - mc
    rel = (diff / mc) if mc else float("nan")
    var = float(np.var(c, ddof=1)) / nc + float(np.var(t_, ddof=1)) / nt
    se = math.sqrt(var) if var > 0 else 0.0
    # Degenerate: every unit within an arm scored identically, so there is no basis for a variance
    # estimate and the boundary is undefined -- unbounded, not zero-width. Mirrors
    # `Estimate.significant`, which refuses a verdict on a non-positive SE for the same reason: a
    # negative control with identical rates in both arms once came back p=0.0000 and withheld a
    # valid result.
    #
    # The test is RELATIVE, not `var <= 0`. Exact zero is not the hazard: `np.var` on 50 copies of
    # 0.09 returns 1.97e-34 rather than 0.0, because 0.09 is not representable and the two-pass
    # mean leaves float residue. That is strictly positive, so it passes a `<= 0` guard and yields
    # a ~1e-17-wide interval reading SEPARATED FROM ZERO -- false certainty from rounding noise.
    scale = max(abs(mc), abs(mt), se)
    if not math.isfinite(se) or se <= 1e-8 * scale:
        return SequentialEstimate(
            diff=diff, rel=rel, low=float("-inf"), high=float("inf"),
            n_control=nc, n_treatment=nt, variance_reduction=variance_reduction,
            estimand=estimand,
        )
    radius = _normal_mixture_radius(var, nc + nt, alpha, rho)
    return SequentialEstimate(
        diff=diff, rel=rel, low=diff - radius, high=diff + radius,
        n_control=nc, n_treatment=nt, variance_reduction=variance_reduction,
        estimand=estimand,
    )

def covariate_balance(
    covariate: np.ndarray, treated: np.ndarray, covered: np.ndarray, resamples: int = 2000
) -> tuple[float, float]:
    """Re-randomization check on a CUPED covariate. Returns ``(imbalance, p_value)``.

    A covariate is only allowed to adjust the outcome if the treatment cannot have moved it. That
    is an empirical question, not a design assumption, and it was got wrong here once already: the
    holdout's pre-period like rate reads 0.00839 control vs 0.00999 treatment, which looks like
    treatment leakage because it points the same way as the effect. Re-randomizing the arm labels
    shows it is not — measured 2026-09-01, observed +0.00242 against a null sd of 0.00325,
    two-sided p=0.459, and the sign REVERSES in an earlier window. It is chance.

    Compares only COVERED units, so a difference in who has pre-period data at all cannot
    masquerade as a difference in the covariate's value.
    """
    c = np.asarray(covariate, dtype=float)
    m = np.asarray(covered, dtype=bool)
    t = np.asarray(treated, dtype=bool)
    if m.sum() < 4 or (t & m).sum() < 2 or (~t & m).sum() < 2:
        return float("nan"), float("nan")
    vals, arms = c[m], t[m]
    observed = float(vals[arms].mean() - vals[~arms].mean())
    n_t = int(arms.sum())
    rng = np.random.default_rng(0)  # fixed: this is a diagnostic, it must not flicker hourly
    idx = np.arange(vals.size)
    hits = 0
    for _ in range(resamples):
        pick = rng.permutation(idx)[:n_t]
        mask = np.zeros(vals.size, dtype=bool)
        mask[pick] = True
        if abs(float(vals[mask].mean() - vals[~mask].mean())) >= abs(observed):
            hits += 1
    return observed, hits / resamples


def insufficient_data_gate(estimate: Estimate, min_observations: int) -> tuple[bool, str]:
    """Refuse to report a verdict below a minimum sample size.

    Significance testing alone is not enough protection. The motivating incident was a 37-response
    sample reading ``no_user_data`` at 14% when the value at 706 responses was 49.3% — the small
    sample was not *significant*, it was simply **wrong**, and it was believed because it was
    looked at. A hard floor stops a verdict being formed from a sample that cannot support one.
    """
    if estimate.n_obs < min_observations:
        return True, (
            f"WITHHELD: {estimate.n_obs} observations is below the {min_observations} minimum. "
            "Small samples do not merely widen intervals, they produce point estimates that are "
            "simply wrong; no verdict is formed at this size."
        )
    return False, f"{estimate.n_obs} observations meets the {min_observations} minimum"


@dataclass(frozen=True)
class ControlVerdict:
    name: str
    reason: str
    estimate: Estimate
    moved: bool


#: A control must move by at least this share of the primary effect to invalidate it. A confound that
#: could account for 2% of an effect is not a reason to discard the other 98%; one that accounts for
#: most of it is. The motivating incident sits far above this line: the fallback surface moved
#: +1.46pp against the treatment surface's +1.34pp, a share of 109%.
CONTROL_MOVE_SHARE = 0.25


def control_moved(
    control: Estimate, primary: Estimate, min_share: float = CONTROL_MOVE_SHARE
) -> bool:
    """Whether a negative control has moved enough to invalidate the primary effect.

    Significance alone is the wrong test in both directions. It fires on degenerate fits (a control
    with an exactly-zero difference was reported at ``p = 0.0000``), and on huge samples it fires for
    effects far too small to explain anything. So require the control to be both statistically
    distinguishable from zero *and* materially large next to the effect it would explain away.
    """
    if not math.isfinite(control.diff) or control.diff == 0.0:
        return False
    if not control.significant:
        return False
    if math.isfinite(primary.diff) and primary.diff != 0.0:
        return abs(control.diff) >= min_share * abs(primary.diff)
    # No primary effect to protect: any real movement on a control is still worth surfacing.
    return True


def negative_control_gate(
    verdicts: list[ControlVerdict], alpha: float = 0.05
) -> tuple[bool, str]:
    """Decide whether a result may be reported at all.

    Returns ``(withheld, explanation)``. A control the treatment *cannot* affect must not move; if
    one does, the observed effect is not attributable to the treatment and reporting it would be
    the exact failure mode this package exists to prevent.
    """
    movers = [v for v in verdicts if v.moved]
    if not movers:
        return False, "all negative controls flat"
    detail = "; ".join(
        f"{v.name} moved (p={v.estimate.p_value:.3f}, {v.reason})" for v in movers
    )
    return True, (
        "WITHHELD: a negative control moved, so the observed effect cannot be attributed to the "
        f"treatment. {detail}"
    )
