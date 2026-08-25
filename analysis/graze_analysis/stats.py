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


def always_valid_ci(
    values: np.ndarray, alpha: float = 0.05, rho: float = 1.0
) -> tuple[float, float, float]:
    """Normal-mixture confidence sequence for a mean — valid under continuous monitoring.

    Returns ``(estimate, low, high)``. Unlike a fixed-horizon interval this may be inspected at any
    time, any number of times, without inflating the error rate; the price is a wider interval.
    Boundary follows the standard normal-mixture form (Howard et al., *Time-uniform
    Chernoff bounds*): with ``t`` observations and variance proxy ``s2``,

        radius = sqrt( s2 * 2 * (t*rho + 1) / (t^2 * rho) * ln( sqrt(t*rho + 1) / alpha ) )
    """
    v = np.asarray(values, dtype=float)
    t = v.size
    if t < 2:
        return (float(v.mean()) if t else float("nan"), float("-inf"), float("inf"))
    mean = float(v.mean())
    s2 = float(np.var(v, ddof=1))
    if s2 <= 0:
        return mean, mean, mean
    inner = math.sqrt(t * rho + 1.0) / alpha
    radius = math.sqrt(s2 * 2.0 * (t * rho + 1.0) / (t * t * rho) * math.log(inner))
    return mean, mean - radius, mean + radius


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
