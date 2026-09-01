"""Properties the inference routines must hold, independent of any experiment."""

from __future__ import annotations

import math

import numpy as np
import pytest

from graze_analysis.stats import (
    _normal_mixture_radius,
    always_valid_ci,
    always_valid_diff_ci,
    cuped_adjust,
)


class TestCuped:
    def test_reduces_variance_with_a_correlated_covariate(self):
        rng = np.random.default_rng(1)
        pre = rng.normal(0, 1, 4000)
        post = pre * 0.8 + rng.normal(0, 0.6, 4000)
        adjusted, reduction = cuped_adjust(post, pre)
        assert reduction > 0.4, f"expected substantial reduction, got {reduction:.3f}"
        assert np.var(adjusted) < np.var(post)

    def test_preserves_the_mean(self):
        rng = np.random.default_rng(2)
        pre = rng.normal(5, 2, 2000)
        post = pre * 0.5 + rng.normal(0, 1, 2000)
        adjusted, _ = cuped_adjust(post, pre)
        # CUPED is unbiased: it removes variance, not signal.
        assert abs(float(np.mean(adjusted)) - float(np.mean(post))) < 1e-9

    def test_uncorrelated_covariate_costs_nothing(self):
        rng = np.random.default_rng(3)
        post = rng.normal(0, 1, 3000)
        noise = rng.normal(0, 1, 3000)
        adjusted, reduction = cuped_adjust(post, noise)
        assert reduction < 0.05
        assert abs(float(np.mean(adjusted)) - float(np.mean(post))) < 1e-9

    def test_degenerate_inputs_are_safe(self):
        y = np.array([1.0, 2.0, 3.0])
        adjusted, reduction = cuped_adjust(y, np.array([7.0, 7.0, 7.0]))
        assert reduction == 0.0
        assert np.allclose(adjusted, y)


class TestAlwaysValidCI:
    def test_is_wider_than_a_fixed_horizon_interval(self):
        rng = np.random.default_rng(4)
        v = rng.normal(0, 1, 1000)
        _, low, high = always_valid_ci(v)
        av_width = high - low
        fixed = 2 * 1.96 * float(np.std(v, ddof=1)) / np.sqrt(v.size)
        assert av_width > fixed, "an anytime-valid interval must pay for the right to peek"

    def test_narrows_as_evidence_accumulates(self):
        rng = np.random.default_rng(5)
        v = rng.normal(0, 1, 20000)
        widths = []
        for n in (200, 2000, 20000):
            _, lo, hi = always_valid_ci(v[:n])
            widths.append(hi - lo)
        assert widths[0] > widths[1] > widths[2]

    def test_covers_the_truth_under_repeated_peeking(self):
        # The whole point: peek at every step and the interval should still almost always cover 0.
        rng = np.random.default_rng(6)
        misses = 0
        trials = 200
        for _ in range(trials):
            v = rng.normal(0, 1, 400)
            for n in range(20, 400, 20):
                _, lo, hi = always_valid_ci(v[:n])
                if not (lo <= 0.0 <= hi):
                    misses += 1
                    break
        # Nominal alpha is 0.05 across the whole path; allow slack for a 200-trial estimate.
        assert misses / trials < 0.12, f"coverage too low under peeking: {misses}/{trials} missed"

    def test_tiny_samples_are_uninformative_rather_than_wrong(self):
        est, lo, hi = always_valid_ci(np.array([1.0]))
        assert lo == float("-inf") and hi == float("inf")


class TestAlwaysValidDiffCI:
    """The sequence on the ARM DIFFERENCE — the quantity the verdict is actually formed on.

    The one-sample version was being handed a sample pooled across both arms, which bounds a
    *level*: for a non-negative rate that interval cannot contain zero, so it could never have
    withheld anything.
    """

    def test_the_two_boundaries_are_the_same_function(self):
        # Guards the refactor: the one-sample radius must remain exactly the old closed form.
        for t, s2 in ((5, 0.3), (2437, 0.0038), (68702, 1.2)):
            expected = math.sqrt(
                s2 * 2.0 * (t + 1.0) / (t * t) * math.log(math.sqrt(t + 1.0) / 0.05)
            )
            assert _normal_mixture_radius(s2 / t, t, 0.05, 1.0) == pytest.approx(expected)

    def test_covers_zero_under_repeated_peeking(self):
        rng = np.random.default_rng(11)
        misses = 0
        trials = 200
        for _ in range(trials):
            c = rng.gamma(0.3, 0.04, 400)
            t = rng.gamma(0.3, 0.04, 400)
            for n in range(40, 400, 40):
                est = always_valid_diff_ci(c[:n], t[:n])
                if est.conclusive:
                    misses += 1
                    break
        assert misses / trials < 0.12, f"separated from zero under a true null: {misses}/{trials}"

    def test_finds_a_real_difference(self):
        rng = np.random.default_rng(12)
        c = rng.gamma(0.3, 0.04, 800)
        t = rng.gamma(0.3, 0.04, 800) * 2.5
        est = always_valid_diff_ci(c, t)
        assert est.conclusive and est.low > 0.0, est.describe()

    def test_is_wider_than_the_fixed_horizon_two_sample_interval(self):
        rng = np.random.default_rng(13)
        c = rng.normal(0, 1, 900)
        t = rng.normal(0.1, 1, 900)
        est = always_valid_diff_ci(c, t)
        se = math.sqrt(np.var(c, ddof=1) / c.size + np.var(t, ddof=1) / t.size)
        assert (est.high - est.low) > 2 * 1.96 * se

    def test_a_degenerate_arm_is_uninformative_rather_than_certain(self):
        # Identical values within each arm give no variance estimate, so the bound is undefined --
        # not zero-width. `Estimate.significant` refuses a verdict on a non-positive SE for the
        # same reason: a negative control with identical rates once returned p=0.0000.
        est = always_valid_diff_ci(np.full(50, 0.01), np.full(50, 0.09))
        assert not est.conclusive
        assert est.low == float("-inf") and est.high == float("inf")

    def test_too_few_units_is_uninformative(self):
        est = always_valid_diff_ci(np.array([0.1]), np.array([0.2, 0.3]))
        assert not est.conclusive

    def test_cuped_reduction_is_carried_for_reporting(self):
        rng = np.random.default_rng(14)
        est = always_valid_diff_ci(
            rng.gamma(0.3, 0.04, 100), rng.gamma(0.3, 0.04, 100), variance_reduction=0.457
        )
        assert "cuped=45.7%" in est.describe()
