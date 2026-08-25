"""Properties the inference routines must hold, independent of any experiment."""

from __future__ import annotations

import numpy as np

from graze_analysis.stats import always_valid_ci, cuped_adjust


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
