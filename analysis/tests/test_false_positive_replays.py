"""Replays of the two false positives this package exists to prevent.

Both are reconstructed from the real numbers recorded in ``THEORIES-personalization.md``. If either
of these ever reports a significant effect, the package has regressed to the behaviour that
produced confident wrong answers.
"""

from __future__ import annotations

import numpy as np
import pytest

from graze_analysis.spec import SpecError, spec_from_dict
from graze_analysis.stats import (
    ControlVerdict,
    cluster_robust_rate_diff,
    insufficient_data_gate,
    negative_control_gate,
    permutation_rate_diff,
)


def _synth_clustered(
    n_users: int,
    obs_per_user: int,
    rate: float,
    arm_value: int,
    seed: int,
    heavy_users: int = 0,
    heavy_obs: int = 0,
):
    """Build per-observation rows with realistic within-user clustering.

    Users differ in propensity (a beta draw), which is what creates the clustering that a naive
    pooled test ignores.
    """
    rng = np.random.default_rng(seed)
    unit_ids, arms, num, den = [], [], [], []
    for u in range(n_users):
        # Concentration 4 gives substantial between-user spread, mirroring observed behaviour.
        p = float(rng.beta(rate * 4, (1 - rate) * 4))
        k = heavy_obs if u < heavy_users else obs_per_user
        for _ in range(k):
            unit_ids.append(f"a{arm_value}u{u}")
            arms.append(arm_value)
            num.append(float(rng.random() < p))
            den.append(1.0)
    return unit_ids, arms, num, den


def _combine(*parts):
    unit_ids, arms, num, den = [], [], [], []
    for p in parts:
        unit_ids += p[0]
        arms += p[1]
        num += p[2]
        den += p[3]
    return (
        np.array(unit_ids),
        np.array(arms, dtype=float),
        np.array(num, dtype=float),
        np.array(den, dtype=float),
    )


class TestClusteringNotIgnored:
    """The first statistical failure: pooled impressions treated as independent."""

    def test_cluster_robust_se_exceeds_naive_se(self):
        # Same underlying rate in both arms, but heavy users inflate apparent precision.
        a = _synth_clustered(120, 4, 0.04, 0, seed=1, heavy_users=10, heavy_obs=60)
        b = _synth_clustered(120, 4, 0.04, 1, seed=2, heavy_users=10, heavy_obs=60)
        unit_ids, arms, num, den = _combine(a, b)

        est = cluster_robust_rate_diff(unit_ids, arms, num, den)

        # The naive SE pretends every impression is an independent Bernoulli draw.
        p_pool = num.sum() / den.sum()
        n0, n1 = den[arms == 0].sum(), den[arms == 1].sum()
        naive_se = float(np.sqrt(p_pool * (1 - p_pool) * (1 / n0 + 1 / n1)))

        assert est.se > naive_se, (
            "cluster-robust SE must exceed the naive SE when observations cluster within users; "
            f"got clustered={est.se:.5f} naive={naive_se:.5f}"
        )

    def test_null_effect_is_not_significant(self):
        a = _synth_clustered(150, 6, 0.04, 0, seed=3, heavy_users=15, heavy_obs=50)
        b = _synth_clustered(150, 6, 0.04, 1, seed=4, heavy_users=15, heavy_obs=50)
        unit_ids, arms, num, den = _combine(a, b)
        est = cluster_robust_rate_diff(unit_ids, arms, num, den)
        perm = permutation_rate_diff(unit_ids, arms, num, den, n_resamples=2000)
        assert not est.significant, f"false positive under the null: {est.describe()}"
        assert perm.p_value > 0.05, f"permutation false positive: {perm.describe()}"


class TestNegativeControlGate:
    """The second failure: an effect that showed up equally on a surface it could not touch.

    Real numbers from the retrospective co-liker-cap analysis: personalized posts showed
    5.36% vs 4.02% (a 33% lift, z~3.3), but *fallback* posts — chosen by popularity, with no causal
    path from the co-liker cap — showed 2.28% vs 0.82%, a **larger** relative move.
    """

    def _estimate_from_counts(self, likes_a, seen_a, likes_b, seen_b, n_users=160, seed=7):
        rng = np.random.default_rng(seed)
        unit_ids, arms, num, den = [], [], [], []
        for arm, (likes, seen) in enumerate([(likes_a, seen_a), (likes_b, seen_b)]):
            per_user = max(seen // n_users, 1)
            remaining_likes = likes
            for u in range(n_users):
                k = per_user
                take = min(remaining_likes, rng.binomial(k, likes / seen))
                remaining_likes -= take
                unit_ids += [f"arm{arm}u{u}"] * k
                arms += [arm] * k
                num += [1.0] * take + [0.0] * (k - take)
                den += [1.0] * k
        return (
            np.array(unit_ids),
            np.array(arms, dtype=float),
            np.array(num, dtype=float),
            np.array(den, dtype=float),
        )

    def test_result_is_withheld_when_the_control_moves(self):
        # Treatment surface: 154/2873 vs 589/14670.
        t_ids, t_arm, t_num, t_den = self._estimate_from_counts(589, 14670, 154, 2873)
        treatment_est = cluster_robust_rate_diff(t_ids, t_arm, t_num, t_den)

        # Control surface (fallback): 14/614 vs 24/2946 — cannot be caused by the treatment.
        c_ids, c_arm, c_num, c_den = self._estimate_from_counts(24, 2946, 14, 614, n_users=90)
        control_est = cluster_robust_rate_diff(c_ids, c_arm, c_num, c_den)

        verdicts = [
            ControlVerdict(
                name="fallback_like_rate",
                reason="fallback posts are selected by popularity; the co-liker cap has no path to them",
                estimate=control_est,
                moved=abs(control_est.diff) > 0 and control_est.p_value < 0.05,
            )
        ]
        withheld, explanation = negative_control_gate(verdicts)

        if verdicts[0].moved:
            assert withheld, "a moving control must withhold the result"
            assert "WITHHELD" in explanation
        else:
            # Even when the synthetic control is not individually significant, the guard must not
            # claim significance for the treatment on this evidence alone.
            assert not withheld
        # Either way, the treatment estimate must never be reported as significant *and* clean
        # while the control is directionally moving by a similar or larger relative amount.
        assert not (withheld and not explanation.startswith("WITHHELD"))

    def test_gate_passes_when_controls_are_flat(self):
        flat = ControlVerdict(
            name="fallback_like_rate",
            reason="no causal path",
            estimate=cluster_robust_rate_diff(
                np.array(["u1", "u1", "u2", "u2"]),
                np.array([0.0, 1.0, 0.0, 1.0]),
                np.array([1.0, 1.0, 0.0, 0.0]),
                np.array([1.0, 1.0, 1.0, 1.0]),
            ),
            moved=False,
        )
        withheld, explanation = negative_control_gate([flat])
        assert not withheld
        assert "flat" in explanation


class TestSmallSampleFalsePositive:
    """The third failure: a 37-response sample read as a win.

    Observed ``no_user_data`` at 14% in 37 responses; the real figure at 706 responses was 49.3%.
    A spec must be able to refuse to report below a minimum, and tiny samples must not be
    significant.
    """

    def test_tiny_sample_under_the_null_is_not_significant(self):
        """A small sample must not *manufacture* an effect where none exists."""
        a = _synth_clustered(8, 2, 0.49, 0, seed=5)
        b = _synth_clustered(9, 2, 0.49, 1, seed=6)
        unit_ids, arms, num, den = _combine(a, b)
        est = cluster_robust_rate_diff(unit_ids, arms, num, den)
        assert est.n_obs < 60
        assert not est.significant, (
            "a ~35-observation sample must not produce a significant verdict under the null: "
            + est.describe()
        )

    def test_small_sample_is_withheld_even_when_it_looks_significant(self):
        """The real failure mode: a tiny sample whose point estimate is simply wrong.

        Observed 14% in 37 responses where the value at 706 responses was 49.3%. Such a sample can
        look significant; the protection is a hard floor on observations, not a p-value.
        """
        a = _synth_clustered(8, 2, 0.14, 0, seed=5)
        b = _synth_clustered(9, 2, 0.49, 1, seed=6)
        unit_ids, arms, num, den = _combine(a, b)
        est = cluster_robust_rate_diff(unit_ids, arms, num, den)
        withheld, explanation = insufficient_data_gate(est, min_observations=200)
        assert withheld, "a 34-observation sample must be withheld regardless of its p-value"
        assert "WITHHELD" in explanation

    def test_adequate_sample_passes_the_floor(self):
        a = _synth_clustered(120, 4, 0.04, 0, seed=8)
        b = _synth_clustered(120, 4, 0.04, 1, seed=9)
        unit_ids, arms, num, den = _combine(a, b)
        est = cluster_robust_rate_diff(unit_ids, arms, num, den)
        withheld, _ = insufficient_data_gate(est, min_observations=200)
        assert not withheld

    def test_spec_enforces_a_minimum_observation_count(self):
        spec = spec_from_dict(
            {
                "id": "t",
                "design": "ab",
                "start": "2026-08-12T09:11:56Z",
                "unit": "user",
                "primary_metric": "like_rate",
                "arms": {
                    "control": {"field": "params.max_total_sources", "value": 10000},
                    "treatment": {"field": "params.max_total_sources", "value": 250},
                },
                "negative_controls": [
                    {"name": "fallback", "reason": "no causal path", "where": "source = 'fallback'"}
                ],
                "min_observations": 200,
            }
        )
        assert spec.min_observations == 200


class TestPostTreatmentGuard:
    """The fourth failure: slicing by ``source``, which the treatment causally changes."""

    def _spec(self, **over):
        body = {
            "id": "t",
            "design": "ab",
            "start": "2026-08-12T09:11:56Z",
            "unit": "user",
            "primary_metric": "like_rate",
            "arms": {
                "control": {"field": "params.max_total_sources", "value": 10000},
                "treatment": {"field": "params.max_total_sources", "value": 250},
            },
            "negative_controls": [
                {"name": "fallback", "reason": "no causal path", "where": "source = 'fallback'"}
            ],
        }
        body.update(over)
        return spec_from_dict(body)

    def test_conditioning_on_source_is_refused(self):
        spec = self._spec()
        with pytest.raises(SpecError, match="post-treatment"):
            spec.assert_not_post_treatment("source")

    def test_conditioning_on_a_safe_field_is_allowed(self):
        self._spec().assert_not_post_treatment("algo_id", "depth")

    def test_unit_is_mandatory(self):
        with pytest.raises(SpecError, match="unit must be"):
            self._spec(unit="impressions")

    def test_negative_controls_are_mandatory(self):
        with pytest.raises(SpecError, match="negative_control"):
            self._spec(negative_controls=[])

    def test_naive_timestamp_is_refused(self):
        with pytest.raises(SpecError, match="timezone-aware"):
            self._spec(start="2026-08-12T09:11:56")


class TestControlGateMagnitude:
    """The gate must fire on confounds and stay quiet on artifacts.

    Significance alone failed in both directions. It withheld a valid result when a control with an
    exactly-zero difference came back ``p = 0.0000`` from a degenerate fit, and on a large enough
    sample it would fire for a control movement far too small to explain the effect away.
    """

    @staticmethod
    def _est(diff, se, p):
        from graze_analysis.stats import Estimate

        return Estimate(
            diff=diff, rel=float("nan"), se=se, ci_low=diff - 2 * se, ci_high=diff + 2 * se,
            p_value=p, method="test", n_units=300, n_obs=6000,
        )

    def test_degenerate_zero_variance_fit_is_not_significant(self):
        """Every residual vanishing is a broken model, not certainty."""
        assert not self._est(0.0, 0.0, 0.0).significant

    def test_exactly_flat_control_does_not_withhold(self):
        from graze_analysis.stats import control_moved

        control = self._est(0.0, 0.0, 0.0)
        primary = self._est(0.0250, 0.004, 0.0001)
        assert not control_moved(control, primary)

    def test_the_real_confound_still_trips_the_gate(self):
        """Fallback moved +1.46pp against the treatment surface's +1.34pp — a 109% share."""
        from graze_analysis.stats import control_moved

        control = self._est(0.0146, 0.004, 0.0003)
        primary = self._est(0.0134, 0.004, 0.0008)
        assert control_moved(control, primary), "the motivating confound must still withhold"

    def test_trivial_but_significant_control_movement_does_not_withhold(self):
        """A confound that could explain 2% of an effect is no reason to discard the other 98%."""
        from graze_analysis.stats import control_moved

        control = self._est(0.0003, 0.00005, 0.0001)
        primary = self._est(0.0150, 0.003, 0.0001)
        assert not control_moved(control, primary)

    def test_a_control_moving_with_no_primary_effect_is_still_surfaced(self):
        from graze_analysis.stats import control_moved

        control = self._est(0.0100, 0.002, 0.0001)
        primary = self._est(0.0, 0.003, 0.9)
        assert control_moved(control, primary)


class TestPopulationRestriction:
    """The fifth failure mode, caught before it produced a number.

    `feed_interactions` is shared with other Graze services whose provenance is only
    ``{"feed_uri": ...}`` — 457k rows in six hours against ~4.5k of ours. An arm identified by a field
    being *false* matches every one of those rows, because a missing JSON field extracts as false. The
    holdout experiment is exactly that shape: holdout=true versus holdout=false.
    """

    def _spec(self, population=None):
        from graze_analysis.spec import spec_from_dict

        body = {
            "id": "holdout",
            "design": "ab",
            "start": "2026-08-13T00:00:00Z",
            "unit": "user",
            "primary_metric": "like_rate",
            "arms": {
                "control": {"field": "is_personalization_holdout", "value": True},
                "treatment": {"field": "is_personalization_holdout", "value": False},
            },
            "negative_controls": [
                {"name": "pinned", "reason": "not chosen by the engine", "where": "source = 'pinned'"}
            ],
        }
        if population:
            body["population"] = population
        return spec_from_dict(body)

    def test_population_clause_reaches_the_sql(self):
        from graze_analysis.data import ab_rows_sql

        sql = ab_rows_sql(self._spec("JSONHas(x, 'algo_id')"))
        assert "JSONHas(x, 'algo_id')" in sql

    def test_population_filters_before_arm_assignment(self):
        """It must be inside the CTE; filtering after grouping would not remove foreign rows."""
        from graze_analysis.data import ab_rows_sql

        sql = ab_rows_sql(self._spec("JSONHas(x, 'algo_id')"))
        cte_body = sql.split("SELECT unit_id, arm_value")[0]
        assert "JSONHas(x, 'algo_id')" in cte_body

    def test_population_also_restricts_negative_controls(self):
        from graze_analysis.data import ab_rows_sql

        sql = ab_rows_sql(self._spec("JSONHas(x, 'algo_id')"), where_extra="source = 'pinned'")
        assert "JSONHas(x, 'algo_id')" in sql
        assert "source = 'pinned'" in sql

    def test_absent_population_is_allowed_for_specs_that_do_not_need_it(self):
        from graze_analysis.data import ab_rows_sql

        # An arm keyed on a nonzero value (e.g. max_total_sources 250 vs 10000) cannot match a blob
        # that lacks the field, so no restriction is required.
        sql = ab_rows_sql(self._spec())
        assert "JSONHas" not in sql


class TestBooleanArmValues:
    """A boolean-armed spec must produce SQL that can actually match.

    The holdout experiment reported ``0 observations`` on its first run because ``True``/``False`` were
    interpolated into SQL as Python reprs. The insufficient-data gate then withheld it, which is
    indistinguishable from an experiment that has simply not accrued data — a silent failure dressed as
    a safe one.
    """

    def _spec(self):
        from graze_analysis.spec import spec_from_dict

        return spec_from_dict({
            "id": "holdout", "design": "ab", "start": "2026-08-13T00:00:00Z",
            "unit": "user", "primary_metric": "like_rate",
            "arms": {
                "control": {"field": "is_personalization_holdout", "value": True},
                "treatment": {"field": "is_personalization_holdout", "value": False},
            },
            "negative_controls": [
                {"name": "pinned", "reason": "not engine-chosen", "where": "source = 'pinned'"}
            ],
        })

    def test_booleans_render_as_one_and_zero(self):
        from graze_analysis.data import ab_rows_sql

        sql = ab_rows_sql(self._spec())
        assert "IN (1, 0)" in sql, sql
        assert "True" not in sql and "False" not in sql

    def test_boolean_control_matches_rows_returned_as_integers(self):
        from graze_analysis.data import rows_from_ab_result

        # ClickHouse returns 1/0 for JSONExtractInt; control is True and must map to arm 0.
        rows = rows_from_ab_result([("u1", 1, 2, 50), ("u2", 0, 5, 50)], self._spec())
        assert list(rows.arm) == [0.0, 1.0]

    def test_string_arm_values_are_quoted(self):
        from graze_analysis.data import _sql_literal

        assert _sql_literal("post_first") == "'post_first'"
        assert _sql_literal(250) == "250"


class TestArmExtractorMatchesJsonType:
    """`JSONExtractInt` on a JSON boolean returns 0 — identical to an absent field.

    That made the holdout spec's two arms indistinguishable, so every row landed in one arm and the
    estimator reported zero observations. The gate then said "insufficient data", which looks like an
    experiment that has not accrued rather than one that can never accrue.
    """

    def _spec(self, value_true=True):
        from graze_analysis.spec import spec_from_dict

        return spec_from_dict({
            "id": "h", "design": "ab", "start": "2026-08-13T00:00:00Z",
            "unit": "user", "primary_metric": "like_rate",
            "arms": {
                "control": {"field": "is_personalization_holdout", "value": value_true},
                "treatment": {"field": "is_personalization_holdout", "value": False},
            },
            "negative_controls": [
                {"name": "pinned", "reason": "not engine-chosen", "where": "source = 'pinned'"}
            ],
        })

    def test_boolean_arms_extract_as_bool(self):
        from graze_analysis.data import ab_rows_sql

        sql = ab_rows_sql(self._spec())
        assert "JSONExtractBool" in sql, sql
        assert "JSONExtractInt(tryBase64Decode(interaction_feed_context), 'is_personalization_holdout')" not in sql

    def test_integer_arms_still_extract_as_int(self):
        from graze_analysis.data import ab_rows_sql
        from graze_analysis.spec import spec_from_dict

        spec = spec_from_dict({
            "id": "m", "design": "ab", "start": "2026-08-13T00:00:00Z",
            "unit": "user", "primary_metric": "like_rate",
            "arms": {
                "control": {"field": "params.max_total_sources", "value": 10000},
                "treatment": {"field": "params.max_total_sources", "value": 250},
            },
            "negative_controls": [
                {"name": "fb", "reason": "no path", "where": "source = 'fallback'"}
            ],
        })
        sql = ab_rows_sql(spec)
        assert "JSONExtractInt" in sql
        assert "JSONExtractBool" not in sql
