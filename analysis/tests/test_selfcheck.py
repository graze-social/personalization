"""Tests for the harness's own negative control.

The regression these lock in is a real one from this session: the check was deployed, it failed at
123 pairs / 15 drafts, that failure exposed a genuine bug (each arm derived co-liker weights
independently while the seed shuffle used ``thread_rng``), and after the fix the same check reported
2 pairs / 47 drafts. Both of those numbers appear below as fixtures, so neither the bug nor the
over-strict criterion can come back unnoticed.
"""

from __future__ import annotations

import json

from graze_analysis.selfcheck import (
    MAX_DISAGREEMENT_RATE,
    Draft,
    drafts_from_logs,
    evaluate,
)


def _uniform(n: int, pairs: int, shared: int, ctl: int, trt: int) -> list[Draft]:
    return [Draft(pairs, shared, ctl, trt) for _ in range(n)]


class TestTheRealMeasurements:
    def test_the_pre_fix_state_fails(self):
        """123 pairs across 15 drafts on 1,904 shared items — a ~6% disagreement floor."""
        drafts = _uniform(15, pairs=123 // 15, shared=1904 // 15, ctl=141, trt=142)
        result = evaluate(drafts, min_drafts=15)
        assert not result.passed
        assert result.disagreement_rate > 0.05
        assert "could not be distinguished from harness noise" in result.reason

    def test_the_post_fix_state_passes(self):
        """2 pairs across 47 drafts on 4,192 shared items — a 0.05% floor."""
        drafts = _uniform(46, pairs=0, shared=91, ctl=89, trt=89)
        drafts.append(Draft(competitive_pairs=2, shared_items=20, control_scored=23, treatment_scored=24))
        result = evaluate(drafts)
        assert result.passed, result.render()
        assert result.disagreement_rate < 0.001
        assert "noise and not bias" in result.reason

    def test_the_floor_sits_well_below_a_detectable_effect(self):
        """The tolerance must be small enough that a 1% effect is never mistaken for noise."""
        assert MAX_DISAGREEMENT_RATE <= 0.01 / 2


class TestSystematicImbalanceIsCaughtSeparately:
    """One post scored differently is a race; a consistent gap is a defect."""

    def test_incidental_single_post_race_is_tolerated(self):
        drafts = _uniform(40, pairs=0, shared=100, ctl=100, trt=100)
        drafts.append(Draft(0, 100, 100, 101))
        assert evaluate(drafts).passed

    def test_systematic_gap_fails_even_with_zero_pairs(self):
        """Zero pairs cannot launder arms that are seeing different candidate sets."""
        result = evaluate(_uniform(40, pairs=0, shared=100, ctl=100, trt=130))
        assert not result.passed
        assert "same candidate set" in result.reason


class TestWithholding:
    def test_too_few_drafts_is_withheld_not_passed(self):
        result = evaluate(_uniform(3, pairs=0, shared=100, ctl=100, trt=100))
        assert not result.passed
        assert "WITHHELD" in result.reason

    def test_no_drafts_at_all_is_withheld(self):
        result = evaluate([])
        assert not result.passed
        assert "WITHHELD" in result.reason
        assert result.disagreement_rate == 0.0


class TestLogParsing:
    def test_parses_nested_and_flat_records(self):
        lines = [
            json.dumps({"fields": {"competitive_pairs": 2, "shared_items": 20,
                                   "control_scored": 23, "treatment_scored": 24}}),
            json.dumps({"competitive_pairs": 0, "shared_items": 91,
                        "control_scored": 89, "treatment_scored": 89}),
        ]
        drafts = drafts_from_logs(lines)
        assert [d.competitive_pairs for d in drafts] == [2, 0]
        assert [d.shared_items for d in drafts] == [20, 91]

    def test_ignores_unrelated_and_malformed_lines(self):
        lines = [
            "not json at all",
            json.dumps({"message": "coliker_computed", "fields": {"count": 3}}),
            "{competitive_pairs: broken",
            json.dumps({"fields": {"competitive_pairs": 1, "shared_items": 5,
                                   "control_scored": 5, "treatment_scored": 5}}),
        ]
        assert len(drafts_from_logs(lines)) == 1
