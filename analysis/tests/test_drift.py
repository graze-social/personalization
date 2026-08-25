"""Tests for the density-drift guard.

Fixtures are the real measured numbers from when the gate was enabled at 0.04, so the thing the guard
exists to catch is pinned to actual production values rather than invented ones.
"""

from __future__ import annotations

from graze_analysis.drift import FeedDensity, classify, render

GATE = 0.04

# Measured 2026-08-17, published by candidate-sync into am:{algo}.
MEASURED = {
    5395: FeedDensity(5395, post_count=992, scoreable=29),    # 2.92% — below gate, correctly skipped
    4051: FeedDensity(4051, post_count=1252, scoreable=20),   # 1.60% — below gate, correctly skipped
    2304: FeedDensity(2304, post_count=5215, scoreable=334),  # 6.40% — lowest-density PRODUCTIVE feed
    8386: FeedDensity(8386, post_count=5653, scoreable=475),  # 8.40% — 25% of all scored output
    2323: FeedDensity(2323, post_count=14894, scoreable=2040),  # 13.70%
    396: FeedDensity(396, post_count=7277, scoreable=2587),   # 35.55%
}


class TestDetection:
    """The failure that has actually happened, which is what matters."""

    def test_feed_that_went_silent_under_the_gate_is_detected(self):
        # 2304 was productive, then its density fell below the gate and its output went to zero.
        dens = dict(MEASURED)
        dens[2304] = FeedDensity(2304, post_count=5215, scoreable=150)  # 2.88%, drifted under
        findings = classify(dens, impressions_now={2304: 0}, impressions_before={2304: 800}, gate=GATE)
        kinds = {f.algo: f.kind for f in findings}
        assert kinds.get(2304) == "newly_gated", findings
        assert "stopped being personalized" in next(f for f in findings if f.algo == 2304).detail

    def test_feed_below_gate_that_was_never_productive_is_not_flagged(self):
        # 5395 and 4051 are supposed to be gated. Flagging them would make the guard cry wolf forever.
        findings = classify(
            MEASURED, impressions_now={5395: 0, 4051: 0}, impressions_before={5395: 0, 4051: 0}, gate=GATE
        )
        assert findings == [], findings

    def test_quiet_feed_is_not_called_a_regression(self):
        # Below min_impressions of history: absence of output proves nothing. This is the small-sample
        # trap that produced two wrong readings earlier in this project.
        dens = {2304: FeedDensity(2304, post_count=5215, scoreable=150)}
        findings = classify(dens, {2304: 0}, {2304: 3}, gate=GATE, min_impressions=50)
        assert findings == []


class TestPrediction:
    def test_productive_feed_near_the_gate_is_warned_about(self):
        # 2304 at 6.40% against a 4% gate is a 1.6x margin — inside the default 1.5x? No: 6.40/4.00 = 1.6,
        # so it should NOT warn at 1.5x but SHOULD at 1.75x. Pinning both directions.
        assert not any(
            f.algo == 2304
            for f in classify(MEASURED, {2304: 500}, {2304: 500}, gate=GATE, warn_factor=1.5)
        )
        warned = classify(MEASURED, {2304: 500}, {2304: 500}, gate=GATE, warn_factor=1.75)
        assert any(f.algo == 2304 and f.kind == "at_risk" for f in warned), warned

    def test_high_density_feeds_are_never_warned_about(self):
        findings = classify(MEASURED, {396: 5000, 2323: 5000}, {396: 5000, 2323: 5000}, gate=GATE)
        assert not any(f.algo in (396, 2323) for f in findings)

    def test_detection_outranks_prediction_in_the_report(self):
        dens = dict(MEASURED)
        dens[2304] = FeedDensity(2304, post_count=5215, scoreable=150)   # under gate, went silent
        dens[8386] = FeedDensity(8386, post_count=5653, scoreable=250)   # 4.42%, productive, near gate
        findings = classify(dens, {2304: 0, 8386: 900}, {2304: 800, 8386: 900}, gate=GATE)
        out = render(findings, GATE, len(dens))
        assert out.index("NEWLY_GATED") < out.index("AT_RISK"), out


class TestRendering:
    def test_clean_run_says_so_with_the_feed_count(self):
        out = render([], GATE, 59)
        assert "OK" in out and "59 feeds" in out

    def test_share_of_zero_post_count_does_not_divide_by_zero(self):
        d = FeedDensity(1, post_count=0, scoreable=0)
        assert d.share == 0.0
