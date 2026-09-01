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
        # Traffic is unchanged — fallback took the slots personalization used to fill.
        dens = dict(MEASURED)
        dens[2304] = FeedDensity(2304, post_count=5215, scoreable=150)  # 2.88%, drifted under
        findings = classify(
            dens,
            impressions_now={2304: 0},
            impressions_baseline={2304: 800},
            traffic_now={2304: 950},
            gate=GATE,
        )
        kinds = {f.algo: f.kind for f in findings}
        assert kinds.get(2304) == "newly_gated", findings
        assert "stopped being personalized" in next(f for f in findings if f.algo == 2304).detail

    def test_feed_below_gate_that_was_never_productive_is_not_flagged(self):
        # 5395 and 4051 are supposed to be gated. Flagging them would make the guard cry wolf forever.
        findings = classify(
            MEASURED,
            impressions_now={5395: 0, 4051: 0},
            impressions_baseline={5395: 0, 4051: 0},
            traffic_now={5395: 900, 4051: 900},
            gate=GATE,
        )
        assert findings == [], findings

    def test_quiet_feed_is_not_called_a_regression(self):
        # Below min_impressions of history: absence of output proves nothing. This is the small-sample
        # trap that produced two wrong readings earlier in this project.
        dens = {2304: FeedDensity(2304, post_count=5215, scoreable=150)}
        findings = classify(dens, {2304: 0}, {2304: 3}, {2304: 900}, gate=GATE, min_impressions=50)
        assert findings == []

    def test_feed_nobody_requested_is_not_called_a_regression(self):
        # The other half of the same trap. Output of zero is only evidence if the feed was actually
        # being served: with no traffic there is nothing for the engine to have failed to rank.
        dens = {2304: FeedDensity(2304, post_count=5215, scoreable=150)}
        findings = classify(dens, {2304: 0}, {2304: 800}, {2304: 4}, gate=GATE, min_impressions=50)
        assert findings == []


class TestDiurnalFalsePositive:
    """Algo 6445, 2026-09-01 — the reading that made this comparison change.

    The feed crosses the gate downward every afternoon and back up overnight, because its pool is
    truncated by ALGO_POSTS_LIMIT to ~23h and therefore tracks posting volume, which is itself
    diurnal. Comparing consecutive windows reads that cycle as a feed that has died.
    """

    #: 38,546 posts, 1,452 clearing 10 likers — 3.77% against a 4.00% gate, measured 19:17Z.
    DIURNAL = {6445: FeedDensity(6445, post_count=38546, scoreable=1452)}

    def test_the_afternoon_trough_is_not_a_regression(self):
        # Real numbers from the run that fired: 361 personalized in the 07:17-13:17Z peak, then 0
        # personalized across 34 total impressions in the 13:17-19:17Z trough. The same hours the
        # day before held 35 personalized impressions, below the floor, so there is nothing to
        # compare against and no verdict should be formed.
        findings = classify(
            self.DIURNAL,
            impressions_now={6445: 0},
            impressions_baseline={6445: 35},
            traffic_now={6445: 34},
            gate=GATE,
        )
        assert findings == [], findings

    def test_the_old_consecutive_window_comparison_is_what_misfired(self):
        # Pinning the bug itself: hand the guard the previous window as the baseline, the way it used
        # to, and the same inputs produce the false detection. This asserts the fix is the comparison
        # basis and not the thresholds.
        findings = classify(
            self.DIURNAL,
            impressions_now={6445: 0},
            impressions_baseline={6445: 361},  # the morning peak, i.e. the OLD baseline
            traffic_now={6445: 900},           # and enough traffic to clear the volume floor
            gate=GATE,
        )
        assert [f.kind for f in findings] == ["newly_gated"], findings

    def test_a_feed_that_really_stopped_is_still_caught_across_the_cycle(self):
        # The guard must not be deadened. Same feed, same diurnal traffic, but yesterday's matching
        # window was productive and today's is not — that is a real regression and must still fire.
        findings = classify(
            self.DIURNAL,
            impressions_now={6445: 0},
            impressions_baseline={6445: 340},
            traffic_now={6445: 420},
            gate=GATE,
        )
        assert [f.kind for f in findings] == ["newly_gated"], findings
        assert "in this window yesterday" in findings[0].detail


class TestPrediction:
    def test_productive_feed_near_the_gate_is_warned_about(self):
        # 2304 at 6.40% against a 4% gate is a 1.6x margin — inside the default 1.5x? No: 6.40/4.00 = 1.6,
        # so it should NOT warn at 1.5x but SHOULD at 1.75x. Pinning both directions.
        assert not any(
            f.algo == 2304
            for f in classify(
                MEASURED, {2304: 500}, {2304: 500}, {2304: 700}, gate=GATE, warn_factor=1.5
            )
        )
        warned = classify(
            MEASURED, {2304: 500}, {2304: 500}, {2304: 700}, gate=GATE, warn_factor=1.75
        )
        assert any(f.algo == 2304 and f.kind == "at_risk" for f in warned), warned

    def test_high_density_feeds_are_never_warned_about(self):
        findings = classify(
            MEASURED,
            {396: 5000, 2323: 5000},
            {396: 5000, 2323: 5000},
            {396: 6000, 2323: 6000},
            gate=GATE,
        )
        assert not any(f.algo in (396, 2323) for f in findings)

    def test_detection_outranks_prediction_in_the_report(self):
        dens = dict(MEASURED)
        dens[2304] = FeedDensity(2304, post_count=5215, scoreable=150)   # under gate, went silent
        dens[8386] = FeedDensity(8386, post_count=5653, scoreable=250)   # 4.42%, productive, near gate
        findings = classify(
            dens,
            {2304: 0, 8386: 900},
            {2304: 800, 8386: 900},
            {2304: 950, 8386: 1100},
            gate=GATE,
        )
        out = render(findings, GATE, len(dens))
        assert out.index("NEWLY_GATED") < out.index("AT_RISK"), out


class TestRendering:
    def test_clean_run_says_so_with_the_feed_count(self):
        out = render([], GATE, 59)
        assert "OK" in out and "59 feeds" in out

    def test_share_of_zero_post_count_does_not_divide_by_zero(self):
        d = FeedDensity(1, post_count=0, scoreable=0)
        assert d.share == 0.0
