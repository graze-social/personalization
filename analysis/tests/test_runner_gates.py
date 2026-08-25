"""The runner must refuse to print an effect before its gates pass.

Both historical false positives were believed because the effect was seen first and the caveat
second. These tests pin the ordering.
"""

from __future__ import annotations

from graze_analysis.data import rows_from_ab_result
from graze_analysis.runner import analyse_ab, run
from graze_analysis.spec import load_spec, spec_from_dict


def _spec(**over):
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
        "min_observations": 200,
    }
    body.update(over)
    return spec_from_dict(body)


class FakeReader:
    """Returns canned (unit_id, arm_value, likes, seen) rows, primary first then controls."""

    def __init__(self, primary, control):
        self._primary = primary
        self._control = control
        self.calls = 0

    def query(self, sql):
        self.calls += 1
        return self._primary if "source = 'fallback'" not in sql else self._control


def _rows(n_users, likes_per_user, seen_per_user, arm_value):
    return [
        (f"u{arm_value}_{i}", arm_value, likes_per_user, seen_per_user) for i in range(n_users)
    ]


def test_withheld_when_below_the_observation_floor():
    primary = _rows(5, 1, 4, 10000) + _rows(5, 2, 4, 250)
    readout = analyse_ab(_spec(), FakeReader(primary, []))
    assert "WITHHELD" in readout.verdict
    assert "insufficient data" in readout.verdict
    # Crucially, no effect size is printed at all.
    assert not any("primary" in line for line in readout.lines)


def test_withheld_when_a_negative_control_moves():
    # Big effect on the primary surface...
    primary = _rows(120, 1, 20, 10000) + _rows(120, 8, 20, 250)
    # ...and an equally large one on a surface the treatment cannot touch.
    control = _rows(120, 1, 20, 10000) + _rows(120, 8, 20, 250)
    readout = analyse_ab(_spec(), FakeReader(primary, control))
    assert "WITHHELD" in readout.verdict, readout.render()
    assert "negative control" in readout.verdict
    joined = "\n".join(readout.lines)
    assert "WITHHELD" in joined


def test_effect_is_reported_when_controls_are_flat():
    primary = _rows(150, 1, 20, 10000) + _rows(150, 6, 20, 250)
    control = _rows(150, 2, 20, 10000) + _rows(150, 2, 20, 250)  # identical -> flat
    readout = analyse_ab(_spec(), FakeReader(primary, control))
    assert "WITHHELD" not in readout.verdict, readout.render()
    assert any("primary" in line for line in readout.lines)


def test_real_spec_files_load_and_dispatch():
    for path, design in [
        ("experiments/max_sources_250_vs_10000.yaml", "ab"),
        ("experiments/interleave_self_check.yaml", "interleaving"),
    ]:
        spec = load_spec(path)
        assert spec.design == design


def test_interleaving_self_check_reports_no_preference():
    spec = load_spec("experiments/interleave_self_check.yaml")

    class R:
        # (unit_id, treatment_wins, control_wins, tagged_impressions)
        def query(self, sql):
            return [(f"u{i}", 0, 0, 5) for i in range(60)]

    readout = run(spec, R())
    assert "NO PREFERENCE" in readout.verdict, readout.render()


def test_interleaving_detects_a_real_preference():
    spec = load_spec("experiments/interleave_self_check.yaml")

    class R:
        def query(self, sql):
            # 45 users prefer treatment, 5 prefer control.
            return [(f"t{i}", 3, 0, 6) for i in range(45)] + [
                (f"c{i}", 0, 3, 6) for i in range(5)
            ]

    readout = run(spec, R())
    assert "TREATMENT WINS" in readout.verdict, readout.render()
