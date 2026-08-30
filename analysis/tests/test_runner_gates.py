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


class GuardrailReader:
    """Routes by numerator event: the guardrail query asks for a different one.

    Mirrors FakeReader but distinguishes a third query shape, so a test can give the guardrail
    metric a different effect from the primary one.
    """

    def __init__(self, primary, control, guardrail):
        self._primary = primary
        self._control = control
        self._guardrail = guardrail

    def query(self, sql):
        if "requestLess" in sql:
            return self._guardrail
        if "source = 'fallback'" not in sql:
            return self._primary
        return self._control


def _guardrail_spec(**over):
    return _spec(guardrails=[{"metric": "request_less_rate", "max": 0.0}], **over)


def test_guardrail_breach_is_reported_but_does_not_withhold_the_result():
    """A guardrail is not a negative control.

    A negative control that moves means the experiment is broken, so the result is withheld. A
    guardrail that moves means the change had a real cost — the result must still be shown, or the
    reader cannot weigh the trade the guardrail exists to expose.
    """
    primary = _rows(150, 1, 20, 10000) + _rows(150, 6, 20, 250)
    control = _rows(150, 1, 20, 10000) + _rows(150, 1, 20, 250)
    # Treatment produces far more "show me less" — a genuine regression.
    guardrail = _rows(150, 0, 20, 10000) + _rows(150, 5, 20, 250)

    readout = analyse_ab(_guardrail_spec(), GuardrailReader(primary, control, guardrail))
    joined = "\n".join(readout.lines)

    assert "WITHHELD" not in readout.verdict, readout.render()
    assert "guardrail request_less_rate" in joined, joined
    assert "BREACH" in joined, joined
    # The primary effect is still reported alongside it.
    assert any("primary" in line for line in readout.lines), joined


def test_quiet_guardrail_reports_its_event_count_so_it_is_not_read_as_safety():
    """An underpowered guardrail must not read as evidence of safety.

    requestLess is rare — 221 events across the holdout's first 11 days — so "ok" here usually
    means "no power", exactly the trap the negative-control write-up hit when p=0.0555 on 3 likes
    was described as 'nearly moved'.
    """
    primary = _rows(150, 1, 20, 10000) + _rows(150, 3, 20, 250)
    control = _rows(150, 1, 20, 10000) + _rows(150, 1, 20, 250)
    guardrail = _rows(150, 0, 20, 10000) + _rows(150, 0, 20, 250)

    readout = analyse_ab(_guardrail_spec(), GuardrailReader(primary, control, guardrail))
    joined = "\n".join(readout.lines)

    assert "BREACH" not in joined, joined
    assert "n=0 request_less_rate events" in joined, joined
    assert "weak evidence" in joined, joined


def test_unknown_guardrail_metric_is_skipped_loudly_not_silently():
    """A typo'd metric name must not look like a passing guardrail."""
    primary = _rows(150, 1, 20, 10000) + _rows(150, 3, 20, 250)
    control = _rows(150, 1, 20, 10000) + _rows(150, 1, 20, 250)
    spec = _spec(guardrails=[{"metric": "clickthrough_rate", "max": 0.0}])

    readout = analyse_ab(spec, GuardrailReader(primary, control, []))
    joined = "\n".join(readout.lines)

    assert "SKIPPED" in joined, joined
    assert "unknown metric" in joined, joined


def test_spec_without_guardrails_prints_none():
    primary = _rows(150, 1, 20, 10000) + _rows(150, 3, 20, 250)
    control = _rows(150, 1, 20, 10000) + _rows(150, 1, 20, 250)
    readout = analyse_ab(_spec(), FakeReader(primary, control))
    assert "guardrail" not in "\n".join(readout.lines)


class _KeysReader:
    """Returns canned key listings for the contract check."""

    def __init__(self, top, nested):
        self._top = [(k, 1) for k in top]
        self._nested = [(k, 1) for k in nested]

    def query(self, sql):
        return self._nested if "'params'" in sql else self._top


def test_contract_accepts_a_top_level_field_that_exists():
    from graze_analysis.contract import field_is_present

    assert field_is_present("follow_seed", {"follow_seed", "algo_id"}, set())


def test_contract_rejects_a_field_nothing_writes():
    """The regression this pins.

    `follow_seed` moved from `params.follow_seed` to the top level and a consumer kept reading the
    old path. It did not error -- it matched 54 stale blobs and reported NOT DELIVERED for 24 hours
    while the treatment was working.
    """
    from graze_analysis.contract import field_is_present

    assert not field_is_present("params.follow_seed", {"follow_seed"}, set())
    assert not field_is_present("ranker", {"algo_id", "source"}, set())


def test_contract_handles_nested_params_paths():
    from graze_analysis.contract import field_is_present

    top, nested = {"params", "algo_id"}, {"max_total_sources"}
    assert field_is_present("params.max_total_sources", top, nested)
    assert not field_is_present("params.not_written", top, nested)
    # A nested path under anything other than `params` is a spec error, not a lookup.
    assert not field_is_present("source.nope", {"source"}, {"nope"})


def _scroll_spec(**over):
    return _spec(scroll_depth={"winsorize_quantile": 0.9}, **over)


def _scroll_spec_from(sd):
    """Same base spec, with an arbitrary scroll_depth block."""
    return _spec(scroll_depth=sd)


def test_scroll_depth_is_parsed_and_defaults_are_explicit():
    s = _scroll_spec()
    assert s.scroll_depth is not None
    assert s.scroll_depth.winsorize_quantile == 0.9
    # Absent means absent -- it must not silently default to being on.
    assert _spec().scroll_depth is None
    # An empty dict still gets the documented default rather than crashing.
    assert _scroll_spec_from(dict()).scroll_depth.winsorize_quantile == 0.9


def test_scroll_depth_sql_pools_the_cap_across_arms():
    """A per-arm cap would itself be an outcome of the treatment and bias the comparison."""
    from graze_analysis.data import scroll_depth_sql

    sql = scroll_depth_sql(_scroll_spec(), 0.9)
    cap = sql[sql.index("cap AS ("): sql.index("SELECT\n  unit_id")]
    assert "quantile(0.9)" in cap
    assert "arm_value IN (10000, 250)" in cap, "cap must be computed over BOTH arms"
    # The cast matters: least() on UInt64 and Float64 is an illegal-type error in ClickHouse.
    assert "least(toFloat64(impressions)" in sql
    # One row per unit, so the clustered SE reduces to the ordinary one.
    assert "1 AS seen" in sql


def test_scroll_depth_is_reported_but_never_replaces_the_primary():
    primary = _rows(150, 1, 20, 10000) + _rows(150, 3, 20, 250)
    control = _rows(150, 1, 20, 10000) + _rows(150, 1, 20, 250)
    scroll = _rows(150, 8, 1, 10000) + _rows(150, 14, 1, 250)

    class R:
        def query(self, sql):
            if "cap AS (" in sql:
                return scroll
            return primary if "source = 'fallback'" not in sql else control

    readout = analyse_ab(_scroll_spec(), R())
    joined = "\n".join(readout.lines)
    assert "scroll_depth (winsorized p90" in joined, joined
    assert "secondary outcome" in joined, joined
    # The primary must still be present and still first.
    assert any("primary" in line for line in readout.lines)
    assert readout.lines[0].strip().startswith("primary")
