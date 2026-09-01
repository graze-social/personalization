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
    # The primary must still be present, and a secondary must never outrank it. The anytime-valid
    # GATE leads (it is the quantity the verdict is formed on); the primary sits immediately under
    # it, and scroll depth stays below both.
    assert any("primary" in line for line in readout.lines)
    assert readout.lines[0].strip().startswith("GATE")
    assert readout.lines[1].strip().startswith("primary")
    primary_at = next(i for i, l in enumerate(readout.lines) if "primary" in l)
    scroll_at = next(i for i, l in enumerate(readout.lines) if "scroll_depth" in l)
    assert scroll_at > primary_at, readout.lines


# --- The anytime-valid gate -------------------------------------------------------------------
#
# This readout is scheduled hourly and read whenever, so a fixed-horizon p-value has no nominal
# error rate here. The verdict is formed on a confidence sequence over the arm DIFFERENCE. These
# tests exist because the previous "anytime-valid" line was computed on the per-unit rate POOLED
# across arms -- an interval on a level, which for a non-negative rate cannot contain zero -- and
# nothing checked that any sequential quantity reached the verdict.


def _uneven(prefix, n_users, likes, seen, arm_value):
    """Rows with an explicit prefix, so two cohorts can share one arm without colliding ids."""
    return [(f"{prefix}{i}", arm_value, likes, seen) for i in range(n_users)]


def _weight_trap_rows():
    """An 'effect' that exists only under impression weighting.

    620 users per arm-pair, of whom 20 carry 500 impressions each and all the likes. The
    impression-weighted fit reads this as overwhelming (p ~ 1e-13); the unweighted per-unit
    sequence establishes nothing. This is the real shape of the data, not a contrivance: 3.3% of
    users carry 36% of impressions on this service.
    """
    return (
        _uneven("cl", 300, 0, 10, 10000)
        + _uneven("ch", 10, 5, 500, 10000)
        + _uneven("tl", 300, 0, 10, 250)
        + _uneven("th", 10, 40, 500, 250)
    )


def _null_control():
    return _rows(120, 1, 20, 10000) + _rows(120, 1, 20, 250)


def test_the_verdict_is_formed_on_the_gate_not_the_impression_weighted_fit():
    readout = analyse_ab(_spec(), FakeReader(_weight_trap_rows(), _null_control()))
    joined = "\n".join(readout.lines)

    # The fixed-horizon fit is emphatic...
    primary_line = next(l for l in readout.lines if "primary" in l)
    assert "SIGNIFICANT" in primary_line, primary_line
    # ...and the gate is not, so no effect may be declared.
    assert "NO EFFECT ESTABLISHED" in readout.verdict, readout.render()
    assert "uncorrected for repeated inspection" in readout.verdict, readout.verdict
    assert "includes zero" in readout.lines[0], readout.lines[0]
    assert "EFFECT ESTABLISHED (anytime-valid)" not in readout.verdict


def test_the_gate_leads_and_the_fixed_horizon_tests_are_marked_diagnostic():
    readout = analyse_ab(_spec(), FakeReader(_weight_trap_rows(), _null_control()))
    assert readout.lines[0].strip().startswith("GATE"), readout.lines[0]
    assert "arm DIFFERENCE" in readout.lines[0]
    for name in ("primary", "permutation"):
        line = next(l for l in readout.lines if name in l)
        assert "diagnostic" in line, line


def test_the_pooled_level_line_cannot_be_read_as_an_effect():
    readout = analyse_ab(_spec(), FakeReader(_weight_trap_rows(), _null_control()))
    level = next(l for l in readout.lines if "pooled base rate" in l)
    assert "not an effect" in level, level
    # The wording that invited the misreading must not come back.
    joined = "\n".join(readout.lines)
    assert "anytime-valid CI on the per-unit rate" not in joined


def test_a_real_unit_level_effect_still_clears_the_gate():
    # Rates must VARY within an arm. Identical per-user rates leave only float residue as variance
    # (np.var on 50 copies of 0.09 is 1.97e-34, not 0.0), which the degenerate guard rejects.
    primary = []
    for k in range(3):
        primary += _uneven(f"c{k}_", 134, k, 20, 10000)
        primary += _uneven(f"t{k}_", 134, 6 + 2 * k, 20, 250)
    readout = analyse_ab(_spec(), FakeReader(primary, _null_control()))
    assert "EFFECT ESTABLISHED" in readout.verdict, readout.render()
    assert "SEPARATED FROM ZERO" in readout.lines[0]


def test_units_appearing_in_both_arms_are_flagged_above_everything():
    """A per-REQUEST coin flip once put 18% of users in both arms and cost a window reset."""
    primary = _weight_trap_rows() + [("cl0", 250, 3, 20), ("cl1", 250, 3, 20)]
    readout = analyse_ab(_spec(), FakeReader(primary, _null_control()))
    assert "BOTH arms" in readout.lines[0], readout.lines[0]
    assert "2 unit(s)" in readout.lines[0]


def _cuped_rows(covariate_shift=0):
    """Varying per-user rates with a strongly (but not perfectly) correlated pre-period covariate.

    `covariate_shift` displaces the TREATMENT arm's covariate, simulating a covariate the
    treatment moved -- the one case where adjusting would subtract real signal.
    """
    primary, covariate = [], []
    for i in range(300):
        primary.append((f"c{i}", 10000, (i % 5) + (i % 2), 20))
        covariate.append((f"c{i}", i % 5, 20))
    for i in range(300):
        primary.append((f"t{i}", 250, (i % 5) + (i % 2) + 1, 20))
        covariate.append((f"t{i}", (i % 5) + covariate_shift, 20))
    return primary, covariate


def _cuped_reader(primary, covariate):
    class R:
        def query(self, sql):
            if "pre_likes" in sql:
                return covariate
            return primary if "source = 'fallback'" not in sql else _null_control()

    return R()


def _gate_width(readout):
    lo, hi = readout.lines[0].split("CI [")[1].split("]")[0].split(", ")
    return float(hi) - float(lo)


def test_a_balanced_covariate_adjusts_the_gate():
    """CUPED is APPLIED, not merely reported.

    It was first computed and discarded into `_`, then demoted to a diagnostic on the theory that a
    reset window's pre-period is already under treatment. Re-randomization refuted that (observed
    +0.00242 vs a null sd of 0.00325, p=0.459), and the adjustment is worth ~24% of the gate width
    on live data, which is real power on a question that has never had enough.
    """
    primary, covariate = _cuped_rows()
    plain = analyse_ab(_spec(), _cuped_reader(primary, covariate))
    adjusted = analyse_ab(
        _spec(cuped_covariate="pre_period_like_rate"), _cuped_reader(primary, covariate)
    )

    line = next(l for l in adjusted.lines if "CUPED" in l)
    assert "APPLIED to the gate" in line, line
    assert _gate_width(adjusted) < _gate_width(plain), (adjusted.lines[0], plain.lines[0])


def test_the_covariate_balance_check_is_reported_with_real_coverage():
    primary, covariate = _cuped_rows()
    readout = analyse_ab(
        _spec(cuped_covariate="pre_period_like_rate"), _cuped_reader(primary, covariate)
    )
    bal = next(l for l in readout.lines if "covariate balance" in l)
    assert "re-randomization p=" in bal, bal
    # COVERAGE means "has pre-period data", not "has a nonzero pre-period rate". Conflating the two
    # is what produced a bogus 11.9%/17.3% "coverage gap" that read as treatment contamination;
    # the real figures were 75.6%/74.5%, p=0.64.
    assert "pre-period data for 100.0% of control / 100.0% of treatment" in bal, bal


def test_a_covariate_the_treatment_moved_is_withheld_from_the_gate():
    """The fallback that keeps the adjustment honest."""
    primary, covariate = _cuped_rows(covariate_shift=4)
    readout = analyse_ab(
        _spec(cuped_covariate="pre_period_like_rate"), _cuped_reader(primary, covariate)
    )
    line = next(l for l in readout.lines if "CUPED" in l)
    assert "WITHHELD from the gate" in line, line
    assert any("would subtract real signal" in l for l in readout.lines), readout.render()

    # And the gate really is the unadjusted one.
    plain = analyse_ab(_spec(), _cuped_reader(primary, covariate))
    assert readout.lines[0] == plain.lines[0], (readout.lines[0], plain.lines[0])


def test_a_lone_straddler_is_flagged_without_crying_wolf():
    primary = _weight_trap_rows() + [("cl0", 250, 3, 20)]
    readout = analyse_ab(_spec(), FakeReader(primary, _null_control()))
    assert "BOTH arms" in readout.lines[0], readout.lines[0]
    assert "watch it rather than act on it" in readout.lines[0], readout.lines[0]
    assert "Fix before reading" not in readout.lines[0]


def test_widespread_straddling_does_demand_a_fix():
    extra = [(f"cl{i}", 250, 3, 20) for i in range(40)]
    readout = analyse_ab(_spec(), FakeReader(_weight_trap_rows() + extra, _null_control()))
    assert "Fix before reading" in readout.lines[0], readout.lines[0]


# --- The secondary gets the same peeking protection ------------------------------------------
#
# scroll_depth is read hourly like everything else. Being a SECONDARY means it does not move the
# top-line verdict, not that it may be read loosely -- and on 2026-09-01 it was the only
# significant-looking number left once the primary was gated (+14.0%, p=0.0155).


def _scroll_reader(primary, scroll, control=None):
    class R:
        def query(self, sql):
            if "cap AS (" in sql:
                return scroll
            return primary if "source = 'fallback'" not in sql else (control or _null_control())

    return R()


def test_scroll_depth_carries_its_own_anytime_valid_gate():
    # Counts vary within arm, so the variance estimate is real rather than float residue.
    scroll = []
    for k in range(3):
        scroll += _uneven(f"sc{k}_", 100, 8 + k, 1, 10000)
        scroll += _uneven(f"st{k}_", 100, 14 + k, 1, 250)
    readout = analyse_ab(_scroll_spec(), _scroll_reader(_weight_trap_rows(), scroll))

    gate = next(l for l in readout.lines if "scroll_depth" in l and "GATE" in l)
    # Labelled as a COUNT, not a rate -- the denominator is 1 per unit here.
    assert "winsorized impressions/user" in gate, gate
    assert "unweighted per-unit rate" not in gate, gate
    assert "arm DIFFERENCE" in gate, gate
    # A real, large gap in impressions/user must clear it.
    assert "SEPARATED FROM ZERO" in gate, gate


def test_scroll_depths_fixed_horizon_line_is_marked_diagnostic():
    scroll = _uneven("sc", 150, 8, 1, 10000) + _uneven("st", 150, 9, 1, 250)
    readout = analyse_ab(_scroll_spec(), _scroll_reader(_weight_trap_rows(), scroll))
    idx = next(i for i, l in enumerate(readout.lines) if "scroll_depth" in l and "GATE" in l)
    assert "[fixed-horizon, diagnostic]" in readout.lines[idx + 1], readout.lines[idx + 1]
    assert "secondary outcome" in readout.lines[idx + 2], readout.lines[idx + 2]


def test_a_significant_secondary_whose_sequence_is_quiet_is_flagged_not_believed():
    """The 2026-09-01 shape: fixed-horizon significant, sequence includes zero.

    NOTE the mechanism differs from the primary's. scroll_depth carries a denominator of 1 per
    unit, so impression weighting is uniform and both estimands agree exactly (+11.6129 here).
    The divergence is purely the peeking correction: a boundary of ~3.7 SE against 1.96 SE. A
    heavy tail is what makes the SE large enough for that to bite -- 10 users of 310 hold most of
    the mass, which is the real shape of this data.
    """
    scroll = (
        _uneven("scl", 300, 1, 1, 10000)
        + _uneven("sch", 10, 40, 1, 10000)
        + _uneven("stl", 300, 1, 1, 250)
        + _uneven("sth", 10, 400, 1, 250)
    )
    readout = analyse_ab(_scroll_spec(), _scroll_reader(_weight_trap_rows(), scroll))
    idx = next(i for i, l in enumerate(readout.lines) if "scroll_depth" in l and "GATE" in l)
    gate, fixed = readout.lines[idx], readout.lines[idx + 1]

    # Precondition, asserted rather than assumed: the two genuinely disagree here.
    assert "SIGNIFICANT" in fixed and "not significant" not in fixed, fixed
    assert "includes zero" in gate, gate
    # ...so the reader must be told which one holds.
    assert "the sequence is the one that holds" in readout.lines[idx + 3], readout.lines[idx + 3]


def test_the_secondary_gate_never_moves_the_top_line_verdict():
    """A secondary may not promote itself into the verdict, gated or not."""
    # Primary gate is quiet; scroll depth is enormous and clears its own gate.
    scroll = []
    for k in range(3):
        scroll += _uneven(f"sc{k}_", 100, 1 + k, 1, 10000)
        scroll += _uneven(f"st{k}_", 100, 90 + k, 1, 250)
    readout = analyse_ab(_scroll_spec(), _scroll_reader(_weight_trap_rows(), scroll))

    scroll_gate = next(l for l in readout.lines if "scroll_depth" in l and "GATE" in l)
    assert "SEPARATED FROM ZERO" in scroll_gate, scroll_gate
    # ...and the verdict still reflects the PRIMARY only.
    assert "NO EFFECT ESTABLISHED" in readout.verdict, readout.render()
    # The primary's gate leads; the secondary sits below it.
    assert readout.lines[0].strip().startswith("GATE")
    primary_at = next(i for i, l in enumerate(readout.lines) if "primary" in l)
    scroll_at = next(i for i, l in enumerate(readout.lines) if "scroll_depth" in l)
    assert scroll_at > primary_at
