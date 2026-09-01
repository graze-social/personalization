"""Experiment readout.

Ordering here is deliberate and not merely stylistic: the **gates run before the effect is ever
printed**, so a withheld result cannot be read as a number with a caveat attached. Both false
positives in this system's history were believed because someone (me) saw the effect first and the
caveat second.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .data import (
    METRIC_EVENTS,
    scroll_depth_sql,
    ClickHouseReader,
    ab_rows_sql,
    cuped_covariate_sql,
    interleaving_rows_sql,
    rows_from_ab_result,
)
from .spec import ExperimentSpec, load_spec
from .stats import (
    ControlVerdict,
    Estimate,
    always_valid_ci,
    always_valid_diff_ci,
    cluster_robust_rate_diff,
    control_moved,
    cuped_adjust,
    insufficient_data_gate,
    negative_control_gate,
    permutation_rate_diff,
)


@dataclass
class Readout:
    spec_id: str
    verdict: str
    lines: list[str]

    def render(self) -> str:
        head = f"=== {self.spec_id} — {self.verdict} ==="
        return "\n".join([head, *self.lines])




def analyse_ab(spec: ExperimentSpec, reader: ClickHouseReader) -> Readout:
    lines: list[str] = []

    primary_rows = rows_from_ab_result(reader.query(ab_rows_sql(spec)), spec)
    primary = cluster_robust_rate_diff(
        primary_rows.unit_ids, primary_rows.arm, primary_rows.numerator, primary_rows.denominator
    )

    # --- Gate 1: enough data to form any verdict at all ---
    thin, thin_msg = insufficient_data_gate(primary, spec.min_observations)
    if thin:
        return Readout(spec.id, "WITHHELD (insufficient data)", [thin_msg])

    # --- Gate 2: negative controls, before the effect is shown ---
    verdicts: list[ControlVerdict] = []
    for nc in spec.negative_controls:
        rows = rows_from_ab_result(reader.query(ab_rows_sql(spec, where_extra=nc.where)), spec)
        est = cluster_robust_rate_diff(
            rows.unit_ids, rows.arm, rows.numerator, rows.denominator
        )
        verdicts.append(
            ControlVerdict(
                name=nc.name,
                reason=nc.reason,
                estimate=est,
                moved=control_moved(est, primary),
            )
        )
        lines.append(f"  control {nc.name}: {est.describe()}")

    withheld, gate_msg = negative_control_gate(verdicts)
    if withheld:
        return Readout(spec.id, "WITHHELD (negative control moved)", [gate_msg, *lines])

    # --- Only now is it legitimate to report the effect ---
    #
    # ORDERING IS THE POINT. The gate is the confidence sequence on the arm DIFFERENCE, and it is
    # printed first. Everything under it is a fixed-horizon diagnostic whose nominal error rate
    # does not apply here, because this job runs hourly and is read whenever: the holdout window
    # opened 2026-08-28 and its WLS p crossed 0.05 on roughly the 96th look (2026-09-01,
    # p=0.0466) while the distribution-free permutation test sat flat at ~0.238 through every
    # window. That is the signature of alpha inflation, not of an effect emerging.
    #
    # The harness did previously print an "anytime-valid" line, but on the per-unit rate POOLED
    # across arms -- an interval on a level, which for a non-negative rate cannot contain zero,
    # occupying the slot a reader scans for sequential evidence. Same failure shape as the six
    # before it: a claim in a comment that the code did not implement.
    per_unit_rate = primary_rows.numerator / np.maximum(primary_rows.denominator, 1.0)

    # A unit must sit in exactly one arm. The holdout window was once reset because a per-REQUEST
    # coin flip put 18% of users in both arms, and the sequence's variance assumes independent
    # arms, so this is checked rather than assumed.
    first_arm: dict[str, float] = {}
    straddlers = 0
    for u, a in zip(primary_rows.unit_ids, primary_rows.arm):
        if u not in first_arm:
            first_arm[u] = a
        elif first_arm[u] != a:
            straddlers += 1

    treated = primary_rows.arm > 0.5
    # The gate is the UNADJUSTED difference. CUPED is computed and shown below, but deliberately
    # does not drive the verdict: `cuped_covariate_sql` measures the 14 days before `spec.start`,
    # and for a window that was RESET rather than begun fresh, that period is already under
    # treatment. Measured 2026-09-01 on the holdout -- pre-period like rate 0.00839 control vs
    # 0.00999 treatment, an "imbalance" pointing the same way as the effect, because the holdout
    # has suppressed personalization for its control arm continuously since 2026-08-14.
    # Subtracting that removes real signal rather than noise: it shrank the estimate from
    # +0.00180 to +0.00045. Coverage is thin besides (11.9% of control units, 17.3% of treatment
    # have any pre-period row), so the reduction rests on a minority of the sample.
    seq = always_valid_diff_ci(per_unit_rate[~treated], per_unit_rate[treated])

    lines.insert(0, f"  GATE     : {seq.describe()}")
    lines.insert(1, f"  primary  : {primary.describe()}   [fixed-horizon, diagnostic]")
    if straddlers:
        lines.insert(
            0,
            f"  !! {straddlers} unit(s) appear in BOTH arms -- randomization is leaking and the "
            "gate's independence assumption does not hold. Fix before reading.",
        )

    perm = permutation_rate_diff(
        primary_rows.unit_ids, primary_rows.arm, primary_rows.numerator, primary_rows.denominator
    )
    lines.append(f"  permutation: {perm.describe()}   [fixed-horizon, diagnostic]")

    _, low, high = always_valid_ci(per_unit_rate)
    lines.append(
        f"  pooled base rate, both arms: [{low:+.4f}, {high:+.4f}] "
        "(a LEVEL, not an effect -- cannot contain zero)"
    )

    if spec.cuped_covariate:
        pre = {str(u): (float(l), float(s)) for u, l, s in reader.query(cuped_covariate_sql(spec))}
        cov = np.array(
            [
                (pre[u][0] / pre[u][1]) if u in pre and pre[u][1] else 0.0
                for u in primary_rows.unit_ids
            ]
        )
        adjusted, reduction = cuped_adjust(per_unit_rate, cov)
        adj_seq = always_valid_diff_ci(
            adjusted[~treated], adjusted[treated], variance_reduction=reduction
        )
        lines.append(
            f"  CUPED variance reduction: {reduction:.1%}; adjusted gate would be "
            f"[{adj_seq.low:+.4f}, {adj_seq.high:+.4f}]"
        )
        lines.append(
            "    (DIAGNOSTIC, not the gate: the pre-period overlaps the treatment on a reset "
            f"window, and pre-period coverage is {(cov[~treated] > 0).mean():.1%} control / "
            f"{(cov[treated] > 0).mean():.1%} treatment)"
        )

    lines.extend(_scroll_depth_lines(spec, reader))
    lines.extend(_guardrail_lines(spec, reader))

    # The gate decides. The fixed-horizon tests only colour the message.
    fixed_agree = primary.significant and perm.p_value < 0.05
    if seq.conclusive:
        verdict = "EFFECT ESTABLISHED (anytime-valid)"
        if not fixed_agree:
            verdict += " -- fixed-horizon tests disagree"
    else:
        verdict = "NO EFFECT ESTABLISHED (sequence includes zero)"
        if primary.significant:
            verdict += " -- WLS reads significant, uncorrected for repeated inspection"
    return Readout(spec.id, verdict, lines)


def _scroll_depth_lines(spec: ExperimentSpec, reader: ClickHouseReader) -> list[str]:
    """Winsorized impressions-per-user, reported ALONGSIDE the primary and never in place of it.

    It exists because the primary is starved: ~1,065 likes against 31,182 impressions. But density
    is not power on its own -- raw impressions/user has an MDE of ~31% of the mean because 3.3% of
    users carry 36% of the impressions. Winsorizing at the declared quantile is what makes it
    usable, taking the MDE to roughly 14-18%.

    This is a SECONDARY outcome. More scrolling is not unambiguously better: this codebase already
    measured zero-seed users paginating at a ~1s cadence, which is scanning rather than reading. A
    move here is a prompt to investigate, not a result on its own.

    It gets the same anytime-valid treatment as the primary, and for the same reason: it is read
    hourly. It needed it MORE, not less. On 2026-09-01 this was the only significant-looking number
    left in the readout once the primary was gated (+14.0%, p=0.0155) -- which is exactly the
    position a fixed-horizon test should not be trusted from. Being a secondary means it does not
    move the top-line verdict; it does not mean it may be read loosely.
    """
    if spec.scroll_depth is None:
        return []
    q = spec.scroll_depth.winsorize_quantile
    rows = rows_from_ab_result(reader.query(scroll_depth_sql(spec, q)), spec)
    if len(rows) == 0:
        return [f"  scroll_depth (winsorized p{int(q * 100)}): no rows"]
    est = cluster_robust_rate_diff(rows.unit_ids, rows.arm, rows.numerator, rows.denominator)
    # Denominator is 1 per unit here, so this is the winsorized impression COUNT per user, not a
    # rate. The estimand is labelled accordingly rather than inheriting the default.
    treated = rows.arm > 0.5
    per_unit = rows.numerator / np.maximum(rows.denominator, 1.0)
    seq = always_valid_diff_ci(
        per_unit[~treated], per_unit[treated], estimand="winsorized impressions/user"
    )
    out = [
        f"  scroll_depth (winsorized p{int(q * 100)}) GATE: {seq.describe()}",
        f"    {est.describe()}   [fixed-horizon, diagnostic]",
        f"    (secondary outcome; cap declared in the spec, pooled across arms, "
        f"units={len(rows):,})",
    ]
    if est.significant and not seq.conclusive:
        out.append(
            "    !! the fixed-horizon test reads significant and the sequence does not. This line "
            "is read hourly, so the sequence is the one that holds."
        )
    return out


def _guardrail_lines(spec: ExperimentSpec, reader: ClickHouseReader) -> list[str]:
    """Evaluate each guardrail against the same randomization as the primary metric.

    A guardrail is *not* a negative control and is deliberately handled differently. A negative
    control that moves means the experiment is broken, so it withholds the result. A guardrail that
    moves means the change had a cost that is real and worth seeing — the result stands, and the
    reader decides. Withholding here would hide the very thing the guardrail exists to surface.

    Breach reporting mirrors the density drift guard's discipline: BREACH only when the confidence
    interval clears the bound (a detection), WATCH when the point estimate clears it but the
    interval does not (a prediction). The bound's sign is in the spec's own arm direction — for the
    holdout spec, `control` is the holdout, so a positive diff means the treatment produced *more*
    of the event.
    """
    if not spec.guardrails:
        return []

    out = []
    for g in spec.guardrails:
        event = METRIC_EVENTS.get(g.metric)
        if event is None:
            out.append(
                f"  guardrail {g.metric}: SKIPPED — unknown metric "
                f"(known: {', '.join(sorted(METRIC_EVENTS))})"
            )
            continue

        rows = rows_from_ab_result(
            reader.query(ab_rows_sql(spec, numerator_event=event)), spec
        )
        events = int(rows.numerator.sum())
        est = cluster_robust_rate_diff(
            rows.unit_ids, rows.arm, rows.numerator, rows.denominator
        )

        status = "ok"
        if g.max is not None:
            if est.ci_low > g.max:
                status = f"BREACH (CI low {est.ci_low:+.4f} > max {g.max:+.4f})"
            elif est.diff > g.max:
                status = f"watch (point {est.diff:+.4f} > max {g.max:+.4f}, CI does not clear it)"
        if status == "ok" and g.min is not None:
            if est.ci_high < g.min:
                status = f"BREACH (CI high {est.ci_high:+.4f} < min {g.min:+.4f})"
            elif est.diff < g.min:
                status = f"watch (point {est.diff:+.4f} < min {g.min:+.4f}, CI does not clear it)"

        out.append(f"  guardrail {g.metric}: {est.describe()} [{status}]")
        # State the raw event count unconditionally. These signals are rare — measured at 221
        # requestLess events across 11 days — and an underpowered "ok" must never be mistaken for
        # evidence of safety.
        out.append(f"    (n={events} {g.metric} events; a quiet guardrail here is weak evidence)")
    return out


def analyse_interleaving(spec: ExperimentSpec, reader: ClickHouseReader) -> Readout:
    """Per-user preference readout for a team-draft experiment."""
    rows = reader.query(interleaving_rows_sql(spec))
    if not rows:
        return Readout(spec.id, "WITHHELD (no tagged impressions)", ["  no interleaved items found"])

    tau = np.array([float(t) - float(c) for _, t, c, _ in rows])
    tagged = int(sum(int(x[3]) for x in rows))
    users = tau.size

    if tagged < spec.min_observations:
        return Readout(
            spec.id,
            "WITHHELD (insufficient data)",
            [f"  {tagged} tagged impressions is below the {spec.min_observations} minimum"],
        )

    prefer_t = int((tau > 0).sum())
    prefer_c = int((tau < 0).sum())
    decided = prefer_t + prefer_c
    lines = [
        f"  users with a preference: {decided} of {users} "
        f"(treatment {prefer_t}, control {prefer_c})",
        f"  tagged impressions: {tagged}",
    ]

    if decided == 0:
        # This is exactly what a self-check run should produce.
        lines.append(
            "  no user preferred either ranker — expected when both arms are the same ranker "
            "(the harness's own negative control)"
        )
        return Readout(spec.id, "NO PREFERENCE (self-check clean)", lines)

    # Sign test on users, which respects the unit of randomization.
    p = _two_sided_binomial(prefer_t, decided)
    _, low, high = always_valid_ci(np.sign(tau[tau != 0]))
    lines.append(f"  sign test on users: p={p:.4f}")
    lines.append(f"  anytime-valid CI on mean preference: [{low:+.3f}, {high:+.3f}]")
    verdict = "TREATMENT WINS" if p < 0.05 and prefer_t > prefer_c else (
        "CONTROL WINS" if p < 0.05 else "NOT SIGNIFICANT"
    )
    return Readout(spec.id, verdict, lines)


def _two_sided_binomial(k: int, n: int, p: float = 0.5) -> float:
    """Exact two-sided binomial test, no scipy dependency."""
    from math import comb

    if n == 0:
        return 1.0
    obs = comb(n, k) * p**k * (1 - p) ** (n - k)
    total = 0.0
    for i in range(n + 1):
        prob = comb(n, i) * p**i * (1 - p) ** (n - i)
        if prob <= obs + 1e-15:
            total += prob
    return min(total, 1.0)


def run(spec: ExperimentSpec, reader: ClickHouseReader) -> Readout:
    if spec.design == "interleaving":
        return analyse_interleaving(spec, reader)
    return analyse_ab(spec, reader)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Analyse a personalization experiment.")
    ap.add_argument("specs", nargs="+", help="paths to experiment spec YAML files")
    args = ap.parse_args(argv)

    reader = ClickHouseReader()
    failures = 0
    for path in args.specs:
        try:
            spec = load_spec(Path(path))
            print(run(spec, reader).render())
        except Exception as exc:  # noqa: BLE001 - a readout failure must be loud, not silent
            failures += 1
            print(f"=== {path} — ERROR ===\n  {type(exc).__name__}: {exc}", file=sys.stderr)
        print()
    return 1 if failures else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
