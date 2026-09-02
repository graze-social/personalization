"""Experiment readout.

Ordering here is deliberate and not merely stylistic: the **gates run before the effect is ever
printed**, so a withheld result cannot be read as a number with a caveat attached. Both false
positives in this system's history were believed because someone (me) saw the effect first and the
caveat second.
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .data import (
    METRIC_EVENTS,
    Rows,
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
    covariate_balance,
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




#: How each supported primary metric is described on the gate line. The default label would call a
#: winsorized impression COUNT a "rate", which it is not.
PRIMARY_ESTIMANDS = {
    "scroll_depth": "winsorized impressions/user",
}


def _primary_metric_rows(
    spec: ExperimentSpec, reader: ClickHouseReader, where_extra: str = ""
) -> "Rows":
    """Rows for whichever metric the spec nominates as primary.

    `primary_metric` was declared in every spec from the start and READ BY NOTHING: `analyse_ab`
    called `ab_rows_sql(spec)`, whose numerator defaults to a like, so the primary was always the
    like rate no matter what the YAML said. A spec asking for `request_more_rate` would have been
    answered with like_rate, silently. Same failure shape as the rest of this file's history: a
    declaration the code did not implement.

    Negative controls route through here too, so a control is always measured on the same metric as
    the primary it guards.
    """
    if spec.primary_metric == "scroll_depth":
        quantile = spec.scroll_depth.winsorize_quantile
        return rows_from_ab_result(
            reader.query(scroll_depth_sql(spec, quantile, where_extra=where_extra)), spec
        )
    event = METRIC_EVENTS[spec.primary_metric]
    return rows_from_ab_result(
        reader.query(ab_rows_sql(spec, where_extra=where_extra, numerator_event=event)), spec
    )


def analyse_ab(spec: ExperimentSpec, reader: ClickHouseReader) -> Readout:
    lines: list[str] = []

    # Unknown primary is FATAL, unlike an unknown guardrail (which is skipped loudly). There is no
    # readout without a primary, and silently substituting one is how `primary_metric` came to be
    # ignored in the first place.
    if spec.primary_metric != "scroll_depth" and spec.primary_metric not in METRIC_EVENTS:
        known = ", ".join(sorted({*METRIC_EVENTS, "scroll_depth"}))
        return Readout(
            spec.id,
            "WITHHELD (unknown primary metric)",
            [f"  primary_metric {spec.primary_metric!r} is not measurable here (known: {known})"],
        )

    primary_rows = _primary_metric_rows(spec, reader)
    primary = cluster_robust_rate_diff(
        primary_rows.unit_ids, primary_rows.arm, primary_rows.numerator, primary_rows.denominator
    )

    # --- Gate 1: enough data to form any verdict at all ---
    thin, thin_msg = insufficient_data_gate(primary, spec.min_observations, spec.min_units)
    if thin:
        return Readout(spec.id, "WITHHELD (insufficient data)", [thin_msg])

    # --- Gate 2: negative controls, before the effect is shown ---
    verdicts: list[ControlVerdict] = []
    for nc in spec.negative_controls:
        rows = _primary_metric_rows(spec, reader, where_extra=nc.where)
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
        # A control that passes only because it is too thin to reach significance is not
        # protection, and it renders identically to a genuinely flat one. Measured 2026-09-02 on
        # the scroll_depth primary: pinned_and_rotating had 44 units against the primary's 267 and
        # a point estimate of -0.8727 against the primary's -0.4633 -- nearly double, same
        # direction -- and cleared the gate purely for lack of significance. Say so, in the same
        # spirit as the guardrail lines' event counts.
        if (
            not est.significant
            and math.isfinite(est.diff)
            and math.isfinite(primary.diff)
            and abs(est.diff) >= abs(primary.diff)
        ):
            share = est.n_units / primary.n_units if primary.n_units else float("nan")
            lines.append(
                f"    !! this control's point estimate ({est.diff:+.4f}) is at least as large as "
                f"the primary's ({primary.diff:+.4f}) and it passes only for lack of "
                f"significance, on {est.n_units} units ({share:.0%} of the primary's). Weak "
                "protection, not a clean placebo."
            )

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

    # CUPED adjusts the outcome BEFORE the gate is formed -- but only after the covariate is shown
    # to be something the treatment cannot have moved. That order matters, and it is the second
    # thing this readout got wrong: the covariate was first computed and discarded into `_`, then
    # (2026-09-01) demoted to a diagnostic on the theory that a reset window's pre-period is
    # "already under treatment", because the imbalance pointed the same way as the effect.
    # Re-randomizing the arm labels refuted that -- observed +0.00242 against a null sd of 0.00325,
    # p=0.459, sign REVERSING in an earlier window. It is chance, and a pre-treatment window is
    # measurably WORSE (29.3% reduction against 46.2%) because it is further from the outcome.
    # So it adjusts, and the balance check below is what keeps that honest.
    outcome, reduction, balance = per_unit_rate, 0.0, None
    if spec.cuped_covariate:
        pre = {str(u): (float(l), float(s_)) for u, l, s_ in reader.query(cuped_covariate_sql(spec))}
        covered = np.array([u in pre and pre[u][1] > 0 for u in primary_rows.unit_ids])
        # `pre_period_impressions` uses the raw pre-window impression count; anything else uses
        # the pre-window like RATE. Which one is right depends on the primary: adjusting a
        # winsorized impression count by a like rate buys almost nothing on a population whose
        # like rate is ~0 (measured 3.4% reduction), while its own past impressions predict it
        # strongly. The covariate must match the outcome or CUPED is decoration.
        if spec.cuped_covariate == "pre_period_impressions":
            cov = np.array(
                [float(pre[u][1]) if u in pre else 0.0 for u in primary_rows.unit_ids]
            )
        else:
            cov = np.array(
                [
                    (pre[u][0] / pre[u][1]) if u in pre and pre[u][1] else 0.0
                    for u in primary_rows.unit_ids
                ]
            )
        imbalance, p_bal = covariate_balance(cov, treated, covered)
        adjusted, reduction = cuped_adjust(per_unit_rate, cov)
        # A covariate the treatment moved would subtract real signal. Fall back rather than guess.
        balanced = not (p_bal == p_bal) or p_bal > 0.05  # nan -> too little data to reject
        if balanced:
            outcome = adjusted
        balance = (imbalance, p_bal, covered, balanced, reduction)
        # Only advertise a reduction that was actually taken. A withheld adjustment that still
        # printed `cuped=60.0%` on the gate line would claim precision the interval does not have.
        applied_reduction = reduction if balanced else 0.0
    else:
        applied_reduction = 0.0

    seq = always_valid_diff_ci(
        outcome[~treated],
        outcome[treated],
        variance_reduction=applied_reduction,
        estimand=PRIMARY_ESTIMANDS.get(spec.primary_metric, "unweighted per-unit rate"),
    )

    lines.insert(0, f"  GATE     : {seq.describe()}")
    lines.insert(1, f"  primary  : {primary.describe()}   [fixed-horizon, diagnostic]")
    if straddlers:
        share = straddlers / max(len(set(primary_rows.unit_ids)), 1)
        note = (
            " Fix before reading."
            if share > 0.01
            else " Too few to move the estimate; watch it rather than act on it."
        )
        lines.insert(
            0,
            f"  !! {straddlers} unit(s) ({share:.2%}) appear in BOTH arms -- the gate assumes "
            f"independent arms.{note}",
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

    if balance is not None:
        imbalance, p_bal, covered, balanced, reduction = balance
        state = "APPLIED to the gate" if balanced else "WITHHELD from the gate"
        lines.append(f"  CUPED variance reduction: {reduction:.1%} ({state})")
        lines.append(
            f"    covariate balance: {imbalance:+.5f} between arms, re-randomization p={p_bal:.3f}"
            f"; pre-period data for {covered[~treated].mean():.1%} of control / "
            f"{covered[treated].mean():.1%} of treatment units"
        )
        if not balanced:
            lines.append(
                "    !! the treatment appears to have MOVED the covariate, so adjusting for it "
                "would subtract real signal. The gate above is unadjusted."
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
    if spec.scroll_depth is None or spec.primary_metric == "scroll_depth":
        # When it IS the primary it is already the gate; repeating it as a "secondary" would
        # invite reading the same number twice as if it were corroboration.
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
