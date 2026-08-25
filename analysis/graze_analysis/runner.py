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
    lines.insert(0, f"  primary  : {primary.describe()}")

    perm = permutation_rate_diff(
        primary_rows.unit_ids, primary_rows.arm, primary_rows.numerator, primary_rows.denominator
    )
    lines.append(f"  permutation: {perm.describe()}")

    per_unit_rate = primary_rows.numerator / np.maximum(primary_rows.denominator, 1.0)
    _, low, high = always_valid_ci(per_unit_rate)
    lines.append(
        f"  anytime-valid CI on the per-unit rate: [{low:+.4f}, {high:+.4f}] "
        "(safe to inspect repeatedly)"
    )

    if spec.cuped_covariate:
        pre = {str(u): (float(l), float(s)) for u, l, s in reader.query(cuped_covariate_sql(spec))}
        cov = np.array(
            [
                (pre[u][0] / pre[u][1]) if u in pre and pre[u][1] else 0.0
                for u in primary_rows.unit_ids
            ]
        )
        _, reduction = cuped_adjust(per_unit_rate, cov)
        lines.append(f"  CUPED variance reduction: {reduction:.1%}")

    agree = primary.significant == (perm.p_value < 0.05)
    verdict = (
        "SIGNIFICANT" if primary.significant and agree else
        "INCONCLUSIVE (tests disagree)" if not agree else
        "NOT SIGNIFICANT"
    )
    return Readout(spec.id, verdict, lines)


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
