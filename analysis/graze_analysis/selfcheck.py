"""The interleaving harness's own negative control.

Interleave a ranker **against itself**. Any competitive pair the harness reports is then, by
construction, not a preference — it is noise in the harness or in the ranker. This must pass before
any real comparison, because that noise is indistinguishable from a treatment effect and silently
consumes the sensitivity interleaving exists to buy.

**The criterion is a tolerance, not zero.** My first version asserted zero pairs and the check
"failed" at 2 pairs in 4,192 shared items. That assertion was wrong, for a reason worth stating:
the two arms are scored sequentially against a **live** like stream, so a like landing in Valkey in
the milliseconds between them makes one more post scorable for the second arm. Observed exactly
that — ``control_scored=23, treatment_scored=24`` on the single disagreeing draft. It is noise, not
bias, because draft order is coin-flipped per user, so the extra item is equally likely to land in
either arm.

So the honest question is not "are there zero pairs" but "is the disagreement rate small enough that
it cannot be mistaken for an effect". The floor is what matters: a 6% floor swamps the ~1-5% effects
we are looking for, a 0.05% floor does not.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

#: Fraction of shared items that may disagree before the harness is considered unsound.
#: Set an order of magnitude below the smallest effect worth detecting (~1%), so a real effect can
#: never be confused with the floor.
MAX_DISAGREEMENT_RATE = 0.005

#: Relative imbalance allowed between the two arms' scored counts. Both arms run the same ranker, so
#: a systematic difference here means they are not seeing the same candidate set — a real defect,
#: unlike the incidental single-post races above.
MAX_SCORED_IMBALANCE = 0.01


@dataclass(frozen=True)
class Draft:
    competitive_pairs: int
    shared_items: int
    control_scored: int
    treatment_scored: int


@dataclass(frozen=True)
class SelfCheckResult:
    drafts: int
    pairs: int
    shared: int
    control_scored: int
    treatment_scored: int
    passed: bool
    reason: str

    @property
    def disagreement_rate(self) -> float:
        total = self.shared + self.pairs
        return self.pairs / total if total else 0.0

    @property
    def scored_imbalance(self) -> float:
        total = self.control_scored + self.treatment_scored
        if not total:
            return 0.0
        return abs(self.treatment_scored - self.control_scored) / (total / 2)

    def render(self) -> str:
        verdict = "PASS" if self.passed else "FAIL"
        return "\n".join(
            [
                f"=== interleave self-check — {verdict} ===",
                f"  drafts observed     : {self.drafts}",
                f"  competitive pairs   : {self.pairs} of {self.shared + self.pairs} items "
                f"({self.disagreement_rate:.3%}, tolerance {MAX_DISAGREEMENT_RATE:.1%})",
                f"  scored control/trt  : {self.control_scored} / {self.treatment_scored} "
                f"(imbalance {self.scored_imbalance:.3%}, tolerance {MAX_SCORED_IMBALANCE:.0%})",
                f"  {self.reason}",
            ]
        )


def evaluate(drafts: Iterable[Draft], min_drafts: int = 25) -> SelfCheckResult:
    """Apply the criterion. Too few drafts is *withheld*, never a pass."""
    drafts = list(drafts)
    pairs = sum(d.competitive_pairs for d in drafts)
    shared = sum(d.shared_items for d in drafts)
    ctl = sum(d.control_scored for d in drafts)
    trt = sum(d.treatment_scored for d in drafts)
    result = SelfCheckResult(len(drafts), pairs, shared, ctl, trt, False, "")

    if len(drafts) < min_drafts:
        return _with(result, False, f"WITHHELD: {len(drafts)} drafts is below the {min_drafts} minimum")
    if result.disagreement_rate > MAX_DISAGREEMENT_RATE:
        return _with(
            result,
            False,
            f"the disagreement floor is {result.disagreement_rate:.2%}; a treatment effect of that "
            "size could not be distinguished from harness noise",
        )
    if result.scored_imbalance > MAX_SCORED_IMBALANCE:
        return _with(
            result,
            False,
            "the arms are not scoring the same candidate set, which no amount of data will fix",
        )
    return _with(
        result,
        True,
        "residual disagreement is consistent with likes landing between the two arms' reads, "
        "which is noise and not bias (draft order is coin-flipped per user)",
    )


def _with(r: SelfCheckResult, passed: bool, reason: str) -> SelfCheckResult:
    return SelfCheckResult(
        r.drafts, r.pairs, r.shared, r.control_scored, r.treatment_scored, passed, reason
    )


def drafts_from_logs(lines: Iterable[str]) -> list[Draft]:
    """Parse draft telemetry out of the service's JSON log stream."""
    out: list[Draft] = []
    for line in lines:
        if "competitive_pairs" not in line:
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        f = record.get("fields", record)
        if "competitive_pairs" not in f:
            continue
        out.append(
            Draft(
                competitive_pairs=int(f.get("competitive_pairs", 0)),
                shared_items=int(f.get("shared_items", 0)),
                control_scored=int(f.get("control_scored", 0)),
                treatment_scored=int(f.get("treatment_scored", 0)),
            )
        )
    return out


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - thin CLI
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="Evaluate an interleaving self-check log dump.")
    ap.add_argument("log", nargs="?", help="path to a JSON log dump; defaults to stdin")
    args = ap.parse_args(argv)
    lines = open(args.log) if args.log else sys.stdin
    result = evaluate(drafts_from_logs(lines))
    print(result.render())
    return 0 if result.passed else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
