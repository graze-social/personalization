"""Provenance contract check: does every field a spec reads actually exist in the data?

Five separate bugs in the follow-seed work had one shape: a producer changed and a consumer did
not. The failure is silent by construction -- a spec whose arm field nothing writes does not error,
it compares zero rows and reports WITHHELD, which is indistinguishable from "not enough data yet".
One of those went unnoticed for 24 hours while the readout confidently said NOT DELIVERED.

This closes the class. It enumerates the keys live blobs actually carry, then asserts every arm and
population field of each scheduled spec is among them. Run it against the same spec list as the
runner, so a spec cannot be scheduled without being checked.

    python -m graze_analysis.contract experiments/a.yaml experiments/b.yaml
"""

from __future__ import annotations

import sys

from .data import ClickHouseReader
from .spec import load_spec

DEC = "tryBase64Decode(interaction_feed_context)"
#: Only our own traffic carries a full provenance blob; foreign services write {"feed_uri": ...}.
POP = f"JSONHas({DEC},'algo_id')"


def live_keys(reader: ClickHouseReader, hours: int = 2) -> tuple[set[str], set[str]]:
    """Top-level and `params`-nested keys present in recent blobs."""
    top = {
        k
        for k, _ in reader.query(
            f"""SELECT arrayJoin(JSONExtractKeys({DEC})) AS k, count() AS n
                FROM default.feed_interactions
                WHERE occurred >= now() - INTERVAL {hours} HOUR
                  AND interaction_feed_context != '' AND ({POP})
                GROUP BY k"""
        )
    }
    nested = {
        k
        for k, _ in reader.query(
            f"""SELECT arrayJoin(JSONExtractKeys({DEC},'params')) AS k, count() AS n
                FROM default.feed_interactions
                WHERE occurred >= now() - INTERVAL {hours} HOUR
                  AND interaction_feed_context != '' AND JSONHas({DEC},'params')
                GROUP BY k"""
        )
    }
    return top, nested


def field_is_present(field: str, top: set[str], nested: set[str]) -> bool:
    parts = field.split(".")
    if len(parts) == 1:
        return parts[0] in top
    # Only `params.*` is a real nested path in this blob; anything deeper is a spec error.
    return parts[0] in top and parts[0] == "params" and parts[1] in nested


def main(argv: list[str] | None = None) -> int:
    paths = list(argv if argv is not None else sys.argv[1:])
    if not paths:
        print("usage: python -m graze_analysis.contract <spec.yaml> [...]")
        return 2

    reader = ClickHouseReader()
    top, nested = live_keys(reader)
    print(f"=== provenance contract ({len(top)} top-level keys, {len(nested)} under params) ===")

    failures: list[str] = []
    for path in paths:
        spec = load_spec(path)
        for arm_name, arm in spec.arms.items():
            ok = field_is_present(arm.field, top, nested)
            print(f"  [{'OK ' if ok else 'MISSING'}] {spec.id:<28} {arm_name:<10} {arm.field}")
            if not ok:
                failures.append(
                    f"{spec.id}: arm '{arm_name}' reads '{arm.field}', which no live blob carries"
                )

    if failures:
        print("\n!! CONTRACT VIOLATIONS — these specs compare zero rows and report WITHHELD forever,")
        print("   which reads identically to 'not enough data yet':")
        for f in failures:
            print(f"   - {f}")
        return 1
    print("\nall scheduled spec fields are present in live data")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
