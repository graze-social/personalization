"""ClickHouse extraction for experiment analysis.

The provenance blob (``interaction_feed_context``) is a base64 JSON document stored verbatim
alongside every impression and interaction, which is what makes retrospective analysis possible at
all. This module is the only place that knows its shape.

Two invariants are enforced here rather than left to the caller:

- **Rows before ``spec.start`` are excluded.** Before the experiment began, the same field was set
  by adaptive Thompson sampling, so including those rows silently reintroduces the confounded
  assignment the randomized design exists to avoid. This mistake was made once already.
- **The unit of aggregation comes from the spec**, never from whatever is convenient.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import numpy as np

from .spec import ExperimentSpec

SEEN = "app.bsky.feed.defs#interactionSeen"
LIKE = "app.bsky.feed.defs#interactionLike"


def _sql_literal(value: Any) -> str:
    """Render an arm value as SQL.

    Booleans need explicit handling: Python renders them as ``True``/``False``, which is not what
    ClickHouse compares against a ``JSONExtractInt`` result. Left implicit, a boolean-armed spec
    silently matches nothing and reports zero observations — which the insufficient-data gate then
    reports as "withheld", indistinguishable from an experiment that simply has not accrued yet.
    """
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    return str(value)


def _json_path_expr(decoded: str, path: str, kind: str = "Int") -> str:
    """Build a ClickHouse JSONExtract call for a dotted path."""
    keys = ", ".join(f"'{p}'" for p in path.split("."))
    return f"JSONExtract{kind}({decoded}, {keys})"


@dataclass
class Rows:
    """Per-observation rows, already restricted to the experiment window and arms."""

    unit_ids: np.ndarray
    arm: np.ndarray
    numerator: np.ndarray
    denominator: np.ndarray
    #: Present only for interleaving designs.
    ranker: np.ndarray | None = None

    def __len__(self) -> int:  # pragma: no cover - trivial
        return int(self.unit_ids.size)


class ClickHouseReader:
    """Thin reader. Kept small so tests can substitute a fake."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str = "default",
        secure: bool = True,
    ) -> None:
        self.host = host or os.environ["CLICKHOUSE_HOST"]
        self.port = int(port or os.environ.get("CLICKHOUSE_PORT", "8443"))
        self.user = user or os.environ.get("CLICKHOUSE_USER", "default")
        self.password = password or os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.database = database
        self.secure = secure

    def query(self, sql: str) -> list[tuple[Any, ...]]:  # pragma: no cover - needs a server
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
            secure=self.secure,
        )
        try:
            return list(client.query(sql).result_rows)
        finally:
            client.close()


def ab_rows_sql(spec: ExperimentSpec, where_extra: str = "") -> str:
    """SQL for an A/B design: one row per (unit, impression) with a like flag.

    Note the deliberate absence of any ``source`` filter — this is intention-to-treat. Restricting
    to personalized impressions would condition on a post-treatment variable, which biases the
    estimate no matter how much data is collected.
    """
    control = spec.arms["control"]
    treatment = next(a for n, a in spec.arms.items() if n != "control")
    if control.field != treatment.field:
        raise ValueError("A/B arms must be identified by the same provenance field")

    decoded = "tryBase64Decode(interaction_feed_context)"
    # The extractor must match the JSON type. `JSONExtractInt` on a JSON `true` yields 0, the same
    # value an *absent* field yields — so a boolean arm read as Int collapses both arms into one and
    # the estimator reports zero observations. Measured: the holdout spec returned 0 obs twice before
    # this was traced. Booleans therefore extract as Bool, which yields 1/0 as intended.
    kind = "Bool" if isinstance(control.value, bool) else "Int"
    arm_expr = _json_path_expr(decoded, control.field, kind)
    unit_col = "did" if spec.unit == "user" else "concat(did, '|', toString(occurred))"
    end_clause = f"AND occurred <= toDateTime('{spec.end:%Y-%m-%d %H:%M:%S}')" if spec.end else ""
    extra = f"AND {where_extra}" if where_extra else ""
    # Applied inside the CTE so it filters before arm assignment, not after.
    population = f"AND ({spec.population})" if spec.population else ""

    return f"""
WITH d AS (
  SELECT
    {unit_col} AS unit_id,
    {arm_expr} AS arm_value,
    JSONExtractString({decoded}, 'source') AS source,
    interaction_event AS ev
  FROM default.feed_interactions
  WHERE occurred >= toDateTime('{spec.start:%Y-%m-%d %H:%M:%S}')
    {end_clause}
    AND interaction_feed_context != ''
    {population}
    AND interaction_event IN ('{SEEN}', '{LIKE}')
)
SELECT unit_id, arm_value,
       countIf(ev = '{LIKE}') AS likes,
       countIf(ev = '{SEEN}') AS seen
FROM d
WHERE arm_value IN ({_sql_literal(control.value)}, {_sql_literal(treatment.value)}) {extra}
GROUP BY unit_id, arm_value
HAVING seen > 0
"""


def interleaving_rows_sql(spec: ExperimentSpec) -> str:
    """SQL for an interleaving design: per-user wins for each ranker.

    Only items carrying a ``ranker`` tag count. Items both rankers offered are untagged by the
    harness precisely so they cannot contribute — they carry no preference information.
    """
    decoded = "tryBase64Decode(interaction_feed_context)"
    end_clause = f"AND occurred <= toDateTime('{spec.end:%Y-%m-%d %H:%M:%S}')" if spec.end else ""
    control = spec.arms["control"]
    treatment = next(a for n, a in spec.arms.items() if n != "control")
    return f"""
SELECT did AS unit_id,
       countIf(JSONExtractString({decoded}, 'ranker') = '{treatment.value}'
               AND interaction_event = '{LIKE}') AS treatment_wins,
       countIf(JSONExtractString({decoded}, 'ranker') = '{control.value}'
               AND interaction_event = '{LIKE}') AS control_wins,
       countIf(JSONExtractString({decoded}, 'ranker') != ''
               AND interaction_event = '{SEEN}') AS tagged_impressions
FROM default.feed_interactions
WHERE occurred >= toDateTime('{spec.start:%Y-%m-%d %H:%M:%S}')
  {end_clause}
  AND interaction_feed_context != ''
  AND interaction_event IN ('{SEEN}', '{LIKE}')
GROUP BY did
HAVING tagged_impressions > 0
"""


def cuped_covariate_sql(spec: ExperimentSpec, lookback_days: int = 14) -> str:
    """Per-unit pre-experiment like rate, for variance reduction.

    Strictly *before* ``spec.start`` — a covariate measured during the experiment could be affected
    by the treatment, which would bias rather than de-noise the estimate.
    """
    return f"""
SELECT did AS unit_id,
       countIf(interaction_event = '{LIKE}') AS pre_likes,
       countIf(interaction_event = '{SEEN}') AS pre_seen
FROM default.feed_interactions
WHERE occurred < toDateTime('{spec.start:%Y-%m-%d %H:%M:%S}')
  AND occurred >= toDateTime('{spec.start:%Y-%m-%d %H:%M:%S}') - INTERVAL {lookback_days} DAY
  AND interaction_event IN ('{SEEN}', '{LIKE}')
GROUP BY did
HAVING pre_seen > 0
"""


def rows_from_ab_result(
    result: list[tuple[Any, ...]], spec: ExperimentSpec
) -> Rows:
    """Convert ``(unit_id, arm_value, likes, seen)`` tuples into arrays."""
    control_value = spec.arms["control"].value
    # Compare in the same space the SQL emitted: a bool arm comes back as 0/1, not True/False.
    if isinstance(control_value, bool):
        control_value = 1 if control_value else 0
    unit_ids, arms, num, den = [], [], [], []
    for unit_id, arm_value, likes, seen in result:
        unit_ids.append(str(unit_id))
        arms.append(0.0 if arm_value == control_value else 1.0)
        num.append(float(likes))
        den.append(float(seen))
    return Rows(
        unit_ids=np.array(unit_ids),
        arm=np.array(arms, dtype=float),
        numerator=np.array(num, dtype=float),
        denominator=np.array(den, dtype=float),
    )
