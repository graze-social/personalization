"""Declarative experiment specifications.

Every field here exists because its absence caused a concrete analysis error during the
investigation that motivated this package (see ``THEORIES-personalization.md``):

- ``unit`` is **mandatory** because predicting a per-request outcome from a per-user measurement
  overstated one intervention's reach by ~3x.
- ``negative_controls`` is **mandatory** because two separate results looked significant until a
  surface the treatment could not affect was checked and moved just as much.
- ``post_treatment_fields`` exists because slicing by ``source`` biased an estimate: the treatment
  causally changed the personalized/fallback mix, so conditioning on it conditions on the outcome.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

Design = Literal["ab", "interleaving"]
Unit = Literal["user", "request"]

#: Fields that are *caused by* the treatment and therefore must never be conditioned on.
#: Slicing on any of these produces a biased estimate no matter how large the sample.
DEFAULT_POST_TREATMENT_FIELDS: tuple[str, ...] = (
    "source",
    "fallback_tranche",
    "personalized",
    "personalization_type",
    "personalized_count",
    "ranker",
)


class SpecError(ValueError):
    """Raised for a malformed or unsafe specification.

    Deliberately fatal: a silently-misinterpreted spec yields a confident wrong answer, which is
    strictly worse than a crash.
    """


@dataclass(frozen=True)
class Arm:
    """One side of a comparison, identified by a value in the provenance blob."""

    name: str
    #: Dotted path within the decoded ``feedContext`` (e.g. ``params.max_total_sources``).
    field: str
    #: Value identifying membership. ``None`` for interleaving, where the ranker tag identifies it.
    value: Any = None


@dataclass(frozen=True)
class NegativeControl:
    """A surface the treatment cannot causally affect.

    If it moves, the result is withheld. This is the single check that caught the two false
    positives in the motivating investigation.
    """

    name: str
    #: Why the treatment cannot affect it. Required so the reasoning is reviewable rather than
    #: assumed — a "control" the treatment *can* affect is worse than none, because it launders
    #: a confounded result.
    reason: str
    #: Restriction selecting the control surface, e.g. ``source = 'fallback'``.
    where: str


@dataclass(frozen=True)
class Guardrail:
    """A metric that must not regress, independent of the primary outcome."""

    metric: str
    max: float | None = None
    min: float | None = None


@dataclass(frozen=True)
class ExperimentSpec:
    id: str
    design: Design
    #: Experiment start. Data before this is a *different* assignment mechanism and must be
    #: excluded — mixing in pre-experiment rows silently reintroduces adaptive assignment.
    start: _dt.datetime
    unit: Unit
    primary_metric: str
    arms: dict[str, Arm]
    negative_controls: tuple[NegativeControl, ...]
    end: _dt.datetime | None = None
    guardrails: tuple[Guardrail, ...] = ()
    cuped_covariate: str | None = None
    post_treatment_fields: tuple[str, ...] = DEFAULT_POST_TREATMENT_FIELDS
    #: SQL restricting rows to the population the experiment can possibly apply to.
    #:
    #: Required when an arm is identified by the *absence* of a field. `feed_interactions` is shared
    #: with other Graze services whose provenance blobs contain only `{"feed_uri": ...}` — 457k rows
    #: in 6 hours against ~4.5k of ours. A spec matching "field is false" silently swallows all of
    #: them, which would put another service's traffic in the treatment arm.
    population: str | None = None
    #: Minimum competitive pairs / observations before any verdict is reported.
    min_observations: int = 200
    notes: str = ""

    def assert_not_post_treatment(self, *fields: str) -> None:
        """Fail if a caller tries to stratify by something the treatment causes."""
        bad = [f for f in fields if f in self.post_treatment_fields]
        if bad:
            raise SpecError(
                f"refusing to condition on post-treatment field(s) {bad}: the treatment can "
                "change these, so stratifying on them biases the estimate regardless of sample "
                "size. Analyse intention-to-treat instead."
            )


def _parse_dt(value: Any, field_name: str) -> _dt.datetime:
    if isinstance(value, _dt.datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:  # pragma: no cover - message clarity only
            raise SpecError(f"{field_name}: not an ISO-8601 timestamp: {value!r}") from exc
    else:
        raise SpecError(f"{field_name}: expected a timestamp, got {type(value).__name__}")
    if dt.tzinfo is None:
        # The service works in UTC while the dev host is not; an ambiguous timestamp already
        # cost one round of wrong numbers, so refuse rather than guess.
        raise SpecError(f"{field_name}: timestamp must be timezone-aware (append 'Z' for UTC)")
    return dt.astimezone(_dt.timezone.utc)


def load_spec(path: str | Path) -> ExperimentSpec:
    """Load and validate a spec. Raises :class:`SpecError` on anything ambiguous."""
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: expected a mapping at the top level")
    return spec_from_dict(raw, source=str(path))


def spec_from_dict(raw: dict[str, Any], source: str = "<dict>") -> ExperimentSpec:
    def require(key: str) -> Any:
        if key not in raw or raw[key] in (None, "", [], {}):
            raise SpecError(f"{source}: '{key}' is required")
        return raw[key]

    design = require("design")
    if design not in ("ab", "interleaving"):
        raise SpecError(f"{source}: design must be 'ab' or 'interleaving', got {design!r}")

    unit = require("unit")
    if unit not in ("user", "request"):
        raise SpecError(
            f"{source}: unit must be 'user' or 'request', got {unit!r}. This is mandatory: "
            "a per-user effect and a per-request effect are different quantities, and "
            "conflating them has already produced a wrong prediction here."
        )

    arms_raw = require("arms")
    if not isinstance(arms_raw, dict) or len(arms_raw) != 2:
        raise SpecError(f"{source}: 'arms' must map exactly two names to definitions")
    arms: dict[str, Arm] = {}
    for name, body in arms_raw.items():
        if not isinstance(body, dict) or "field" not in body:
            raise SpecError(f"{source}: arm '{name}' needs a 'field'")
        arms[name] = Arm(name=name, field=body["field"], value=body.get("value"))
    if "control" not in arms:
        raise SpecError(f"{source}: one arm must be named 'control'")

    controls_raw = raw.get("negative_controls")
    if not controls_raw:
        raise SpecError(
            f"{source}: at least one negative_control is required. A surface the treatment "
            "cannot affect is the only cheap protection against confounding, and it is what "
            "caught both false positives in this system's history."
        )
    controls = tuple(
        NegativeControl(
            name=c["name"],
            reason=c.get("reason")
            or _raise(SpecError(f"{source}: negative control '{c['name']}' needs a 'reason'")),
            where=c["where"],
        )
        for c in controls_raw
    )

    guardrails = tuple(
        Guardrail(metric=g["metric"], max=g.get("max"), min=g.get("min"))
        for g in raw.get("guardrails", [])
    )

    return ExperimentSpec(
        id=str(require("id")),
        design=design,
        start=_parse_dt(require("start"), "start"),
        end=_parse_dt(raw["end"], "end") if raw.get("end") else None,
        unit=unit,
        primary_metric=str(require("primary_metric")),
        arms=arms,
        negative_controls=controls,
        guardrails=guardrails,
        cuped_covariate=raw.get("cuped_covariate"),
        population=raw.get("population"),
        post_treatment_fields=tuple(
            raw.get("post_treatment_fields", DEFAULT_POST_TREATMENT_FIELDS)
        ),
        min_observations=int(raw.get("min_observations", 200)),
        notes=str(raw.get("notes", "")),
    )


def _raise(exc: Exception):  # pragma: no cover - helper for inline validation
    raise exc
