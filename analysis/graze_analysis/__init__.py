"""Experiment analysis that is correct by default.

Exists because the motivating investigation produced four confident wrong answers in two days —
three of them statistical. Each default here (intention-to-treat, cluster-robust SEs, mandatory
negative controls, always-valid intervals) closes one of those failures.
"""

from .spec import ExperimentSpec, SpecError, load_spec, spec_from_dict
from .stats import (
    ControlVerdict,
    Estimate,
    SequentialEstimate,
    always_valid_ci,
    always_valid_diff_ci,
    cluster_robust_rate_diff,
    covariate_balance,
    cuped_adjust,
    insufficient_data_gate,
    negative_control_gate,
    permutation_rate_diff,
)

__all__ = [
    "ExperimentSpec",
    "SpecError",
    "load_spec",
    "spec_from_dict",
    "Estimate",
    "ControlVerdict",
    "cluster_robust_rate_diff",
    "permutation_rate_diff",
    "cuped_adjust",
    "always_valid_ci",
    "always_valid_diff_ci",
    "SequentialEstimate",
    "covariate_balance",
    "insufficient_data_gate",
    "negative_control_gate",
]
