#!/usr/bin/env python3
"""
Graze ML Ranking — LightGBM trainer

Reads labelled impressions from ClickHouse, trains a binary classifier to predict
positive engagement (like / repost / reply / see-more), exports to ONNX, and
writes feature-importance JSON alongside the model.

Usage
-----
python train.py \
    --ch-host localhost \
    --ch-port 8123 \
    --ch-database graze \
    --output ranking_model.onnx \
    [--algo-id 1] \
    [--days 30] \
    [--positive-events like,repost,reply,see_more] \
    [--negative-events see_less] \
    [--min-impressions 10000] \
    [--test-size 0.2] \
    [--lgbm-params '{"num_leaves": 63, "n_estimators": 500}']

Environment variables (override CLI flags if set):
    CH_HOST, CH_PORT, CH_DATABASE, CH_USER, CH_PASSWORD
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score
import onnxmltools
from skl2onnx.common.data_types import FloatTensorType

try:
    import clickhouse_driver
except ImportError:
    print("clickhouse-driver not installed: pip install clickhouse-driver", file=sys.stderr)
    sys.exit(1)


# ─── Feature columns (must match feed_impressions table + PostFeatures::to_array) ───

FEATURE_COLS = [
    "raw_score",
    "final_score",
    "num_paths",
    "liker_count",
    "popularity_penalty",
    "paths_boost",
    "max_contribution",
    "score_concentration",
    "newest_like_age_hours",
    "oldest_like_age_hours",
    "was_liker_cache_hit",
    "coliker_count",
    "top_coliker_weight",
    "top5_weight_sum",
    "mean_coliker_weight",
    "weight_concentration",
    "depth",
    "user_like_count",
    "user_segment_encoded",  # derived: cold=0, warm=1, active=2, unknown=3
    "richness_ratio",
    "hour_of_day",
    "day_of_week",
    "is_first_page",
    "is_holdout",
]

POSITIVE_EVENTS_DEFAULT = {"like", "repost", "reply", "see_more"}
NEGATIVE_EVENTS_DEFAULT = {"see_less"}


# ─── ClickHouse query ────────────────────────────────────────────────────────────

QUERY = """
SELECT
    fi.impression_id,
    fi.depth,
    fi.raw_score,
    fi.final_score,
    fi.num_paths,
    fi.liker_count,
    fi.popularity_penalty,
    fi.paths_boost,
    fi.max_contribution,
    fi.score_concentration,
    fi.newest_like_age_hours,
    fi.oldest_like_age_hours,
    fi.was_liker_cache_hit,
    fi.coliker_count,
    fi.top_coliker_weight,
    fi.top5_weight_sum,
    fi.mean_coliker_weight,
    fi.weight_concentration,
    fi.user_like_count,
    fi.user_segment,
    fi.richness_ratio,
    fi.hour_of_day,
    fi.day_of_week,
    fi.is_first_page,
    fi.is_holdout,
    -- Aggregate all interaction event types for this impression
    groupArray(ia.interaction_event)  AS events
FROM feed_impressions AS fi
LEFT JOIN feed_interactions AS ia
    ON fi.impression_id = ia.impression_id
    AND ia.occurred >= fi.served_at
    AND ia.occurred <= fi.served_at + INTERVAL 24 HOUR
WHERE
    fi.served_at >= now() - INTERVAL {days} DAY
    AND fi.is_holdout = 0
    {algo_filter}
GROUP BY
    fi.impression_id, fi.depth, fi.raw_score, fi.final_score, fi.num_paths,
    fi.liker_count, fi.popularity_penalty, fi.paths_boost, fi.max_contribution,
    fi.score_concentration, fi.newest_like_age_hours, fi.oldest_like_age_hours,
    fi.was_liker_cache_hit, fi.coliker_count, fi.top_coliker_weight,
    fi.top5_weight_sum, fi.mean_coliker_weight, fi.weight_concentration,
    fi.user_like_count, fi.user_segment, fi.richness_ratio,
    fi.hour_of_day, fi.day_of_week, fi.is_first_page, fi.is_holdout
HAVING length(fi.impression_id) > 0
"""


# ─── Helpers ────────────────────────────────────────────────────────────────────

def encode_user_segment(s: str) -> int:
    return {"cold": 0, "warm": 1, "active": 2}.get(s, 3)


def build_labels(events_series: pd.Series, positive_events: set, negative_events: set) -> pd.Series:
    """
    Binary label:
      1 if any positive event fired
      0 if no positive event fired (includes explicit see_less negatives)

    Rows with ONLY see_less (explicit negative) are kept; rows that have a
    positive AND see_less are still positive (rare but valid — user liked then
    clicked see-less later).
    """
    def label(events):
        event_set = set(events) if events else set()
        if event_set & positive_events:
            return 1
        return 0

    return events_series.apply(label)


def inverse_propensity_weight(depth: pd.Series) -> pd.Series:
    """
    Position-based IPS: deeper positions get higher weight to correct for
    the fact that users are less likely to see (and therefore interact with)
    posts further down the feed.

    w(d) = (1 + d)^0.5  — mild sqrt correction; not overly aggressive.
    """
    return (1.0 + depth.astype(float)) ** 0.5


# ─── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Train Graze ML ranking model")
    parser.add_argument("--ch-host", default=os.getenv("CH_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CH_PORT", "9000")),
                        help="ClickHouse native port (default 9000; HTTP port is 8123 but native is faster)")
    parser.add_argument("--ch-database", default=os.getenv("CH_DATABASE", "graze"))
    parser.add_argument("--ch-user", default=os.getenv("CH_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CH_PASSWORD", ""))
    parser.add_argument("--output", default="ranking_model.onnx",
                        help="Path for the exported ONNX model file")
    parser.add_argument("--algo-id", type=int, default=None,
                        help="Limit training data to one algo_id (default: all)")
    parser.add_argument("--days", type=int, default=30,
                        help="How many days of impressions to include (default: 30)")
    parser.add_argument("--positive-events",
                        default=",".join(sorted(POSITIVE_EVENTS_DEFAULT)),
                        help="Comma-separated interaction event names that count as positive")
    parser.add_argument("--negative-events",
                        default=",".join(sorted(NEGATIVE_EVENTS_DEFAULT)),
                        help="Comma-separated interaction event names that count as explicit negative")
    parser.add_argument("--min-impressions", type=int, default=10_000,
                        help="Abort if fewer than this many impressions are available")
    parser.add_argument("--test-size", type=float, default=0.2,
                        help="Fraction of data held out for evaluation (default: 0.2)")
    parser.add_argument("--lgbm-params", default="{}", type=json.loads,
                        help='JSON dict of extra LightGBM params (e.g. \'{"num_leaves": 63}\')')
    args = parser.parse_args()

    positive_events = set(args.positive_events.split(","))
    negative_events = set(args.negative_events.split(","))

    # ── Connect ──────────────────────────────────────────────────────────────
    print(f"Connecting to ClickHouse {args.ch_host}:{args.ch_port}/{args.ch_database} …")
    client = clickhouse_driver.Client(
        host=args.ch_host,
        port=args.ch_port,
        database=args.ch_database,
        user=args.ch_user,
        password=args.ch_password,
    )

    # ── Fetch data ────────────────────────────────────────────────────────────
    algo_filter = f"AND fi.algo_id = {args.algo_id}" if args.algo_id is not None else ""
    query = QUERY.format(days=args.days, algo_filter=algo_filter)

    print(f"Fetching impressions for the last {args.days} days …")
    rows, col_names = client.execute(query, with_column_types=True)
    columns = [c[0] for c in col_names]
    df = pd.DataFrame(rows, columns=columns)
    print(f"  → {len(df):,} impression rows fetched")

    if len(df) < args.min_impressions:
        print(f"ERROR: only {len(df):,} rows, need at least {args.min_impressions:,}. "
              "Enable ML_IMPRESSIONS_ENABLED and collect more data first.", file=sys.stderr)
        sys.exit(1)

    # ── Feature engineering ───────────────────────────────────────────────────
    df["user_segment_encoded"] = df["user_segment"].apply(encode_user_segment)
    df["was_liker_cache_hit"] = df["was_liker_cache_hit"].astype(float)
    df["is_first_page"] = df["is_first_page"].astype(float)
    df["is_holdout"] = df["is_holdout"].astype(float)

    # ── Labels ────────────────────────────────────────────────────────────────
    df["label"] = build_labels(df["events"], positive_events, negative_events)
    pos_rate = df["label"].mean()
    print(f"  → positive rate: {pos_rate:.3%}  ({df['label'].sum():,} positives)")

    # ── IPS weights ───────────────────────────────────────────────────────────
    df["ips_weight"] = inverse_propensity_weight(df["depth"])

    # ── Train / test split ────────────────────────────────────────────────────
    X = df[FEATURE_COLS].astype(np.float32)
    y = df["label"].astype(np.int32)
    w = df["ips_weight"].astype(np.float32)

    X_train, X_test, y_train, y_test, w_train, w_test = train_test_split(
        X, y, w, test_size=args.test_size, random_state=42, stratify=y
    )
    print(f"  → train: {len(X_train):,}  test: {len(X_test):,}")

    # ── LightGBM ──────────────────────────────────────────────────────────────
    default_params = {
        "objective": "binary",
        "metric": ["auc", "average_precision"],
        "num_leaves": 63,
        "max_depth": -1,
        "learning_rate": 0.05,
        "n_estimators": 500,
        "min_child_samples": 50,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 0.1,
        "class_weight": "balanced",
        "n_jobs": -1,
        "verbose": -1,
    }
    default_params.update(args.lgbm_params)

    print("Training LightGBM …")
    model = lgb.LGBMClassifier(**default_params)
    model.fit(
        X_train, y_train,
        sample_weight=w_train,
        eval_set=[(X_test, y_test)],
        eval_sample_weight=[w_test],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(50)],
    )

    # ── Evaluate ──────────────────────────────────────────────────────────────
    y_pred_prob = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_prob, sample_weight=w_test)
    ap = average_precision_score(y_test, y_pred_prob, sample_weight=w_test)
    print(f"\nTest AUC: {auc:.4f}  |  Average Precision: {ap:.4f}")

    # ── Feature importance ────────────────────────────────────────────────────
    importance = dict(zip(FEATURE_COLS, model.feature_importances_.tolist()))
    importance_sorted = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
    importance_path = args.output.replace(".onnx", "_feature_importance.json")
    with open(importance_path, "w") as f:
        json.dump(importance_sorted, f, indent=2)
    print(f"Feature importance → {importance_path}")
    print("Top-5 features:")
    for feat, imp in list(importance_sorted.items())[:5]:
        print(f"  {feat:<35} {imp:.1f}")

    # ── Export to ONNX ────────────────────────────────────────────────────────
    print(f"\nExporting ONNX model → {args.output}")
    n_features = len(FEATURE_COLS)
    initial_type = [("float_input", FloatTensorType([None, n_features]))]
    onnx_model = onnxmltools.convert_lightgbm(model, initial_types=initial_type)
    onnxmltools.utils.save_model(onnx_model, args.output)

    # Quick sanity check with onnxruntime
    try:
        import onnxruntime as rt
        sess = rt.InferenceSession(args.output, providers=["CPUExecutionProvider"])
        sample = X_test.values[:5].astype(np.float32)
        out = sess.run(None, {"float_input": sample})
        print(f"ONNX sanity check passed — sample probabilities: {out[1][:5]}")
    except Exception as e:
        print(f"Warning: ONNX sanity check failed ({e}); model file may still be valid.")

    print(f"\nDone. Model saved to: {args.output}")
    print(f"  Best iteration: {model.best_iteration_}")
    print(f"  Trees: {model.n_estimators_}")
    print(f"  AUC: {auc:.4f}  AP: {ap:.4f}")
    print(f"\nDeploy by setting:")
    print(f"  ML_RERANKER_ENABLED=true")
    print(f"  ML_MODEL_PATH={os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
