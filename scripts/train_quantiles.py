"""
CLI to train the LightGBM-based quantile regression models for gestation timelines.

Example:
    python -m scripts.train_quantiles --lookback-days 1095 --val-size 0.2 --model-version v1
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, ParameterSampler


from src.config import OUTPUTS_DIR
from src.core.ml.dataset import FeatureLoader, GESTATION_CATEGORY_COLUMNS
from src.core.ml.quantiles import QuantileModel
from src.database.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("train_quantiles")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _hash_dataset(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _train_val_split(
    df: pd.DataFrame,
    *,
    val_size: float,
    seed: int,  # kept for compatibility
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if val_size <= 0 or val_size >= 1:
        return df.reset_index(drop=True), pd.DataFrame()
    if "date_created" in df.columns:
        sorted_df = df.sort_values("date_created").reset_index(drop=True)
    else:
        sorted_df = df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    cutoff = int(len(sorted_df) * (1 - val_size))
    cutoff = max(1, min(cutoff, len(sorted_df) - 1))
    train_df = sorted_df.iloc[:cutoff].reset_index(drop=True)
    val_df = sorted_df.iloc[cutoff:].reset_index(drop=True)
    return train_df, val_df

def _calibrate_quantiles(model: QuantileModel, df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if df.empty:
        return None
    target = df["gestation_target"].to_numpy(dtype=float)
    offsets: Dict[str, float] = {}
    for idx, alpha in enumerate(model.alphas):
        label = f"p{int(alpha * 100)}"
        residuals = []
        for row_idx in range(len(df)):
            preds = model.predict(df.iloc[[row_idx]])
            pred_value = preds.get(label)
            if pred_value is None:
                continue
            residuals.append(target[row_idx] - pred_value)
        if not residuals:
            continue
        delta = float(np.quantile(residuals, alpha))
        offsets[label] = delta
    if offsets:
        model.update_calibration(offsets)
    return offsets or None


def _tune_hyperparams(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    *,
    base_params: Dict[str, float],
    min_rows: int,
    alphas,
    trials: int,
    random_state: int,
) -> Tuple[QuantileModel, Dict[str, float], Optional[Dict[str, float]]]:
    if trials <= 0 or val_df.empty:
        model = QuantileModel(min_training_rows=min_rows, boosting_params=base_params, alphas=alphas)
        metrics = model.fit(train_df)
        val_metrics = _evaluate_split(model, val_df)
        return model, base_params, val_metrics

    search_space = {
        "learning_rate": [0.03, 0.05, 0.07],
        "n_estimators": [300, 500, 700],
        "max_depth": [-1, 8, 12],
        "min_child_samples": [20, 40, 80],
        "subsample": [0.8, 0.9, 1.0],
        "colsample_bytree": [0.7, 0.9, 1.0],
        "reg_alpha": [0.0, 0.1, 0.3],
        "reg_lambda": [0.1, 0.3, 0.6],
    }
    sampler = ParameterSampler(search_space, n_iter=trials, random_state=random_state)

    best_score = float("inf")
    best_model: Optional[QuantileModel] = None
    best_params = base_params
    best_val_metrics: Optional[Dict[str, float]] = None

    for params in sampler:
        candidate_params = {**base_params, **params}
        candidate = QuantileModel(min_training_rows=min_rows, boosting_params=candidate_params, alphas=alphas)
        candidate.fit(train_df)
        if not candidate.is_fitted:
            continue
        metrics = _evaluate_split(candidate, val_df)
        score = metrics.get("mae_p50") if metrics else None
        if score is None:
            continue
        if score < best_score:
            best_score = score
            best_model = candidate
            best_params = candidate_params
            best_val_metrics = metrics

    if best_model is None:
        best_model = QuantileModel(min_training_rows=min_rows, boosting_params=base_params, alphas=alphas)
        best_model.fit(train_df)
        best_val_metrics = _evaluate_split(best_model, val_df)

    return best_model, best_params, best_val_metrics


def _apply_category_min_frequency(
    df: pd.DataFrame,
    *,
    min_freq: int,
) -> pd.DataFrame:
    if min_freq <= 1:
        return df
    working = df.copy()
    for col in GESTATION_CATEGORY_COLUMNS:
        if col not in working.columns:
            continue
        counts = working[col].value_counts()
        rare = counts[counts < min_freq].index
        working.loc[working[col].isin(rare), col] = "Other"
    return working


def _evaluate_split(model: QuantileModel, df: pd.DataFrame) -> Optional[Dict[str, float]]:
    if df.empty:
        return None

    actual = df["gestation_target"].to_numpy(dtype=float)
    mae = {"p25": [], "p50": [], "p75": []}
    widths = []
    coverage_hits = []

    for idx in range(len(df)):
        row = df.iloc[[idx]]
        preds = model.predict(row)
        p25, p50, p75 = preds.get("p25"), preds.get("p50"), preds.get("p75")
        if None in (p25, p50, p75):
            continue
        truth = actual[idx]
        mae["p25"].append(abs(p25 - truth))
        mae["p50"].append(abs(p50 - truth))
        mae["p75"].append(abs(p75 - truth))
        widths.append(max(p75 - p25, 0))
        coverage_hits.append(1.0 if p25 <= truth <= p75 else 0.0)

    if not widths:
        return None

    span = max(actual) - min(actual) or 1.0
    metrics = {
        "rows_evaluated": len(widths),
        "mae_p25": float(np.mean(mae["p25"])) if mae["p25"] else None,
        "mae_p50": float(np.mean(mae["p50"])) if mae["p50"] else None,
        "mae_p75": float(np.mean(mae["p75"])) if mae["p75"] else None,
        "coverage_p25_p75": float(np.mean(coverage_hits)),
        "pinaw": float(np.mean(widths) / span),
    }
    return metrics


def _serialize_metadata(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Saved quantile training metadata → %s", path)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the LightGBM quantile regression models.")
    parser.add_argument("--lookback-days", type=int, default=5475, help="Historical window for training rows.")
    parser.add_argument("--val-size", type=float, default=0.2, help="Validation split ratio.")
    parser.add_argument("--min-rows", type=int, default=200, help="Minimum rows required to fit the model.")
    parser.add_argument("--min-gestation-days", type=int, default=5, help="Lower bound for gestation targets.")
    parser.add_argument("--max-gestation-days", type=int, default=730, help="Upper bound for gestation targets.")
    parser.add_argument(
        "--clip-quantiles",
        nargs=2,
        type=float,
        metavar=("LOWER", "UPPER"),
        default=(0.01, 0.99),
        help="Quantile clipping bounds applied to gestation_target.",
    )
    parser.add_argument("--category-min-frequency", type=int, default=10, help="Minimum category count before collapsing to Other.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for splits.")
    parser.add_argument("--model-version", default="v1", help="Version tag stored alongside artifacts.")
    parser.add_argument("--snapshot-dir", default="data/processed", help="Directory to write flattened training snapshots.")
    parser.add_argument("--output-dir", default="data/models", help="Directory for serialized quantile models.")
    parser.add_argument("--metadata-dir", default=str(OUTPUTS_DIR / "analysis"), help="Where to store the training metadata JSON.")
    parser.add_argument("--n-estimators", type=int, default=500, help="LightGBM estimators per quantile.")
    parser.add_argument("--learning-rate", type=float, default=0.05, help="LightGBM learning rate.")
    parser.add_argument("--max-depth", type=int, default=-1, help="Maximum tree depth (-1 = unlimited).")
    parser.add_argument("--min-child-samples", type=int, default=40, help="Minimum data in a leaf.")
    parser.add_argument("--subsample", type=float, default=0.9, help="Row subsampling fraction.")
    parser.add_argument("--colsample-bytree", type=float, default=0.9, help="Feature subsampling fraction.")
    parser.add_argument("--reg-alpha", type=float, default=0.1, help="L1 regularization term.")
    parser.add_argument("--reg-lambda", type=float, default=0.3, help="L2 regularization term.")
    parser.add_argument("--tune-trials", type=int, default=0, help="Number of random search trials for hyperparameter tuning.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    snapshot_dir = Path(args.snapshot_dir)
    model_dir = Path(args.output_dir)
    metadata_dir = Path(args.metadata_dir)

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    client = SupabaseClient()
    loader = FeatureLoader(client)

    snapshot_path = snapshot_dir / f"gestation_training_{timestamp}.csv"
    dataset = loader.build_gestation_frame(
        lookback_days=args.lookback_days,
        min_gestation_days=args.min_gestation_days,
        max_gestation_days=args.max_gestation_days,
        clip_quantiles=tuple(args.clip_quantiles),
        snapshot_path=snapshot_path,
    )

    if dataset.empty:
        raise SystemExit("No gestation training data available; aborting.")

    dataset = _apply_category_min_frequency(dataset, min_freq=args.category_min_frequency)
    dataset_hash = _hash_dataset(dataset)

    train_df, val_df = _train_val_split(dataset, val_size=args.val_size, seed=args.random_state)
    logger.info("Quantile dataset rows → train=%s val=%s", len(train_df), len(val_df))

    boosting_params = {
        "n_estimators": args.n_estimators,
        "learning_rate": args.learning_rate,
        "max_depth": args.max_depth,
        "min_child_samples": args.min_child_samples,
        "subsample": args.subsample,
        "colsample_bytree": args.colsample_bytree,
        "reg_alpha": args.reg_alpha,
        "reg_lambda": args.reg_lambda,
        "verbose": -1,
    }

    model, chosen_params, val_metrics = _tune_hyperparams(
        train_df,
        val_df,
        base_params=boosting_params,
        min_rows=args.min_rows,
        alphas=QuantileModel.DEFAULT_ALPHAS,
        trials=args.tune_trials,
        random_state=args.random_state,
    )

    if not model.is_fitted:
        raise SystemExit("Quantile model failed to fit; see logs for details.")

    train_metrics = model.training_metrics or {}

    calibration_offsets = _calibrate_quantiles(model, val_df) if not val_df.empty else None

    val_metrics = _evaluate_split(model, val_df) if not val_df.empty else None

    artifact_path = model_dir / f"quantiles_{args.model_version}_{timestamp}.pkl"
    model.save(artifact_path)

    metadata = {
        "model_version": args.model_version,
        "artifact_path": str(artifact_path),
        "snapshot_path": str(snapshot_path),
        "generated_at_utc": timestamp,
        "lookback_days": args.lookback_days,
        "val_size": args.val_size,
        "min_rows": args.min_rows,
        "min_gestation_days": args.min_gestation_days,
        "max_gestation_days": args.max_gestation_days,
        "clip_quantiles": tuple(args.clip_quantiles),
        "category_min_frequency": args.category_min_frequency,
        "dataset_hash": dataset_hash,
        "train_rows": len(train_df),
        "val_rows": len(val_df),
        "boosting_params": chosen_params,
        "train_metrics": train_metrics,
        "val_metrics": val_metrics,
        "calibration_offsets": calibration_offsets,
    }

    metadata_path = Path(metadata_dir) / f"quantiles_training_{timestamp}.json"
    _serialize_metadata(metadata_path, metadata)
    logger.info("Quantile training complete. Artifact: %s", artifact_path)


if __name__ == "__main__":
    main()