"""
Evaluate a trained survival model against a snapshot and emit JSON metrics.

Usage:
    python -m scripts.evaluate_survival --snapshot data/processed/survival_training_20251124_142413.csv --model data/models/survival_v1_20251124_142413.pkl
"""

import argparse
import hashlib
import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from lifelines.utils import concordance_index
from sklearn.metrics import precision_recall_curve, roc_auc_score

from src.core.ml.survival import SurvivalModel

logger = logging.getLogger("evaluate_survival")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _hash_dataset(df: pd.DataFrame) -> str:
    payload = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _load_model(path: Path) -> SurvivalModel:
    model = SurvivalModel()
    model.load(path)
    return model


def _compute_c_index(model: SurvivalModel, df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    try:
        return model.score(df)
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to compute c-index: %s", exc)
        return None


def _prepare_horizon_labels(df: pd.DataFrame, horizon_days: int = 90) -> pd.Series:
    labels = (df["duration_days"] <= horizon_days) & (df["event_observed"] == 1)
    return labels.astype(int)

def _filter_for_horizon_evaluation(df: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """
    Filter to projects where we can meaningfully evaluate a horizon-based prediction.
    
    Include:
    1. Projects that WON (event_observed=1) - we know the outcome
    2. Projects that are LOST/CLOSED (status is resolved) - we know they didn't win
    3. Open projects only if they've been open > horizon_days (we know they didn't win within horizon)
    
    Exclude:
    - Open projects with duration < horizon_days (outcome is unknown)
    """
    df = df.copy()
    
    # Projects that have resolved (won or lost)
    resolved_mask = df["event_observed"] == 1  # Won
    
    # Check for lost/closed status if available
    if "censor_reason" in df.columns:
        resolved_mask = resolved_mask | (df["censor_reason"] == "lost")
    if "status_category" in df.columns:
        resolved_mask = resolved_mask | df["status_category"].isin(["Lost", "Closed"])
    
    # Open projects that have been observed long enough
    open_long_enough = (
        (df["event_observed"] == 0) & 
        (df["duration_days"] >= horizon_days)
    )
    
    # Include resolved OR open-long-enough
    valid_mask = resolved_mask | open_long_enough
    
    filtered = df[valid_mask].reset_index(drop=True)
    logger.info(
        "Filtered for %d-day horizon: %d → %d rows (removed %d unresolved open projects)",
        horizon_days,
        len(df),
        len(filtered),
        len(df) - len(filtered),
    )
    return filtered


def _compute_roc_pr(probabilities: np.ndarray, labels: np.ndarray) -> Dict[str, float]:
    if len(np.unique(labels)) < 2:
        return {"roc_auc": None, "pr_auc": None}
    roc_auc = roc_auc_score(labels, probabilities)
    precision, recall, _ = precision_recall_curve(labels, probabilities)
    pr_auc = np.trapz(precision[::-1], recall[::-1])
    return {"roc_auc": float(roc_auc), "pr_auc": float(pr_auc)}


def _predict_probabilities_batch(
    model: SurvivalModel, df: pd.DataFrame, horizon_days: int, backtest_mode: bool = True
) -> np.ndarray:
    """
    Compute win probabilities for each row in the dataframe.
    
    The model's predict_win_probabilities is designed for single-project inference,
    so we iterate over rows to compute probabilities for evaluation.
    
    Args:
        model: Trained SurvivalModel instance.
        df: DataFrame with project features.
        horizon_days: Prediction horizon in days.
        backtest_mode: If True, set time_open_days=0 to simulate prediction at project
            creation time. This fixes the evaluation bug where time_open_days computed
            from current timestamp makes historical evaluation meaningless.
    
    Returns:
        Array of win probabilities for each row.
    """
    probs = []
    for idx in range(len(df)):
        row_df = df.iloc[[idx]].copy()
        if backtest_mode:
            # Simulate prediction at project creation time
            row_df["time_open_days"] = 0
        prob = model.predict_win_probabilities(row_df, horizons=[horizon_days])[horizon_days]
        probs.append(prob)
    return np.array(probs)


def _temporal_split(df: pd.DataFrame, val_size: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split by date_created: older projects for training, recent for validation.
    
    This matches the training script's logic and simulates production where
    we predict on future projects.
    
    Args:
        df: DataFrame with date_created column.
        val_size: Fraction of data to use for validation (most recent projects).
    
    Returns:
        Tuple of (train_df, val_df).
    """
    if val_size <= 0 or val_size >= 1:
        return df, pd.DataFrame()
    
    # Ensure date_created is datetime
    df = df.copy()
    df["date_created"] = pd.to_datetime(df["date_created"], errors="coerce")
    
    # Sort by date_created (oldest first)
    df_sorted = df.sort_values("date_created").reset_index(drop=True)
    
    # Split: older projects for training, recent for validation
    split_idx = int(len(df_sorted) * (1 - val_size))
    train_df = df_sorted.iloc[:split_idx].reset_index(drop=True)
    val_df = df_sorted.iloc[split_idx:].reset_index(drop=True)
    
    return train_df, val_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate a survival model against a snapshot.")
    parser.add_argument("--snapshot", required=True, help="Path to the flattened CSV snapshot.")
    parser.add_argument("--model", required=True, help="Path to the trained model artifact (.pkl).")
    parser.add_argument("--val-size", type=float, default=0.2, help="Holdout ratio if snapshot lacks partition.")
    parser.add_argument("--horizon-days", type=int, default=270, help="Primary horizon for ROC/PR metrics (default 270 based on win time analysis).")
    parser.add_argument(
        "--multi-horizon",
        action="store_true",
        default=False,
        help="Evaluate multiple horizons (30, 60, 90, 120, 180, 270, 365 days).",
    )
    parser.add_argument("--output-dir", default="outputs/analysis", help="Directory for the evaluation JSON.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility (must match training).")
    parser.add_argument(
        "--backtest-mode",
        action="store_true",
        default=True,
        help="Use time_open_days=0 for backtesting (simulates prediction at project creation). Default: True.",
    )
    parser.add_argument(
        "--no-backtest-mode",
        action="store_false",
        dest="backtest_mode",
        help="Disable backtest mode (use actual time_open_days values).",
    )
    args = parser.parse_args()

    snapshot_path = Path(args.snapshot)
    model_path = Path(args.model)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(snapshot_path)
    dataset_hash = _hash_dataset(df)

    val_marker = df.columns.intersection(["partition", "split", "fold"])
    if val_marker.any():
        marker = val_marker[0]
        train_df = df[df[marker].str.lower() != "val"].reset_index(drop=True)
        val_df = df[df[marker].str.lower() == "val"].reset_index(drop=True)
    else:
        # Use temporal split matching train_survival.py logic
        train_df, val_df = _temporal_split(df, val_size=args.val_size)

    model = _load_model(model_path)
    train_c = _compute_c_index(model, train_df)
    val_c = _compute_c_index(model, val_df)

    # HORIZON EVALUATION
    val_df_filtered = None
    roc_metrics = {"roc_auc": None, "pr_auc": None}
    multi_horizon_results = {}
    
    if val_df.empty:
        pass  # roc_metrics already set to None
    else:
        # Filter to projects where we can evaluate the primary horizon outcome
        val_df_filtered = _filter_for_horizon_evaluation(val_df, args.horizon_days)

        if val_df_filtered.empty or val_df_filtered["event_observed"].sum() == 0:
            logger.warning("No evaluable projects for %d-day horizon ROC/PR", args.horizon_days)
        else:
            probs = _predict_probabilities_batch(
                model, val_df_filtered, args.horizon_days, backtest_mode=args.backtest_mode
            )
            labels = _prepare_horizon_labels(val_df_filtered, horizon_days=args.horizon_days)
            roc_metrics = _compute_roc_pr(probs, labels.to_numpy())
        
        # Multi-horizon evaluation
        if args.multi_horizon:
            horizons_to_test = [30, 60, 90, 120, 180, 270, 365]
            logger.info("Running multi-horizon evaluation...")
            
            for h in horizons_to_test:
                h_filtered = _filter_for_horizon_evaluation(val_df, h)
                if h_filtered.empty:
                    multi_horizon_results[h] = {
                        "roc_auc": None, "pr_auc": None, 
                        "n_positive": 0, "n_total": 0, "positive_rate": 0
                    }
                    continue
                
                h_labels = _prepare_horizon_labels(h_filtered, horizon_days=h)
                n_positive = int(h_labels.sum())
                n_total = len(h_labels)
                positive_rate = n_positive / n_total if n_total > 0 else 0
                
                if n_positive == 0 or n_positive == n_total:
                    multi_horizon_results[h] = {
                        "roc_auc": None, "pr_auc": None,
                        "n_positive": n_positive, "n_total": n_total, 
                        "positive_rate": positive_rate
                    }
                    continue
                
                h_probs = _predict_probabilities_batch(
                    model, h_filtered, h, backtest_mode=args.backtest_mode
                )
                h_metrics = _compute_roc_pr(h_probs, h_labels.to_numpy())
                multi_horizon_results[h] = {
                    **h_metrics,
                    "n_positive": n_positive,
                    "n_total": n_total,
                    "positive_rate": round(positive_rate, 4),
                }
                logger.info(
                    "  %3dd: ROC=%.3f PR=%.3f pos=%d/%d (%.1f%%)",
                    h, h_metrics.get("roc_auc") or 0, h_metrics.get("pr_auc") or 0,
                    n_positive, n_total, positive_rate * 100
                )
  

    report = {
        "model_artifact": str(model_path),
        "snapshot_path": str(snapshot_path),
        "dataset_hash": dataset_hash,
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "split_method": "temporal",  # Uses date_created ordering instead of random split
        "train_c_index": train_c,
        "val_c_index": val_c,
        "horizon_days": args.horizon_days,
        "backtest_mode": args.backtest_mode,
        "roc_auc": roc_metrics["roc_auc"],
        "pr_auc": roc_metrics["pr_auc"],
        "val_events": int(val_df["event_observed"].sum()) if not val_df.empty else 0,
        "val_open": int((val_df["event_observed"] == 0).sum()) if not val_df.empty else 0,
        "val_filtered_rows": len(val_df_filtered) if val_df_filtered is not None else None,
        "multi_horizon_results": multi_horizon_results if multi_horizon_results else None,
        "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "report_id": str(uuid.uuid4()),
    }

    report_path = output_dir / f"survival_eval_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    logger.info("Saved evaluation report → %s", report_path)

    print(json.dumps({"report_path": str(report_path), **report}, indent=2))


if __name__ == "__main__":
    main()