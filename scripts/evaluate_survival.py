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
from typing import Dict, Optional

import numpy as np
import pandas as pd
from lifelines.utils import concordance_index
from sklearn.metrics import precision_recall_curve, roc_auc_score, roc_curve

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


def _compute_roc_pr(probabilities: np.ndarray, labels: np.ndarray) -> Dict[str, float]:
    if len(np.unique(labels)) < 2:
        return {"roc_auc": None, "pr_auc": None}
    roc_auc = roc_auc_score(labels, probabilities)
    precision, recall, _ = precision_recall_curve(labels, probabilities)
    pr_auc = np.trapz(precision[::-1], recall[::-1])
    return {"roc_auc": float(roc_auc), "pr_auc": float(pr_auc)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate a survival model against a snapshot.")
    parser.add_argument("--snapshot", required=True, help="Path to the flattened CSV snapshot.")
    parser.add_argument("--model", required=True, help="Path to the trained model artifact (.pkl).")
    parser.add_argument("--val-size", type=float, default=0.2, help="Holdout ratio if snapshot lacks partition.")
    parser.add_argument("--horizon-days", type=int, default=90, help="Horizon for ROC/PR metrics.")
    parser.add_argument("--output-dir", default="outputs/analysis", help="Directory for the evaluation JSON.")
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
        cutoff = int(len(df) * (1 - args.val_size))
        train_df = df.iloc[:cutoff].reset_index(drop=True)
        val_df = df.iloc[cutoff:].reset_index(drop=True)

    model = _load_model(model_path)
    train_c = _compute_c_index(model, train_df)
    val_c = _compute_c_index(model, val_df)

    if val_df.empty:
        roc_metrics = {"roc_auc": None, "pr_auc": None}
    else:
        probs = model.predict_win_probabilities(val_df, horizons=[args.horizon_days])[args.horizon_days]
        labels = _prepare_horizon_labels(val_df, horizon_days=args.horizon_days)
        roc_metrics = _compute_roc_pr(np.array(probs, ndmin=1), labels.to_numpy())

    report = {
        "model_artifact": str(model_path),
        "snapshot_path": str(snapshot_path),
        "dataset_hash": dataset_hash,
        "train_rows": int(len(train_df)),
        "val_rows": int(len(val_df)),
        "train_c_index": train_c,
        "val_c_index": val_c,
        "horizon_days": args.horizon_days,
        "roc_auc": roc_metrics["roc_auc"],
        "pr_auc": roc_metrics["pr_auc"],
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