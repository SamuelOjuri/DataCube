"""
CLI script to train the censor-aware survival model in shadow mode.

Usage:
    python -m scripts.train_survival --lookback-days 1095 --val-size 0.2

#######

### Survival Model Usage Examples:

# Train with default settings (includes new temporal features)
python -m scripts.train_survival

# Train with feature interactions enabled
python -m scripts.train_survival --enable-interactions

# Tune hyperparameters first
python -m scripts.tune_survival --n-folds 5

# Train with tuned parameters
python -m scripts.train_survival --params-file data/tuning/best_survival_params.json

# Evaluate with backtest mode (default)
python -m scripts.evaluate_survival --snapshot data/processed/survival_training_*.csv --model data/models/survival_*.pkl

"""


import argparse
import json
import logging
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

from src.core.ml.dataset import FeatureLoader
from src.core.ml.survival import SurvivalModel
from src.database.supabase_client import SupabaseClient
from src.config import OUTPUTS_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("train_survival")


def _prepare_dataset(df: pd.DataFrame, min_duration: int) -> pd.DataFrame:
    cleaned = df.copy()
    required_cols = {"duration_days", "event_observed"}
    missing = required_cols - set(cleaned.columns)
    if missing:
        raise ValueError(f"Dataset missing required columns: {missing}")

    cleaned = cleaned[cleaned["duration_days"].fillna(0) >= min_duration]
    cleaned = cleaned[cleaned["event_observed"].notna()]
    cleaned = cleaned.reset_index(drop=True)
    if cleaned.empty:
        raise ValueError("No rows left after filtering for survival training.")
    return cleaned


def _temporal_split(df: pd.DataFrame, val_size: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split by date_created: older projects for training, recent for validation.
    
    This simulates production where we predict on future projects, providing
    a more realistic estimate of model performance than random splitting.
    
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
    parser = argparse.ArgumentParser(description="Train the censor-aware survival model.")
    parser.add_argument("--lookback-days", type=int, default=1095, help="Historical window for training data.")
    parser.add_argument("--val-size", type=float, default=0.2, help="Validation split ratio.")
    parser.add_argument("--min-duration", type=int, default=5, help="Minimum observed days to keep a row.")
    parser.add_argument("--model-version", default="v1", help="Version tag stored with the artifact.")
    parser.add_argument("--output-dir", default="data/models", help="Directory for serialized models.")
    parser.add_argument("--snapshot-dir", default="data/processed", help="Directory for dataset snapshots.")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed for reproducibility.")
    parser.add_argument("--fit-aft", action="store_true", help="Also fit a Weibull AFT model for diagnostics.")
    parser.add_argument("--penalizer", type=float, default=0.5, help="L2 penalizer applied to the Cox model.")
    parser.add_argument("--l1-ratio", type=float, default=0.1, help="Elastic-net mixing parameter (0=l2, 1=l1).")
    parser.add_argument("--max-steps", type=int, default=512, help="Max Newton-Raphson steps for Cox fitting.")
    parser.add_argument(
        "--category-min-frequency",
        type=int,
        default=10,
        help="Minimum occurrences required before a categorical level is kept.",
    )
    parser.add_argument(
        "--evaluator",
        default="scripts.evaluate_survival",
        help="Module path for the evaluation CLI.",
    )
    parser.add_argument(
        "--params-file",
        type=str,
        default=None,
        help="JSON file with tuned hyperparameters (overrides CLI args for penalizer, l1_ratio, category_min_frequency).",
    )
    parser.add_argument(
        "--enable-interactions",
        action="store_true",
        default=False,
        help="Enable feature interactions (e.g., value_band x type) to capture non-linear relationships.",
    )
    args = parser.parse_args()

    # Load tuned hyperparameters from file if provided
    if args.params_file:
        params_path = Path(args.params_file)
        if params_path.exists():
            with params_path.open("r", encoding="utf-8") as fh:
                tuned_params = json.load(fh)
            logger.info("Loaded tuned parameters from %s", params_path)
            # Override CLI args with tuned values
            if "penalizer" in tuned_params:
                args.penalizer = tuned_params["penalizer"]
                logger.info("  penalizer: %.4f", args.penalizer)
            if "l1_ratio" in tuned_params:
                args.l1_ratio = tuned_params["l1_ratio"]
                logger.info("  l1_ratio: %.2f", args.l1_ratio)
            if "category_min_frequency" in tuned_params:
                args.category_min_frequency = tuned_params["category_min_frequency"]
                logger.info("  category_min_frequency: %d", args.category_min_frequency)
        else:
            logger.warning("Params file not found: %s (using CLI defaults)", params_path)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snapshot_dir = Path(args.snapshot_dir)
    model_dir = Path(args.output_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Fetching historical data (lookback=%s days)...", args.lookback_days)
    client = SupabaseClient()
    loader = FeatureLoader(client)
    raw_df = loader.fetch_historical_data(lookback_days=args.lookback_days)
    if raw_df.empty:
        raise SystemExit("No historical data returned; aborting.")

    dataset = _prepare_dataset(raw_df, min_duration=args.min_duration)
    train_df, val_df = _temporal_split(dataset, val_size=args.val_size)

    logger.info("Training rows: %s | Validation rows: %s", len(train_df), len(val_df))

    model = SurvivalModel(
        penalizer=args.penalizer,
        l1_ratio=args.l1_ratio,
        max_steps=args.max_steps,
        category_min_frequency=args.category_min_frequency,
        enable_interactions=args.enable_interactions,
    )

    train_metrics = model.fit(
        train_df,
        duration_col="duration_days",
        event_col="event_observed",
        fit_aft=args.fit_aft,
    )

    val_c_index = model.score(val_df) if not val_df.empty else None

    if val_c_index is not None:
        logger.info("Validation concordance index: %.4f", val_c_index)

    snapshot_path = snapshot_dir / f"survival_training_{timestamp}.csv"
    dataset.to_csv(snapshot_path, index=False)
    logger.info("Wrote training snapshot → %s", snapshot_path)

    artifact_path = model_dir / f"survival_{args.model_version}_{timestamp}.pkl"
    model.save(artifact_path)

    metadata = {
        "model_version": args.model_version,
        "artifact_path": str(artifact_path),
        "snapshot_path": str(snapshot_path),
        "lookback_days": args.lookback_days,
        "val_size": args.val_size,
        "split_method": "temporal",  # Uses date_created ordering instead of random split
        "min_duration": args.min_duration,
        "penalizer": args.penalizer,
        "l1_ratio": args.l1_ratio,
        "max_steps": args.max_steps,
        "category_min_frequency": args.category_min_frequency,
        "enable_interactions": args.enable_interactions,
        "params_file": args.params_file,  # Track if tuned params were used
        "timestamp_utc": timestamp,
        "train_concordance": train_metrics.get("c_index_train"),
        "val_concordance": val_c_index,
        "n_train_rows": train_metrics.get("n_rows"),
        "n_features": train_metrics.get("n_features"),
        "evaluation_report": None,
        "evaluation_metrics": None,
    }

    try:
        eval_cmd = [
            sys.executable,
            "-m",
            args.evaluator,
            "--snapshot",
            str(snapshot_path),
            "--model",
            str(artifact_path),
            "--val-size",
            str(args.val_size),
            "--output-dir",
            str(OUTPUTS_DIR / "analysis"),
            "--multi-horizon",  # Evaluate multiple horizons for comprehensive analysis
        ]
        logger.info("Running evaluator: %s", " ".join(eval_cmd))
        completed = subprocess.run(eval_cmd, check=True, capture_output=True, text=True)
        evaluation_output = json.loads(completed.stdout)
        metadata["evaluation_report"] = evaluation_output.get("report_path")
        metadata["evaluation_metrics"] = {
            k: evaluation_output.get(k)
            for k in ["train_c_index", "val_c_index", "roc_auc", "pr_auc", "dataset_hash", "multi_horizon_results"]
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Evaluation script failed: %s", exc)

    metrics_path = artifact_path.with_suffix(".json")
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2)
    logger.info("Saved training metadata → %s", metrics_path)


if __name__ == "__main__":
    main()