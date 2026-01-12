"""
CLI to train CatBoost AFT models for gestation timeline predictions.

This replaces the LightGBM quantile regression approach with CatBoost AFT
which has shown improved accuracy (lower MAE, maintained C-index).

Example:
    python -m scripts.train_aft --clip-gestation-days 730 --calibrate-p50-bias
    python -m scripts.train_aft --lookback-days 1825 --model-version v1
    python -m scripts.train_aft --no-sweep --dist Logistic --scale 2.0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from datetime import datetime
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import OUTPUTS_DIR, DATA_DIR
from src.core.ml.dataset import FeatureLoader
from src.core.ml.aft_trainer import (
    AFTTrainer,
    AFTTrainerConfig,
    save_training_metadata,
)
from src.database.supabase_client import SupabaseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("train_aft")


def _hash_dataset(df: pd.DataFrame) -> str:
    """Compute SHA256 hash of dataset for reproducibility tracking."""
    payload = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train CatBoost AFT models for gestation predictions."
    )
    
    # Data options
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1096,
        # default=1825,
        help="Historical window for training rows (default: 1096 = 3 years).",
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help="Path to pre-built CSV dataset (skips Supabase fetch if provided).",
    )
    parser.add_argument(
        "--min-gestation-days",
        type=int,
        default=0,
        help="Minimum gestation days for events (default: 0).",
    )
    parser.add_argument(
        "--max-gestation-days",
        type=int,
        default=None,
        help="Maximum gestation days cap for events (default: no cap).",
    )
    parser.add_argument(
        "--clip-gestation-days",
        type=float,
        default=None,
        help="Optional upper clip for gestation_target (events only) to reduce tails.",
    )
    
    # Hyperparameter sweep options
    parser.add_argument(
        "--no-sweep",
        action="store_true",
        help="Skip hyperparameter sweep, use --dist and --scale directly.",
    )
    parser.add_argument(
        "--dist",
        type=str,
        default="Logistic",
        choices=["Normal", "Logistic"],
        help="AFT distribution (default: Logistic).",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=2.0,
        help="AFT scale parameter (default: 2.0).",
    )
    parser.add_argument(
        "--sweep-dists",
        type=str,
        nargs="+",
        default=["Normal", "Logistic"],
        help="Distributions to sweep (default: Normal Logistic).",
    )
    parser.add_argument(
        "--sweep-scales",
        type=float,
        nargs="+",
        default=[0.5, 1.0, 1.5, 2.0],
        help="Scales to sweep (default: 0.5 1.0 1.5 2.0).",
    )
    
    # Validation split options
    parser.add_argument(
        "--no-time-split",
        action="store_true",
        help="Use random split instead of time-based split.",
    )
    parser.add_argument(
        "--validation-months",
        type=int,
        default=6,
        help="Months for validation window in time-based split (default: 6).",
    )
    
    # Recency weighting options
    parser.add_argument(
        "--no-recency-weights",
        action="store_true",
        help="Disable recency weighting.",
    )
    parser.add_argument(
        "--recent-weight",
        type=float,
        default=2.0,
        help="Weight multiplier for recent samples (default: 2.0).",
    )
    parser.add_argument(
        "--recent-cutoff-months",
        type=int,
        default=12,
        help="Months defining 'recent' for weighting (default: 12).",
    )
    
    # CatBoost training options
    parser.add_argument(
        "--iterations",
        type=int,
        default=1500,
        help="Maximum CatBoost iterations (default: 1500).",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.03,
        help="CatBoost learning rate (default: 0.03).",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=6,
        help="CatBoost tree depth (default: 6).",
    )
    parser.add_argument(
        "--early-stopping",
        type=int,
        default=100,
        help="Early stopping rounds (default: 100).",
    )
    parser.add_argument(
        "--calibrate-p50-bias",
        action="store_true",
        help="Compute median p50 bias on val events and store as calibration offset (legacy).",
    )
    
    # Post-hoc calibration options (LinearRegression-based)
    parser.add_argument(
        "--no-calibration",
        action="store_true",
        help="Disable post-hoc calibration (global + segmented).",
    )
    parser.add_argument(
        "--no-segmented-calibration",
        action="store_true",
        help="Disable segment-specific calibration (use global only).",
    )
    parser.add_argument(
        "--min-segment-events",
        type=int,
        default=5,
        help="Minimum events to fit a segment calibrator (default: 5).",
    )
    
    # Output options
    parser.add_argument(
        "--model-version",
        type=str,
        default="v1",
        help="Version tag for model artifact (default: v1).",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=str,
        default="data/processed",
        help="Directory for training data snapshots.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/models",
        help="Directory for model artifacts.",
    )
    parser.add_argument(
        "--metadata-dir",
        type=str,
        default=str(OUTPUTS_DIR / "analysis"),
        help="Directory for training metadata JSON.",
    )
    
    # Other options
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42).",
    )
    
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    # Setup directories
    snapshot_dir = Path(args.snapshot_dir)
    model_dir = Path(args.output_dir)
    metadata_dir = Path(args.metadata_dir)
    
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    if args.csv_path:
        logger.info("Loading dataset from CSV: %s", args.csv_path)
        dataset = pd.read_csv(args.csv_path)
    else:
        logger.info("Fetching data from Supabase (lookback=%d days)", args.lookback_days)
        client = SupabaseClient()
        loader = FeatureLoader(client)
        
        snapshot_path = snapshot_dir / f"aft_training_{timestamp}.csv"
        dataset = loader.build_aft_frame(
            lookback_days=args.lookback_days,
            min_gestation_days=args.min_gestation_days,
            max_gestation_days=args.max_gestation_days,
            snapshot_path=snapshot_path,
        )
    
    if dataset.empty:
        raise SystemExit("No training data available; aborting.")
    
    # Log dataset stats
    n_events = int(dataset["event_observed"].sum()) if "event_observed" in dataset.columns else 0
    n_total = len(dataset)
    logger.info(
        "Dataset loaded: %d rows, %d events (%.1f%%), %d censored",
        n_total, n_events, 100 * n_events / n_total if n_total else 0,
        n_total - n_events,
    )
    
    dataset_hash = _hash_dataset(dataset)
    
    # Build trainer config
    config = AFTTrainerConfig(
        run_sweep=not args.no_sweep,
        sweep_distributions=args.sweep_dists,
        sweep_scales=args.sweep_scales,
        use_time_split=not args.no_time_split,
        validation_months=args.validation_months,
        apply_recency_weights=not args.no_recency_weights,
        recent_weight=args.recent_weight,
        recent_cutoff_months=args.recent_cutoff_months,
        iterations=args.iterations,
        learning_rate=args.learning_rate,
        depth=args.depth,
        early_stopping_rounds=args.early_stopping,
        seed=args.random_state,
        target_clip_days=args.clip_gestation_days,
        calibrate_p50_bias=args.calibrate_p50_bias,
        # Post-hoc calibration settings
        apply_calibration=not args.no_calibration,
        segmented_calibration=not args.no_segmented_calibration,
        min_segment_events=args.min_segment_events,
    )
    
    # If not sweeping, override with specified dist/scale
    if args.no_sweep:
        config.sweep_distributions = [args.dist]
        config.sweep_scales = [args.scale]
    
    # Train
    trainer = AFTTrainer(config)
    result = trainer.train(dataset, model_version=args.model_version)
    
    # Save model artifact
    artifact_path = model_dir / f"aft_{args.model_version}_{timestamp}.pkl"
    result.model.save(artifact_path)

    # Build metrics payload (similar to survival script)
    metrics = {
        "model_version": args.model_version,
        "artifact_path": str(artifact_path),
        "snapshot_path": str(snapshot_path) if not args.csv_path else str(args.csv_path),
        "timestamp_utc": timestamp,
        "dataset_hash": dataset_hash,
        "best_dist": result.best_dist,
        "best_scale": result.best_scale,
        "baseline_mae": result.baseline_mae,
        "median_ae": result.median_ae,
        "capped_mae": result.capped_mae,
        "baseline_c_index": result.baseline_c_index,
        "train_rows": result.train_rows,
        "val_rows": result.val_rows,
        "val_events": result.val_events,
        "sweep_results": result.sweep_results,
        "config": result.config,
        "calibration": result.calibration_info,  # Post-hoc calibration details
    }

    metrics_path = artifact_path.with_suffix(".json")
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    logger.info("Saved training metadata → %s", metrics_path)

    # Save training metadata 
    metadata_path = save_training_metadata(result, metadata_dir)
    
    # Also save a comprehensive summary
    summary = {
        "model_version": args.model_version,
        "artifact_path": str(artifact_path),
        "metadata_path": str(metadata_path),
        "timestamp": timestamp,
        "dataset_hash": dataset_hash,
        "best_dist": result.best_dist,
        "best_scale": result.best_scale,
        "baseline_mae": result.baseline_mae,
        "median_ae": result.median_ae,
        "capped_mae": result.capped_mae,
        "baseline_c_index": result.baseline_c_index,
        "train_rows": result.train_rows,
        "val_rows": result.val_rows,
        "val_events": result.val_events,
        "sweep_results": result.sweep_results,
        "config": result.config,
        "calibration": result.calibration_info,  # Post-hoc calibration details
    }
    
    summary_path = metadata_dir / f"aft_summary_{timestamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    
    # Extract calibration info for display
    cal_info = result.calibration_info or {}
    cal_method = cal_info.get("selected_method", "none")
    cal_final_mae = cal_info.get("final_mae")
    cal_improvement = cal_info.get("improvement_days", 0)
    
    # Print summary
    print("\n" + "=" * 70)
    print("AFT TRAINING COMPLETE")
    print("=" * 70)
    print(f"  Model artifact: {artifact_path}")
    print(f"  Best config: dist={result.best_dist}, scale={result.best_scale}")
    print(f"  Validation MAE: {result.baseline_mae:.2f} days")
    print(f"  Validation Median AE: {result.median_ae:.2f} days")
    print(f"  Validation Capped MAE (730d): {result.capped_mae:.2f} days")
    print(f"  Validation C-index: {result.baseline_c_index:.4f}")
    print(f"  Train rows: {result.train_rows}, Val rows: {result.val_rows}")
    print(f"  Val events: {result.val_events}")
    print()
    print("  Calibration:")
    print(f"    Method: {cal_method}")
    if cal_final_mae is not None and cal_improvement > 0:
        print(f"    Calibrated MAE: {cal_final_mae:.2f} days (improved by {cal_improvement:.2f} days)")
    elif cal_method != "none":
        print(f"    Calibrated MAE: {cal_final_mae:.2f} days" if cal_final_mae else "    No improvement")
    print("=" * 70)
    
    logger.info("AFT training complete. Artifact: %s", artifact_path)


if __name__ == "__main__":
    main()

