"""
Hyperparameter tuning for the survival model using temporal cross-validation.

This script performs grid search over regularization parameters using an expanding
window cross-validation approach that respects the temporal ordering of data.

Usage:
    python -m scripts.tune_survival --lookback-days 1095 --n-folds 5
"""

import argparse
import json
import logging
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from src.core.ml.dataset import FeatureLoader
from src.core.ml.survival import SurvivalModel
from src.database.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("tune_survival")

# Default hyperparameter search space
PARAM_GRID = {
    "penalizer": [0.01, 0.1, 0.5, 1.0, 2.0],
    "l1_ratio": [0.0, 0.1, 0.3, 0.5, 1.0],
    "category_min_frequency": [5, 10, 20],
}


def _prepare_dataset(df: pd.DataFrame, min_duration: int) -> pd.DataFrame:
    """Filter dataset for survival training requirements."""
    cleaned = df.copy()
    required_cols = {"duration_days", "event_observed", "date_created"}
    missing = required_cols - set(cleaned.columns)
    if missing:
        raise ValueError(f"Dataset missing required columns: {missing}")

    cleaned = cleaned[cleaned["duration_days"].fillna(0) >= min_duration]
    cleaned = cleaned[cleaned["event_observed"].notna()]
    cleaned["date_created"] = pd.to_datetime(cleaned["date_created"], errors="coerce")
    cleaned = cleaned[cleaned["date_created"].notna()]
    cleaned = cleaned.sort_values("date_created").reset_index(drop=True)
    
    if cleaned.empty:
        raise ValueError("No rows left after filtering for survival training.")
    return cleaned


def _temporal_cv_splits(
    df: pd.DataFrame, n_folds: int, min_train_size: float = 0.3
) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Generate temporal cross-validation splits using expanding window.
    
    Each fold uses all data up to a certain point for training, and the next
    chunk for validation. This simulates production where we train on past
    data and predict on future data.
    
    Args:
        df: DataFrame sorted by date_created.
        n_folds: Number of CV folds.
        min_train_size: Minimum fraction of data for the first training set.
    
    Returns:
        List of (train_df, val_df) tuples.
    """
    n = len(df)
    min_train_idx = int(n * min_train_size)
    remaining = n - min_train_idx
    fold_size = remaining // n_folds
    
    if fold_size < 10:
        logger.warning("Fold size too small (%d), reducing n_folds", fold_size)
        n_folds = max(2, remaining // 10)
        fold_size = remaining // n_folds
    
    splits = []
    for i in range(n_folds):
        val_start = min_train_idx + i * fold_size
        val_end = min(val_start + fold_size, n) if i < n_folds - 1 else n
        
        train_df = df.iloc[:val_start].reset_index(drop=True)
        val_df = df.iloc[val_start:val_end].reset_index(drop=True)
        
        if not train_df.empty and not val_df.empty:
            splits.append((train_df, val_df))
    
    return splits


def _evaluate_params(
    df: pd.DataFrame,
    params: Dict[str, Any],
    n_folds: int,
    min_train_size: float,
) -> Dict[str, Any]:
    """
    Evaluate a single parameter combination using temporal CV.
    
    Returns:
        Dictionary with parameter values and CV metrics.
    """
    splits = _temporal_cv_splits(df, n_folds, min_train_size)
    
    train_scores = []
    val_scores = []
    
    for fold_idx, (train_df, val_df) in enumerate(splits):
        try:
            model = SurvivalModel(
                penalizer=params["penalizer"],
                l1_ratio=params["l1_ratio"],
                category_min_frequency=params["category_min_frequency"],
            )
            metrics = model.fit(
                train_df,
                duration_col="duration_days",
                event_col="event_observed",
            )
            
            train_c = metrics.get("c_index_train")
            val_c = model.score(val_df)
            
            if train_c is not None:
                train_scores.append(train_c)
            if val_c is not None:
                val_scores.append(val_c)
                
        except Exception as exc:
            logger.warning("Fold %d failed for params %s: %s", fold_idx, params, exc)
            continue
    
    result = {
        **params,
        "n_folds_completed": len(val_scores),
        "mean_train_c_index": float(np.mean(train_scores)) if train_scores else None,
        "std_train_c_index": float(np.std(train_scores)) if train_scores else None,
        "mean_val_c_index": float(np.mean(val_scores)) if val_scores else None,
        "std_val_c_index": float(np.std(val_scores)) if val_scores else None,
    }
    
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hyperparameter tuning for survival model using temporal CV."
    )
    parser.add_argument(
        "--lookback-days", type=int, default=1095,
        help="Historical window for training data."
    )
    parser.add_argument(
        "--min-duration", type=int, default=5,
        help="Minimum observed days to keep a row."
    )
    parser.add_argument(
        "--n-folds", type=int, default=5,
        help="Number of temporal CV folds."
    )
    parser.add_argument(
        "--min-train-size", type=float, default=0.3,
        help="Minimum fraction of data for the first training fold."
    )
    parser.add_argument(
        "--output-dir", default="data/tuning",
        help="Directory for tuning results."
    )
    parser.add_argument(
        "--penalizers", type=float, nargs="+",
        default=PARAM_GRID["penalizer"],
        help="Penalizer values to search."
    )
    parser.add_argument(
        "--l1-ratios", type=float, nargs="+",
        default=PARAM_GRID["l1_ratio"],
        help="L1 ratio values to search."
    )
    parser.add_argument(
        "--category-min-frequencies", type=int, nargs="+",
        default=PARAM_GRID["category_min_frequency"],
        help="Category minimum frequency values to search."
    )
    args = parser.parse_args()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Fetch data
    logger.info("Fetching historical data (lookback=%s days)...", args.lookback_days)
    client = SupabaseClient()
    loader = FeatureLoader(client)
    raw_df = loader.fetch_historical_data(lookback_days=args.lookback_days)
    if raw_df.empty:
        raise SystemExit("No historical data returned; aborting.")

    dataset = _prepare_dataset(raw_df, min_duration=args.min_duration)
    logger.info("Dataset prepared: %d rows", len(dataset))

    # Build parameter grid
    param_grid = {
        "penalizer": args.penalizers,
        "l1_ratio": args.l1_ratios,
        "category_min_frequency": args.category_min_frequencies,
    }
    
    param_combinations = [
        dict(zip(param_grid.keys(), values))
        for values in product(*param_grid.values())
    ]
    
    logger.info("Starting grid search over %d parameter combinations...", len(param_combinations))
    
    results = []
    for idx, params in enumerate(param_combinations):
        logger.info(
            "[%d/%d] Evaluating: penalizer=%.3f, l1_ratio=%.2f, category_min_frequency=%d",
            idx + 1, len(param_combinations),
            params["penalizer"], params["l1_ratio"], params["category_min_frequency"]
        )
        result = _evaluate_params(
            dataset, params, n_folds=args.n_folds, min_train_size=args.min_train_size
        )
        results.append(result)
        
        if result["mean_val_c_index"] is not None:
            logger.info(
                "  -> mean_val_c_index=%.4f (+/- %.4f)",
                result["mean_val_c_index"],
                result["std_val_c_index"] or 0.0
            )

    # Sort by validation c-index (descending)
    valid_results = [r for r in results if r["mean_val_c_index"] is not None]
    if not valid_results:
        logger.error("No valid results from tuning. Check data and parameters.")
        raise SystemExit("Tuning failed - no valid results.")
    
    valid_results.sort(key=lambda x: x["mean_val_c_index"], reverse=True)
    best = valid_results[0]
    
    logger.info("=" * 60)
    logger.info("Best parameters:")
    logger.info("  penalizer: %.4f", best["penalizer"])
    logger.info("  l1_ratio: %.2f", best["l1_ratio"])
    logger.info("  category_min_frequency: %d", best["category_min_frequency"])
    logger.info("  mean_val_c_index: %.4f (+/- %.4f)", 
                best["mean_val_c_index"], best["std_val_c_index"] or 0.0)
    logger.info("=" * 60)

    # Save results
    output = {
        "timestamp_utc": timestamp,
        "lookback_days": args.lookback_days,
        "min_duration": args.min_duration,
        "n_folds": args.n_folds,
        "min_train_size": args.min_train_size,
        "n_rows": len(dataset),
        "param_grid": param_grid,
        "best_params": {
            "penalizer": best["penalizer"],
            "l1_ratio": best["l1_ratio"],
            "category_min_frequency": best["category_min_frequency"],
        },
        "best_metrics": {
            "mean_val_c_index": best["mean_val_c_index"],
            "std_val_c_index": best["std_val_c_index"],
            "mean_train_c_index": best["mean_train_c_index"],
            "std_train_c_index": best["std_train_c_index"],
        },
        "all_results": results,
    }
    
    results_path = output_dir / f"survival_tuning_{timestamp}.json"
    with results_path.open("w", encoding="utf-8") as fh:
        json.dump(output, fh, indent=2)
    logger.info("Saved tuning results → %s", results_path)
    
    # Also save best params for easy loading by train_survival.py
    best_params_path = output_dir / "best_survival_params.json"
    with best_params_path.open("w", encoding="utf-8") as fh:
        json.dump(output["best_params"], fh, indent=2)
    logger.info("Saved best params → %s", best_params_path)


if __name__ == "__main__":
    main()
