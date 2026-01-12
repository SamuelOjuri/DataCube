"""
Quick analysis of an AFT training CSV to diagnose accuracy gap.

Usage:
    python -m scripts.analyze_csv                     # analyze latest CSV
    python -m scripts.analyze_csv path/to/csv         # analyze specific CSV
    python -m scripts.analyze_csv --with-model        # include model predictions & MAE
    python -m scripts.analyze_csv --cap 730           # set target cap for capped MAE
"""
import argparse
import glob
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

OUTPUT_PATH = Path("data/analysis_output.txt")


def log(msg: str = "") -> None:
    print(msg)
    with OUTPUT_PATH.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")


def latest_snapshot() -> Optional[Path]:
    candidates = sorted(
        glob.glob(str(Path("data/processed") / "aft_training*.csv")),
        key=lambda p: Path(p).stat().st_mtime,
        reverse=True,
    )
    return Path(candidates[0]) if candidates else None


def percentiles(series: pd.Series, pts=None) -> str:
    pts = pts or [1, 5, 10, 25, 50, 75, 90, 95, 99]
    parts = []
    for p in pts:
        parts.append(f"p{p:02d}={series.quantile(p/100):.1f}")
    return " ".join(parts)


def describe_events(events: pd.DataFrame) -> None:
    log("=" * 60)
    log("EVENT ROWS gestation_target DISTRIBUTION")
    log("=" * 60)
    log(f"Event count: {len(events)}")
    if events.empty:
        return
    log(f"gestation_target describe:\n{events['gestation_target'].describe()}")
    log("Percentiles: " + percentiles(events["gestation_target"]))

    zero_targets = len(events[events["gestation_target"] == 0])
    small_targets = len(events[events["gestation_target"] < 10])
    nan_targets = events["gestation_target"].isna().sum()
    log(f"Events with gestation_target == 0: {zero_targets}")
    log(f"Events with gestation_target < 10 days: {small_targets}")
    log(f"Events with gestation_target NaN: {nan_targets}")

    log("\n" + "=" * 60)
    log("gestation_target PERCENTILES FOR EVENTS")
    log("=" * 60)
    log(percentiles(events["gestation_target"]))

    log("\n" + "=" * 60)
    log("DATE RANGE CHECK")
    log("=" * 60)
    if "date_created" in events.columns:
        log(f"Events date range: {events['date_created'].min()} to {events['date_created'].max()}")


def compute_model_mae(events: pd.DataFrame, target_cap: float = 730.0) -> None:
    """Compute MAE metrics when pred_p50 column is available."""
    if "pred_p50" not in events.columns:
        log("\n(No pred_p50 column; skipping MAE computation)")
        return
    
    true_vals = events["gestation_target"].values
    pred_vals = events["pred_p50"].values
    errors = np.abs(pred_vals - true_vals)
    
    raw_mae = errors.mean()
    median_ae = np.median(errors)
    
    true_capped = np.clip(true_vals, 0, target_cap)
    pred_capped = np.clip(pred_vals, 0, target_cap)
    capped_mae = np.abs(pred_capped - true_capped).mean()
    
    log_errors = np.abs(np.log1p(pred_vals) - np.log1p(true_vals))
    log_mae = log_errors.mean()
    
    log("\n" + "=" * 60)
    log("MAE EVALUATION (WITH MODEL PREDICTIONS)")
    log("=" * 60)
    log(f"  Raw MAE: {raw_mae:.2f} days")
    log(f"  Capped MAE (cap={target_cap}d): {capped_mae:.2f} days")
    log(f"  Median AE: {median_ae:.2f} days")
    log(f"  Log-space MAE: {log_mae:.4f}")
    
    # MAE by year
    if "created_year" in events.columns:
        log("\n" + "=" * 60)
        log("MAE BY CREATED_YEAR (WITH PREDICTIONS)")
        log("=" * 60)
        for year in sorted(events["created_year"].unique()):
            mask = events["created_year"] == year
            grp = events[mask]
            y_true = grp["gestation_target"].values
            y_pred = grp["pred_p50"].values
            mae = np.abs(y_pred - y_true).mean()
            med = np.median(np.abs(y_pred - y_true))
            cap_mae = np.abs(
                np.clip(y_pred, 0, target_cap) - np.clip(y_true, 0, target_cap)
            ).mean()
            log(f"  {int(year)}: n={len(grp):4d}  MAE={mae:6.1f}  medAE={med:6.1f}  cappedMAE={cap_mae:6.1f}")
    
    # MAE by bucket
    log("\n" + "=" * 60)
    log("MAE BY GESTATION BUCKET")
    log("=" * 60)
    buckets = [
        (0, 90, "0-90d"),
        (90, 180, "90-180d"),
        (180, 365, "180-365d"),
        (365, 730, "365-730d"),
        (730, float("inf"), ">730d"),
    ]
    for lo, hi, label in buckets:
        mask = (true_vals >= lo) & (true_vals < hi)
        if mask.sum() == 0:
            continue
        bucket_mae = errors[mask].mean()
        bucket_med = np.median(errors[mask])
        log(f"  {label:10s}: n={mask.sum():4d}  MAE={bucket_mae:6.1f}  medAE={bucket_med:6.1f}")


def score_events_with_model(events: pd.DataFrame) -> pd.DataFrame:
    """Load the latest AFT model and add pred_p50 column to events."""
    try:
        from src.core.ml.aft_model import load_aft_model
        model = load_aft_model(Path("data/models"))
        log(f"Loaded AFT model: dist={model.dist}, scale={model.scale}")
        
        preds = []
        for i in range(len(events)):
            pred = model.predict(events.iloc[[i]])["p50"]
            preds.append(pred)
        events = events.copy()
        events["pred_p50"] = preds
        return events
    except Exception as e:
        log(f"Failed to load model or score events: {e}")
        return events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze AFT training CSV for accuracy diagnostics."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=None,
        help="Path to CSV file (default: latest aft_training*.csv)",
    )
    parser.add_argument(
        "--with-model",
        action="store_true",
        help="Load the AFT model and compute MAE with predictions.",
    )
    parser.add_argument(
        "--cap",
        type=float,
        default=730.0,
        help="Target cap for capped MAE computation (default: 730).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    OUTPUT_PATH.write_text("", encoding="utf-8")  # reset

    csv_path = Path(args.csv_path) if args.csv_path else latest_snapshot()
    if not csv_path or not csv_path.exists():
        print("No CSV found.")
        return

    df = pd.read_csv(csv_path)
    log(f"Analyzing CSV: {csv_path}")

    log("=" * 60)
    log("OVERALL DATASET")
    log("=" * 60)
    log(f"Total rows: {len(df)}")
    log(f"Event rows (event_observed==1): {(df['event_observed']==1).sum()}")
    log(f"Censored rows (event_observed==0): {(df['event_observed']==0).sum()}")

    events = df[df["event_observed"] == 1].copy()
    
    # Filter invalid events
    events = events[events["gestation_target"] > 0]
    
    # Optionally score with model
    if args.with_model:
        log("\nScoring events with AFT model...")
        events = score_events_with_model(events)
    
    describe_events(events)

    if "date_created" in df.columns:
        log("\n" + "=" * 60)
        log("DATE RANGE (ALL)")
        log("=" * 60)
        log(f"Earliest date_created: {df['date_created'].min()}")
        log(f"Latest date_created: {df['date_created'].max()}")

    log("\n" + "=" * 60)
    log("COHORT PERCENTILES BY YEAR (EVENTS)")
    log("=" * 60)
    if "created_year" in events.columns:
        for year, grp in events.groupby("created_year"):
            log(f"Year {int(year)}: n={len(grp)}; " + percentiles(grp["gestation_target"], [10, 25, 50, 75, 90, 95, 99]))

    log("\n" + "=" * 60)
    log("TAIL CHECK (gestation_target > 730d)")
    log("=" * 60)
    if not events.empty:
        tail = events[events["gestation_target"] > 730]
        log(f"Tail count >730d: {len(tail)}")
        if len(tail) > 0:
            cols = ["monday_id", "project_name", "gestation_target", "created_year"]
            cols = [c for c in cols if c in events.columns]
            log(tail[cols].head(10).to_string(index=False))

    # Compute MAE if predictions available
    if "pred_p50" in events.columns:
        compute_model_mae(events, target_cap=args.cap)

    print(f"\nOutput saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
