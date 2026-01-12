"""
Test AFT model inference:
- Load latest AFT artifact
- Run analysis on provided project IDs
- Compute MAE on events from a local CSV (optional)
"""

import os
from pathlib import Path
import pandas as pd
import random
import glob

from src.core.ml.aft_model import load_aft_model
from src.database.supabase_client import SupabaseClient
from src import config

# ---------------------------------------------------------------------
# CONFIGURE HERE

SURVIVAL_MODELS_DIR = "data/models"
AFT_MODELS_DIR = "data/models"
# CSV_PATH = "data/processed/aft_training_20251207_010628.csv"  # set to None to skip MAE calc

# How many random project IDs to test
N_RANDOM_IDS = 15

def fetch_random_project_ids(n: int = N_RANDOM_IDS):
    client = SupabaseClient()
    # Fetch a pool of recent projects with a monday_id; adjust filters if needed
    resp = (
        client.client.table("projects")
        .select("monday_id")
        .not_.is_("monday_id", None)
        .order("date_created", desc=True)
        .limit(500)  # pull a pool; we’ll sample locally
        .execute()
    )
    rows = resp.data or []
    pool = [r["monday_id"] for r in rows if r.get("monday_id")]
    if len(pool) < n:
        print(f"Only {len(pool)} IDs available; using all.")
        return pool
    return random.sample(pool, n)

def latest_survival_artifact(dir_path: str = SURVIVAL_MODELS_DIR) -> str:
    """Pick the newest survival_*.pkl from the directory."""
    candidates = sorted(
        glob.glob(str(Path(dir_path) / "survival_*.pkl")),
        key=lambda p: Path(p).stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return ""
    return candidates[0]

# ---------------------------------------------------------------------
def latest_aft_training_csv(dir_path: str = "data/processed") -> str:
    candidates = sorted(
        glob.glob(str(Path(dir_path) / "aft_training*.csv")),
        key=lambda p: Path(p).stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else ""

def load_latest_aft():
    model = load_aft_model(AFT_MODELS_DIR)
    print(f"Loaded AFT model: dist={model.dist}, scale={model.scale}, fitted={model.is_fitted}")
    return model

def run_shadow_on_ids(ids):
    from src.services.ml_analysis_service import MLAnalysisService  # import after config override

    svc = MLAnalysisService(db_client=SupabaseClient())

    results = []

    for pid in ids:
        out = svc.analyze_and_store(pid)
        results.append(out)
        print(f"[shadow] {pid} -> success={out.get('success')}")
        if out.get("success"):
            res = out["result"]
            actual = res.get("actual_gestation_days")
            print(
                f"  win30={res.get('win_prob_30d'):.3f} "
                f"win90={res.get('win_prob_90d'):.3f} "
                f"eventual={res.get('eventual_win_prob'):.3f} "
                f"p50={res.get('gestation_p50')} "
                f"p25={res.get('gestation_p25')} "
                f"p75={res.get('gestation_p75')} "
                f"actual={actual if actual is not None else 'n/a'} "
                f"ms={res.get('processing_time_ms')}"
            )
        else:
            print(f"  error={out.get('error')}")

    return results

def compute_mae_from_csv(model, csv_path: str, target_cap: float = 730.0):
    """
    Compute multiple MAE metrics on events from CSV:
    - Raw MAE (no cap)
    - Capped MAE (targets clipped to target_cap)
    - Median AE (robust to tails)
    - Log-space MAE (log1p transform)
    - MAE by created_year cohort
    """
    import numpy as np
    
    if not csv_path or not Path(csv_path).exists():
        print("CSV not provided or not found; skipping MAE computation.")
        return
    df = pd.read_csv(csv_path)
    if "event_observed" not in df.columns or "gestation_target" not in df.columns:
        print("CSV missing required columns; skipping MAE.")
        return
    events = df[df["event_observed"] == 1].copy()
    
    # Filter out invalid zero-target rows (gestation cannot be 0 days)
    zero_count = (events["gestation_target"] == 0).sum()
    if zero_count > 0:
        print(f"Filtering out {zero_count} events with gestation_target=0 (invalid data)")
    events = events[events["gestation_target"] > 0]
    
    if events.empty:
        print("No valid event rows; skipping MAE.")
        return
    
    # Generate predictions
    print(f"Scoring {len(events)} events...")
    preds = []
    for i in range(len(events)):
        pred = model.predict(events.iloc[[i]])["p50"]
        preds.append(pred)
    events["pred_p50"] = preds
    
    # Compute errors
    true_vals = events["gestation_target"].values
    pred_vals = events["pred_p50"].values
    errors = np.abs(pred_vals - true_vals)
    
    # Raw MAE
    raw_mae = errors.mean()
    
    # Capped MAE (cap targets at target_cap, same as training)
    true_capped = np.clip(true_vals, 0, target_cap)
    pred_capped = np.clip(pred_vals, 0, target_cap)
    capped_errors = np.abs(pred_capped - true_capped)
    capped_mae = capped_errors.mean()
    
    # Median AE (robust to tails)
    median_ae = np.median(errors)
    
    # Log-space MAE (log1p transform)
    log_errors = np.abs(np.log1p(pred_vals) - np.log1p(true_vals))
    log_mae = log_errors.mean()
    
    # Summary
    print("\n" + "=" * 60)
    print("MAE EVALUATION SUMMARY")
    print("=" * 60)
    print(f"  Events scored: {len(events)}")
    print(f"  Raw MAE: {raw_mae:.2f} days")
    print(f"  Capped MAE (cap={target_cap}d): {capped_mae:.2f} days")
    print(f"  Median AE: {median_ae:.2f} days")
    print(f"  Log-space MAE: {log_mae:.4f}")
    
    # MAE by created_year
    if "created_year" in events.columns:
        print("\n" + "=" * 60)
        print("MAE BY CREATED_YEAR COHORT")
        print("=" * 60)
        for year in sorted(events["created_year"].unique()):
            mask = events["created_year"] == year
            year_events = events[mask]
            year_true = year_events["gestation_target"].values
            year_pred = year_events["pred_p50"].values
            year_mae = np.abs(year_pred - year_true).mean()
            year_median_ae = np.median(np.abs(year_pred - year_true))
            year_capped_mae = np.abs(
                np.clip(year_pred, 0, target_cap) - np.clip(year_true, 0, target_cap)
            ).mean()
            print(
                f"  {int(year)}: n={len(year_events):4d}  "
                f"MAE={year_mae:6.1f}  medAE={year_median_ae:6.1f}  "
                f"cappedMAE={year_capped_mae:6.1f}"
            )
    
    # Tail breakdown
    print("\n" + "=" * 60)
    print("MAE BY GESTATION BUCKET")
    print("=" * 60)
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
        bucket_median = np.median(errors[mask])
        print(f"  {label:10s}: n={mask.sum():4d}  MAE={bucket_mae:6.1f}  medAE={bucket_median:6.1f}")
    
    print("=" * 60)

def main():
    # Force AFT for gestation; pick latest survival for win probs
    os.environ["GESTATION_MODEL_TYPE"] = "aft"
    surv_path = latest_survival_artifact()
    if surv_path:
        config.SURVIVAL_MODEL_PATH = Path(surv_path)  # force the correct survival file
        print(f"Using survival artifact: {surv_path}")
    else:
        print("No survival_*.pkl found; survival probs will be neutral (0.5).")

    model = load_latest_aft()

    project_ids = fetch_random_project_ids()
    print(f"Testing {len(project_ids)} random projects: {project_ids}")

    if project_ids:
        run_shadow_on_ids(project_ids)

    csv_path = latest_aft_training_csv()
    if csv_path:
        print(f"Using AFT training CSV: {csv_path}")
    else:
        print("No aft_training*.csv found; skipping MAE.")

    compute_mae_from_csv(model, csv_path)

if __name__ == "__main__":
    main()