# -*- coding: utf-8 -*-
"""
CatBoost AFT Survival Analysis Pipeline (V2)
=============================================
Enhanced with:
- Hyperparameter sweep (dist/scale)
- Post-hoc calibration layer
- Time-based validation split
- Recency weighting
- Residual diagnostics
"""

# ==================================================================================
# SURVIVAL ANALYSIS PIPELINE (V2): CATBOOST AFT QUANTILE REGRESSION
# ==================================================================================

# ----------------------------------------------------------------------------------
# 1. SETUP & IMPORTS
# ----------------------------------------------------------------------------------
try:
    import catboost  # noqa: F401
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "catboost"])

try:
    import lifelines  # noqa: F401
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "lifelines"])

import json
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from catboost import CatBoostRegressor, Pool
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from lifelines.utils import concordance_index
from scipy.stats import norm, logistic

try:
    from importlib import import_module
    display = import_module("IPython.display").display
except (ImportError, ModuleNotFoundError):
    def display(obj):
        print(obj)

# ----------------------------------------------------------------------------------
# 2. CONFIGURATION
# ----------------------------------------------------------------------------------
# Toggle features on/off
CONFIG = {
    # Data path
    "csv_path": r"data\processed\survival_training_20251124_142413.csv",
    
    # Run modes
    "run_hyperparameter_sweep": True,      # Step 2: AFT dist/scale search
    "use_time_based_split": True,          # Step 4: Time-based validation
    "apply_recency_weights": True,         # Step 4: Sample weighting
    "apply_calibration": True,             # Step 3: Post-hoc calibration
    "segmented_calibration": True,         # Step 3: Segment-specific calibration
    "run_diagnostics": True,               # Step 5: Residual diagnostics
    
    # Sweep settings
    "sweep_distributions": ["Normal", "Logistic"],  # Gumbel can be added
    "sweep_scales": [0.5, 1.0, 1.5, 2.0],
    
    # Time-split settings (months for validation window)
    "validation_months": 6,
    
    # Recency weighting
    "recent_weight": 2.0,        # Weight for recent samples
    "recent_cutoff_months": 12,  # Samples within this window get boosted
    
    # Calibration segment column
    "calibration_segment_col": "value_band",
    "min_segment_events": 5,     # Minimum events to fit segment calibrator
    
    # Output directory
    "output_dir": "outputs/analysis",
}

# Reproducibility
SEED = 42
np.random.seed(SEED)

plt.style.use("seaborn-v0_8")
plt.rcParams["figure.figsize"] = (12, 5)

print("Libraries imported.")
print(f"Configuration: sweep={CONFIG['run_hyperparameter_sweep']}, "
      f"time_split={CONFIG['use_time_based_split']}, "
      f"calibration={CONFIG['apply_calibration']}")

# ----------------------------------------------------------------------------------
# 3. LOAD DATA & BASIC INSPECTION
# ----------------------------------------------------------------------------------
CSV_PATH = CONFIG["csv_path"]

df = pd.read_csv(CSV_PATH)
print(f"\nData loaded. Shape: {df.shape}")

display(df.head())

survival_cols = [
    "gestation_target",
    "duration_days",
    "time_open_days",
    "status_category",
    "event_observed",
    "is_censored",
]
print("\n--- Survival Column Summary ---")
display(df[survival_cols].describe(include="all"))

# Censoring breakdown
n_total = len(df)
n_events = int(df["event_observed"].sum())
n_cens = n_total - n_events
print(f"\nTotal enquiries:   {n_total}")
print(f"Events (Won):      {n_events} ({n_events / n_total:.1%})")
print(f"Censored (Open/Lost): {n_cens} ({n_cens / n_total:.1%})")

# ----------------------------------------------------------------------------------
# 4. BUILD SURVIVAL TARGET FOR CATBOOST AFT
# ----------------------------------------------------------------------------------
"""
CatBoost AFT expects a 2D label: [t_lower, t_upper]

- For events (event_observed == 1):
    t_lower = t_upper = gestation_target
- For right-censored (event_observed == 0):
    t_lower = time_open_days (time observed so far)
    t_upper = -1  (CatBoost convention for [t_lower, +inf))
"""

EPS = 1.0

mask_event = df["event_observed"] == 1

t_lower = np.zeros(len(df), dtype=float)
t_upper = np.zeros(len(df), dtype=float)

# Events: exact gestation
t_event = df.loc[mask_event, "gestation_target"].astype(float).clip(lower=EPS)
t_lower[mask_event] = t_event
t_upper[mask_event] = t_event

# Censored: age so far, then infinity
t_cens = df.loc[~mask_event, "time_open_days"].astype(float).clip(lower=EPS)
t_lower[~mask_event] = t_cens
t_upper[~mask_event] = -1.0

y_aft = pd.DataFrame({"lower": t_lower, "upper": t_upper}, index=df.index)

print("\n--- AFT Target Preview ---")
display(y_aft.head())
print("Target shape:", y_aft.shape)

# ----------------------------------------------------------------------------------
# 5. FEATURE ENGINEERING & PREPROCESSING
# ----------------------------------------------------------------------------------
"""
We remove:
- IDs
- Survival targets and outcome columns
- Strong label-leaky fields (status_category, censor_reason, lost_to_who_or_why, feedback)

We:
- Parse date columns and create 'days from date_created' features
- Drop raw datetime columns afterwards
- Let CatBoost handle numeric NaNs
- Explicitly encode missing for categorical features as 'Missing'
"""

drop_cols = [
    # IDs
    "id",
    "monday_id",
    "item_name",
    "project_name",
    # Survival targets / leakage
    "gestation_period",
    "gestation_target",
    "duration_days",
    "time_open_days",
    "event_observed",
    "is_censored",
    "observation_end_date",
    # Final-outcome leakage
    "status_category",
    "censor_reason",
    "lost_to_who_or_why",
    "feedback",
]

X = df.drop(columns=drop_cols, errors="ignore").copy()

# --- Date parsing & relative features ---
date_cols = [
    "date_created",
    "expected_start_date",
    "follow_up_date",
    "first_date_designed",
    "last_date_designed",
    "first_date_invoiced",
    "last_synced_at",
    "created_at",
    "updated_at",
]

# Parse to datetime with UTC
for col in date_cols:
    if col in X.columns:
        X[col] = pd.to_datetime(X[col], errors="coerce", utc=True)

# Store date_created for time-based splitting BEFORE dropping
if "date_created" in X.columns:
    baseline = X["date_created"].copy()
    df["_date_created_parsed"] = baseline  # Store in df for splitting
elif "created_at" in X.columns:
    baseline = X["created_at"].copy()
    df["_date_created_parsed"] = baseline
else:
    baseline = None

if baseline is not None:
    for col in date_cols:
        if col in X.columns and col != baseline.name:
            X[f"{col}_delta_days"] = (X[col] - baseline).dt.days

    # Calendar features from baseline
    X["creation_year"] = baseline.dt.year
    X["creation_month"] = baseline.dt.month

# Drop raw datetime columns after constructing deltas
X = X.drop(columns=[c for c in date_cols if c in X.columns])

# --- Categorical handling ---
cat_features_names = X.select_dtypes(include=["object", "category"]).columns.tolist()
for col in cat_features_names:
    X[col] = X[col].astype("string").fillna("Missing")

print("\n--- Feature Matrix Info ---")
print("Shape:", X.shape)
print("Categorical features:", cat_features_names)

# ----------------------------------------------------------------------------------
# 6. TRAIN / VALIDATION SPLIT (TIME-BASED OR RANDOM)
# ----------------------------------------------------------------------------------
stratify_col = df["event_observed"]

if CONFIG["use_time_based_split"] and "_date_created_parsed" in df.columns:
    print("\n--- Using Time-Based Validation Split ---")
    
    # Find cutoff date for validation window
    max_date = df["_date_created_parsed"].max()
    cutoff_date = max_date - pd.DateOffset(months=CONFIG["validation_months"])
    
    train_mask = df["_date_created_parsed"] <= cutoff_date
    val_mask = df["_date_created_parsed"] > cutoff_date
    
    X_train = X.loc[train_mask].copy()
    X_val = X.loc[val_mask].copy()
    y_train = y_aft.loc[train_mask].copy()
    y_val = y_aft.loc[val_mask].copy()
    
    print(f"  Cutoff date: {cutoff_date}")
    print(f"  Train: {X_train.shape[0]} rows (before cutoff)")
    print(f"  Val:   {X_val.shape[0]} rows (after cutoff)")
    print(f"  Train events: {df.loc[train_mask, 'event_observed'].sum()}")
    print(f"  Val events:   {df.loc[val_mask, 'event_observed'].sum()}")
else:
    print("\n--- Using Random Stratified Split ---")
    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y_aft,
        test_size=0.2,
        random_state=SEED,
        stratify=stratify_col,
    )

print("\n--- Train / Validation Split ---")
print("X_train:", X_train.shape, "X_val:", X_val.shape)

# ----------------------------------------------------------------------------------
# 7. SAMPLE WEIGHTS (RECENCY WEIGHTING)
# ----------------------------------------------------------------------------------
train_weights = None

if CONFIG["apply_recency_weights"] and "_date_created_parsed" in df.columns:
    print("\n--- Applying Recency Weights ---")
    
    max_date = df["_date_created_parsed"].max()
    recency_cutoff = max_date - pd.DateOffset(months=CONFIG["recent_cutoff_months"])
    
    train_dates = df.loc[X_train.index, "_date_created_parsed"]
    train_weights = np.where(
        train_dates > recency_cutoff,
        CONFIG["recent_weight"],
        1.0
    )
    
    n_boosted = (train_weights > 1).sum()
    print(f"  Recency cutoff: {recency_cutoff}")
    print(f"  Boosted samples: {n_boosted} ({n_boosted/len(train_weights):.1%})")
    print(f"  Weight for recent: {CONFIG['recent_weight']}")

# ----------------------------------------------------------------------------------
# 8. QUANTILE PREDICTION HELPER
# ----------------------------------------------------------------------------------
def predict_gestation_quantiles(
    model: CatBoostRegressor,
    X_data: pd.DataFrame,
    scale: float = 1.0,
    dist: str = "Normal",
    calibrator=None,
    segment_calibrators=None,
    segment_col: str = None,
    df_segments: pd.Series = None,
) -> pd.DataFrame:
    """
    Compute gestation time quantiles (days) from CatBoost AFT log-time predictions.
    
    AFT model:
        log(T) = mu + scale * Z,   Z ~ chosen distribution
    
    Supports optional calibration (global or segmented).
    """
    # Get raw log-time prediction
    log_mu = model.predict(X_data).astype(float)
    
    # Apply calibration if provided
    if segment_calibrators is not None and segment_col is not None and df_segments is not None:
        # Segmented calibration
        log_mu_cal = log_mu.copy()
        segments = df_segments.loc[X_data.index]
        
        for seg, cal in segment_calibrators.items():
            seg_mask = segments == seg
            if seg_mask.any():
                log_mu_cal[seg_mask] = cal.predict(log_mu[seg_mask].reshape(-1, 1)).flatten()
        
        # Fallback to global calibrator for segments without specific calibrators
        if calibrator is not None:
            uncalibrated_mask = ~segments.isin(segment_calibrators.keys())
            if uncalibrated_mask.any():
                log_mu_cal[uncalibrated_mask] = calibrator.predict(
                    log_mu[uncalibrated_mask].reshape(-1, 1)
                ).flatten()
        
        log_mu = log_mu_cal
        
    elif calibrator is not None:
        # Global calibration
        log_mu = calibrator.predict(log_mu.reshape(-1, 1)).flatten()
    
    # Clip for numeric stability
    log_mu = np.clip(log_mu, -10, 10)
    
    # Compute quantiles based on distribution
    if dist == "Normal":
        z25 = norm.ppf(0.25)
        z50 = norm.ppf(0.50)
        z75 = norm.ppf(0.75)
    elif dist == "Logistic":
        z25 = logistic.ppf(0.25)
        z50 = logistic.ppf(0.50)
        z75 = logistic.ppf(0.75)
    else:  # Default to Normal
        z25 = norm.ppf(0.25)
        z50 = norm.ppf(0.50)
        z75 = norm.ppf(0.75)
    
    t25 = np.exp(log_mu + scale * z25)
    t50 = np.exp(log_mu + scale * z50)
    t75 = np.exp(log_mu + scale * z75)
    
    return pd.DataFrame(
        {
            "pred_p25_days": t25,
            "pred_p50_days": t50,
            "pred_p75_days": t75,
        },
        index=X_data.index,
    )

# ----------------------------------------------------------------------------------
# 9. EVALUATION HELPER
# ----------------------------------------------------------------------------------
def evaluate_model(
    model,
    X_val,
    y_val,
    df,
    scale=1.0,
    dist="Normal",
    calibrator=None,
    segment_calibrators=None,
    segment_col=None,
    verbose=True,
):
    """Evaluate model on validation set, returning MAE and C-index."""
    
    # Get segment data if needed
    df_segments = None
    if segment_calibrators is not None and segment_col is not None:
        df_segments = df[segment_col]
    
    val_quantiles = predict_gestation_quantiles(
        model, X_val, scale=scale, dist=dist,
        calibrator=calibrator,
        segment_calibrators=segment_calibrators,
        segment_col=segment_col,
        df_segments=df_segments,
    )
    
    val_results = df.loc[X_val.index, ["gestation_target", "event_observed", "time_open_days"]].copy()
    val_results = val_results.join(val_quantiles)
    
    # MAE on uncensored events
    unc_mask = val_results["event_observed"] == 1
    unc_res = val_results[unc_mask]
    
    mae = np.nan
    if len(unc_res) > 0:
        mae = np.mean(np.abs(unc_res["gestation_target"] - unc_res["pred_p50_days"]))
    
    # C-index
    val_times = y_val["lower"].values
    val_events = df.loc[X_val.index, "event_observed"].values.astype(bool)
    val_log_pred = model.predict(X_val).astype(float)
    
    c_idx = concordance_index(val_times, val_log_pred, val_events)
    
    if verbose:
        print(f"  MAE (uncensored): {mae:.2f} days | C-index: {c_idx:.4f}")
    
    return mae, c_idx, val_results

# ----------------------------------------------------------------------------------
# 10. HYPERPARAMETER SWEEP FOR AFT HEAD
# ----------------------------------------------------------------------------------
best_config = {"dist": "Normal", "scale": 1.0}
sweep_results = []

if CONFIG["run_hyperparameter_sweep"]:
    print("\n" + "="*70)
    print("HYPERPARAMETER SWEEP: AFT dist/scale")
    print("="*70)
    
    for dist in CONFIG["sweep_distributions"]:
        for scale in CONFIG["sweep_scales"]:
            print(f"\n--- Testing dist={dist}, scale={scale} ---")
            
            loss_fn = f"SurvivalAft:dist={dist};scale={scale}"
            
            train_pool = Pool(
                X_train, label=y_train, cat_features=cat_features_names,
                weight=train_weights,
            )
            val_pool = Pool(X_val, label=y_val, cat_features=cat_features_names)
            
            model = CatBoostRegressor(
                loss_function=loss_fn,
                eval_metric="SurvivalAft",
                iterations=1500,
                learning_rate=0.03,
                depth=6,
                l2_leaf_reg=3.0,
                random_seed=SEED,
                verbose=0,
                od_type="Iter",
                od_wait=100,
                allow_writing_files=False,
            )
            
            model.fit(train_pool, eval_set=val_pool, use_best_model=True)
            
            mae, c_idx, _ = evaluate_model(
                model, X_val, y_val, df, scale=scale, dist=dist, verbose=True
            )
            
            sweep_results.append({
                "dist": dist,
                "scale": scale,
                "mae": mae,
                "c_index": c_idx,
                "best_iteration": model.get_best_iteration(),
            })
    
    # Find best config by MAE (ensure C-index doesn't collapse)
    sweep_df = pd.DataFrame(sweep_results)
    valid_configs = sweep_df[sweep_df["c_index"] > 0.6]  # Minimum acceptable C-index
    
    if len(valid_configs) > 0:
        best_row = valid_configs.loc[valid_configs["mae"].idxmin()]
        best_config["dist"] = best_row["dist"]
        best_config["scale"] = best_row["scale"]
    
    print("\n--- Sweep Results ---")
    display(sweep_df)
    print(f"\nBest config: dist={best_config['dist']}, scale={best_config['scale']}")

# ----------------------------------------------------------------------------------
# 11. TRAIN FINAL MODEL WITH BEST CONFIG
# ----------------------------------------------------------------------------------
print("\n" + "="*70)
print(f"TRAINING FINAL MODEL: dist={best_config['dist']}, scale={best_config['scale']}")
print("="*70)

loss_fn = f"SurvivalAft:dist={best_config['dist']};scale={best_config['scale']}"

train_pool = Pool(
    X_train, label=y_train, cat_features=cat_features_names,
    weight=train_weights,
)
val_pool = Pool(X_val, label=y_val, cat_features=cat_features_names)

model = CatBoostRegressor(
    loss_function=loss_fn,
    eval_metric="SurvivalAft",
    iterations=1500,
    learning_rate=0.03,
    depth=6,
    l2_leaf_reg=3.0,
    random_seed=SEED,
    verbose=100,
    od_type="Iter",
    od_wait=100,
    allow_writing_files=False,
)

print("\n--- Training model ---")
model.fit(train_pool, eval_set=val_pool, use_best_model=True)

# ----------------------------------------------------------------------------------
# 12. BASELINE EVALUATION (BEFORE CALIBRATION)
# ----------------------------------------------------------------------------------
print("\n" + "="*70)
print("BASELINE EVALUATION (No Calibration)")
print("="*70)

baseline_mae, baseline_cidx, baseline_results = evaluate_model(
    model, X_val, y_val, df,
    scale=best_config["scale"],
    dist=best_config["dist"],
    verbose=True,
)

# Store baseline metrics
baseline_metrics = {
    "mae": float(baseline_mae),
    "c_index": float(baseline_cidx),
    "dist": best_config["dist"],
    "scale": best_config["scale"],
    "n_val_events": int(baseline_results["event_observed"].sum()),
    "n_val_total": len(baseline_results),
    "timestamp": datetime.now().isoformat(),
}

print(f"\n  Baseline MAE: {baseline_mae:.2f} days")
print(f"  Baseline C-index: {baseline_cidx:.4f}")

# ----------------------------------------------------------------------------------
# 13. POST-HOC CALIBRATION LAYER
# ----------------------------------------------------------------------------------
global_calibrator = None
segment_calibrators = {}

if CONFIG["apply_calibration"]:
    print("\n" + "="*70)
    print("POST-HOC CALIBRATION")
    print("="*70)
    
    # Get training events for calibration fitting
    train_events_mask = df.loc[X_train.index, "event_observed"] == 1
    X_train_events = X_train[train_events_mask]
    
    if len(X_train_events) >= 5:
        # Get raw predictions and true log-times
        train_log_pred = model.predict(X_train_events).astype(float).reshape(-1, 1)
        train_true_time = df.loc[X_train_events.index, "gestation_target"].clip(lower=EPS)
        train_log_true = np.log(train_true_time).values
        
        # Fit global calibrator: log(T_true) ~ a + b * mu_raw
        global_calibrator = LinearRegression()
        global_calibrator.fit(train_log_pred, train_log_true)
        
        print(f"\n--- Global Calibration ---")
        print(f"  Training events used: {len(X_train_events)}")
        print(f"  Calibrator: log(T) = {global_calibrator.intercept_:.3f} + "
              f"{global_calibrator.coef_[0]:.3f} * mu_raw")
        
        # Segmented calibration
        if CONFIG["segmented_calibration"]:
            segment_col = CONFIG["calibration_segment_col"]
            
            if segment_col in df.columns:
                print(f"\n--- Segmented Calibration by '{segment_col}' ---")
                
                segments = df.loc[X_train_events.index, segment_col]
                
                for seg in segments.unique():
                    seg_mask = segments == seg
                    n_seg_events = seg_mask.sum()
                    
                    if n_seg_events >= CONFIG["min_segment_events"]:
                        seg_log_pred = train_log_pred[seg_mask]
                        seg_log_true = train_log_true[seg_mask]
                        
                        seg_calibrator = LinearRegression()
                        seg_calibrator.fit(seg_log_pred, seg_log_true)
                        segment_calibrators[seg] = seg_calibrator
                        
                        print(f"  {seg}: n={n_seg_events}, "
                              f"a={seg_calibrator.intercept_:.3f}, "
                              f"b={seg_calibrator.coef_[0]:.3f}")
                    else:
                        print(f"  {seg}: n={n_seg_events} (too few, using global)")
        
        # Evaluate with global calibration only first
        print("\n--- Global Calibration Evaluation ---")
        global_cal_mae, global_cal_cidx, global_cal_results = evaluate_model(
            model, X_val, y_val, df,
            scale=best_config["scale"],
            dist=best_config["dist"],
            calibrator=global_calibrator,
            verbose=True,
        )
        
        # Evaluate with segmented calibration if available
        if segment_calibrators:
            print("\n--- Segmented Calibration Evaluation ---")
            seg_cal_mae, seg_cal_cidx, seg_cal_results = evaluate_model(
                model, X_val, y_val, df,
                scale=best_config["scale"],
                dist=best_config["dist"],
                calibrator=global_calibrator,
                segment_calibrators=segment_calibrators,
                segment_col=CONFIG["calibration_segment_col"],
                verbose=True,
            )
        else:
            seg_cal_mae = float("inf")
        
        # Choose best calibration approach (or none)
        print("\n--- Calibration Selection ---")
        print(f"  Baseline MAE:         {baseline_mae:.2f} days")
        print(f"  Global Calibration:   {global_cal_mae:.2f} days")
        if segment_calibrators:
            print(f"  Segment Calibration:  {seg_cal_mae:.2f} days")
        
        # Select the best approach
        best_cal_mae = min(baseline_mae, global_cal_mae, seg_cal_mae)
        
        if best_cal_mae == baseline_mae:
            print("\n  >> Using NO CALIBRATION (baseline performs best)")
            global_calibrator = None
            segment_calibrators = {}
            cal_mae = baseline_mae
            cal_results = baseline_results
        elif best_cal_mae == global_cal_mae:
            print("\n  >> Using GLOBAL CALIBRATION")
            segment_calibrators = {}  # Don't use segmented
            cal_mae = global_cal_mae
            cal_results = global_cal_results
        else:
            print("\n  >> Using SEGMENTED CALIBRATION")
            cal_mae = seg_cal_mae
            cal_results = seg_cal_results
        
        print(f"\n  Final MAE improvement: {baseline_mae - cal_mae:.2f} days "
              f"({(baseline_mae - cal_mae) / baseline_mae * 100:.1f}%)")
    else:
        print("  Insufficient training events for calibration")
        cal_mae = baseline_mae
        cal_results = baseline_results

# ----------------------------------------------------------------------------------
# 14. RESIDUAL DIAGNOSTICS
# ----------------------------------------------------------------------------------
if CONFIG["run_diagnostics"]:
    print("\n" + "="*70)
    print("RESIDUAL DIAGNOSTICS")
    print("="*70)
    
    # Use calibrated results if available, otherwise baseline
    if CONFIG["apply_calibration"] and global_calibrator is not None:
        diag_results = cal_results.copy()
        diag_label = "Calibrated"
    else:
        diag_results = baseline_results.copy()
        diag_label = "Baseline"
    
    # Compute residuals for events
    unc_mask = diag_results["event_observed"] == 1
    unc_res = diag_results[unc_mask].copy()
    unc_res["residual"] = unc_res["pred_p50_days"] - unc_res["gestation_target"]
    
    if len(unc_res) > 0:
        print(f"\n--- Residual Summary ({diag_label}, n={len(unc_res)} events) ---")
        print(f"  Mean residual: {unc_res['residual'].mean():.2f} days")
        print(f"  Std residual:  {unc_res['residual'].std():.2f} days")
        print(f"  Median residual: {unc_res['residual'].median():.2f} days")
        
        # Segment-wise residual analysis
        segment_col = CONFIG["calibration_segment_col"]
        if segment_col in df.columns:
            print(f"\n--- Residuals by '{segment_col}' ---")
            
            seg_vals = df.loc[unc_res.index, segment_col]
            unc_res["segment"] = seg_vals
            
            seg_summary = unc_res.groupby("segment")["residual"].agg(
                ["count", "mean", "std", "median"]
            ).round(2)
            display(seg_summary)
            
            # Save segment diagnostics
            output_dir = Path(CONFIG["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)
            seg_summary.to_csv(output_dir / "residuals_by_segment.csv")
        
        # Diagnostic plots
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # 1. True vs Predicted
        ax1 = axes[0, 0]
        ax1.scatter(unc_res["gestation_target"], unc_res["pred_p50_days"], alpha=0.6)
        max_val = max(unc_res["gestation_target"].max(), unc_res["pred_p50_days"].max())
        ax1.plot([0, max_val], [0, max_val], "r--", label="Perfect")
        ax1.set_xlabel("True gestation (days)")
        ax1.set_ylabel("Predicted median (days)")
        ax1.set_title(f"True vs Predicted ({diag_label})")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. Residual distribution
        ax2 = axes[0, 1]
        sns.histplot(unc_res["residual"], kde=True, ax=ax2)
        ax2.axvline(x=0, color="r", linestyle="--", label="Zero")
        ax2.set_xlabel("Residual (pred - true) days")
        ax2.set_title("Residual Distribution")
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. Residuals by segment (if available)
        ax3 = axes[1, 0]
        if "segment" in unc_res.columns:
            sns.boxplot(data=unc_res, x="segment", y="residual", ax=ax3)
            ax3.axhline(y=0, color="r", linestyle="--")
            ax3.set_xlabel(segment_col)
            ax3.set_ylabel("Residual (days)")
            ax3.set_title(f"Residuals by {segment_col}")
            ax3.tick_params(axis="x", rotation=45)
        else:
            ax3.text(0.5, 0.5, "No segment data", ha="center", va="center")
        
        # 4. Distribution comparison
        ax4 = axes[1, 1]
        sns.histplot(unc_res["gestation_target"], kde=True, alpha=0.4, label="True", ax=ax4)
        sns.histplot(unc_res["pred_p50_days"], kde=True, alpha=0.4, label="Pred p50", ax=ax4)
        ax4.set_xlabel("Days")
        ax4.set_title("Distribution: True vs Predicted")
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(Path(CONFIG["output_dir"]) / "diagnostics.png", dpi=150)
        plt.show()
        
        # Censored consistency check
        cens_res = diag_results[~unc_mask].copy()
        if len(cens_res) > 0:
            pct_consistent = (
                (cens_res["pred_p50_days"] > cens_res["time_open_days"]).mean() * 100
            )
            print(
                f"\n--- Censored Consistency ---\n"
                f"  Censored count: {len(cens_res)}\n"
                f"  Predicted median > current age: {pct_consistent:.1f}%"
            )

# ----------------------------------------------------------------------------------
# 15. FINAL C-INDEX
# ----------------------------------------------------------------------------------
print("\n" + "="*70)
print("FINAL VALIDATION METRICS")
print("="*70)

val_times = y_val["lower"].values
val_events = df.loc[X_val.index, "event_observed"].values.astype(bool)
val_log_pred = model.predict(X_val).astype(float)
c_idx = concordance_index(val_times, val_log_pred, val_events)

print(f"\n  Final C-index: {c_idx:.4f}")

# ----------------------------------------------------------------------------------
# 16. SAVE RESULTS
# ----------------------------------------------------------------------------------
output_dir = Path(CONFIG["output_dir"])
output_dir.mkdir(parents=True, exist_ok=True)

# Save baseline metrics
results_file = output_dir / "aft_training_results.json"
final_results = {
    "baseline": baseline_metrics,
    "best_config": best_config,
    "sweep_results": sweep_results if sweep_results else None,
    "calibration": {
        "global_intercept": float(global_calibrator.intercept_) if global_calibrator else None,
        "global_slope": float(global_calibrator.coef_[0]) if global_calibrator else None,
        "segment_calibrators": {
            k: {"intercept": float(v.intercept_), "slope": float(v.coef_[0])}
            for k, v in segment_calibrators.items()
        } if segment_calibrators else None,
    },
    "final_c_index": float(c_idx),
    "config": CONFIG,
}

with open(results_file, "w") as f:
    json.dump(final_results, f, indent=2, default=str)

print(f"\n  Results saved to: {results_file}")
print("\n" + "="*70)
print("PIPELINE COMPLETE")
print("="*70)
