"""
CatBoost AFT Training Pipeline for gestation predictions.

This module provides a production-ready trainer that encapsulates:
- Data loading and survival target construction
- Time-based validation splits
- Recency weighting
- Hyperparameter sweep (dist/scale)
- Post-hoc calibration
- Artifact and metrics persistence
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from catboost import CatBoostRegressor, Pool
except ImportError:
    CatBoostRegressor = None
    Pool = None

try:
    from lifelines.utils import concordance_index
except ImportError:
    concordance_index = None

try:
    from scipy.stats import norm, logistic
except ImportError:
    norm = None
    logistic = None

from .aft_model import (
    AFTGestationModel,
    AFT_CATEGORICAL_FEATURES,
    AFT_DATE_COLUMNS,
    AFT_DROP_COLUMNS,
    LinearCalibrator,
    CalibrationState,
)

logger = logging.getLogger(__name__)


@dataclass
class AFTTrainerConfig:
    """Configuration for AFT training pipeline."""
    
    # Hyperparameter sweep
    run_sweep: bool = True
    sweep_distributions: List[str] = field(default_factory=lambda: ["Normal", "Logistic"])
    sweep_scales: List[float] = field(default_factory=lambda: [0.5, 1.0, 1.5, 2.0])
    
    # Time-based split
    use_time_split: bool = True
    validation_months: int = 6
    
    # Recency weighting
    apply_recency_weights: bool = True
    recent_weight: float = 2.0
    recent_cutoff_months: int = 12

    # Target clipping (legacy)
    target_clip_days: Optional[float] = None
    calibrate_p50_bias: bool = False  # Legacy: simple median bias offset
    
    # Post-hoc calibration (LinearRegression-based)
    apply_calibration: bool = True  # Enable post-hoc calibration fitting
    segmented_calibration: bool = True  # Also try segment-specific calibrators
    calibration_segment_col: str = "value_band"  # Column for segmentation
    min_segment_events: int = 5  # Minimum events to fit a segment calibrator
    
    # CatBoost training params
    iterations: int = 1500
    learning_rate: float = 0.03
    depth: int = 6
    l2_leaf_reg: float = 3.0
    early_stopping_rounds: int = 100
    
    # Random seed
    seed: int = 42
    
    # Minimum survival time epsilon
    eps: float = 1.0


@dataclass
class AFTTrainingResult:
    """Results from AFT training pipeline."""
    
    model: AFTGestationModel
    best_dist: str
    best_scale: float
    baseline_mae: float
    baseline_c_index: float
    median_ae: float
    capped_mae: float
    sweep_results: List[Dict[str, Any]]
    train_rows: int
    val_rows: int
    val_events: int
    feature_columns: List[str]
    categorical_features: List[str]
    timestamp: str
    config: Dict[str, Any]
    calibration_info: Optional[Dict[str, Any]] = None  # Calibration metrics and state


class AFTTrainer:
    """
    CatBoost AFT trainer for gestation timeline predictions.
    
    This trainer implements:
    1. Survival target construction for CatBoost AFT
    2. Feature engineering (date deltas, categorical encoding)
    3. Time-based validation splits
    4. Optional recency weighting
    5. Hyperparameter sweep over dist/scale
    6. Model evaluation (MAE, C-index)
    """
    
    def __init__(self, config: Optional[AFTTrainerConfig] = None):
        self.config = config or AFTTrainerConfig()
        
        if CatBoostRegressor is None:
            raise ImportError("catboost is required for AFT training")
        
        self.categorical_features = list(AFT_CATEGORICAL_FEATURES)
        self.date_columns = list(AFT_DATE_COLUMNS)
        self.drop_columns = list(AFT_DROP_COLUMNS)

    def train(
        self,
        df: pd.DataFrame,
        *,
        model_version: str = "v1",
    ) -> AFTTrainingResult:
        """
        Train CatBoost AFT model on provided dataframe.
        
        Args:
            df: Preprocessed dataset with survival columns.
            model_version: Version tag for the model artifact.
            
        Returns:
            AFTTrainingResult with trained model and metrics.
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        np.random.seed(self.config.seed)
        
        logger.info("Starting AFT training | rows=%d", len(df))
        df = self._apply_target_clipping(df)
        
        # Build AFT targets
        y_aft, df = self._build_aft_targets(df)
        
        # Engineer features
        X, cat_features = self._engineer_features(df)
        
        logger.info(
            "Feature matrix: %d rows x %d features | categoricals=%d",
            X.shape[0], X.shape[1], len(cat_features),
        )
        
        # Split data
        X_train, X_val, y_train, y_val, df_train, df_val = self._split_data(
            X, y_aft, df
        )

        self._log_event_distributions(df_train, df_val)
        
        logger.info(
            "Split: train=%d val=%d | val_events=%d",
            len(X_train), len(X_val),
            int(df_val["event_observed"].sum()) if "event_observed" in df_val.columns else 0,
        )
        
        # Compute sample weights
        train_weights = self._compute_weights(df_train)
        
        # Run hyperparameter sweep or use defaults
        if self.config.run_sweep:
            best_dist, best_scale, sweep_results = self._sweep_hyperparams(
                X_train, y_train, X_val, y_val, df_val,
                cat_features, train_weights,
            )
        else:
            best_dist = "Logistic"
            best_scale = 2.0
            sweep_results = []
        
        # Train final model with best config and post-hoc calibration
        model, metrics, calibration_state = self._train_final_model(
            X_train, y_train, X_val, y_val, df_train, df_val,
            cat_features, train_weights,
            best_dist, best_scale,
        )
        
        # Create AFTGestationModel wrapper
        aft_model = AFTGestationModel(
            categorical_features=cat_features,
        )
        aft_model.model = model
        aft_model.dist = best_dist
        aft_model.scale = best_scale
        aft_model.feature_columns = list(X_train.columns)
        aft_model.training_metrics = metrics
        aft_model.calibration_offset_days = float(metrics.get("calibration_offset_days", 0.0))
        aft_model.calibration = calibration_state  # New: v2 calibration
        aft_model.metadata = {
            "model_version": model_version,
            "timestamp_utc": timestamp,
            "train_rows": len(X_train),
            "val_rows": len(X_val),
            "sweep_results": sweep_results,
        }
        aft_model.is_fitted = True
        
        # Extract calibration info for result
        calibration_info = metrics.get("calibration_info", {})
        
        return AFTTrainingResult(
            model=aft_model,
            best_dist=best_dist,
            best_scale=best_scale,
            baseline_mae=metrics.get("mae", float("nan")),
            baseline_c_index=metrics.get("c_index", float("nan")),
            median_ae=metrics.get("median_ae", float("nan")),
            capped_mae=metrics.get("capped_mae", float("nan")),
            sweep_results=sweep_results,
            train_rows=len(X_train),
            val_rows=len(X_val),
            val_events=int(df_val["event_observed"].sum()) if "event_observed" in df_val.columns else 0,
            feature_columns=list(X_train.columns),
            categorical_features=cat_features,
            timestamp=timestamp,
            config=self._config_to_dict(),
            calibration_info=calibration_info if calibration_info else None,
        )

    def _build_aft_targets(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Build CatBoost AFT survival targets.
        
        AFT expects [t_lower, t_upper]:
        - Events: t_lower = t_upper = gestation_target
        - Censored: t_lower = time_open_days, t_upper = -1 (infinity)
        """
        eps = self.config.eps
        
        mask_event = df["event_observed"] == 1
        
        t_lower = np.zeros(len(df), dtype=float)
        t_upper = np.zeros(len(df), dtype=float)
        
        # Events: exact gestation
        t_event = df.loc[mask_event, "gestation_target"].astype(float).clip(lower=eps)
        t_lower[mask_event] = t_event.values
        t_upper[mask_event] = t_event.values
        
        # Censored: observed time, then infinity
        t_cens = df.loc[~mask_event, "time_open_days"].astype(float).clip(lower=eps)
        t_lower[~mask_event] = t_cens.values
        t_upper[~mask_event] = -1.0
        
        y_aft = pd.DataFrame({"lower": t_lower, "upper": t_upper}, index=df.index)
        
        return y_aft, df

    def _engineer_features(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Engineer features for CatBoost AFT model.
        
        Returns:
            Tuple of (feature_matrix, categorical_feature_names)
        """
        X = df.drop(columns=self.drop_columns, errors="ignore").copy()
        
        # Parse date columns
        for col in self.date_columns:
            if col in X.columns:
                X[col] = pd.to_datetime(X[col], errors="coerce", utc=True)
        
        # Store parsed date_created for splitting
        if "date_created" in X.columns:
            baseline = X["date_created"].copy()
            df["_date_created_parsed"] = baseline
        elif "created_at" in X.columns:
            baseline = X["created_at"].copy()
            df["_date_created_parsed"] = baseline
        else:
            baseline = None
        
        # Create date delta features
        if baseline is not None:
            for col in self.date_columns:
                if col in X.columns and col != baseline.name:
                    X[f"{col}_delta_days"] = (X[col] - baseline).dt.days
            
            X["creation_year"] = baseline.dt.year
            X["creation_month"] = baseline.dt.month
        
        # Drop raw datetime columns
        X = X.drop(columns=[c for c in self.date_columns if c in X.columns])
        
        # Handle categorical features
        cat_features = X.select_dtypes(include=["object", "category"]).columns.tolist()
        for col in cat_features:
            X[col] = X[col].astype("string").fillna("Missing")
        
        return X, cat_features

    def _split_data(
        self,
        X: pd.DataFrame,
        y_aft: pd.DataFrame,
        df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Split data into train/validation sets.
        
        Uses time-based split if configured and date_created is available,
        otherwise falls back to random stratified split.
        """
        if self.config.use_time_split and "_date_created_parsed" in df.columns:
            max_date = df["_date_created_parsed"].max()
            cutoff = max_date - pd.DateOffset(months=self.config.validation_months)
            
            train_mask = df["_date_created_parsed"] <= cutoff
            val_mask = df["_date_created_parsed"] > cutoff
            
            X_train = X.loc[train_mask].copy()
            X_val = X.loc[val_mask].copy()
            y_train = y_aft.loc[train_mask].copy()
            y_val = y_aft.loc[val_mask].copy()
            df_train = df.loc[train_mask].copy()
            df_val = df.loc[val_mask].copy()
            
            logger.info("Time-based split: cutoff=%s", cutoff)
        else:
            # Random stratified split
            from sklearn.model_selection import train_test_split
            
            stratify = df["event_observed"] if "event_observed" in df.columns else None
            
            train_idx, val_idx = train_test_split(
                df.index,
                test_size=0.2,
                random_state=self.config.seed,
                stratify=stratify,
            )
            
            X_train = X.loc[train_idx].copy()
            X_val = X.loc[val_idx].copy()
            y_train = y_aft.loc[train_idx].copy()
            y_val = y_aft.loc[val_idx].copy()
            df_train = df.loc[train_idx].copy()
            df_val = df.loc[val_idx].copy()
            
            logger.info("Random stratified split")
        
        return X_train, X_val, y_train, y_val, df_train, df_val

    def _compute_weights(self, df_train: pd.DataFrame) -> Optional[np.ndarray]:
        """
        Compute sample weights based on recency.
        """
        if not self.config.apply_recency_weights:
            return None
        
        if "_date_created_parsed" not in df_train.columns:
            return None
        
        max_date = df_train["_date_created_parsed"].max()
        cutoff = max_date - pd.DateOffset(months=self.config.recent_cutoff_months)
        
        weights = np.where(
            df_train["_date_created_parsed"] > cutoff,
            self.config.recent_weight,
            1.0,
        )
        
        n_boosted = (weights > 1).sum()
        logger.info(
            "Recency weights: %d/%d samples boosted (%.1f%%)",
            n_boosted, len(weights), 100 * n_boosted / len(weights),
        )
        
        return weights

    def _sweep_hyperparams(
        self,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        df_val: pd.DataFrame,
        cat_features: List[str],
        train_weights: Optional[np.ndarray],
    ) -> Tuple[str, float, List[Dict[str, Any]]]:
        """
        Sweep over dist/scale combinations to find best config.
        """
        logger.info("Starting hyperparameter sweep...")
        
        results = []
        
        for dist in self.config.sweep_distributions:
            for scale in self.config.sweep_scales:
                mae, c_idx, _, _ = self._train_and_evaluate(
                    X_train, y_train, X_val, y_val, df_val,
                    cat_features, train_weights,
                    dist, scale, verbose=False,
                )
                
                results.append({
                    "dist": dist,
                    "scale": scale,
                    "mae": float(mae) if not np.isnan(mae) else None,
                    "c_index": float(c_idx) if not np.isnan(c_idx) else None,
                })
                
                logger.info(
                    "  dist=%s scale=%.1f → MAE=%.2f C-index=%.4f",
                    dist, scale, mae, c_idx,
                )
        
        # Find best config by MAE (with C-index sanity check)
        valid = [r for r in results if r["c_index"] and r["c_index"] > 0.6]
        if valid:
            best = min(valid, key=lambda r: r["mae"] or float("inf"))
        else:
            best = results[0] if results else {"dist": "Logistic", "scale": 2.0}
        
        logger.info(
            "Best config: dist=%s scale=%.1f (MAE=%.2f)",
            best["dist"], best["scale"], best.get("mae", float("nan")),
        )
        
        return best["dist"], best["scale"], results

    def _train_and_evaluate(
        self,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        df_val: pd.DataFrame,
        cat_features: List[str],
        train_weights: Optional[np.ndarray],
        dist: str,
        scale: float,
        verbose: bool = True,
    ) -> Tuple[float, float, CatBoostRegressor, Optional[Dict[str, np.ndarray]]]:
        """
        Train model with given dist/scale and evaluate on validation set.
        """
        loss_fn = f"SurvivalAft:dist={dist};scale={scale}"
        
        train_pool = Pool(
            X_train, label=y_train, cat_features=cat_features,
            weight=train_weights,
        )
        val_pool = Pool(X_val, label=y_val, cat_features=cat_features)
        
        model = CatBoostRegressor(
            loss_function=loss_fn,
            eval_metric="SurvivalAft",
            iterations=self.config.iterations,
            learning_rate=self.config.learning_rate,
            depth=self.config.depth,
            l2_leaf_reg=self.config.l2_leaf_reg,
            random_seed=self.config.seed,
            verbose=100 if verbose else 0,
            od_type="Iter",
            od_wait=self.config.early_stopping_rounds,
            allow_writing_files=False,
        )
        
        model.fit(train_pool, eval_set=val_pool, use_best_model=True)
        
        # Evaluate
        preds = self._predict_quantiles(model, X_val, dist, scale)
        mae, c_idx = self._evaluate(model, X_val, y_val, df_val, dist, scale, preds=preds)
        
        return mae, c_idx, model, preds

    def _train_final_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.DataFrame,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        df_train: pd.DataFrame,
        df_val: pd.DataFrame,
        cat_features: List[str],
        train_weights: Optional[np.ndarray],
        dist: str,
        scale: float,
    ) -> Tuple[CatBoostRegressor, Dict[str, Any], Optional[CalibrationState]]:
        """
        Train final model with best hyperparameters and post-hoc calibration.
        
        Returns:
            Tuple of (model, metrics_dict, calibration_state).
        """
        logger.info("Training final model: dist=%s scale=%.1f", dist, scale)

        mae, c_idx, model, preds = self._train_and_evaluate(
            X_train, y_train, X_val, y_val, df_val,
            cat_features, train_weights,
            dist, scale, verbose=True,
        )

        # Legacy calibration: simple median bias offset
        calib_offset = 0.0
        calib_mae = float(mae)
        if self.config.calibrate_p50_bias and preds is not None:
            calib_offset = self._compute_calibration_offset(preds["p50"], df_val)
            if calib_offset != 0.0:
                calib_pred = np.maximum(0, preds["p50"] - calib_offset)
                calib_mae = self._compute_event_mae(calib_pred, df_val)
                logger.info(
                    "Legacy calibration offset: %.2f days | raw_mae=%.2f calibrated_mae=%.2f",
                    calib_offset, mae, calib_mae,
                )

        # Compute additional robust metrics
        median_ae = float("nan")
        capped_mae = float("nan")
        if preds is not None:
            median_ae = self._compute_event_median_ae(preds["p50"], df_val)
            capped_mae = self._compute_event_capped_mae(preds["p50"], df_val, cap=730.0)

        # Post-hoc LinearRegression calibration
        calibration_state: Optional[CalibrationState] = None
        calibration_info: Dict[str, Any] = {}
        
        if self.config.apply_calibration:
            logger.info("Fitting post-hoc calibrators...")
            
            # Fit calibrators on training events
            global_cal, segment_cals = self._fit_calibrators(
                model, X_train, df_train,
            )
            
            # Select best calibration method
            if global_cal is not None:
                calibration_state, calibration_info = self._select_best_calibration(
                    model, X_val, df_val, dist, scale,
                    baseline_mae=mae,
                    global_calibrator=global_cal,
                    segment_calibrators=segment_cals,
                )
                
                # Update MAE if calibration improved it
                if calibration_info.get("final_mae", mae) < mae:
                    logger.info(
                        "Calibration improved MAE: %.2f -> %.2f (%.1f%% reduction)",
                        mae,
                        calibration_info["final_mae"],
                        calibration_info.get("improvement_pct", 0),
                    )

        metrics = {
            "mae": float(mae),
            "calibrated_mae": float(calib_mae),
            "median_ae": float(median_ae),
            "capped_mae": float(capped_mae),
            "calibration_offset_days": float(calib_offset),  # Legacy
            "c_index": float(c_idx),
            "dist": dist,
            "scale": scale,
            "best_iteration": model.get_best_iteration() if model else 0,
            "mae_by_year": self._compute_mae_by_year(preds["p50"], df_val) if preds is not None else {},
            "calibration_info": calibration_info,  # New: detailed calibration metrics
        }

        logger.info(
            "Final model: MAE=%.2f C-index=%.4f iterations=%d calibration=%s",
            mae, c_idx, metrics["best_iteration"],
            calibration_state.method if calibration_state else "none",
        )

        return model, metrics, calibration_state

    def _evaluate(
        self,
        model: CatBoostRegressor,
        X_val: pd.DataFrame,
        y_val: pd.DataFrame,
        df_val: pd.DataFrame,
        dist: str,
        scale: float,
        preds: Optional[Dict[str, np.ndarray]] = None,
    ) -> Tuple[float, float]:
        """
        Evaluate model on validation set.
        
        Returns:
            Tuple of (MAE on events, C-index)
        """
        if norm is None or logistic is None:
            return float("nan"), float("nan")
        
        preds = preds or self._predict_quantiles(model, X_val, dist, scale)
        log_mu = preds["log_mu"]
        pred_p50 = preds["p50"]
        
        # MAE on uncensored events
        mae = self._compute_event_mae(pred_p50, df_val)
        
        # C-index
        if concordance_index is not None:
            try:
                val_times = y_val["lower"].values
                val_events = df_val["event_observed"].values.astype(bool)
                c_idx = float(concordance_index(val_times, log_mu, val_events))
            except Exception:
                c_idx = float("nan")
        else:
            c_idx = float("nan")
        
        return mae, c_idx

    # ------------------------------------------------------------------ #
    # Helper utilities
    # ------------------------------------------------------------------ #

    def _predict_quantiles(
        self,
        model: CatBoostRegressor,
        X: pd.DataFrame,
        dist: str,
        scale: float,
    ) -> Dict[str, np.ndarray]:
        """Predict p25/p50/p75 and raw log_mu for a dataframe."""
        log_mu = model.predict(X).astype(float)
        log_mu = np.clip(log_mu, -10, 10)

        if dist == "Logistic":
            z25 = logistic.ppf(0.25)
            z50 = logistic.ppf(0.50)
            z75 = logistic.ppf(0.75)
        else:
            z25 = norm.ppf(0.25)
            z50 = norm.ppf(0.50)
            z75 = norm.ppf(0.75)

        p25 = np.exp(log_mu + scale * z25)
        p50 = np.exp(log_mu + scale * z50)
        p75 = np.exp(log_mu + scale * z75)

        return {"p25": p25, "p50": p50, "p75": p75, "log_mu": log_mu}

    def _compute_event_mae(self, pred_p50: np.ndarray, df_val: pd.DataFrame) -> float:
        """MAE on uncensored events; returns NaN if none."""
        if "event_observed" not in df_val.columns or "gestation_target" not in df_val.columns:
            return float("nan")
        events_mask = df_val["event_observed"] == 1
        if not events_mask.any():
            return float("nan")
        true_vals = df_val.loc[events_mask, "gestation_target"].values
        pred_vals = pred_p50[events_mask.values]
        return float(np.mean(np.abs(true_vals - pred_vals)))

    def _compute_event_median_ae(self, pred_p50: np.ndarray, df_val: pd.DataFrame) -> float:
        """Median AE on uncensored events; robust to tails."""
        if "event_observed" not in df_val.columns or "gestation_target" not in df_val.columns:
            return float("nan")
        events_mask = df_val["event_observed"] == 1
        if not events_mask.any():
            return float("nan")
        true_vals = df_val.loc[events_mask, "gestation_target"].values
        pred_vals = pred_p50[events_mask.values]
        return float(np.median(np.abs(true_vals - pred_vals)))

    def _compute_event_capped_mae(
        self, pred_p50: np.ndarray, df_val: pd.DataFrame, cap: float = 730.0
    ) -> float:
        """MAE with both predictions and targets capped; reduces tail impact."""
        if "event_observed" not in df_val.columns or "gestation_target" not in df_val.columns:
            return float("nan")
        events_mask = df_val["event_observed"] == 1
        if not events_mask.any():
            return float("nan")
        true_vals = np.clip(df_val.loc[events_mask, "gestation_target"].values, 0, cap)
        pred_vals = np.clip(pred_p50[events_mask.values], 0, cap)
        return float(np.mean(np.abs(true_vals - pred_vals)))

    def _compute_calibration_offset(self, pred_p50: np.ndarray, df_val: pd.DataFrame) -> float:
        """Median bias (pred - true) on events; used as simple offset."""
        if "event_observed" not in df_val.columns or "gestation_target" not in df_val.columns:
            return 0.0
        events_mask = df_val["event_observed"] == 1
        if not events_mask.any():
            return 0.0
        true_vals = df_val.loc[events_mask, "gestation_target"].values
        pred_vals = pred_p50[events_mask.values]
        bias = np.median(pred_vals - true_vals)
        return float(bias)

    # ------------------------------------------------------------------ #
    # Post-hoc Calibration (LinearRegression-based)
    # ------------------------------------------------------------------ #

    def _fit_calibrators(
        self,
        model: CatBoostRegressor,
        X_train: pd.DataFrame,
        df_train: pd.DataFrame,
    ) -> Tuple[Optional[LinearCalibrator], Dict[str, LinearCalibrator]]:
        """
        Fit global and segment-specific LinearRegression calibrators.
        
        Calibration is fit on training events in log-space:
            log(T_true) = intercept + slope * log_mu_raw
        
        Args:
            model: Trained CatBoost model.
            X_train: Training features.
            df_train: Training dataframe with event_observed and gestation_target.
            
        Returns:
            Tuple of (global_calibrator, segment_calibrators_dict).
        """
        from sklearn.linear_model import LinearRegression
        
        # Get training events
        events_mask = df_train["event_observed"] == 1
        X_events = X_train[events_mask]
        
        if len(X_events) < 5:
            logger.warning("Insufficient training events for calibration: %d", len(X_events))
            return None, {}
        
        # Get raw log-mu predictions and true log-times
        log_pred = model.predict(X_events).astype(float).reshape(-1, 1)
        true_times = df_train.loc[X_events.index, "gestation_target"].clip(lower=self.config.eps)
        log_true = np.log(true_times).values
        
        # Fit global calibrator: log(T_true) = a + b * mu_raw
        global_lr = LinearRegression()
        global_lr.fit(log_pred, log_true)
        global_cal = LinearCalibrator(
            intercept=float(global_lr.intercept_),
            slope=float(global_lr.coef_[0]),
        )
        
        logger.info(
            "Global calibrator: intercept=%.4f slope=%.4f (fitted on %d events)",
            global_cal.intercept, global_cal.slope, len(X_events),
        )
        
        # Fit per-segment calibrators
        segment_cals: Dict[str, LinearCalibrator] = {}
        
        if self.config.segmented_calibration:
            seg_col = self.config.calibration_segment_col
            
            if seg_col in df_train.columns:
                segments = df_train.loc[X_events.index, seg_col]
                
                for seg in segments.unique():
                    if pd.isna(seg):
                        continue
                    
                    seg_mask = (segments == seg).values
                    n_seg_events = seg_mask.sum()
                    
                    if n_seg_events >= self.config.min_segment_events:
                        seg_lr = LinearRegression()
                        seg_lr.fit(log_pred[seg_mask], log_true[seg_mask])
                        segment_cals[str(seg)] = LinearCalibrator(
                            intercept=float(seg_lr.intercept_),
                            slope=float(seg_lr.coef_[0]),
                        )
                        logger.info(
                            "  Segment '%s': intercept=%.4f slope=%.4f (n=%d)",
                            seg, segment_cals[str(seg)].intercept,
                            segment_cals[str(seg)].slope, n_seg_events,
                        )
                    else:
                        logger.info(
                            "  Segment '%s': skipped (n=%d < min=%d)",
                            seg, n_seg_events, self.config.min_segment_events,
                        )
            else:
                logger.warning(
                    "Segment column '%s' not found in training data", seg_col
                )
        
        return global_cal, segment_cals

    def _predict_with_calibration(
        self,
        model: CatBoostRegressor,
        X: pd.DataFrame,
        df: pd.DataFrame,
        dist: str,
        scale: float,
        calibrator: Optional[LinearCalibrator] = None,
        segment_calibrators: Optional[Dict[str, LinearCalibrator]] = None,
    ) -> np.ndarray:
        """
        Predict p50 with optional calibration applied in log-space.
        
        Args:
            model: CatBoost model.
            X: Feature dataframe.
            df: Original dataframe with segment column.
            dist: AFT distribution.
            scale: AFT scale.
            calibrator: Global calibrator (used as fallback).
            segment_calibrators: Per-segment calibrators.
            
        Returns:
            Array of calibrated p50 predictions.
        """
        # Get raw log-mu predictions
        log_mu = model.predict(X).astype(float)
        log_mu = np.clip(log_mu, -10, 10)
        
        # Apply calibration
        if segment_calibrators and self.config.calibration_segment_col in df.columns:
            # Segmented calibration
            log_mu_cal = log_mu.copy()
            segments = df.loc[X.index, self.config.calibration_segment_col]
            
            for seg, cal in segment_calibrators.items():
                seg_mask = (segments == seg).values
                if seg_mask.any():
                    log_mu_cal[seg_mask] = cal.transform(log_mu[seg_mask])
            
            # Fallback to global for segments without specific calibrators
            if calibrator is not None:
                calibrated_segs = set(segment_calibrators.keys())
                uncalibrated_mask = ~segments.isin(calibrated_segs).values
                if uncalibrated_mask.any():
                    log_mu_cal[uncalibrated_mask] = calibrator.transform(log_mu[uncalibrated_mask])
            
            log_mu = log_mu_cal
            
        elif calibrator is not None:
            # Global calibration only
            log_mu = calibrator.transform(log_mu)
        
        # Compute p50
        if dist == "Logistic":
            z50 = logistic.ppf(0.50)
        else:
            z50 = norm.ppf(0.50)
        
        p50 = np.exp(log_mu + scale * z50)
        return p50

    def _evaluate_calibration_mae(
        self,
        model: CatBoostRegressor,
        X_val: pd.DataFrame,
        df_val: pd.DataFrame,
        dist: str,
        scale: float,
        calibrator: Optional[LinearCalibrator] = None,
        segment_calibrators: Optional[Dict[str, LinearCalibrator]] = None,
    ) -> float:
        """
        Compute validation MAE with given calibration.
        
        Returns:
            MAE on validation events.
        """
        p50 = self._predict_with_calibration(
            model, X_val, df_val, dist, scale,
            calibrator=calibrator,
            segment_calibrators=segment_calibrators,
        )
        return self._compute_event_mae(p50, df_val)

    def _select_best_calibration(
        self,
        model: CatBoostRegressor,
        X_val: pd.DataFrame,
        df_val: pd.DataFrame,
        dist: str,
        scale: float,
        baseline_mae: float,
        global_calibrator: Optional[LinearCalibrator],
        segment_calibrators: Dict[str, LinearCalibrator],
    ) -> Tuple[CalibrationState, Dict[str, Any]]:
        """
        Select best calibration method by comparing validation MAE.
        
        Compares:
        1. No calibration (baseline)
        2. Global calibration only
        3. Segmented calibration (with global fallback)
        
        Returns:
            Tuple of (CalibrationState, calibration_info_dict).
        """
        results = {"baseline_mae": baseline_mae}
        
        # Evaluate global calibration
        global_mae = float("inf")
        if global_calibrator is not None:
            global_mae = self._evaluate_calibration_mae(
                model, X_val, df_val, dist, scale,
                calibrator=global_calibrator,
            )
            results["global_mae"] = global_mae
            logger.info("Global calibration MAE: %.2f", global_mae)
        
        # Evaluate segmented calibration
        segmented_mae = float("inf")
        if segment_calibrators:
            segmented_mae = self._evaluate_calibration_mae(
                model, X_val, df_val, dist, scale,
                calibrator=global_calibrator,  # fallback
                segment_calibrators=segment_calibrators,
            )
            results["segmented_mae"] = segmented_mae
            logger.info("Segmented calibration MAE: %.2f", segmented_mae)
        
        # Select best method
        best_mae = min(baseline_mae, global_mae, segmented_mae)
        
        if best_mae == baseline_mae:
            method = "none"
            logger.info(
                "Calibration selection: NONE (baseline MAE %.2f is best)",
                baseline_mae,
            )
            calibration = CalibrationState(
                method="none",
                baseline_mae=baseline_mae,
                global_mae=global_mae if global_mae != float("inf") else None,
                segmented_mae=segmented_mae if segmented_mae != float("inf") else None,
            )
        elif best_mae == global_mae:
            method = "global"
            logger.info(
                "Calibration selection: GLOBAL (MAE %.2f < baseline %.2f)",
                global_mae, baseline_mae,
            )
            calibration = CalibrationState(
                method="global",
                global_calibrator=global_calibrator,
                segment_column=self.config.calibration_segment_col,
                baseline_mae=baseline_mae,
                global_mae=global_mae,
                segmented_mae=segmented_mae if segmented_mae != float("inf") else None,
            )
        else:
            method = "segmented"
            logger.info(
                "Calibration selection: SEGMENTED (MAE %.2f < baseline %.2f)",
                segmented_mae, baseline_mae,
            )
            calibration = CalibrationState(
                method="segmented",
                global_calibrator=global_calibrator,
                segment_calibrators=segment_calibrators,
                segment_column=self.config.calibration_segment_col,
                baseline_mae=baseline_mae,
                global_mae=global_mae if global_mae != float("inf") else None,
                segmented_mae=segmented_mae,
            )
        
        results["selected_method"] = method
        results["final_mae"] = best_mae
        results["improvement_days"] = baseline_mae - best_mae
        results["improvement_pct"] = (
            (baseline_mae - best_mae) / baseline_mae * 100
            if baseline_mae > 0 else 0
        )
        
        if global_calibrator:
            results["global_calibrator"] = global_calibrator.to_dict()
        if segment_calibrators:
            results["segment_calibrators"] = {
                k: v.to_dict() for k, v in segment_calibrators.items()
            }
        
        return calibration, results

    def _compute_mae_by_year(self, pred_p50: np.ndarray, df_val: pd.DataFrame) -> Dict[str, float]:
        """MAE bucketed by creation year for events."""
        if "created_year" not in df_val.columns:
            return {}
        events = df_val[df_val["event_observed"] == 1].copy()
        if events.empty:
            return {}
        events = events.assign(pred_p50=pred_p50[events.index])
        grouped = events.groupby("created_year")
        return {
            str(int(year)): float((grp["pred_p50"] - grp["gestation_target"]).abs().mean())
            for year, grp in grouped
        }

    def _apply_target_clipping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optionally clip gestation_target for events to reduce tail impact."""
        clip_val = self.config.target_clip_days
        if clip_val is None:
            return df
        df = df.copy()
        mask = df.get("event_observed", 0) == 1
        if mask.any():
            df.loc[mask, "gestation_target"] = (
                pd.to_numeric(df.loc[mask, "gestation_target"], errors="coerce")
                .fillna(0.0)
                .clip(upper=clip_val)
            )
        return df

    def _log_event_distributions(self, df_train: pd.DataFrame, df_val: pd.DataFrame) -> None:
        """Log quantiles for train/val events to highlight distribution shift."""
        def _describe(events: pd.DataFrame, label: str) -> None:
            if events.empty:
                logger.info("%s events=0", label)
                return
            qs = events["gestation_target"].quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
            logger.info(
                "%s events=%d p10=%.1f p25=%.1f p50=%.1f p75=%.1f p90=%.1f p95=%.1f p99=%.1f",
                label,
                len(events),
                qs.loc[0.10],
                qs.loc[0.25],
                qs.loc[0.50],
                qs.loc[0.75],
                qs.loc[0.90],
                qs.loc[0.95],
                qs.loc[0.99],
            )

        train_events = df_train[df_train.get("event_observed", 0) == 1]
        val_events = df_val[df_val.get("event_observed", 0) == 1]
        _describe(train_events, "Train events")
        _describe(val_events, "Val events")

    def _config_to_dict(self) -> Dict[str, Any]:
        """Convert config to serializable dict."""
        return {
            "run_sweep": self.config.run_sweep,
            "sweep_distributions": self.config.sweep_distributions,
            "sweep_scales": self.config.sweep_scales,
            "use_time_split": self.config.use_time_split,
            "validation_months": self.config.validation_months,
            "apply_recency_weights": self.config.apply_recency_weights,
            "recent_weight": self.config.recent_weight,
            "recent_cutoff_months": self.config.recent_cutoff_months,
            "iterations": self.config.iterations,
            "learning_rate": self.config.learning_rate,
            "depth": self.config.depth,
            "l2_leaf_reg": self.config.l2_leaf_reg,
            "early_stopping_rounds": self.config.early_stopping_rounds,
            "seed": self.config.seed,
            "target_clip_days": self.config.target_clip_days,
            "calibrate_p50_bias": self.config.calibrate_p50_bias,
            # Post-hoc calibration settings
            "apply_calibration": self.config.apply_calibration,
            "segmented_calibration": self.config.segmented_calibration,
            "calibration_segment_col": self.config.calibration_segment_col,
            "min_segment_events": self.config.min_segment_events,
        }


def save_training_metadata(
    result: AFTTrainingResult,
    output_dir: Path,
) -> Path:
    """
    Save training metadata to JSON.

    Args:
        result: Training result object.
        output_dir: Directory to save metadata.

    Returns:
        Path to saved metadata file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "timestamp": result.timestamp,
        "best_dist": result.best_dist,
        "best_scale": result.best_scale,
        "baseline_mae": result.baseline_mae,
        "baseline_c_index": result.baseline_c_index,
        "median_ae": result.median_ae,
        "capped_mae": result.capped_mae,
        "train_rows": result.train_rows,
        "val_rows": result.val_rows,
        "val_events": result.val_events,
        "n_features": len(result.feature_columns),
        "categorical_features": result.categorical_features,
        "sweep_results": result.sweep_results,
        "config": result.config,
        "calibration": result.calibration_info,  # Post-hoc calibration details
    }

    path = output_dir / f"aft_training_{result.timestamp}.json"
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    logger.info("Saved training metadata → %s", path)
    return path

