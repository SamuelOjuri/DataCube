"""
CatBoost AFT Model for gestation timeline predictions.

This module provides a production-ready inference helper that loads
trained CatBoost AFT models and provides quantile predictions (p25/p50/p75).
"""

from __future__ import annotations

import logging
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

try:
    from catboost import CatBoostRegressor
except ImportError:
    CatBoostRegressor = None

try:
    from scipy.stats import norm, logistic
except ImportError:
    norm = None
    logistic = None

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Calibration Data Structures
# ---------------------------------------------------------------------------

@dataclass
class LinearCalibrator:
    """
    Stores intercept and slope for log-space calibration.
    
    Transforms raw log-mu predictions: calibrated_log_mu = intercept + slope * log_mu
    """
    intercept: float
    slope: float
    
    def transform(self, log_mu: np.ndarray) -> np.ndarray:
        """Apply linear calibration in log-space."""
        return self.intercept + self.slope * log_mu
    
    def to_dict(self) -> Dict[str, float]:
        """Serialize to dict for JSON output."""
        return {"intercept": self.intercept, "slope": self.slope}
    
    @classmethod
    def from_dict(cls, d: Dict[str, float]) -> "LinearCalibrator":
        """Deserialize from dict."""
        return cls(intercept=d["intercept"], slope=d["slope"])


@dataclass
class CalibrationState:
    """
    Full calibration state for the AFT model.
    
    Supports three calibration methods:
    - "none": No calibration applied
    - "global": Single LinearCalibrator for all predictions
    - "segmented": Per-segment LinearCalibrators with global fallback
    """
    method: str = "none"  # "none", "global", "segmented"
    global_calibrator: Optional[LinearCalibrator] = None
    segment_calibrators: Dict[str, LinearCalibrator] = field(default_factory=dict)
    segment_column: str = "value_band"
    
    # Metrics from calibration selection
    baseline_mae: Optional[float] = None
    global_mae: Optional[float] = None
    segmented_mae: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for persistence."""
        return {
            "method": self.method,
            "global_calibrator": self.global_calibrator.to_dict() if self.global_calibrator else None,
            "segment_calibrators": {
                k: v.to_dict() for k, v in self.segment_calibrators.items()
            } if self.segment_calibrators else {},
            "segment_column": self.segment_column,
            "baseline_mae": self.baseline_mae,
            "global_mae": self.global_mae,
            "segmented_mae": self.segmented_mae,
        }
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CalibrationState":
        """Deserialize from dict."""
        global_cal = None
        if d.get("global_calibrator"):
            global_cal = LinearCalibrator.from_dict(d["global_calibrator"])
        
        segment_cals = {}
        if d.get("segment_calibrators"):
            segment_cals = {
                k: LinearCalibrator.from_dict(v)
                for k, v in d["segment_calibrators"].items()
            }
        
        return cls(
            method=d.get("method", "none"),
            global_calibrator=global_cal,
            segment_calibrators=segment_cals,
            segment_column=d.get("segment_column", "value_band"),
            baseline_mae=d.get("baseline_mae"),
            global_mae=d.get("global_mae"),
            segmented_mae=d.get("segmented_mae"),
        )


# ---------------------------------------------------------------------------
# Feature Configuration Constants
# ---------------------------------------------------------------------------

# Default categorical features for AFT model (aligned with training)
AFT_CATEGORICAL_FEATURES = [
    "pipeline_stage",
    "type",
    "category",
    "zip_code",
    "sales_representative",
    "funding",
    "account",
    "product_type",
    "value_band",
]

# Default date columns for delta feature engineering
AFT_DATE_COLUMNS = [
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

# Columns to drop (IDs, targets, leakage)
AFT_DROP_COLUMNS = [
    "id",
    "monday_id",
    "item_name",
    "project_name",
    "gestation_period",
    "gestation_target",
    "duration_days",
    "time_open_days",
    "event_observed",
    "is_censored",
    "observation_end_date",
    "status_category",
    "censor_reason",
    "lost_to_who_or_why",
    "feedback",
]


class AFTGestationModel:
    """
    CatBoost AFT-based gestation timeline predictor.
    
    Loads trained CatBoost AFT model artifacts and provides p25/p50/p75
    quantile predictions for project gestation timelines.
    
    Supports post-hoc calibration via LinearRegression in log-space:
    - Global calibration: single calibrator for all predictions
    - Segmented calibration: per-segment calibrators with global fallback
    """

    MODEL_PAYLOAD_VERSION = 2  # Bumped for calibration state support
    DEFAULT_DIST = "Logistic"
    DEFAULT_SCALE = 2.0

    def __init__(
        self,
        *,
        categorical_features: Optional[Sequence[str]] = None,
        date_columns: Optional[Sequence[str]] = None,
        drop_columns: Optional[Sequence[str]] = None,
    ) -> None:
        self.categorical_features: List[str] = list(
            categorical_features or AFT_CATEGORICAL_FEATURES
        )
        self.date_columns: List[str] = list(date_columns or AFT_DATE_COLUMNS)
        self.drop_columns: List[str] = list(drop_columns or AFT_DROP_COLUMNS)
        
        self.model: Optional[CatBoostRegressor] = None
        self.dist: str = self.DEFAULT_DIST
        self.scale: float = self.DEFAULT_SCALE
        self.feature_columns: List[str] = []
        self.training_metrics: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        self.calibration_offset_days: float = 0.0  # Legacy: kept for v1 compat
        self.calibration: Optional[CalibrationState] = None  # New: v2 calibration
        self.is_fitted = False

    def load(self, path: Path) -> None:
        """
        Load a trained AFT model artifact from disk.
        
        Supports both v1 (simple offset calibration) and v2 (LinearRegression calibration).
        
        Args:
            path: Path to the .pkl artifact file.
        """
        if CatBoostRegressor is None:
            raise RuntimeError("catboost not installed. Cannot load AFT model.")
        
        path = Path(path)
        with path.open("rb") as fh:
            payload = pickle.load(fh)
        
        version = payload.get("version", 1)
        if version > self.MODEL_PAYLOAD_VERSION:
            logger.warning(
                "AFT model version newer than supported (model=%s, supported=%s)",
                version,
                self.MODEL_PAYLOAD_VERSION,
            )
        
        self.model = payload.get("model")
        self.dist = payload.get("dist", self.DEFAULT_DIST)
        self.scale = payload.get("scale", self.DEFAULT_SCALE)
        self.categorical_features = payload.get(
            "categorical_features", self.categorical_features
        )
        self.feature_columns = payload.get("feature_columns", [])
        self.training_metrics = payload.get("training_metrics", {})
        self.metadata = payload.get("metadata", {})
        
        # Legacy v1 calibration (simple offset)
        self.calibration_offset_days = float(payload.get("calibration_offset_days", 0.0))
        
        # v2 calibration state (LinearRegression-based)
        self.calibration = None
        if "calibration" in payload and payload["calibration"] is not None:
            self.calibration = CalibrationState.from_dict(payload["calibration"])
        
        self.is_fitted = self.model is not None
        
        # Log calibration info
        cal_method = self.calibration.method if self.calibration else "none"
        if cal_method == "none" and self.calibration_offset_days:
            cal_method = "legacy_offset"
        
        logger.info(
            "Loaded AFT model from %s | dist=%s scale=%.1f features=%d calibration=%s",
            path,
            self.dist,
            self.scale,
            len(self.feature_columns),
            cal_method,
        )

    def save(self, path: Path) -> Path:
        """
        Save the model artifact to disk.
        
        Args:
            path: Destination path for the .pkl file.
            
        Returns:
            The path where the artifact was saved.
        """
        if not self.is_fitted:
            raise RuntimeError("Cannot save AFT model before fitting/loading.")
        
        payload = {
            "version": self.MODEL_PAYLOAD_VERSION,
            "model": self.model,
            "dist": self.dist,
            "scale": self.scale,
            "categorical_features": self.categorical_features,
            "feature_columns": self.feature_columns,
            "training_metrics": self.training_metrics,
            "metadata": self.metadata,
            "calibration_offset_days": self.calibration_offset_days,  # Legacy v1
            "calibration": self.calibration.to_dict() if self.calibration else None,  # v2
        }
        
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            pickle.dump(payload, fh)
        
        cal_method = self.calibration.method if self.calibration else "none"
        logger.info("Saved AFT model artifact → %s (calibration=%s)", path, cal_method)
        return path

    def predict(
        self,
        project_features: pd.DataFrame,
        segment_value: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Predict gestation quantiles for a single project.
        
        Args:
            project_features: DataFrame with project features (single row).
            segment_value: Optional segment value for calibration lookup.
                If not provided, will be extracted from project_features using
                the calibration segment column (e.g., value_band).
            
        Returns:
            Dict with integer days for p25/p50/p75, cycle_category, and confidence_note.
        """
        empty_result = {
            "p25": None,
            "p50": None,
            "p75": None,
            "cycle_category": None,
            "confidence_note": None,
        }
        
        if not self.is_fitted or self.model is None:
            return empty_result
        
        if norm is None or logistic is None:
            logger.warning("scipy not installed. Cannot compute quantiles.")
            return empty_result
        
        try:
            # Encode features
            encoded = self._encode_features(project_features)
            if encoded.empty:
                return empty_result
            
            # Get raw log-time prediction from model
            log_mu = float(self.model.predict(encoded.iloc[[0]])[0])
            log_mu = np.clip(log_mu, -10, 10)  # Numeric stability
            
            # Apply log-space calibration if available (v2 approach)
            log_mu = self._apply_calibration(
                log_mu, project_features, segment_value
            )
            
            # Compute quantiles based on distribution
            if self.dist == "Logistic":
                z25 = logistic.ppf(0.25)
                z50 = logistic.ppf(0.50)
                z75 = logistic.ppf(0.75)
            else:  # Normal (default)
                z25 = norm.ppf(0.25)
                z50 = norm.ppf(0.50)
                z75 = norm.ppf(0.75)
            
            t25 = np.exp(log_mu + self.scale * z25)
            t50 = np.exp(log_mu + self.scale * z50)
            t75 = np.exp(log_mu + self.scale * z75)

            # Legacy v1 calibration: apply offset after exp() if no v2 calibration
            if self.calibration is None and self.calibration_offset_days:
                t25 = t25 - self.calibration_offset_days
                t50 = t50 - self.calibration_offset_days
                t75 = t75 - self.calibration_offset_days
            
            p25 = max(0, int(round(t25)))
            p50 = max(0, int(round(t50)))
            p75 = max(0, int(round(t75)))
            
            # Determine cycle category and confidence note
            if p50 > 365:
                cycle_category = "long"
                confidence_note = "Long-cycle project; wider uncertainty expected"
            elif p50 > 180:
                cycle_category = "medium"
                confidence_note = "Medium-cycle project"
            else:
                cycle_category = "short"
                confidence_note = "Short-cycle project; higher confidence"
            
            return {
                "p25": p25,
                "p50": p50,
                "p75": p75,
                "cycle_category": cycle_category,
                "confidence_note": confidence_note,
            }
            
        except Exception as exc:
            logger.warning("AFT prediction failed: %s", exc)
            return empty_result

    def _apply_calibration(
        self,
        log_mu: float,
        project_features: pd.DataFrame,
        segment_value: Optional[str] = None,
    ) -> float:
        """
        Apply log-space calibration to raw prediction.
        
        Args:
            log_mu: Raw log-time prediction from model.
            project_features: Original features (for segment lookup).
            segment_value: Optional explicit segment value.
            
        Returns:
            Calibrated log_mu value.
        """
        if self.calibration is None or self.calibration.method == "none":
            return log_mu
        
        # Determine which calibrator to use
        calibrator = None
        
        if self.calibration.method == "segmented":
            # Try to get segment value
            if segment_value is None:
                seg_col = self.calibration.segment_column
                if seg_col in project_features.columns:
                    segment_value = str(project_features[seg_col].iloc[0])
            
            # Use segment calibrator if available
            if segment_value and segment_value in self.calibration.segment_calibrators:
                calibrator = self.calibration.segment_calibrators[segment_value]
            else:
                # Fall back to global calibrator
                calibrator = self.calibration.global_calibrator
        
        elif self.calibration.method == "global":
            calibrator = self.calibration.global_calibrator
        
        # Apply calibration
        if calibrator is not None:
            # Transform expects array, we have scalar
            calibrated = calibrator.transform(np.array([log_mu]))[0]
            return float(calibrated)
        
        return log_mu

    def predict_log_mu(self, project_features: pd.DataFrame) -> Optional[float]:
        """
        Return raw log-time prediction for diagnostics.
        
        Args:
            project_features: DataFrame with project features.
            
        Returns:
            Log-time prediction or None if unavailable.
        """
        if not self.is_fitted or self.model is None:
            return None
        
        try:
            encoded = self._encode_features(project_features)
            if encoded.empty:
                return None
            return float(self.model.predict(encoded.iloc[[0]])[0])
        except Exception as exc:
            logger.warning("Log-mu prediction failed: %s", exc)
            return None

    def _encode_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Encode features to match training-time feature matrix.
        
        Handles date parsing, delta features, categorical encoding,
        and alignment with training columns.
        """
        working = df.copy()
        
        # Drop target/leakage columns
        working = working.drop(columns=self.drop_columns, errors="ignore")
        
        # Parse date columns to UTC
        for col in self.date_columns:
            if col in working.columns:
                working[col] = pd.to_datetime(working[col], errors="coerce", utc=True)
        
        # Use date_created as baseline for delta features
        if "date_created" in working.columns:
            baseline = working["date_created"]
        elif "created_at" in working.columns:
            baseline = working["created_at"]
        else:
            baseline = None
        
        # Create date delta features
        if baseline is not None:
            for col in self.date_columns:
                if col in working.columns and col != baseline.name:
                    working[f"{col}_delta_days"] = (working[col] - baseline).dt.days
            
            # Calendar features
            working["creation_year"] = baseline.dt.year
            working["creation_month"] = baseline.dt.month
        
        # Drop raw datetime columns
        working = working.drop(
            columns=[c for c in self.date_columns if c in working.columns]
        )
        
        # Handle categorical features
        cat_cols_present = [c for c in self.categorical_features if c in working.columns]
        for col in cat_cols_present:
            working[col] = working[col].astype("string").fillna("Missing")
        
        # If we have training columns, align with them
        if self.feature_columns:
            # Ensure all expected columns exist
            for col in self.feature_columns:
                if col not in working.columns:
                    working[col] = 0 if col not in cat_cols_present else "Missing"
            working = working[self.feature_columns]
        
        return working


def load_aft_model(path: Path) -> AFTGestationModel:
    """
    Convenience function to load an AFT model from a path.
    
    Args:
        path: Path to model artifact (.pkl file) or directory.
        
    Returns:
        Loaded AFTGestationModel instance.
    """
    path = Path(path)
    
    # If directory, find most recent .pkl file
    if path.is_dir():
        candidates = sorted(
            path.glob("aft_*.pkl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise FileNotFoundError(f"No AFT model artifacts found in {path}")
        path = candidates[0]
    
    model = AFTGestationModel()
    model.load(path)
    return model

