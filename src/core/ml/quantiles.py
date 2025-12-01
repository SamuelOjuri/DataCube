from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from .dataset import GESTATION_CATEGORY_COLUMNS, GESTATION_NUMERIC_COLUMNS

try:  # pragma: no cover - exercised in integration tests
    import lightgbm as lgb
except ImportError:  # pragma: no cover
    lgb = None

logger = logging.getLogger(__name__)

class QuantileModel:
    """
    LightGBM-based quantile regression for gestation timelines.
    Trains one model per quantile (p25, p50, p75) and keeps encoding
    metadata so inference-time matrices align with training.
    """

    MODEL_PAYLOAD_VERSION = 1
    DEFAULT_ALPHAS: Sequence[float] = (0.25, 0.5, 0.75)
    TARGET_COLUMN = "gestation_target"

    def __init__(
        self,
        *,
        categorical_features: Optional[Sequence[str]] = None,
        numeric_features: Optional[Sequence[str]] = None,
        alphas: Optional[Sequence[float]] = None,
        min_training_rows: int = 100,
        boosting_params: Optional[Dict[str, float]] = None,
    ) -> None:
        self.categorical_features: List[str] = list(categorical_features or GESTATION_CATEGORY_COLUMNS)
        default_numeric = [col for col in GESTATION_NUMERIC_COLUMNS if col != self.TARGET_COLUMN]
        self.numeric_features: List[str] = list(numeric_features or default_numeric)
        self.alphas: Sequence[float] = tuple(alphas or self.DEFAULT_ALPHAS)
        self.min_training_rows = max(10, int(min_training_rows))
        self.boosting_params = boosting_params or {
            "n_estimators": 400,
            "learning_rate": 0.05,
            "max_depth": -1,
            "min_child_samples": 40,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "reg_alpha": 0.1,
            "reg_lambda": 0.3,
            "verbose": -1,
        }
        self.models: Dict[float, "lgb.LGBMRegressor"] = {}
        self.feature_columns: List[str] = []
        self.training_metrics: Dict[str, float] = {}
        self.calibration_offsets: Dict[str, float] = {}
        self.is_fitted = False

    # ------------------------------------------------------------------ #
    # Training
    # ------------------------------------------------------------------ #

    def fit(self, df: pd.DataFrame, *, target_col: Optional[str] = None) -> Dict[str, float]:
        """
        Train LightGBM quantile models for each requested alpha.
        Args:
            df: Preprocessed dataset (e.g., from FeatureLoader.build_gestation_frame()).
            target_col: Optional override for the gestation column (defaults to gestation_target).
        Returns:
            Dictionary of training metrics (MAE per quantile).
        """
        if lgb is None:  # pragma: no cover
            logger.warning("lightgbm not installed. Quantile regression is disabled.")
            return {}

        target = target_col or self.TARGET_COLUMN

        working = df.copy()
        working[target] = pd.to_numeric(working.get(target), errors="coerce")
        working = working[working[target].notna()]

        if len(working) < self.min_training_rows:
            logger.warning(
                "QuantileModel: insufficient rows for training (need %s, have %s).",
                self.min_training_rows,
                len(working),
            )
            return {}

        features = self._encode_features(working, record_training_columns=True)
        target_values = working[target].to_numpy(dtype=float)

        self.models = {}
        self.training_metrics = {}

        for alpha in self.alphas:
            model = lgb.LGBMRegressor(objective="quantile", alpha=float(alpha), **self.boosting_params)
            model.fit(features, target_values)
            preds = model.predict(features)
            mae = float(np.mean(np.abs(preds - target_values)))
            self.training_metrics[f"mae_q{int(alpha * 100)}"] = mae
            self.models[float(alpha)] = model
            logger.info("Trained quantile model α=%.2f | MAE=%.2f | rows=%s", alpha, mae, len(working))

        self.is_fitted = bool(self.models)
        return self.training_metrics.copy()

    # ------------------------------------------------------------------ #
    # Prediction
    # ------------------------------------------------------------------ #

    def predict(self, project_features: pd.DataFrame) -> Dict[str, Optional[int]]:
        """
        Predict gestation quantiles for the provided project feature row.
        Returns:
            Dict with integer days for p25/p50/p75 (or None if unavailable).
        """
        if not self.is_fitted or not self.feature_columns:
            return {"p25": None, "p50": None, "p75": None}

        encoded = self._encode_features(project_features, record_training_columns=False)
        if encoded.empty:
            return {"p25": None, "p50": None, "p75": None}
        encoded = encoded.iloc[[0]]

        results: Dict[str, Optional[int]] = {"p25": None, "p50": None, "p75": None}
        for alpha, model in self.models.items():
            try:
                raw_pred = float(model.predict(encoded)[0])
                clipped = max(raw_pred, 0.0)
                rounded = int(round(clipped))
            except Exception as exc:  # pragma: no cover
                logger.warning("Quantile prediction failed for α=%.2f: %s", alpha, exc)
                rounded = None
            label = f"p{int(alpha * 100)}"
            offset = self.calibration_offsets.get(label, 0.0)
            results[label] = rounded + int(round(offset)) if rounded is not None else None
        return results

    def update_calibration(self, offsets: Dict[str, float]) -> None:
        """
        Store additive adjustments for each quantile (derived from a validation split).
        """
        self.calibration_offsets = offsets or {}

    def predict_interval(self, project_features: pd.DataFrame) -> Optional[tuple[int, int]]:
        """
        Convenience helper returning (p25, p75) as a tuple for coverage checks.
        """
        preds = self.predict(project_features)
        lower, upper = preds.get("p25"), preds.get("p75")
        if lower is None or upper is None:
            return None
        return lower, upper

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #

    def save(self, path: Path) -> Path:
        if not self.is_fitted:
            raise RuntimeError("Cannot save QuantileModel before fitting.")
        payload = {
            "version": self.MODEL_PAYLOAD_VERSION,
            "categorical_features": self.categorical_features,
            "numeric_features": self.numeric_features,
            "alphas": tuple(self.alphas),
            "feature_columns": self.feature_columns,
            "models": self.models,
            "training_metrics": self.training_metrics,
            "calibration_offsets": self.calibration_offsets,
        }
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as handle:
            pickle.dump(payload, handle)
        logger.info("Saved quantile model artifact → %s", path)
        return path

    def load(self, path: Path) -> None:
        if lgb is None:  # pragma: no cover
            raise RuntimeError("Cannot load quantile model because lightgbm is not installed.")
        with Path(path).open("rb") as handle:
            payload = pickle.load(handle)

        version = payload.get("version")
        if version != self.MODEL_PAYLOAD_VERSION:
            logger.warning("QuantileModel artifact version mismatch (expected %s, got %s).", self.MODEL_PAYLOAD_VERSION, version)

        self.categorical_features = list(payload.get("categorical_features", self.categorical_features))
        self.numeric_features = list(payload.get("numeric_features", self.numeric_features))
        self.alphas = tuple(payload.get("alphas", self.DEFAULT_ALPHAS))
        self.feature_columns = list(payload.get("feature_columns", []))
        self.models = payload.get("models", {})
        self.training_metrics = payload.get("training_metrics", {})
        self.calibration_offsets = payload.get("calibration_offsets", {})
        self.is_fitted = bool(self.models)
        logger.info(
            "Loaded quantile model artifact from %s | alphas=%s | features=%s",
            path,
            self.alphas,
            len(self.feature_columns),
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _encode_features(self, df: pd.DataFrame, *, record_training_columns: bool) -> pd.DataFrame:
        working = df.copy()
        for col in self.categorical_features:
            series = (
                working.get(col, "Unknown")
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )
            working[col] = series
        for col in self.numeric_features:
            working[col] = pd.to_numeric(working.get(col), errors="coerce").fillna(0.0)

        encoded = pd.get_dummies(
            working[self.categorical_features + self.numeric_features],
            columns=self.categorical_features,
            drop_first=False,
            dtype=float,
        )

        encoded.columns = self._sanitize_feature_names(encoded.columns)

        if record_training_columns or not self.feature_columns:
            self.feature_columns = list(encoded.columns)
        return encoded.reindex(columns=self.feature_columns, fill_value=0.0)

    @staticmethod
    def _sanitize_feature_names(columns: Sequence[str]) -> List[str]:
        """
        LightGBM stores feature names in JSON; it rejects commas, quotes, etc.
        Replace non-alphanumeric characters with underscores and collapse runs.
        """
        sanitized: List[str] = []
        for name in columns:
            clean = "".join(ch if ch.isalnum() else "_" for ch in name)
            clean = "_".join(filter(None, clean.split("_")))
            sanitized.append(clean or "feature")
        return sanitized
