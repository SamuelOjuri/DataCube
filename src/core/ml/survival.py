import logging

import pickle
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

try:
    from lifelines import CoxPHFitter, WeibullAFTFitter
    from lifelines.utils import concordance_index
except ImportError:  # pragma: no cover
    CoxPHFitter = None
    WeibullAFTFitter = None
    concordance_index = None

logger = logging.getLogger(__name__)

class SurvivalModel:
    """
    Wrapper around lifelines survival models that supports training, serialization,
    and aligned feature transformation for inference-time probability estimates.
    """

    DEFAULT_CATEGORICAL_FEATURES = [
        "account",
        "type",
        "category",
        "product_type",
        "value_band",
        "season",  # Derived from date_created month - safe, no leakage
    ]
    # Note: time_open_days is intentionally excluded from features to prevent data leakage.
    # It is computed using the current timestamp which leaks future information during training.
    # time_open_days IS used at inference time as current_age_col to condition predictions.
    #
    # Safe temporal features (derived from date_created only):
    # - created_year: captures year-over-year trends
    # - created_quarter: captures quarterly seasonality
    # - created_month: captures monthly seasonality
    DEFAULT_NUMERIC_FEATURES = [
        "log_value",
        "new_enquiry_value",
        "created_year",      # Safe: derived from date_created
        "created_quarter",   # Safe: derived from date_created
        "created_month",     # Safe: derived from date_created
    ]

    # Default interaction pairs: (column_a, column_b)
    # These capture value x category interactions that may be predictive
    DEFAULT_INTERACTION_PAIRS: List[Tuple[str, str]] = [
        ("value_band_XLarge (>100k)", "type_New Build"),
        ("value_band_Large (40-100k)", "type_New Build"),
        ("value_band_XLarge (>100k)", "type_Refurbishment"),
    ]

    def __init__(
        self,
        *,
        categorical_features: Optional[Sequence[str]] = None,
        numeric_features: Optional[Sequence[str]] = None,
        penalizer: float = 0.1,
        l1_ratio: float = 0.0,
        max_steps: int = 512,
        category_min_frequency: int = 10,
        enable_interactions: bool = False,
        interaction_pairs: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        self.categorical_features: List[str] = list(categorical_features or self.DEFAULT_CATEGORICAL_FEATURES)
        self.numeric_features: List[str] = list(numeric_features or self.DEFAULT_NUMERIC_FEATURES)
        self.penalizer = penalizer
        self.l1_ratio = l1_ratio
        self.max_steps = max(1, int(max_steps))
        self.category_min_frequency = max(1, int(category_min_frequency))
        self.enable_interactions = enable_interactions
        self.interaction_pairs: List[Tuple[str, str]] = list(
            interaction_pairs or self.DEFAULT_INTERACTION_PAIRS
        )
        self.duration_col = "duration_days"
        self.event_col = "event_observed"
        self.training_columns: List[str] = []
        self.metadata: Dict[str, float] = {}
        self.category_vocab: Dict[str, Set[str]] = {}
        self.cph_model: Optional[CoxPHFitter] = None
        self.aft_model: Optional[WeibullAFTFitter] = None
        self.is_fitted = False


    # ------------------------------------------------------------------ #
    # Training / fitting
    # ------------------------------------------------------------------ #
    def fit(
        self,
        df: pd.DataFrame,
        *,
        duration_col: str = "duration_days",
        event_col: str = "event_observed",
        fit_aft: bool = False,
    ) -> Dict[str, float]:
        """
        Fit the Cox Proportional Hazards model (and optional Weibull AFT) on the provided dataframe.
        Args:
            df: Preprocessed dataset containing features, duration, and event columns.
            duration_col: Column storing time-to-event or censoring.
            event_col: Column storing the event indicator (1=event occurred, 0=censored).
            fit_aft: Whether to train a secondary Weibull AFT model for diagnostics.
        Returns:
            Dictionary with training metrics (e.g., concordance).
        """
        if CoxPHFitter is None:
            logger.warning("lifelines not installed. Survival analysis disabled.")
            self.cph_model = None
            self.aft_model = None
            self.training_columns = []
            self.metadata = {
                "c_index_train": None,
                "n_rows": float(len(df)),
                "n_features": 0.0,
            }
            self.is_fitted = False
            return self.metadata

        working = df.copy()
        working = working[(working[duration_col] > 0) & working[event_col].notna()]
        if working.empty:
            raise ValueError("No valid rows available for survival training.")

        self.duration_col = duration_col
        self.event_col = event_col

        feature_matrix = self._encode_features(working, record_training_columns=True)
        model_df = feature_matrix.copy()
        model_df[self.duration_col] = working[self.duration_col].astype(float)
        model_df[self.event_col] = working[self.event_col].astype(int)

        logger.info("Training survival model on %s rows | %s features", len(model_df), len(feature_matrix.columns))

        self.cph_model = CoxPHFitter(penalizer=self.penalizer, l1_ratio=self.l1_ratio)
        fit_options = {"max_steps": self.max_steps, "step_size": 0.95, "precision": 1e-7}
        self.cph_model.fit(
            model_df,
            duration_col=self.duration_col,
            event_col=self.event_col,
            robust=True,
            fit_options=fit_options,
        )

        metrics: Dict[str, float] = {
            "c_index_train": float(getattr(self.cph_model, "concordance_index_", np.nan)),
            "n_rows": float(len(model_df)),
            "n_features": float(len(feature_matrix.columns)),
            "penalizer": float(self.penalizer),
            "l1_ratio": float(self.l1_ratio),
            "max_steps": float(self.max_steps),
            "category_min_frequency": float(self.category_min_frequency),
        }

        if fit_aft and WeibullAFTFitter is not None:
            try:
                self.aft_model = WeibullAFTFitter(penalizer=0.05)  # try 0.01–0.1
                self.aft_model.fit(
                    model_df,
                    duration_col=self.duration_col,
                    event_col=self.event_col,
                    robust=True,
                )
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to fit Weibull AFT model: %s", exc)

        self.metadata = metrics
        self.is_fitted = True
        logger.info("Survival training complete | c-index %.4f", metrics["c_index_train"])
        return metrics

    # ------------------------------------------------------------------ #
    # Evaluation helpers
    # ------------------------------------------------------------------ #
    def score(self, df: pd.DataFrame) -> Optional[float]:
        """
        Compute the concordance index for a holdout dataframe.
        Returns:
            c-index (float) or None if lifelines is unavailable or the model isn't fitted.
        """
        if not self.is_fitted or concordance_index is None or self.cph_model is None:
            return None
        if df.empty:
            return None
        hazards = self.predict_partial_hazard(df)
        if hazards is None:
            return None
        try:
            return float(
                concordance_index(
                    df[self.duration_col].astype(float),
                    -hazards.values,  # negative hazards -> higher risk == lower survival time
                    df[self.event_col].astype(int),
                )
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("Unable to compute concordance index: %s", exc)
            return None

    # ------------------------------------------------------------------ #
    # Prediction utilities
    # ------------------------------------------------------------------ #
    def predict_partial_hazard(self, df: pd.DataFrame) -> Optional[pd.Series]:
        if not self.is_fitted or self.cph_model is None:
            return None
        encoded = self._encode_features(df, record_training_columns=False)
        if encoded.empty:
            return None
        try:
            hazards = self.cph_model.predict_partial_hazard(encoded)
            hazards.index = df.index
            return hazards
        except Exception as exc:  # pragma: no cover
            logger.error("Partial hazard prediction failed: %s", exc)
            return None

    def predict_win_probabilities(
        self,
        project_features: pd.DataFrame,
        *,
        horizons,
        current_age_col: str = "time_open_days",
    ) -> dict:
        """
        Vectorised helper returning conditional win probabilities for multiple horizons.
        """
        from typing import Sequence, Dict

        if not self.is_fitted or self.cph_model is None or project_features.empty:
            return {h: 0.5 for h in horizons}

        encoded = self._encode_features(project_features, record_training_columns=False)
        if encoded.empty:
            return {h: 0.5 for h in horizons}

        current_age = float(project_features.iloc[0].get(current_age_col, 0.0))
        current_age = max(current_age, 0.0)
        future_ages = [current_age + float(max(h, 0)) for h in horizons]
        times = np.array(sorted({current_age, *future_ages}))

        try:
            surv = self.cph_model.predict_survival_function(encoded, times=times)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to compute survival function: %s", exc)
            return {h: 0.5 for h in horizons}

        try:
            s_current = float(surv.loc[current_age].iloc[0])
        except Exception:
            return {h: 0.5 for h in horizons}

        if s_current <= 0:
            return {h: 0.5 for h in horizons}

        results = {}
        for horizon, future_age in zip(horizons, future_ages):
            try:
                s_future = float(surv.loc[future_age].iloc[0])
            except Exception:
                results[horizon] = 0.5
                continue
            conditional = 1.0 - (s_future / s_current)
            results[horizon] = float(min(max(conditional, 0.0), 1.0))
        return results

    def predict_win_probability(
        self,
        project_features: pd.DataFrame,
        horizon_days: int = 30,
        *,
        current_age_col: str = "time_open_days",
    ) -> float:
        """
        Backwards-compatible single-horizon wrapper using predict_win_probabilities().
        """
        probabilities = self.predict_win_probabilities(
            project_features,
            horizons=[horizon_days],
            current_age_col=current_age_col,
        )
        return probabilities.get(horizon_days, 0.5)

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #
    def save(self, path: Path) -> Path:
        """
        Persist the fitted model and metadata to disk.
        """
        if not self.is_fitted:
            raise RuntimeError("Cannot save survival model before fitting.")
        payload = {
            "cph_model": self.cph_model,
            "aft_model": self.aft_model,
            "categorical_features": self.categorical_features,
            "numeric_features": self.numeric_features,
            "duration_col": self.duration_col,
            "event_col": self.event_col,
            "training_columns": self.training_columns,
            "metadata": self.metadata,
            "category_vocab": self.category_vocab,
            "enable_interactions": self.enable_interactions,
            "interaction_pairs": self.interaction_pairs,
        }
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            pickle.dump(payload, fh)
        logger.info("Saved survival model artifact → %s", path)
        return path

    def load(self, path: Path) -> None:
        """
        Load a previously trained model artifact.
        """
        path = Path(path)
        with path.open("rb") as fh:
            payload = pickle.load(fh)
        self.cph_model = payload.get("cph_model")
        self.aft_model = payload.get("aft_model")
        self.categorical_features = payload.get("categorical_features", self.DEFAULT_CATEGORICAL_FEATURES)
        self.numeric_features = payload.get("numeric_features", self.DEFAULT_NUMERIC_FEATURES)
        self.duration_col = payload.get("duration_col", "duration_days")
        self.event_col = payload.get("event_col", "event_observed")
        self.training_columns = payload.get("training_columns", [])
        self.metadata = payload.get("metadata", {})
        self.category_vocab = payload.get("category_vocab", {})
        self.enable_interactions = payload.get("enable_interactions", False)
        self.interaction_pairs = payload.get("interaction_pairs", self.DEFAULT_INTERACTION_PAIRS)
        self.is_fitted = self.cph_model is not None
        logger.info("Loaded survival model artifact from %s", path)

    # ------------------------------------------------------------------ #
    # Internal feature engineering helpers
    # ------------------------------------------------------------------ #
    def _category_allowlist(self, col: str, series: pd.Series, record_training_columns: bool) -> set:
        fallback: set = {"Other"}
        if series.empty:
            return self.category_vocab.get(col, fallback)
        if record_training_columns or col not in self.category_vocab:
            counts = series.value_counts()
            allowed = set(counts[counts >= self.category_min_frequency].index)
            allowed.add("Other")
            self.category_vocab[col] = allowed
        return self.category_vocab.get(col, fallback)

    def _encode_features(self, df: pd.DataFrame, *, record_training_columns: bool) -> pd.DataFrame:
        """
        Apply consistent encoding to categorical/numeric columns and align with training columns.
        """
        working = df.copy()
        for col in self.categorical_features:
            series = (
                working.get(col)
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
            )
            allowlist = self._category_allowlist(col, series, record_training_columns)
            working[col] = series.where(series.isin(allowlist), other="Other")
        for col in self.numeric_features:
            working[col] = pd.to_numeric(working.get(col), errors="coerce").fillna(0.0)
        encoded = pd.get_dummies(
            working[self.categorical_features + self.numeric_features],
            columns=self.categorical_features,
            drop_first=False,
            dtype=float,
        )
        
        # Add interaction features if enabled
        if self.enable_interactions:
            encoded = self._add_interaction_features(encoded)
        
        if record_training_columns or not self.training_columns:
            self.training_columns = list(encoded.columns)
        encoded = encoded.reindex(columns=self.training_columns, fill_value=0.0)
        return encoded

    def _add_interaction_features(self, encoded: pd.DataFrame) -> pd.DataFrame:
        """
        Add interaction features between categorical/numeric columns.
        
        Interactions capture non-linear relationships, e.g., large projects
        behave differently for New Build vs Refurbishment.
        
        Args:
            encoded: One-hot encoded feature DataFrame.
        
        Returns:
            DataFrame with additional interaction columns.
        """
        for col_a, col_b in self.interaction_pairs:
            if col_a in encoded.columns and col_b in encoded.columns:
                interaction_name = f"interaction_{col_a}_x_{col_b}"
                encoded[interaction_name] = encoded[col_a] * encoded[col_b]
        return encoded

