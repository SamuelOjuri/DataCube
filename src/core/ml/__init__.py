"""
Machine Learning Core Modules for Survival Analysis and Gestation Predictions.

Available modules:
- dataset: Feature loading and preprocessing
- survival: Cox PH and Weibull AFT survival models
- quantiles: LightGBM quantile regression (legacy)
- aft_model: CatBoost AFT model for gestation predictions (recommended)
- aft_trainer: Training pipeline for CatBoost AFT models
"""

from .dataset import FeatureLoader, GESTATION_CATEGORY_COLUMNS, GESTATION_NUMERIC_COLUMNS
from .survival import SurvivalModel
from .aft_model import (
    AFTGestationModel,
    load_aft_model,
    LinearCalibrator,
    CalibrationState,
)
from .aft_trainer import AFTTrainer, AFTTrainerConfig, AFTTrainingResult

__all__ = [
    # Dataset
    "FeatureLoader",
    "GESTATION_CATEGORY_COLUMNS",
    "GESTATION_NUMERIC_COLUMNS",
    # Survival
    "SurvivalModel",
    # AFT (recommended)
    "AFTGestationModel",
    "load_aft_model",
    "LinearCalibrator",
    "CalibrationState",
    "AFTTrainer",
    "AFTTrainerConfig",
    "AFTTrainingResult",
]
