
import logging

from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.ml.survival import SurvivalModel

# Season mapping for temporal features
SEASON_MAP = {
    12: "winter", 1: "winter", 2: "winter",
    3: "spring", 4: "spring", 5: "spring",
    6: "summer", 7: "summer", 8: "summer",
    9: "autumn", 10: "autumn", 11: "autumn",
}


def _synthetic_survival_df(seed: int = 123, n_rows: int = 80) -> pd.DataFrame:
    """
    Generate a noisy dataset where VIP projects close faster but without perfect separation.
    
    Note: time_open_days is included for inference purposes (used as current_age_col in
    predict_win_probabilities), but is NOT used as a training feature to avoid data leakage.
    
    Includes temporal features (season, created_year, created_quarter, created_month) that
    are safe to use as they're derived from date_created.
    """
    rng = np.random.default_rng(seed)
    rows: List[Dict] = []
    categories = ["Commercial", "Industrial", "Education"]
    products = ["System A", "System B", "System C"]
    
    for i in range(n_rows):
        is_vip = rng.random() < 0.5
        base_duration = rng.lognormal(mean=3.6, sigma=0.35)
        duration = base_duration * (0.55 if is_vip else 1.1)
        duration = float(min(max(duration, 5.0), 220.0))
        censor_threshold = rng.uniform(110, 160)
        event = duration <= censor_threshold
        observed_duration = duration if event else censor_threshold
        noise = rng.normal(0, 8000)
        value = (130_000 if is_vip else 45_000) + noise
        value = float(max(7_500, value))
        band = (
            "XLarge (>100k)"
            if value >= 100_000
            else ("Large (40-100k)" if value >= 60_000 else "Medium (15-40k)")
        )
        # time_open_days: used at inference for conditional probability, NOT as a training feature
        time_open = max(3.0, observed_duration + rng.normal(0, 10))
        
        # Generate temporal features (safe - derived from date_created)
        created_year = rng.choice([2022, 2023, 2024])
        created_month = int(rng.integers(1, 13))
        created_quarter = (created_month - 1) // 3 + 1
        season = SEASON_MAP[created_month]
        
        rows.append(
            {
                "account": "VIP" if is_vip else "Standard",
                "type": rng.choice(["New Build", "Refurbishment"]),
                "category": rng.choice(categories),
                "product_type": rng.choice(products),
                "value_band": band,
                "new_enquiry_value": value,
                "log_value": np.log1p(value),
                "time_open_days": time_open,  # For inference only, not training
                "duration_days": observed_duration,
                "event_observed": 1 if event else 0,
                # Temporal features (safe - derived from date_created)
                "created_year": created_year,
                "created_quarter": created_quarter,
                "created_month": created_month,
                "season": season,
            }
        )
    return pd.DataFrame(rows)

def test_survival_model_fit_and_predictions_separate_risk_profiles():
    df = _synthetic_survival_df()
    train = df.iloc[:60].reset_index(drop=True)
    val = df.iloc[60:].reset_index(drop=True)
    model = SurvivalModel(penalizer=0.5)
    metrics = model.fit(train)

    assert metrics["c_index_train"] is not None
    val_score = model.score(val)
    assert val_score is None or val_score > 0.6

    vip_row = val[val["account"] == "VIP"].head(1)
    standard_row = val[val["account"] == "Standard"].head(1)

    vip_prob = model.predict_win_probabilities(vip_row, horizons=[30])[30]
    std_prob = model.predict_win_probabilities(standard_row, horizons=[30])[30]

    assert vip_prob > std_prob

def test_predict_win_probabilities_returns_neutral_when_unfitted():
    df = _synthetic_survival_df().head(1)
    model = SurvivalModel()
    probs = model.predict_win_probabilities(df, horizons=[30, 90])
    assert probs[30] == pytest.approx(0.5)
    assert probs[90] == pytest.approx(0.5)

def test_survival_model_handles_missing_lifelines(monkeypatch, caplog):
    from src.core.ml import survival as survival_module

    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(survival_module, "CoxPHFitter", None)
    monkeypatch.setattr(survival_module, "WeibullAFTFitter", None)

    model = survival_module.SurvivalModel()
    metrics = model.fit(_synthetic_survival_df().iloc[:10])

    assert metrics["c_index_train"] is None
    assert "lifelines not installed" in caplog.text
    assert model.is_fitted is False


def test_survival_model_uses_temporal_features():
    """Test that the model correctly uses temporal features (season, created_year, etc.)."""
    df = _synthetic_survival_df(n_rows=100)
    train = df.iloc[:80].reset_index(drop=True)
    
    model = SurvivalModel(penalizer=0.5)
    metrics = model.fit(train)
    
    # Check that temporal features are in the training columns
    assert metrics["c_index_train"] is not None
    assert any("season" in col for col in model.training_columns), "season features missing"
    assert "created_year" in model.training_columns, "created_year missing"
    assert "created_quarter" in model.training_columns, "created_quarter missing"
    assert "created_month" in model.training_columns, "created_month missing"


def test_survival_model_with_interactions():
    """Test that feature interactions work when enabled."""
    df = _synthetic_survival_df(n_rows=100)
    train = df.iloc[:80].reset_index(drop=True)
    
    # Train with interactions disabled (default)
    model_no_interactions = SurvivalModel(penalizer=0.5, enable_interactions=False)
    metrics_no = model_no_interactions.fit(train)
    n_features_no = metrics_no["n_features"]
    
    # Train with interactions enabled
    model_with_interactions = SurvivalModel(penalizer=0.5, enable_interactions=True)
    metrics_with = model_with_interactions.fit(train)
    n_features_with = metrics_with["n_features"]
    
    # With interactions enabled, we should have more features
    assert n_features_with >= n_features_no, "Interactions should add features"
    
    # Check that interaction columns are present
    interaction_cols = [c for c in model_with_interactions.training_columns if "interaction_" in c]
    assert len(interaction_cols) > 0, "No interaction columns found when interactions enabled"


def test_survival_model_save_load_preserves_interactions(tmp_path):
    """Test that save/load correctly preserves interaction settings."""
    df = _synthetic_survival_df(n_rows=80)
    train = df.iloc[:60].reset_index(drop=True)
    
    # Train with interactions enabled
    model = SurvivalModel(penalizer=0.5, enable_interactions=True)
    model.fit(train)
    
    # Save model
    model_path = tmp_path / "test_model.pkl"
    model.save(model_path)
    
    # Load into new model
    loaded_model = SurvivalModel()
    loaded_model.load(model_path)
    
    # Check that interaction settings are preserved
    assert loaded_model.enable_interactions == True
    assert loaded_model.interaction_pairs == model.interaction_pairs
    assert loaded_model.training_columns == model.training_columns


def test_backtest_with_time_open_days_zero():
    """Test that setting time_open_days=0 gives valid predictions for backtesting."""
    df = _synthetic_survival_df(n_rows=80)
    train = df.iloc[:60].reset_index(drop=True)
    val = df.iloc[60:].reset_index(drop=True)
    
    model = SurvivalModel(penalizer=0.5)
    model.fit(train)
    
    # Test prediction with time_open_days=0 (backtest mode)
    val_backtest = val.copy()
    val_backtest["time_open_days"] = 0
    
    probs = []
    for idx in range(len(val_backtest)):
        row_df = val_backtest.iloc[[idx]]
        prob = model.predict_win_probabilities(row_df, horizons=[30, 90])
        probs.append(prob)
    
    # All probabilities should be valid (between 0 and 1)
    for prob_dict in probs:
        assert 0 <= prob_dict[30] <= 1, "30-day probability out of range"
        assert 0 <= prob_dict[90] <= 1, "90-day probability out of range"
        # 90-day probability should be >= 30-day probability
        assert prob_dict[90] >= prob_dict[30], "90-day prob should be >= 30-day prob"
