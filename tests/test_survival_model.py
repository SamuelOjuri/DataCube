
import logging

from typing import Dict

import numpy as np
import pandas as pd
import pytest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.ml.survival import SurvivalModel

def _synthetic_survival_df(seed: int = 123, n_rows: int = 80) -> pd.DataFrame:
    """Generate a noisy dataset where VIP projects close faster but without perfect separation."""
    rng = np.random.default_rng(seed)
    rows: Dict[str, object] = []
    categories = ["Commercial", "Industrial", "Education"]
    products = ["System A", "System B", "System C"]
    for _ in range(n_rows):
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
        time_open = max(3.0, observed_duration + rng.normal(0, 10))
        rows.append(
            {
                "account": "VIP" if is_vip else "Standard",
                "type": rng.choice(["New Build", "Refurbishment"]),
                "category": rng.choice(categories),
                "product_type": rng.choice(products),
                "value_band": band,
                "new_enquiry_value": value,
                "log_value": np.log1p(value),
                "time_open_days": time_open,
                "duration_days": observed_duration,
                "event_observed": 1 if event else 0,
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
