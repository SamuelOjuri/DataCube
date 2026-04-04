from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import src.core.numeric_analyzer as numeric_analyzer_module
from src.core.numeric_analyzer import NumericBaseline
from src.core.models import PipelineStage, ProjectFeatures, StatusCategory


def _recent(days: int) -> pd.Timestamp:
    return pd.Timestamp.now() - pd.Timedelta(days=days)


def _bias_support_frame(
    n: int = 20,
    product_key: str = "pir_foil",
    value_band: str = "Small (<15k)",
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "type": ["New Build"] * n,
            "category": ["Commercial"] * n,
            "product_key": [product_key] * n,
            "value_band": [value_band] * n,
            "gestation_period": [90 + i for i in range(n)],
        }
    )


def test_gestation_weighting_handles_missing_dates():
    nb = NumericBaseline()
    df = pd.DataFrame(
        {
            'gestation_period': [45, 60, 75, 90],
            'date_created': [
                _recent(10),
                _recent(200),
                None,
                _recent(900),
            ],
        }
    )

    median, stats = nb.calculate_gestation_baseline(df)

    assert median is not None
    assert stats['count'] == 4
    assert stats['weighting'].startswith('time_weighted')


def test_conversion_rate_includes_weighted_metrics():
    nb = NumericBaseline()
    df = pd.DataFrame(
        {
            'pipeline_stage': [
                PipelineStage.WON_CLOSED.value,
                PipelineStage.LOST.value,
                PipelineStage.OPEN_ENQUIRY.value,
            ],
            'status_category': [
                StatusCategory.WON.value,
                StatusCategory.LOST.value,
                StatusCategory.OPEN.value,
            ],
            'date_created': [
                _recent(30),
                _recent(800),
                _recent(400),
            ],
        }
    )

    rate, stats = nb.calculate_conversion_rate(df, method='inclusive')

    assert rate is not None
    assert stats['wins'] == 1
    assert stats['wins_weighted'] != pytest.approx(stats['wins'])
    assert stats['weighting'].startswith('time_weighted')


def test_closed_only_preserves_fractional_weights():
    nb = NumericBaseline()
    df = pd.DataFrame(
        {
            'pipeline_stage': [
                PipelineStage.WON_CLOSED.value,
                PipelineStage.LOST.value,
            ],
            'status_category': [
                StatusCategory.WON.value,
                StatusCategory.LOST.value,
            ],
            'date_created': [
                _recent(15),
                _recent(900),
            ],
        }
    )

    rate, stats = nb.calculate_conversion_rate(df, method='closed_only')

    assert rate is not None
    assert stats['wins_weighted'] != pytest.approx(stats['wins'])
    assert stats['losses_weighted'] != pytest.approx(stats['losses'])
    assert stats['weighting'].startswith('time_weighted')


def test_gestation_bias_prefers_product_key_over_coarse_value_band(monkeypatch):
    nb = NumericBaseline()
    support_data = _bias_support_frame()
    project = ProjectFeatures(
        project_id="123",
        name="Bias Candidate",
        type="New Build",
        category="Commercial",
        product_type="Foil Faced PIR",
        product_key="pir_foil",
        value_band="Small (<15k)",
    )

    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_GLOBAL_DAYS", 0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_GLOBAL_FALLBACK_DAYS", 0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_BY_SEGMENT", {("New Build", "Commercial"): 100})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_SEGMENT_SAMPLE_SIZES", {("New Build", "Commercial"): 20})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_SUPPORT_SHRINKAGE", 1.0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_DAMPING_FACTOR", 1.0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_MAX_ABS_DAYS", 500)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_TIER_WEIGHTS", {tier: 1.0 for tier in range(6)})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_VARIANCE_AWARE_DAMPING_ENABLED", False)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_ENABLED", True)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_MIN_N", 5)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_SHRINKAGE", 1.0)
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_BY_PRODUCT_KEY_SEGMENT",
        {("New Build", "Commercial", "pir_foil"): 20},
    )
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_PRODUCT_KEY_SEGMENT_SAMPLE_SIZES",
        {("New Build", "Commercial", "pir_foil"): 20},
    )
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_VALUE_BAND_ENABLED", True)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_VALUE_BAND_MIN_N", 5)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_VALUE_BAND_SHRINKAGE", 1.0)
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_BY_VALUE_BAND_SEGMENT",
        {("New Build", "Commercial", "Small (<15k)"): 40},
    )
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES",
        {("New Build", "Commercial", "Small (<15k)"): 20},
    )
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_ENABLED", False)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT", {})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_SEGMENT_SAMPLE_SIZES", {})

    adjustment = nb._compute_gestation_bias_adjustment(
        project=project,
        support_data=support_data,
        backoff_tier=3,
        gest_stats={"effective_n": 20.0, "mean": 100.0, "std": 0.0},
    )

    assert adjustment["source"] == "product_key_shrunk"
    assert adjustment["selected_level"] == "product_key"
    assert adjustment["segment_key"] == ("New Build", "Commercial", "pir_foil")
    assert adjustment["segment_bias_days"] == 20
    assert adjustment["raw_bias_days"] == 24
    assert adjustment["adjustment_days"] == 24


def test_gestation_bias_prefers_product_key_value_band_over_product_key(monkeypatch):
    nb = NumericBaseline()
    support_data = _bias_support_frame()
    project = ProjectFeatures(
        project_id="456",
        name="Most Specific Bias Candidate",
        type="New Build",
        category="Commercial",
        product_type="Foil Faced PIR",
        product_key="pir_foil",
        value_band="Small (<15k)",
    )

    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_GLOBAL_DAYS", 0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_GLOBAL_FALLBACK_DAYS", 0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_BY_SEGMENT", {("New Build", "Commercial"): 100})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_SEGMENT_SAMPLE_SIZES", {("New Build", "Commercial"): 20})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_SUPPORT_SHRINKAGE", 1.0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_DAMPING_FACTOR", 1.0)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_MAX_ABS_DAYS", 500)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_TIER_WEIGHTS", {tier: 1.0 for tier in range(6)})
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_VARIANCE_AWARE_DAMPING_ENABLED", False)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_ENABLED", True)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_MIN_N", 5)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_SHRINKAGE", 1.0)
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_BY_PRODUCT_KEY_SEGMENT",
        {("New Build", "Commercial", "pir_foil"): 20},
    )
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_PRODUCT_KEY_SEGMENT_SAMPLE_SIZES",
        {("New Build", "Commercial", "pir_foil"): 20},
    )
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_VALUE_BAND_ENABLED", True)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_VALUE_BAND_MIN_N", 5)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_VALUE_BAND_SHRINKAGE", 1.0)
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_BY_VALUE_BAND_SEGMENT",
        {("New Build", "Commercial", "Small (<15k)"): 40},
    )
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_VALUE_BAND_SEGMENT_SAMPLE_SIZES",
        {("New Build", "Commercial", "Small (<15k)"): 20},
    )
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_ENABLED", True)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_MIN_N", 5)
    monkeypatch.setattr(numeric_analyzer_module, "GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_SHRINKAGE", 1.0)
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_BY_PRODUCT_KEY_VALUE_BAND_SEGMENT",
        {("New Build", "Commercial", "pir_foil", "Small (<15k)"): 10},
    )
    monkeypatch.setattr(
        numeric_analyzer_module,
        "GESTATION_BIAS_PRODUCT_KEY_VALUE_BAND_SEGMENT_SAMPLE_SIZES",
        {("New Build", "Commercial", "pir_foil", "Small (<15k)"): 20},
    )

    adjustment = nb._compute_gestation_bias_adjustment(
        project=project,
        support_data=support_data,
        backoff_tier=3,
        gest_stats={"effective_n": 20.0, "mean": 100.0, "std": 0.0},
    )

    assert adjustment["source"] == "product_key_value_band_shrunk"
    assert adjustment["selected_level"] == "product_key_value_band"
    assert adjustment["segment_key"] == ("New Build", "Commercial", "pir_foil", "Small (<15k)")
    assert adjustment["segment_bias_days"] == 10
    assert adjustment["value_band_key"] == ("New Build", "Commercial", "pir_foil", "Small (<15k)")
    assert adjustment["raw_bias_days"] == 11
    assert adjustment["adjustment_days"] == 11
