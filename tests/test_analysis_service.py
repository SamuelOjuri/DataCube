from pathlib import Path
import sys
import types

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.services.analysis_service import AnalysisService, GLOBAL_BACKOFF_THRESHOLD
from src.core.models import NumericPredictions, SegmentStatistics


class DummyDBClient:
    """Minimal stub that mimics the Supabase client wrapper used by AnalysisService."""

    def __init__(self):
        self.client = object()


class StubNumericBaseline:
    """Test double for NumericBaseline that records calls and returns canned outputs."""

    def __init__(self):
        self.analyze_calls = []
        self.segment_calls = []

    def analyze_project(
        self,
        project,
        historical_data,
        segment_data,
        segment_keys,
        backoff_tier,
        global_data=None,
        prior_strength=20,
    ):
        self.analyze_calls.append(
            {
                "project": project,
                "historical_data": historical_data,
                "segment_data": segment_data,
                "segment_keys": segment_keys,
                "backoff_tier": backoff_tier,
                "global_data": global_data,
            }
        )

        return NumericPredictions(
            expected_gestation_days=42,
            gestation_confidence=0.6,
            gestation_range={"p25": 30, "p75": 60},
            expected_conversion_rate=0.123456,
            conversion_confidence=0.7,
            rating_score=78,
            rating_components={},
        )

    def create_segment_statistics(
        self,
        segment_data,
        segment_keys,
        backoff_tier,
        prior_rate=None,
        prior_strength=None,
    ):
        self.segment_calls.append(
            {
                "segment_data": segment_data,
                "segment_keys": segment_keys,
                "backoff_tier": backoff_tier,
                "prior_rate": prior_rate,
                "prior_strength": prior_strength,
            }
        )

        return SegmentStatistics(
            segment_keys=segment_keys,
            sample_size=len(segment_data),
            backoff_tier=backoff_tier,
        )


def _bind(instance, func):
    """Helper to bind a function as an instance method."""
    return types.MethodType(func, instance)


def _base_project(**overrides):
    data = {
        "monday_id": "123",
        "project_name": "Example Project",
        "account": "Acme",
        "type": "Refurbishment",
        "category": "Healthcare",
        "product_type": "Modular",
        "new_enquiry_value": 15000,
        "gestation_period": None,
        "pipeline_stage": None,
        "status_category": None,
        "value_band": "Small (<15k)",
    }
    data.update(overrides)
    return data


def test_analyze_project_uses_global_data_when_segment_sparse():
    service = AnalysisService(db_client=DummyDBClient(), lookback_days=365)
    stub_baseline = StubNumericBaseline()
    service.numeric_baseline = stub_baseline

    sparse_segment = pd.DataFrame({"metric": [1, 2]})
    global_df = pd.DataFrame({"metric": [10, 11, 12]})

    def fake_fetch_segment(self, key, min_n=15):
        return sparse_segment, ["category"], GLOBAL_BACKOFF_THRESHOLD

    def fake_fetch_global(self):
        return global_df

    service._fetch_segment_df = _bind(service, fake_fetch_segment)
    service._fetch_global_df = _bind(service, fake_fetch_global)

    result = service.analyze_project(_base_project())

    assert stub_baseline.analyze_calls, "Expected NumericBaseline.analyze_project to be called"
    call = stub_baseline.analyze_calls[0]
    assert call["global_data"] is global_df
    assert result["expected_conversion_rate"] == pytest.approx(0.123456)


def test_analyze_project_skips_global_data_for_dense_segment():
    service = AnalysisService(db_client=DummyDBClient(), lookback_days=365)
    stub_baseline = StubNumericBaseline()
    service.numeric_baseline = stub_baseline

    dense_segment = pd.DataFrame({"metric": [1, 2, 3, 4]})

    def fake_fetch_segment(self, key, min_n=15):
        return dense_segment, ["account", "category"], GLOBAL_BACKOFF_THRESHOLD - 2

    def fake_fetch_global(self):
        raise AssertionError("Global data should not be fetched for dense segment tiers")

    service._fetch_segment_df = _bind(service, fake_fetch_segment)
    service._fetch_global_df = _bind(service, fake_fetch_global)

    service.analyze_project(_base_project())

    assert stub_baseline.analyze_calls, "Expected NumericBaseline.analyze_project to be called"
    call = stub_baseline.analyze_calls[0]
    assert call["global_data"] is None


def test_build_segment_stats_reuses_numeric_baseline():
    service = AnalysisService(db_client=DummyDBClient(), lookback_days=365)
    stub_baseline = StubNumericBaseline()
    service.numeric_baseline = stub_baseline

    segment_df = pd.DataFrame({"metric": [5, 6, 7]})

    def fake_fetch_segment(self, key, min_n=15):
        return segment_df, ["category"], 3

    service._fetch_segment_df = _bind(service, fake_fetch_segment)

    stats = service._build_segment_stats(_base_project())

    assert stub_baseline.segment_calls, "Expected NumericBaseline.create_segment_statistics to be called"
    assert stats.sample_size == len(segment_df)

