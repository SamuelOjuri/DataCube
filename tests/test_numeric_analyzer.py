from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.numeric_analyzer import NumericBaseline
from src.core.models import PipelineStage, StatusCategory


def _recent(days: int) -> pd.Timestamp:
    return pd.Timestamp.now() - pd.Timedelta(days=days)


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
