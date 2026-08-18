from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from scripts.validate_segmented_pbix_acceptance import (
    _compare_reports,
    _normalize_report,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACCEPTANCE_SCRIPT = PROJECT_ROOT / "scripts" / "validate_segmented_pbix_acceptance.py"
BASELINE_DIR = (
    PROJECT_ROOT / "outputs" / "segmented_weighted_enquiry_baseline_frozen"
)
LIVE_PBIX = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented-Live.pbix"
)
ROLLBACK_PBIX = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented.pbix"
)


@pytest.fixture(scope="module")
def acceptance_output(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output_dir = tmp_path_factory.mktemp("pbix-acceptance")
    result = subprocess.run(
        [
            sys.executable,
            str(ACCEPTANCE_SCRIPT),
            "--sql-actuals",
            str(BASELINE_DIR / "monthly_leaf_actuals.csv"),
            "--pbix-export",
            str(BASELINE_DIR / "segmented_weighted_enquiry_forecast.csv"),
            "--baseline-metrics",
            str(BASELINE_DIR / "backtest_metrics.csv"),
            "--live-pbix",
            str(LIVE_PBIX),
            "--rollback-pbix",
            str(ROLLBACK_PBIX),
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return output_dir


def test_acceptance_reproduces_pbix_and_backtest(acceptance_output: Path) -> None:
    summary = json.loads(
        (acceptance_output / "acceptance_summary.json").read_text(encoding="utf-8")
    )
    assert summary["automated_gates"] == {
        "sql_actuals_validated": True,
        "embedded_model_structure": True,
        "bottom_up_reconciliation": True,
        "pbix_sql_and_model_parity": True,
        "backtest_review_complete": True,
        "live_pbix_layout_valid": True,
        "rollback_pbix_retained": True,
    }
    assert summary["structure"]["row_count"] == 568
    assert summary["structure"]["history_end"] == "2026-07-01"
    assert summary["structure"]["forecast_end"] == "2027-10-01"
    assert summary["runtime"] == {
        "python": "3.11.8",
        "numpy": "2.4.4",
        "pandas": "3.0.2",
        "scipy": "1.17.1",
        "xgboost": "3.2.0",
    }
    assert summary["overall_backtest"]["review_required"] is False
    assert summary["benchmark"] == {
        "artifact": str(acceptance_output / "overall_benchmark_report.csv"),
        "benchmark_only": True,
        "production_rows": 0,
    }


def test_cutover_remains_blocked_without_manual_gates(acceptance_output: Path) -> None:
    summary = json.loads(
        (acceptance_output / "acceptance_summary.json").read_text(encoding="utf-8")
    )
    assert summary["manual_gates"] == {
        "seven_pages_visually_reviewed": False,
        "gateway_refresh_verified": False,
    }
    assert summary["cutover_ready"] is False


def test_report_comparison_rejects_forecast_drift() -> None:
    expected = _normalize_report(
        pd.read_csv(BASELINE_DIR / "segmented_weighted_enquiry_forecast.csv"),
        "expected",
    )
    changed = expected.copy()
    forecast_index = changed.index[changed["series_type"] == "Forecast"][0]
    changed.loc[forecast_index, "forecast_weighted_enquiry_value"] += 1.0

    with pytest.raises(AssertionError, match="forecast_weighted_enquiry_value"):
        _compare_reports(expected, changed, tolerance=1e-6)


def test_acceptance_writes_required_evidence(acceptance_output: Path) -> None:
    expected_files = {
        "acceptance_summary.json",
        "backtest_comparison.csv",
        "backtest_metrics.csv",
        "backtest_predictions.csv",
        "local_forecast_report.csv",
        "overall_benchmark_report.csv",
        "segment_model_summary.csv",
    }
    assert expected_files.issubset(path.name for path in acceptance_output.iterdir())