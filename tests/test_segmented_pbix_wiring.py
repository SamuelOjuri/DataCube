from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from scripts.build_segmented_pbix_layout import DEFAULT_OUTPUT, build_layout


PROJECT_ROOT = Path(__file__).resolve().parent.parent
UPDATER = PROJECT_ROOT / "scripts" / "update_segmented_pbix_model.ps1"
POWER_BI_SCRIPT = PROJECT_ROOT / "scripts" / "powerbi_segmented_xgb_script.py"


@pytest.fixture(scope="module")
def m_expression() -> str:
    powershell = shutil.which("powershell.exe")
    if powershell is None:
        pytest.skip("Windows PowerShell is required to validate the PBIX updater")

    result = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(UPDATER),
            "-Port",
            "1",
            "-Catalog",
            "PreviewOnly",
            "-SupabaseServer",
            "db.example.supabase.co:5432",
            "-DatabaseName",
            "postgres",
            "-PythonScriptPath",
            str(POWER_BI_SCRIPT),
            "-PreviewM",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def test_partition_uses_postgres_and_embedded_forecast(m_expression: str) -> None:
    required_fragments = (
        "PostgreSQL.Database(",
        "Query=\"SELECT#(lf)    month_start,",
        "FROM public.vw_weighted_enquiry_leaf_monthly_v1",
        "ORDER BY month_start, product_segment, category_segment",
        "SelectedInput = Table.SelectColumns(",
        "        Source,",
        "TypedInput = Table.TransformColumnTypes(",
        "PythonInput = Table.TransformColumns(",
        'each Date.ToText(_, [Format="yyyy-MM-dd", Culture="en-GB"]),',
        "Python.Execute(",
        "[dataset = PythonInput]",
        'ForecastRows = Table.SelectRows(PythonResult, each [Name] = "forecast_report")',
        '"Missing Python result"',
    )
    for fragment in required_fragments:
        assert fragment in m_expression

    assert "File.Contents" not in m_expression
    assert "Csv.Document" not in m_expression
    assert '[Name="overall_benchmark_report"]' not in m_expression


def test_partition_preserves_input_and_output_contract(m_expression: str) -> None:
    input_columns = (
        "month_start",
        "product_segment",
        "category_segment",
        "actual_weighted_enquiry_value",
    )
    output_columns = input_columns + (
        "forecast_weighted_enquiry_value",
        "xgboost_forecast",
        "seasonal_forecast",
        "series_type",
        "model",
        "forecast_horizon_months",
        "history_end",
    )
    for column in output_columns:
        assert f'"{column}"' in m_expression

    updater_source = UPDATER.read_text(encoding="utf-8")
    assert '$tableName = "vw_weighted_enquiry_value_monthly_oct2027"' in updater_source
    assert "Actual Weighted Enquiry Value - oct2027" in updater_source
    assert "Forecast Weighted Enquiry Value - oct2027" in updater_source
    assert "Password" not in updater_source
    assert "Credential" not in updater_source


def test_updater_contains_fail_closed_acceptance_checks() -> None:
    updater_source = UPDATER.read_text(encoding="utf-8")
    required_fragments = (
        "AcceptanceExportPath",
        "ExpectedHistoryStart",
        "ExpectedHistoryEnd",
        "ExpectedForecastMonths",
        "ActualLeafKeyCount",
        "BridgeLeafKeyCount",
        "ForecastLeafKeyCount",
        "MaxProductCategoryDelta",
        "MaxOverallLeafDelta",
        "PBIX acceptance checks passed.",
        "Export-AdomdRows",
    )
    for fragment in required_fragments:
        assert fragment in updater_source


def test_updater_contains_audit_tables_and_safe_measures(
    m_expression: str,
) -> None:
    required_m_fragments = (
        "FROM public.vw_weighted_enquiry_project_leaf_allocation_v1",
        "project_weighted_enquiry_value",
        "allocated_weighted_enquiry_value",
        "product_allocation_method",
        "category_allocation_method",
    )
    for fragment in required_m_fragments:
        assert fragment in m_expression

    updater_source = UPDATER.read_text(encoding="utf-8")
    required_model_fragments = (
        'allocationTableName = "ProjectLeafAllocation"',
        'modelSummaryTableName = "SegmentModelSummary"',
        'name = "Source Weighted Value"',
        'name = "Allocation Reconciliation Delta"',
        'name = "Allocation % of Selected Total"',
        'name = "Fallback Leaf Count"',
        "Microsoft.AnalysisServices.Server.Tabular.dll",
        "Forecast table metadata updated without replacing date variations.",
        "allocation product segment count",
        "model summary fallback leaf count",
    )
    for fragment in required_model_fragments:
        assert fragment in updater_source

    assert "overall_benchmark_report" not in updater_source


def test_live_pbix_has_a_distinct_default_name() -> None:
    assert DEFAULT_OUTPUT.name == "Forward-Looking-Monthly-Outlook-Segmented-Live.pbix"


def test_builder_refuses_to_overwrite_existing_pbix(tmp_path: Path) -> None:
    source = tmp_path / "source.pbix"
    output = tmp_path / "rollback.pbix"
    output.write_bytes(b"existing rollback artifact")

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        build_layout(source, output)

    assert output.read_bytes() == b"existing rollback artifact"


def test_builder_refuses_to_replace_source_pbix(tmp_path: Path) -> None:
    source = tmp_path / "original.pbix"
    source.write_bytes(b"original artifact")

    with pytest.raises(ValueError, match="must differ"):
        build_layout(source, source)

    assert source.read_bytes() == b"original artifact"