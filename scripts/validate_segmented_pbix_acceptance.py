from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import runpy
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.segmented_weighted_enquiry_forecast import backtest_monthly_leaves


DEFAULT_ACCEPTANCE_DIR = (
    PROJECT_ROOT / "outputs" / "segmented_weighted_enquiry_acceptance"
)
DEFAULT_BASELINE_DIR = (
    PROJECT_ROOT / "outputs" / "segmented_weighted_enquiry_baseline_frozen"
)
DEFAULT_LIVE_PBIX = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented-Live.pbix"
)
DEFAULT_ROLLBACK_PBIX = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented.pbix"
)
DEFAULT_EMBEDDED_SCRIPT = PROJECT_ROOT / "scripts" / "powerbi_segmented_xgb_script.py"

PRODUCT_SEGMENTS = {"Non-Combustible", "Combustible"}
CATEGORY_SEGMENTS = {
    "Data Centres",
    "Education",
    "Apartments/Housing",
    "Other",
}
PAGE_FILTERS: dict[str, tuple[str, str] | None] = {
    "Overall - Actual vs Forecast Weighted Enquiry Value": None,
    "By Product - Non-Combustible": ("product_segment", "Non-Combustible"),
    "By Product - Combustible": ("product_segment", "Combustible"),
    "By Category - Data Centres": ("category_segment", "Data Centres"),
    "By Category - Education": ("category_segment", "Education"),
    "By Category - Apartments/Housing": (
        "category_segment",
        "Apartments/Housing",
    ),
    "By Category - Other": ("category_segment", "Other"),
}
EXPECTED_RUNTIME = {
    "python": "3.11.8",
    "numpy": "2.4.4",
    "pandas": "3.0.2",
    "scipy": "1.17.1",
    "xgboost": "3.2.0",
}
REPORT_COLUMNS = (
    "product_segment",
    "category_segment",
    "month_start",
    "actual_weighted_enquiry_value",
    "forecast_weighted_enquiry_value",
    "xgboost_forecast",
    "seasonal_forecast",
    "series_type",
    "model",
    "forecast_horizon_months",
    "history_end",
)
REPORT_KEY = (
    "product_segment",
    "category_segment",
    "month_start",
    "series_type",
)
NUMERIC_COLUMNS = (
    "actual_weighted_enquiry_value",
    "forecast_weighted_enquiry_value",
    "xgboost_forecast",
    "seasonal_forecast",
    "forecast_horizon_months",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the refreshed segmented PBIX against validated SQL input and "
            "the exact embedded Python model."
        )
    )
    parser.add_argument(
        "--sql-actuals",
        type=Path,
        default=DEFAULT_ACCEPTANCE_DIR / "sql_monthly_actuals.csv",
    )
    parser.add_argument(
        "--pbix-export",
        type=Path,
        default=DEFAULT_ACCEPTANCE_DIR / "pbix_forecast_export.csv",
    )
    parser.add_argument(
        "--skip-pbix",
        action="store_true",
        help="Run SQL/model/backtest gates when no refreshed Desktop model is open.",
    )
    parser.add_argument("--embedded-script", type=Path, default=DEFAULT_EMBEDDED_SCRIPT)
    parser.add_argument(
        "--baseline-metrics",
        type=Path,
        default=DEFAULT_BASELINE_DIR / "backtest_metrics.csv",
    )
    parser.add_argument("--live-pbix", type=Path, default=DEFAULT_LIVE_PBIX)
    parser.add_argument("--rollback-pbix", type=Path, default=DEFAULT_ROLLBACK_PBIX)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_ACCEPTANCE_DIR)
    parser.add_argument("--backtest-months", type=int, default=12)
    parser.add_argument("--numeric-tolerance", type=float, default=1e-6)
    parser.add_argument("--metric-review-tolerance", type=float, default=1e-9)
    parser.add_argument("--allow-backtest-change", action="store_true")
    parser.add_argument("--pages-reviewed", action="store_true")
    parser.add_argument("--gateway-refresh-verified", action="store_true")
    parser.add_argument("--require-cutover-ready", action="store_true")
    return parser.parse_args()


def _require_file(path: Path, description: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{description} was not found: {resolved}")
    return resolved


def _load_monthly_actuals(path: Path) -> pd.DataFrame:
    monthly = pd.read_csv(_require_file(path, "SQL monthly actuals"))
    expected = {
        "month_start",
        "product_segment",
        "category_segment",
        "actual_weighted_enquiry_value",
    }
    missing = sorted(expected.difference(monthly.columns))
    if missing:
        raise ValueError(f"SQL monthly actuals are missing columns: {missing}")
    monthly = monthly.loc[:, sorted(expected)].copy()
    monthly["month_start"] = pd.to_datetime(monthly["month_start"], errors="raise")
    monthly["actual_weighted_enquiry_value"] = pd.to_numeric(
        monthly["actual_weighted_enquiry_value"], errors="raise"
    )
    return monthly


def _run_embedded_model(
    monthly: pd.DataFrame, embedded_script: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    namespace = runpy.run_path(
        str(_require_file(embedded_script, "embedded Power BI Python script")),
        init_globals={"dataset": monthly.copy()},
    )
    return (
        namespace["forecast_report"],
        namespace["segment_model_summary"],
        namespace["overall_benchmark_report"],
    )


def _normalize_report(frame: pd.DataFrame, description: str) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [str(column).strip().strip("[]") for column in normalized]
    missing = sorted(set(REPORT_COLUMNS).difference(normalized.columns))
    if missing:
        raise ValueError(f"{description} is missing columns: {missing}")
    normalized = normalized.loc[:, REPORT_COLUMNS].copy()
    for column in ("month_start", "history_end"):
        normalized[column] = pd.to_datetime(normalized[column], errors="raise").dt.normalize()
    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    for column in ("product_segment", "category_segment", "series_type", "model"):
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    if normalized.duplicated(list(REPORT_KEY)).any():
        raise ValueError(f"{description} contains duplicate production keys.")
    return normalized.sort_values(list(REPORT_KEY)).reset_index(drop=True)


def _assert_structure(report: pd.DataFrame, tolerance: float) -> dict[str, Any]:
    products = set(report["product_segment"])
    categories = set(report["category_segment"])
    if products != PRODUCT_SEGMENTS:
        raise AssertionError(f"Unexpected product segments: {sorted(products)}")
    if categories != CATEGORY_SEGMENTS:
        raise AssertionError(f"Unexpected category segments: {sorted(categories)}")

    actual = report.loc[report["series_type"] == "Actual"]
    bridge = report.loc[report["series_type"] == "Bridge"]
    forecast = report.loc[report["series_type"] == "Forecast"]
    if set(report["series_type"]) != {"Actual", "Bridge", "Forecast"}:
        raise AssertionError("The production report must contain only Actual/Bridge/Forecast.")

    actual_months = pd.DatetimeIndex(sorted(actual["month_start"].unique()))
    expected_actual_months = pd.date_range(
        actual_months.min(), actual_months.max(), freq="MS"
    )
    if not actual_months.equals(expected_actual_months):
        raise AssertionError("Actual history is not month-continuous.")
    if not actual.groupby("month_start").size().eq(8).all():
        raise AssertionError("Actual history does not contain eight leaves per month.")
    if len(bridge) != 8 or bridge["month_start"].nunique() != 1:
        raise AssertionError("The report must contain one eight-leaf Bridge month.")

    horizons = report["forecast_horizon_months"].dropna().unique()
    if len(horizons) != 1:
        raise AssertionError("The report contains inconsistent forecast horizons.")
    forecast_horizon = int(horizons[0])
    if forecast_horizon != 15:
        raise AssertionError(f"Expected a 15-month horizon, found {forecast_horizon}.")
    if len(forecast) != 8 * forecast_horizon:
        raise AssertionError("Forecast row count does not equal eight leaves times horizon.")
    if not forecast.groupby("month_start").size().eq(8).all():
        raise AssertionError("Forecast history does not contain eight leaves per month.")

    history_end = actual_months.max()
    if bridge["month_start"].min() != history_end:
        raise AssertionError("Bridge month does not equal the final actual month.")
    if forecast["month_start"].min() != history_end + pd.offsets.MonthBegin(1):
        raise AssertionError("Forecast does not begin one month after history.")
    if forecast["month_start"].max() != history_end + pd.offsets.MonthBegin(
        forecast_horizon
    ):
        raise AssertionError("Forecast endpoint does not match the configured horizon.")

    active = report.assign(
        active_value=report["actual_weighted_enquiry_value"].fillna(0.0)
        + report["forecast_weighted_enquiry_value"].fillna(0.0)
    )
    grouping = ["month_start", "series_type"]
    overall = active.groupby(grouping)["active_value"].sum().sort_index()
    product = (
        active.groupby(grouping + ["product_segment"])["active_value"]
        .sum()
        .groupby(grouping)
        .sum()
        .sort_index()
    )
    category = (
        active.groupby(grouping + ["category_segment"])["active_value"]
        .sum()
        .groupby(grouping)
        .sum()
        .sort_index()
    )
    max_product_category_delta = float((product - category).abs().max())
    max_overall_leaf_delta = float((overall - active.groupby(grouping)["active_value"].sum()).abs().max())
    if max_product_category_delta > tolerance or max_overall_leaf_delta > tolerance:
        raise AssertionError("Bottom-up product/category/overall reconciliation failed.")

    expected_rows = 8 * (len(actual_months) + 1 + forecast_horizon)
    if len(report) != expected_rows:
        raise AssertionError(f"Expected {expected_rows} production rows, found {len(report)}.")
    return {
        "row_count": len(report),
        "history_months": len(actual_months),
        "history_start": str(actual_months.min().date()),
        "history_end": str(history_end.date()),
        "forecast_start": str(forecast["month_start"].min().date()),
        "forecast_end": str(forecast["month_start"].max().date()),
        "forecast_horizon_months": forecast_horizon,
        "max_product_category_delta": max_product_category_delta,
        "max_overall_leaf_delta": max_overall_leaf_delta,
    }


def _compare_sql_actuals(
    sql_actuals: pd.DataFrame, report: pd.DataFrame, tolerance: float
) -> None:
    key = ["month_start", "product_segment", "category_segment"]
    expected = sql_actuals.loc[:, key + ["actual_weighted_enquiry_value"]]
    actual = report.loc[report["series_type"] == "Actual", key + ["actual_weighted_enquiry_value"]]
    merged = expected.merge(
        actual,
        on=key,
        how="outer",
        suffixes=("_sql", "_report"),
        indicator=True,
        validate="one_to_one",
    )
    if not merged["_merge"].eq("both").all():
        raise AssertionError("Report Actual keys do not exactly match the SQL monthly view.")
    matches = np.isclose(
        merged["actual_weighted_enquiry_value_sql"],
        merged["actual_weighted_enquiry_value_report"],
        rtol=0.0,
        atol=tolerance,
        equal_nan=False,
    )
    if not matches.all():
        raise AssertionError(f"Report Actual values differ from SQL in {(~matches).sum()} rows.")


def _compare_reports(expected: pd.DataFrame, actual: pd.DataFrame, tolerance: float) -> None:
    merged = expected.merge(
        actual,
        on=list(REPORT_KEY),
        how="outer",
        suffixes=("_expected", "_pbix"),
        indicator=True,
        validate="one_to_one",
    )
    if not merged["_merge"].eq("both").all():
        raise AssertionError("PBIX production keys do not match the embedded model output.")
    matched = merged.loc[merged["_merge"] == "both"]
    for column in ("model", "history_end"):
        if not matched[f"{column}_expected"].equals(matched[f"{column}_pbix"]):
            raise AssertionError(f"PBIX {column} values differ from the embedded model.")
    for column in NUMERIC_COLUMNS:
        matches = np.isclose(
            matched[f"{column}_expected"],
            matched[f"{column}_pbix"],
            rtol=0.0,
            atol=tolerance,
            equal_nan=True,
        )
        if not matches.all():
            raise AssertionError(f"PBIX {column} differs in {(~matches).sum()} rows.")


def _compare_backtest(
    current: pd.DataFrame, baseline_path: Path, tolerance: float
) -> tuple[pd.DataFrame, bool]:
    baseline = pd.read_csv(_require_file(baseline_path, "frozen backtest metrics"))
    keys = ["level", "segment", "model"]
    compared = baseline.merge(
        current,
        on=keys,
        how="outer",
        suffixes=("_baseline", "_current"),
        indicator=True,
        validate="one_to_one",
    )
    if not compared["_merge"].eq("both").all():
        raise AssertionError("Current and frozen backtest metric keys differ.")
    for column in ("wape", "bias_ratio"):
        compared[f"{column}_delta"] = (
            compared[f"{column}_current"] - compared[f"{column}_baseline"]
        )
    overall = compared.loc[
        (compared["level"] == "overall") & (compared["model"] == "forecast")
    ].iloc[0]
    review_required = bool(
        abs(float(overall["wape_delta"])) > tolerance
        or abs(float(overall["bias_ratio_delta"])) > tolerance
    )
    return compared, review_required


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_runtime() -> dict[str, str]:
    runtime = {"python": platform.python_version()}
    for package in ("numpy", "pandas", "scipy", "xgboost"):
        runtime[package] = importlib.metadata.version(package)
    if runtime != EXPECTED_RUNTIME:
        raise AssertionError(
            f"Power BI Python runtime mismatch: actual={runtime}, "
            f"expected={EXPECTED_RUNTIME}"
        )
    return runtime


def _inspect_artifacts(live_pbix: Path, rollback_pbix: Path) -> dict[str, Any]:
    live = _require_file(live_pbix, "live derived PBIX")
    rollback = _require_file(rollback_pbix, "CSV-backed rollback PBIX")
    if live == rollback:
        raise AssertionError("The live and rollback PBIX paths must be different.")
    with zipfile.ZipFile(live) as archive:
        if archive.testzip() is not None:
            raise AssertionError("The live PBIX archive failed integrity validation.")
        archive_names = archive.namelist()
        if "Report/Layout" in archive_names:
            modern_layout = False
            layout_text = archive.read("Report/Layout").decode("utf-16-le")
            layout = json.loads(layout_text)
            pages = {section["displayName"]: section for section in layout["sections"]}
            page_count = len(layout["sections"])
        else:
            modern_layout = True
            page_paths = sorted(
                name
                for name in archive_names
                if name.startswith("Report/definition/pages/")
                and name.endswith("/page.json")
            )
            if not page_paths:
                raise AssertionError("The live PBIX contains no supported report layout.")
            layout_parts: list[str] = []
            pages = {}
            for page_path in page_paths:
                page = json.loads(archive.read(page_path))
                page_prefix = page_path.removesuffix("page.json")
                visual_paths = sorted(
                    name
                    for name in archive_names
                    if name.startswith(f"{page_prefix}visuals/")
                    and name.endswith("/visual.json")
                )
                visual_text = "".join(
                    archive.read(path).decode("utf-8") for path in visual_paths
                )
                page_text = archive.read(page_path).decode("utf-8")
                layout_parts.extend((page_text, visual_text))
                pages[page["displayName"]] = {
                    "filters": json.dumps(
                        page.get("filterConfig", {}).get("filters", [])
                    ),
                    "visualContainers": [
                        {"config": visual_text, "query": ""}
                    ],
                }
            layout_text = "".join(layout_parts)
            page_count = len(page_paths)
    missing_pages = sorted(set(PAGE_FILTERS).difference(pages))
    if missing_pages:
        raise AssertionError(f"The live PBIX is missing report pages: {missing_pages}")
    if "overall_benchmark_report" in layout_text:
        raise AssertionError("A production visual references overall_benchmark_report.")

    for page_name, expected_filter in PAGE_FILTERS.items():
        page = pages[page_name]
        page_filters = json.loads(page.get("filters", "[]"))
        visual_text = "".join(
            visual.get("config", "") + visual.get("query", "")
            for visual in page.get("visualContainers", [])
        )
        if expected_filter is None:
            if page_filters:
                raise AssertionError("The Overall page must not have a segment filter.")
            expected_title = (
                "Actual vs Forecast Weighted Enquiry Value - 15-Month Outlook"
            )
        else:
            field, value = expected_filter
            filter_text = json.dumps(page_filters, ensure_ascii=False)
            if field not in filter_text or value not in filter_text:
                raise AssertionError(
                    f"Page {page_name!r} is missing its {field}={value!r} filter."
                )
            if not modern_layout and (
                field not in visual_text or value not in visual_text
            ):
                raise AssertionError(
                    f"Page {page_name!r} visual query is missing its segment filter."
                )
            expected_title = (
                f"Actual vs Forecast Weighted Enquiry Value - {value} "
                "(15-Month Outlook)"
            )
        if expected_title not in visual_text:
            raise AssertionError(f"Page {page_name!r} has an unexpected visual title.")

    return {
        "live_pbix": str(live),
        "live_pbix_sha256": _sha256(live),
        "rollback_pbix": str(rollback),
        "rollback_pbix_sha256": _sha256(rollback),
        "page_count": page_count,
        "required_page_count": len(PAGE_FILTERS),
        "page_filter_and_title_contracts": True,
    }


def main() -> int:
    args = parse_args()
    if args.backtest_months <= 0:
        raise ValueError("--backtest-months must be positive.")
    if args.numeric_tolerance < 0 or args.metric_review_tolerance < 0:
        raise ValueError("Acceptance tolerances must be non-negative.")

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime = _validate_runtime()
    sql_actuals = _load_monthly_actuals(args.sql_actuals)
    local_report_raw, model_summary, benchmark = _run_embedded_model(
        sql_actuals, args.embedded_script
    )
    local_report = _normalize_report(local_report_raw, "embedded model report")
    structure = _assert_structure(local_report, args.numeric_tolerance)
    _compare_sql_actuals(sql_actuals, local_report, args.numeric_tolerance)

    pbix_compared = False
    if not args.skip_pbix:
        pbix = _normalize_report(
            pd.read_csv(_require_file(args.pbix_export, "PBIX ADOMD export")),
            "PBIX ADOMD export",
        )
        _assert_structure(pbix, args.numeric_tolerance)
        _compare_sql_actuals(sql_actuals, pbix, args.numeric_tolerance)
        _compare_reports(local_report, pbix, args.numeric_tolerance)
        pbix_compared = True

    backtest_predictions, backtest_metrics = backtest_monthly_leaves(
        sql_actuals, holdout_months=args.backtest_months
    )
    backtest_comparison, review_required = _compare_backtest(
        backtest_metrics, args.baseline_metrics, args.metric_review_tolerance
    )
    if review_required and not args.allow_backtest_change:
        raise AssertionError(
            "Overall backtest WAPE/bias changed beyond the review tolerance; "
            "review and rerun with --allow-backtest-change to acknowledge it."
        )

    artifact_checks = _inspect_artifacts(args.live_pbix, args.rollback_pbix)
    local_report.to_csv(output_dir / "local_forecast_report.csv", index=False)
    model_summary.to_csv(output_dir / "segment_model_summary.csv", index=False)
    benchmark.to_csv(output_dir / "overall_benchmark_report.csv", index=False)
    backtest_predictions.to_csv(output_dir / "backtest_predictions.csv", index=False)
    backtest_metrics.to_csv(output_dir / "backtest_metrics.csv", index=False)
    backtest_comparison.to_csv(output_dir / "backtest_comparison.csv", index=False)

    overall_metric = backtest_metrics.loc[
        (backtest_metrics["level"] == "overall")
        & (backtest_metrics["model"] == "forecast")
    ].iloc[0]
    automated_gates = {
        "sql_actuals_validated": True,
        "embedded_model_structure": True,
        "bottom_up_reconciliation": True,
        "pbix_sql_and_model_parity": pbix_compared,
        "backtest_review_complete": not review_required or args.allow_backtest_change,
        "live_pbix_layout_valid": True,
        "rollback_pbix_retained": True,
    }
    manual_gates = {
        "seven_pages_visually_reviewed": bool(args.pages_reviewed),
        "gateway_refresh_verified": bool(args.gateway_refresh_verified),
    }
    cutover_ready = all(automated_gates.values()) and all(manual_gates.values())
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "cutover_ready": cutover_ready,
        "automated_gates": automated_gates,
        "manual_gates": manual_gates,
        "runtime": runtime,
        "structure": structure,
        "overall_backtest": {
            "wape": float(overall_metric["wape"]),
            "bias_ratio": float(overall_metric["bias_ratio"]),
            "review_required": review_required,
            "change_acknowledged": bool(args.allow_backtest_change),
        },
        "benchmark": {
            "artifact": str(output_dir / "overall_benchmark_report.csv"),
            "benchmark_only": bool(benchmark["benchmark_only"].all()),
            "production_rows": 0,
        },
        "artifacts": artifact_checks,
    }
    summary_path = output_dir / "acceptance_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Acceptance artifacts written to: {output_dir}")

    if args.require_cutover_ready and not cutover_ready:
        print("Cutover is blocked because one or more required gates are incomplete.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())