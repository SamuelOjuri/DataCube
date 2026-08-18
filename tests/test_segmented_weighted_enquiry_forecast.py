from __future__ import annotations

import importlib.util
import runpy
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.services.segmented_weighted_enquiry_forecast import (
    allocate_category_segments,
    allocate_product_segments,
    allocate_projects_to_leaves,
    build_monthly_leaf_actuals,
    forecast_monthly_leaves,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
POWER_BI_SCRIPT = PROJECT_ROOT / "scripts" / "powerbi_segmented_xgb_script.py"
FROZEN_BASELINE = (
    PROJECT_ROOT / "outputs" / "segmented_weighted_enquiry_baseline_frozen"
)


@pytest.fixture(scope="module")
def frozen_monthly_actuals() -> pd.DataFrame:
    monthly = pd.read_csv(FROZEN_BASELINE / "monthly_leaf_actuals.csv")
    monthly["month_start"] = pd.to_datetime(monthly["month_start"])
    return monthly


@pytest.fixture(scope="module")
def embedded_namespace(
    frozen_monthly_actuals: pd.DataFrame,
) -> dict[str, Any]:
    if importlib.util.find_spec("xgboost") is None:
        pytest.skip("xgboost is not installed in this Python environment")
    return runpy.run_path(
        str(POWER_BI_SCRIPT),
        init_globals={"dataset": frozen_monthly_actuals.copy()},
    )


def test_t3_and_rock_products_are_non_combustible() -> None:
    allocation = allocate_product_segments(
        "T3+, ROCKWOOL HardRock Multi-Fix (DD), ROCKDeck"
    )
    assert allocation.shares == {"Non-Combustible": 1.0}
    assert allocation.mapping_status == "mapped"


def test_mixed_products_allocate_by_distinct_canonical_product() -> None:
    allocation = allocate_product_segments(
        "ROCKWOOL HardRock Multi-Fix (DD), ROCKDeck, EPS 150 (SPR)"
    )
    assert allocation.shares["Non-Combustible"] == pytest.approx(2 / 3)
    assert allocation.shares["Combustible"] == pytest.approx(1 / 3)


def test_subitem_values_take_precedence_over_equal_product_split() -> None:
    allocation = allocate_product_segments(
        "ROCKDeck, EPS 150 (SPR)",
        [
            {"product_type": "ROCKDeck", "new_enquiry_value": 70000},
            {"product_type": "EPS 150 (SPR)", "new_enquiry_value": 30000},
        ],
    )
    assert allocation.method == "subitem_value_weighted"
    assert allocation.shares == pytest.approx(
        {"Non-Combustible": 0.7, "Combustible": 0.3}
    )


def test_house_and_apartments_deduplicate_to_one_reporting_segment() -> None:
    allocation = allocate_category_segments("House, Apartments")
    assert allocation.shares == {"Apartments/Housing": 1.0}


def test_multi_product_and_category_project_allocates_to_four_leaves() -> None:
    projects = pd.DataFrame(
        [
            {
                "project_id": "p1",
                "enquiry_month": "2026-07-01",
                "new_enquiry_value": 100000,
                "expected_conversion_rate": 1.0,
                "category_raw": "Education, Datacentre",
                "product_type_raw": "ROCKDeck, EPS 150 (SPR)",
                "subitem_allocations": [],
            }
        ]
    )
    allocations = allocate_projects_to_leaves(projects)
    assert len(allocations) == 4
    assert allocations["allocation_share"].tolist() == pytest.approx([0.25] * 4)
    assert allocations["allocated_weighted_enquiry_value"].sum() == pytest.approx(100000)


def test_monthly_leaf_grid_contains_all_eight_series_and_zero_months() -> None:
    projects = pd.DataFrame(
        [
            {
                "project_id": "p1",
                "enquiry_month": "2026-01-01",
                "new_enquiry_value": 100,
                "expected_conversion_rate": 0.5,
                "category_raw": "Education",
                "product_type_raw": "ROCKDeck",
                "subitem_allocations": [],
            }
        ]
    )
    allocations = allocate_projects_to_leaves(projects)
    monthly = build_monthly_leaf_actuals(allocations, "2026-01-01", "2026-02-01")
    assert len(monthly) == 16
    assert monthly["actual_weighted_enquiry_value"].sum() == pytest.approx(50)


def test_embedded_report_matches_frozen_production_output(
    embedded_namespace: dict[str, Any],
) -> None:
    actual = embedded_namespace["forecast_report"].copy()
    expected = pd.read_csv(FROZEN_BASELINE / "segmented_weighted_enquiry_forecast.csv")
    for frame in (actual, expected):
        frame[["month_start", "history_end"]] = frame[
            ["month_start", "history_end"]
        ].apply(pd.to_datetime)
    expected["model"] = expected["model"].fillna("")
    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
        rtol=0.0,
        atol=1e-9,
    )


def test_embedded_outputs_match_forecast_service(
    embedded_namespace: dict[str, Any],
    frozen_monthly_actuals: pd.DataFrame,
) -> None:
    expected_report, expected_summary = forecast_monthly_leaves(frozen_monthly_actuals)
    actual_report = embedded_namespace["forecast_report"]
    actual_summary = embedded_namespace["segment_model_summary"]

    pd.testing.assert_frame_equal(
        actual_report.reset_index(drop=True),
        expected_report.reset_index(drop=True),
        check_dtype=False,
        rtol=0.0,
        atol=1e-9,
    )
    pd.testing.assert_frame_equal(
        actual_summary.reset_index(drop=True),
        expected_summary.reset_index(drop=True),
        check_dtype=False,
        rtol=0.0,
        atol=1e-9,
    )


def test_embedded_report_contract_and_bottom_up_reconciliation(
    embedded_namespace: dict[str, Any],
) -> None:
    report = embedded_namespace["forecast_report"]
    expected_columns = [
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
    ]
    assert report.columns.tolist() == expected_columns
    assert report["series_type"].value_counts().to_dict() == {
        "Actual": 440,
        "Forecast": 120,
        "Bridge": 8,
    }
    assert report.groupby(["product_segment", "category_segment"]).size().eq(71).all()

    forecasts = report.loc[report["series_type"] == "Forecast"]
    expected_blend = (
        0.75 * forecasts["xgboost_forecast"]
        + 0.25 * forecasts["seasonal_forecast"]
    )
    np.testing.assert_allclose(
        forecasts["forecast_weighted_enquiry_value"],
        expected_blend,
        rtol=0.0,
        atol=1e-9,
    )
    product_overall = (
        forecasts.groupby(["month_start", "product_segment"])[
            "forecast_weighted_enquiry_value"
        ]
        .sum()
        .groupby("month_start")
        .sum()
    )
    category_overall = (
        forecasts.groupby(["month_start", "category_segment"])[
            "forecast_weighted_enquiry_value"
        ]
        .sum()
        .groupby("month_start")
        .sum()
    )
    leaf_overall = forecasts.groupby("month_start")[
        "forecast_weighted_enquiry_value"
    ].sum()
    pd.testing.assert_series_equal(product_overall, leaf_overall)
    pd.testing.assert_series_equal(category_overall, leaf_overall)


def test_sparse_fallback_and_bridge_contract(
    embedded_namespace: dict[str, Any],
) -> None:
    report = embedded_namespace["forecast_report"]
    summary = embedded_namespace["segment_model_summary"]
    sparse = summary.loc[
        (summary["product_segment"] == "Combustible")
        & (summary["category_segment"] == "Data Centres")
    ].iloc[0]
    assert sparse["model"] == "seasonal_average_fallback_sparse"
    assert sparse["fallback_reason"] == "nonzero_months=5"

    sparse_forecast = report.loc[
        (report["product_segment"] == "Combustible")
        & (report["category_segment"] == "Data Centres")
        & (report["series_type"] == "Forecast")
    ]
    np.testing.assert_allclose(
        sparse_forecast["xgboost_forecast"],
        sparse_forecast["seasonal_forecast"],
    )

    actual_last = (
        report.loc[report["series_type"] == "Actual"]
        .sort_values("month_start")
        .groupby(["product_segment", "category_segment"], as_index=False)
        .tail(1)
        .set_index(["product_segment", "category_segment"])
    )
    bridges = report.loc[report["series_type"] == "Bridge"].set_index(
        ["product_segment", "category_segment"]
    )
    pd.testing.assert_series_equal(
        bridges["forecast_weighted_enquiry_value"].sort_index(),
        actual_last["actual_weighted_enquiry_value"].sort_index(),
        check_names=False,
    )


def test_overall_benchmark_is_isolated_from_production(
    embedded_namespace: dict[str, Any],
) -> None:
    report = embedded_namespace["forecast_report"]
    benchmark = embedded_namespace["overall_benchmark_report"]
    assert "benchmark_only" not in report.columns
    assert "product_segment" not in benchmark.columns
    assert "category_segment" not in benchmark.columns
    assert benchmark["benchmark_only"].eq(True).all()
    assert benchmark["series_type"].value_counts().to_dict() == {
        "Actual": 55,
        "Forecast": 15,
        "Bridge": 1,
    }

    bottom_up = (
        report.loc[report["series_type"] == "Forecast"]
        .groupby("month_start")["forecast_weighted_enquiry_value"]
        .sum()
    )
    direct = benchmark.loc[benchmark["series_type"] == "Forecast"].set_index(
        "month_start"
    )["forecast_weighted_enquiry_value"]
    assert not np.allclose(bottom_up, direct, rtol=0.0, atol=0.01)


def test_feature_matrix_order_and_no_target_leakage(
    embedded_namespace: dict[str, Any],
) -> None:
    build_features = embedded_namespace["_build_xgb_feature_matrix"]
    index = pd.date_range("2024-01-01", periods=24, freq="MS")
    series = pd.Series(np.arange(1.0, 25.0), index=index)
    features, target = build_features(
        series,
        lags=(1, 2, 3, 6, 9, 12),
        rolling_windows=(3, 6),
        add_month_features=True,
    )
    assert features.columns.tolist() == [
        "lag_1",
        "lag_2",
        "lag_3",
        "lag_6",
        "lag_9",
        "lag_12",
        "roll_mean_3",
        "roll_std_3",
        "roll_mean_6",
        "roll_std_6",
        "month_sin",
        "month_cos",
    ]
    first_timestamp = index[12]
    first_row = features.loc[first_timestamp]
    assert first_row["lag_1"] == pytest.approx(12.0)
    assert first_row["lag_12"] == pytest.approx(1.0)
    assert first_row["roll_mean_3"] == pytest.approx(np.mean([10.0, 11.0, 12.0]))
    assert first_row["roll_std_3"] == pytest.approx(np.std([10.0, 11.0, 12.0]))
    assert first_row["month_sin"] == pytest.approx(
        np.sin(2 * np.pi * first_timestamp.month / 12.0)
    )
    assert first_row["month_cos"] == pytest.approx(
        np.cos(2 * np.pi * first_timestamp.month / 12.0)
    )
    assert target.loc[first_timestamp] == pytest.approx(13.0)

    changed_target = series.copy()
    changed_target.loc[first_timestamp] = 999.0
    changed_features, _ = build_features(
        changed_target,
        lags=(1, 2, 3, 6, 9, 12),
        rolling_windows=(3, 6),
        add_month_features=True,
    )
    pd.testing.assert_series_equal(
        first_row,
        changed_features.loc[first_timestamp],
    )


def test_recursive_predictions_feed_later_feature_rows(
    embedded_namespace: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, float]] = []

    class RecordingRegressor:
        def __init__(self, **_: Any) -> None:
            pass

        def fit(self, _features: pd.DataFrame, _target: pd.Series) -> None:
            return None

        def predict(self, frame: pd.DataFrame) -> np.ndarray:
            calls.append(frame.iloc[0].to_dict())
            return np.asarray([100.0 + len(calls) - 1])

    forecast_function = embedded_namespace["_xgboost_forecast"]
    monkeypatch.setitem(
        forecast_function.__globals__,
        "XGBRegressor",
        RecordingRegressor,
    )
    train_index = pd.date_range("2024-01-01", periods=13, freq="MS")
    train = pd.Series(np.arange(1.0, 14.0), index=train_index)
    future = pd.date_range("2025-02-01", periods=2, freq="MS")
    config = dict(embedded_namespace["WEIGHTED_VALUE_XGB_CONFIG"])
    config.update(
        {
            "lags": (1,),
            "rolling_windows": (),
            "add_month_features": False,
            "use_log": False,
        }
    )
    predictions = forecast_function(train, future, config)
    np.testing.assert_allclose(predictions, [100.0, 101.0])
    assert calls[0]["lag_1"] == pytest.approx(13.0)
    assert calls[1]["lag_1"] == pytest.approx(100.0)


def test_log_forecast_is_reversed_to_original_scale(
    embedded_namespace: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ConstantLogRegressor:
        def __init__(self, **_: Any) -> None:
            pass

        def fit(self, _features: pd.DataFrame, _target: pd.Series) -> None:
            return None

        def predict(self, _frame: pd.DataFrame) -> np.ndarray:
            return np.asarray([np.log1p(250.0)])

    forecast_function = embedded_namespace["_xgboost_forecast"]
    monkeypatch.setitem(
        forecast_function.__globals__,
        "XGBRegressor",
        ConstantLogRegressor,
    )
    train_index = pd.date_range("2024-01-01", periods=13, freq="MS")
    train = pd.Series(np.arange(1.0, 14.0), index=train_index)
    future = pd.date_range("2025-02-01", periods=1, freq="MS")
    config = dict(embedded_namespace["WEIGHTED_VALUE_XGB_CONFIG"])
    config.update(
        {
            "lags": (1,),
            "rolling_windows": (),
            "add_month_features": False,
            "use_log": True,
        }
    )
    prediction = forecast_function(train, future, config)
    assert prediction.iloc[0] == pytest.approx(250.0)


def test_dense_forecast_is_deterministic(
    embedded_namespace: dict[str, Any],
    frozen_monthly_actuals: pd.DataFrame,
) -> None:
    leaf = frozen_monthly_actuals.loc[
        (frozen_monthly_actuals["product_segment"] == "Combustible")
        & (frozen_monthly_actuals["category_segment"] == "Education")
    ]
    train = leaf.set_index("month_start")["actual_weighted_enquiry_value"]
    future = pd.date_range(train.index.max() + pd.offsets.MonthBegin(1), periods=3, freq="MS")
    forecast_series = embedded_namespace["_forecast_series"]
    first = forecast_series(train, future)
    second = forecast_series(train, future)
    for first_value, second_value in zip(first[:3], second[:3]):
        np.testing.assert_array_equal(first_value.to_numpy(), second_value.to_numpy())
    assert first[3:] == second[3:]


def test_embedded_input_rejects_duplicate_or_incomplete_grids(
    embedded_namespace: dict[str, Any],
    frozen_monthly_actuals: pd.DataFrame,
) -> None:
    prepare = embedded_namespace["_prepare_monthly_actuals"]
    duplicate = pd.concat(
        [frozen_monthly_actuals, frozen_monthly_actuals.iloc[[0]]],
        ignore_index=True,
    )
    with pytest.raises(ValueError, match="duplicate leaf-month"):
        prepare(duplicate)

    incomplete = frozen_monthly_actuals.iloc[1:].copy()
    with pytest.raises(ValueError, match="exactly eight leaves"):
        prepare(incomplete)
