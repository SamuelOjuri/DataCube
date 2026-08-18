"""Bottom-up weighted-enquiry forecasts by product and category segment.

The module is intentionally independent of Power BI and database clients.  It
accepts a project-level pandas DataFrame, allocates each project's weighted
enquiry value across the eight Product x Category leaves, and forecasts each
leaf independently.  Product/category rollups therefore reconcile to the same
bottom-up overall total.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd

from src.core.normalization import compute_product_key, normalize_category_value, normalize_text_key


PRODUCT_SEGMENTS = ("Non-Combustible", "Combustible")
CATEGORY_SEGMENTS = ("Data Centres", "Education", "Apartments/Housing", "Other")
NON_COMBUSTIBLE_PRODUCT_KEYS = frozenset({"hardrock", "rockdeck", "t3_system"})

XGB_WEIGHT = 0.75
SEASONAL_WEIGHT = 0.25
DEFAULT_FORECAST_HORIZON_MONTHS = 15
MIN_NONZERO_MONTHS_FOR_XGB = 12

WEIGHTED_VALUE_XGB_CONFIG = {
    "lags": (1, 2, 3, 6, 9, 12),
    "rolling_windows": (3, 6),
    "max_depth": 1,
    "n_estimators": 700,
    "learning_rate": 0.02,
    "subsample": 0.9,
    "colsample_bytree": 0.85,
    "min_child_weight": 6.0,
    "reg_lambda": 6.0,
    "reg_alpha": 0.25,
    "add_month_features": True,
    "use_log": True,
}

_MULTI_VALUE_SPLIT = re.compile(r"\s*,\s*")


@dataclass(frozen=True)
class AllocationResult:
    shares: dict[str, float]
    method: str
    mapping_status: str
    source_value_total: float = 0.0


def _clean_number(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    if not np.isfinite(numeric):
        return default
    return numeric


def _split_values(raw_value: Any) -> list[str]:
    if raw_value is None or (isinstance(raw_value, float) and np.isnan(raw_value)):
        return []
    return [part.strip() for part in _MULTI_VALUE_SPLIT.split(str(raw_value)) if part.strip()]


def _mapping_status(statuses: Iterable[str]) -> str:
    status_set = set(statuses)
    if not status_set or status_set == {"missing"}:
        return "missing"
    if "unmapped" in status_set:
        return "contains_unmapped"
    return "mapped"


def _canonical_products(raw_value: Any) -> list[tuple[str, str, str]]:
    """Return unique (identity, reporting segment, mapping status) tuples."""
    products: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    for raw_token in _split_values(raw_value):
        canonical = compute_product_key(raw_token)
        if canonical and canonical != "unknown":
            identity = canonical
            status = "mapped"
        else:
            normalized_raw = normalize_text_key(raw_token)
            identity = f"unmapped:{normalized_raw}" if normalized_raw else "missing"
            status = "unmapped" if normalized_raw else "missing"

        if identity in seen:
            continue
        seen.add(identity)
        segment = (
            "Non-Combustible"
            if canonical in NON_COMBUSTIBLE_PRODUCT_KEYS
            else "Combustible"
        )
        products.append((identity, segment, status))

    if not products:
        products.append(("missing", "Combustible", "missing"))
    return products


def _canonical_category_segments(raw_value: Any) -> list[tuple[str, str]]:
    """Return unique final reporting segments and their mapping statuses."""
    segments: list[tuple[str, str]] = []
    seen: set[str] = set()

    for raw_token in _split_values(raw_value):
        canonical = normalize_category_value(raw_token)
        normalized = normalize_text_key(canonical)

        if normalized in {"datacentre", "data centre", "data center"}:
            segment, status = "Data Centres", "mapped"
        elif normalized == "education":
            segment, status = "Education", "mapped"
        elif normalized in {"house", "apartment", "apartments"}:
            segment, status = "Apartments/Housing", "mapped"
        elif canonical and normalize_text_key(canonical) != normalize_text_key(raw_token):
            segment, status = "Other", "mapped"
        elif canonical in {
            "Commercial",
            "Health",
            "Commodity",
            "Industrial",
            "Leisure",
            "Military",
            "Mixed Use",
            "Student Accommodation",
            "Consultancy",
        }:
            segment, status = "Other", "mapped"
        else:
            segment, status = "Other", "unmapped"

        if segment not in seen:
            seen.add(segment)
            segments.append((segment, status))

    if not segments:
        segments.append(("Other", "missing"))
    return segments


def _coerce_subitems(value: Any) -> list[dict[str, Any]]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return value if isinstance(value, list) else []


def allocate_product_segments(
    raw_product_type: Any,
    subitem_allocations: Any = None,
) -> AllocationResult:
    """Allocate a project across product segments using the best available data."""
    product_values: dict[str, float] = {}
    identity_metadata: dict[str, tuple[str, str]] = {}

    for subitem in _coerce_subitems(subitem_allocations):
        value = max(_clean_number(subitem.get("new_enquiry_value")), 0.0)
        if value <= 0:
            continue
        products = _canonical_products(subitem.get("product_type"))
        usable_products = [product for product in products if product[0] != "missing"]
        if not usable_products:
            continue
        share = value / len(usable_products)
        for identity, segment, status in usable_products:
            product_values[identity] = product_values.get(identity, 0.0) + share
            identity_metadata[identity] = (segment, status)

    source_value_total = sum(product_values.values())
    if source_value_total > 0:
        segment_values = {segment: 0.0 for segment in PRODUCT_SEGMENTS}
        statuses: list[str] = []
        for identity, value in product_values.items():
            segment, status = identity_metadata[identity]
            segment_values[segment] += value
            statuses.append(status)
        shares = {
            segment: value / source_value_total
            for segment, value in segment_values.items()
            if value > 0
        }
        return AllocationResult(
            shares=shares,
            method="subitem_value_weighted",
            mapping_status=_mapping_status(statuses),
            source_value_total=source_value_total,
        )

    products = _canonical_products(raw_product_type)
    segment_counts = {segment: 0 for segment in PRODUCT_SEGMENTS}
    statuses = []
    for _, segment, status in products:
        segment_counts[segment] += 1
        statuses.append(status)
    total_products = sum(segment_counts.values())
    shares = {
        segment: count / total_products
        for segment, count in segment_counts.items()
        if count > 0
    }
    return AllocationResult(
        shares=shares,
        method="equal_product_split" if total_products > 1 else "default_mapping",
        mapping_status=_mapping_status(statuses),
    )


def allocate_category_segments(raw_category: Any) -> AllocationResult:
    segments = _canonical_category_segments(raw_category)
    share = 1.0 / len(segments)
    return AllocationResult(
        shares={segment: share for segment, _ in segments},
        method="equal_category_segment_split" if len(segments) > 1 else "default_mapping",
        mapping_status=_mapping_status(status for _, status in segments),
    )


def allocate_projects_to_leaves(projects: pd.DataFrame) -> pd.DataFrame:
    required_columns = {
        "project_id",
        "enquiry_month",
        "new_enquiry_value",
        "expected_conversion_rate",
        "category_raw",
        "product_type_raw",
    }
    missing = sorted(required_columns.difference(projects.columns))
    if missing:
        raise ValueError(f"Project extract is missing required columns: {missing}")

    allocation_rows: list[dict[str, Any]] = []
    for row in projects.to_dict(orient="records"):
        gross_value = max(_clean_number(row.get("new_enquiry_value")), 0.0)
        conversion_rate = min(max(_clean_number(row.get("expected_conversion_rate")), 0.0), 1.0)
        weighted_value = gross_value * conversion_rate

        product_allocation = allocate_product_segments(
            row.get("product_type_raw"),
            row.get("subitem_allocations"),
        )
        category_allocation = allocate_category_segments(row.get("category_raw"))

        source_ratio = (
            product_allocation.source_value_total / gross_value
            if gross_value > 0 and product_allocation.source_value_total > 0
            else np.nan
        )
        for product_segment, product_share in product_allocation.shares.items():
            for category_segment, category_share in category_allocation.shares.items():
                allocation_share = product_share * category_share
                allocation_rows.append(
                    {
                        "project_id": str(row["project_id"]),
                        "enquiry_month": pd.Timestamp(row["enquiry_month"]).to_period("M").to_timestamp(),
                        "product_segment": product_segment,
                        "category_segment": category_segment,
                        "allocation_share": allocation_share,
                        "project_weighted_enquiry_value": weighted_value,
                        "allocated_weighted_enquiry_value": weighted_value * allocation_share,
                        "product_allocation_method": product_allocation.method,
                        "category_allocation_method": category_allocation.method,
                        "product_mapping_status": product_allocation.mapping_status,
                        "category_mapping_status": category_allocation.mapping_status,
                        "subitem_source_value_total": product_allocation.source_value_total,
                        "subitem_to_project_value_ratio": source_ratio,
                    }
                )

    allocations = pd.DataFrame(allocation_rows)
    if allocations.empty:
        raise ValueError("No project allocation rows were produced.")

    by_project = allocations.groupby("project_id", as_index=False).agg(
        project_weighted_enquiry_value=("project_weighted_enquiry_value", "first"),
        allocated_weighted_enquiry_value=("allocated_weighted_enquiry_value", "sum"),
        allocation_share=("allocation_share", "sum"),
    )
    if not np.allclose(by_project["allocation_share"], 1.0, rtol=0.0, atol=1e-9):
        raise ValueError("One or more project allocation shares do not sum to 1.0.")
    if not np.allclose(
        by_project["project_weighted_enquiry_value"],
        by_project["allocated_weighted_enquiry_value"],
        rtol=1e-9,
        atol=0.01,
    ):
        raise ValueError("Allocated project values do not reconcile to source weighted values.")
    return allocations


def build_monthly_leaf_actuals(
    allocations: pd.DataFrame,
    history_start: str | pd.Timestamp,
    history_end: str | pd.Timestamp,
) -> pd.DataFrame:
    start = pd.Timestamp(history_start).to_period("M").to_timestamp()
    end = pd.Timestamp(history_end).to_period("M").to_timestamp()
    if end < start:
        raise ValueError("history_end must not be before history_start.")

    months = pd.date_range(start=start, end=end, freq="MS")
    full_index = pd.MultiIndex.from_product(
        [PRODUCT_SEGMENTS, CATEGORY_SEGMENTS, months],
        names=["product_segment", "category_segment", "month_start"],
    )
    monthly = (
        allocations.groupby(
            ["product_segment", "category_segment", "enquiry_month"],
            as_index=False,
        )["allocated_weighted_enquiry_value"]
        .sum()
        .rename(
            columns={
                "enquiry_month": "month_start",
                "allocated_weighted_enquiry_value": "actual_weighted_enquiry_value",
            }
        )
        .set_index(["product_segment", "category_segment", "month_start"])
        .reindex(full_index, fill_value=0.0)
        .reset_index()
    )
    return monthly


def _non_negative(series: pd.Series) -> pd.Series:
    return pd.Series(
        np.clip(np.asarray(series, dtype=float), 0.0, None),
        index=series.index,
        dtype=float,
    )


def _seasonal_average_forecast(
    train: pd.Series,
    future_index: pd.DatetimeIndex,
    lookback: int = 24,
) -> pd.Series:
    working = train.astype(float).iloc[-lookback:] if lookback else train.astype(float)
    month_means = working.groupby(working.index.month).mean()
    global_mean = float(working.mean()) if len(working) else 0.0
    values = [month_means.get(timestamp.month, global_mean) for timestamp in future_index]
    return _non_negative(pd.Series(values, index=future_index, dtype=float))


def _build_xgb_feature_matrix(
    series: pd.Series,
    lags: Iterable[int],
    rolling_windows: Iterable[int],
    add_month_features: bool,
) -> tuple[pd.DataFrame, pd.Series]:
    frame = pd.DataFrame(index=series.index)
    for lag in sorted({int(value) for value in lags}):
        frame[f"lag_{lag}"] = series.shift(lag)
    shifted = series.shift(1)
    for window in sorted({int(value) for value in rolling_windows}):
        frame[f"roll_mean_{window}"] = shifted.rolling(window).mean()
        frame[f"roll_std_{window}"] = shifted.rolling(window).std(ddof=0).fillna(0.0)
    if add_month_features:
        frame["month_sin"] = np.sin(2 * np.pi * series.index.month / 12.0)
        frame["month_cos"] = np.cos(2 * np.pi * series.index.month / 12.0)
    valid = frame.notna().all(axis=1)
    return frame.loc[valid], series.loc[valid]


def _build_xgb_feature_row(
    history: pd.Series,
    timestamp: pd.Timestamp,
    lags: Iterable[int],
    rolling_windows: Iterable[int],
    add_month_features: bool,
) -> dict[str, float]:
    row: dict[str, float] = {}
    for lag in sorted({int(value) for value in lags}):
        row[f"lag_{lag}"] = float(history.loc[timestamp - pd.DateOffset(months=lag)])
    for window in sorted({int(value) for value in rolling_windows}):
        values = [
            float(history.loc[timestamp - pd.DateOffset(months=step)])
            for step in range(1, window + 1)
        ]
        row[f"roll_mean_{window}"] = float(np.mean(values))
        row[f"roll_std_{window}"] = float(np.std(values, ddof=0))
    if add_month_features:
        row["month_sin"] = float(np.sin(2 * np.pi * timestamp.month / 12.0))
        row["month_cos"] = float(np.cos(2 * np.pi * timestamp.month / 12.0))
    return row


def _xgboost_forecast(
    train: pd.Series,
    future_index: pd.DatetimeIndex,
    config: dict[str, Any],
) -> pd.Series:
    try:
        from xgboost import XGBRegressor
    except ImportError as exc:
        raise RuntimeError("xgboost is required to build the segmented forecasts") from exc

    lags = tuple(sorted({int(value) for value in config["lags"]}))
    working = pd.Series(
        np.log1p(np.clip(train.values, 0.0, None)),
        index=train.index,
    ) if config["use_log"] else train.astype(float)
    features, target = _build_xgb_feature_matrix(
        working,
        lags=lags,
        rolling_windows=config["rolling_windows"],
        add_month_features=config["add_month_features"],
    )
    if len(features) < 12:
        raise ValueError("Insufficient feature rows for XGBoost.")

    model = XGBRegressor(
        objective="reg:squarederror",
        max_depth=config["max_depth"],
        n_estimators=config["n_estimators"],
        learning_rate=config["learning_rate"],
        subsample=config["subsample"],
        colsample_bytree=config["colsample_bytree"],
        min_child_weight=config["min_child_weight"],
        reg_lambda=config["reg_lambda"],
        reg_alpha=config["reg_alpha"],
        random_state=42,
        n_jobs=1,
    )
    model.fit(features, target)

    history = working.copy()
    predictions: list[float] = []
    feature_columns = features.columns.tolist()
    for timestamp in future_index:
        row = _build_xgb_feature_row(
            history,
            timestamp,
            lags=lags,
            rolling_windows=config["rolling_windows"],
            add_month_features=config["add_month_features"],
        )
        step_frame = pd.DataFrame([row], index=[timestamp]).reindex(
            columns=feature_columns,
            fill_value=0.0,
        )
        prediction = float(model.predict(step_frame)[0])
        history.loc[timestamp] = prediction
        predictions.append(prediction)
    values = np.expm1(np.asarray(predictions)) if config["use_log"] else np.asarray(predictions)
    return _non_negative(pd.Series(values, index=future_index))


def _sklearn_gradient_boosting_forecast(
    train: pd.Series,
    future_index: pd.DatetimeIndex,
    config: dict[str, Any],
) -> pd.Series:
    """Local fallback when the Power BI XGBoost dependency is unavailable."""
    from sklearn.ensemble import GradientBoostingRegressor

    lags = tuple(sorted({int(value) for value in config["lags"]}))
    working = pd.Series(
        np.log1p(np.clip(train.values, 0.0, None)),
        index=train.index,
    ) if config["use_log"] else train.astype(float)
    features, target = _build_xgb_feature_matrix(
        working,
        lags=lags,
        rolling_windows=config["rolling_windows"],
        add_month_features=config["add_month_features"],
    )
    if len(features) < 12:
        raise ValueError("Insufficient feature rows for gradient boosting.")

    model = GradientBoostingRegressor(
        loss="squared_error",
        n_estimators=config["n_estimators"],
        learning_rate=config["learning_rate"],
        max_depth=config["max_depth"],
        min_samples_leaf=max(1, int(config["min_child_weight"])),
        subsample=config["subsample"],
        max_features=config["colsample_bytree"],
        random_state=42,
    )
    model.fit(features, target)

    history = working.copy()
    predictions: list[float] = []
    feature_columns = features.columns.tolist()
    for timestamp in future_index:
        row = _build_xgb_feature_row(
            history,
            timestamp,
            lags=lags,
            rolling_windows=config["rolling_windows"],
            add_month_features=config["add_month_features"],
        )
        step_frame = pd.DataFrame([row], index=[timestamp]).reindex(
            columns=feature_columns,
            fill_value=0.0,
        )
        prediction = float(model.predict(step_frame)[0])
        history.loc[timestamp] = prediction
        predictions.append(prediction)
    values = np.expm1(np.asarray(predictions)) if config["use_log"] else np.asarray(predictions)
    return _non_negative(pd.Series(values, index=future_index))


def forecast_series(
    train: pd.Series,
    future_index: pd.DatetimeIndex,
) -> tuple[pd.Series, pd.Series, pd.Series, str, str]:
    """Forecast one monthly series and return blend, components, and audit metadata."""
    seasonal = _seasonal_average_forecast(train, future_index)
    nonzero_months = int((train > 0).sum())
    if nonzero_months < MIN_NONZERO_MONTHS_FOR_XGB:
        boosted = seasonal.copy()
        model_used = "seasonal_average_fallback_sparse"
        fallback_reason = f"nonzero_months={nonzero_months}"
    else:
        try:
            boosted = _xgboost_forecast(train, future_index, WEIGHTED_VALUE_XGB_CONFIG)
            model_used = "xgboost_tuned_75pct_seasonal_25pct"
            fallback_reason = ""
        except RuntimeError as exc:
            boosted = _sklearn_gradient_boosting_forecast(
                train,
                future_index,
                WEIGHTED_VALUE_XGB_CONFIG,
            )
            model_used = "sklearn_gradient_boosting_75pct_seasonal_25pct"
            fallback_reason = str(exc)
        except ValueError as exc:
            boosted = seasonal.copy()
            model_used = "seasonal_average_fallback_features"
            fallback_reason = str(exc)
    forecast = _non_negative((XGB_WEIGHT * boosted) + (SEASONAL_WEIGHT * seasonal))
    return forecast, boosted, seasonal, model_used, fallback_reason


def forecast_monthly_leaves(
    monthly_actuals: pd.DataFrame,
    forecast_horizon_months: int = DEFAULT_FORECAST_HORIZON_MONTHS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if forecast_horizon_months <= 0:
        raise ValueError("forecast_horizon_months must be positive.")

    report_parts: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    for product_segment in PRODUCT_SEGMENTS:
        for category_segment in CATEGORY_SEGMENTS:
            leaf = monthly_actuals.loc[
                (monthly_actuals["product_segment"] == product_segment)
                & (monthly_actuals["category_segment"] == category_segment)
            ].sort_values("month_start")
            train = leaf.set_index("month_start")["actual_weighted_enquiry_value"].astype(float)
            future_index = pd.date_range(
                start=train.index.max() + pd.offsets.MonthBegin(1),
                periods=forecast_horizon_months,
                freq="MS",
            )
            nonzero_months = int((train > 0).sum())
            forecast, xgb, seasonal, model_used, fallback_reason = forecast_series(
                train,
                future_index,
            )
            actual_rows = leaf.assign(
                forecast_weighted_enquiry_value=np.nan,
                xgboost_forecast=np.nan,
                seasonal_forecast=np.nan,
                series_type="Actual",
                model="",
            )
            bridge_rows = pd.DataFrame(
                {
                    "product_segment": [product_segment],
                    "category_segment": [category_segment],
                    "month_start": [train.index.max()],
                    "actual_weighted_enquiry_value": [np.nan],
                    "forecast_weighted_enquiry_value": [float(train.iloc[-1])],
                    "xgboost_forecast": [float(train.iloc[-1])],
                    "seasonal_forecast": [float(train.iloc[-1])],
                    "series_type": ["Bridge"],
                    "model": [model_used],
                }
            )
            forecast_rows = pd.DataFrame(
                {
                    "product_segment": product_segment,
                    "category_segment": category_segment,
                    "month_start": future_index,
                    "actual_weighted_enquiry_value": np.nan,
                    "forecast_weighted_enquiry_value": forecast.values,
                    "xgboost_forecast": xgb.values,
                    "seasonal_forecast": seasonal.values,
                    "series_type": "Forecast",
                    "model": model_used,
                }
            )
            report_parts.extend([actual_rows, bridge_rows, forecast_rows])
            summaries.append(
                {
                    "product_segment": product_segment,
                    "category_segment": category_segment,
                    "model": model_used,
                    "fallback_reason": fallback_reason,
                    "history_start": train.index.min(),
                    "history_end": train.index.max(),
                    "history_months": len(train),
                    "nonzero_months": nonzero_months,
                    "history_total": float(train.sum()),
                    "forecast_horizon_months": forecast_horizon_months,
                    "forecast_start": future_index.min(),
                    "forecast_end": future_index.max(),
                    "xgb_weight": XGB_WEIGHT,
                    "seasonal_weight": SEASONAL_WEIGHT,
                }
            )

    report = pd.concat(report_parts, ignore_index=True).sort_values(
        ["product_segment", "category_segment", "month_start", "series_type"]
    )
    report["forecast_horizon_months"] = forecast_horizon_months
    report["history_end"] = monthly_actuals["month_start"].max()
    return report.reset_index(drop=True), pd.DataFrame(summaries)


def backtest_monthly_leaves(
    monthly_actuals: pd.DataFrame,
    holdout_months: int = 12,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run rolling one-month-ahead backtests and return predictions plus WAPE/bias."""
    all_months = sorted(pd.to_datetime(monthly_actuals["month_start"]).unique())
    if holdout_months <= 0 or holdout_months >= len(all_months):
        raise ValueError("holdout_months must be positive and shorter than history.")
    target_months = pd.DatetimeIndex(all_months[-holdout_months:])
    prediction_rows: list[dict[str, Any]] = []

    for target_month in target_months:
        for product_segment in PRODUCT_SEGMENTS:
            for category_segment in CATEGORY_SEGMENTS:
                leaf = monthly_actuals.loc[
                    (monthly_actuals["product_segment"] == product_segment)
                    & (monthly_actuals["category_segment"] == category_segment)
                ].sort_values("month_start")
                train = leaf.loc[
                    pd.to_datetime(leaf["month_start"]) < target_month
                ].set_index("month_start")["actual_weighted_enquiry_value"].astype(float)
                actual = float(
                    leaf.loc[
                        pd.to_datetime(leaf["month_start"]) == target_month,
                        "actual_weighted_enquiry_value",
                    ].iloc[0]
                )
                forecast, _, seasonal, model_used, fallback_reason = forecast_series(
                    train,
                    pd.DatetimeIndex([target_month]),
                )
                prediction_rows.append(
                    {
                        "month_start": target_month,
                        "product_segment": product_segment,
                        "category_segment": category_segment,
                        "actual": actual,
                        "forecast": float(forecast.iloc[0]),
                        "seasonal_naive": float(seasonal.iloc[0]),
                        "model": model_used,
                        "fallback_reason": fallback_reason,
                    }
                )

    predictions = pd.DataFrame(prediction_rows)
    metric_rows: list[dict[str, Any]] = []

    def append_metrics(level: str, segment: str, rows: pd.DataFrame) -> None:
        actual_total = float(rows["actual"].sum())
        for model_column in ("forecast", "seasonal_naive"):
            error = rows[model_column] - rows["actual"]
            metric_rows.append(
                {
                    "level": level,
                    "segment": segment,
                    "model": model_column,
                    "months": int(rows["month_start"].nunique()),
                    "actual_total": actual_total,
                    "predicted_total": float(rows[model_column].sum()),
                    "wape": float(error.abs().sum() / actual_total) if actual_total else np.nan,
                    "bias_ratio": float(error.sum() / actual_total) if actual_total else np.nan,
                }
            )

    monthly_overall = predictions.groupby("month_start", as_index=False)[
        ["actual", "forecast", "seasonal_naive"]
    ].sum()
    append_metrics("overall", "Overall", monthly_overall)
    for segment, rows in predictions.groupby("product_segment"):
        monthly = rows.groupby("month_start", as_index=False)[
            ["actual", "forecast", "seasonal_naive"]
        ].sum()
        append_metrics("product", str(segment), monthly)
    for segment, rows in predictions.groupby("category_segment"):
        monthly = rows.groupby("month_start", as_index=False)[
            ["actual", "forecast", "seasonal_naive"]
        ].sum()
        append_metrics("category", str(segment), monthly)
    for (product, category), rows in predictions.groupby(
        ["product_segment", "category_segment"]
    ):
        append_metrics("leaf", f"{product} | {category}", rows)

    return predictions, pd.DataFrame(metric_rows)


def build_quality_summary(
    projects: pd.DataFrame,
    allocations: pd.DataFrame,
    report: pd.DataFrame,
) -> dict[str, Any]:
    source_weighted = (
        pd.to_numeric(projects["new_enquiry_value"], errors="coerce").fillna(0.0).clip(lower=0.0)
        * pd.to_numeric(projects["expected_conversion_rate"], errors="coerce")
        .fillna(0.0)
        .clip(lower=0.0, upper=1.0)
    )
    allocated_total = float(allocations["allocated_weighted_enquiry_value"].sum())
    actual_report_total = float(
        report.loc[report["series_type"] == "Actual", "actual_weighted_enquiry_value"].sum()
    )
    future = report.loc[report["series_type"] == "Forecast"]
    product_monthly = future.groupby(["product_segment", "month_start"])[
        "forecast_weighted_enquiry_value"
    ].sum()
    category_monthly = future.groupby(["category_segment", "month_start"])[
        "forecast_weighted_enquiry_value"
    ].sum()
    overall_from_product = product_monthly.groupby("month_start").sum()
    overall_from_category = category_monthly.groupby("month_start").sum()

    project_methods = allocations.groupby("project_id", as_index=False).agg(
        product_allocation_method=("product_allocation_method", "first"),
        product_mapping_status=("product_mapping_status", "first"),
        category_mapping_status=("category_mapping_status", "first"),
        product_segments=("product_segment", "nunique"),
        category_segments=("category_segment", "nunique"),
        subitem_to_project_value_ratio=("subitem_to_project_value_ratio", "first"),
    )
    direct_ratios = project_methods.loc[
        project_methods["product_allocation_method"] == "subitem_value_weighted",
        "subitem_to_project_value_ratio",
    ].dropna()
    return {
        "project_count": int(projects["project_id"].nunique()),
        "history_start": str(pd.Timestamp(projects["enquiry_month"].min()).date()),
        "history_end": str(pd.Timestamp(projects["enquiry_month"].max()).date()),
        "source_weighted_enquiry_value": round(float(source_weighted.sum()), 2),
        "allocated_weighted_enquiry_value": round(allocated_total, 2),
        "actual_report_weighted_enquiry_value": round(actual_report_total, 2),
        "source_to_allocation_delta": round(float(source_weighted.sum()) - allocated_total, 6),
        "allocation_to_report_delta": round(allocated_total - actual_report_total, 6),
        "forecast_hierarchy_max_delta": round(
            float((overall_from_product - overall_from_category).abs().max()), 9
        ),
        "projects_using_subitem_value_allocation": int(
            (project_methods["product_allocation_method"] == "subitem_value_weighted").sum()
        ),
        "subitem_value_ratio_outside_0_8_to_1_2": int(
            ((direct_ratios < 0.8) | (direct_ratios > 1.2)).sum()
        ),
        "projects_spanning_product_segments": int((project_methods["product_segments"] > 1).sum()),
        "projects_spanning_category_segments": int((project_methods["category_segments"] > 1).sum()),
        "projects_with_missing_or_unmapped_product": int(
            (project_methods["product_mapping_status"] != "mapped").sum()
        ),
        "projects_with_missing_or_unmapped_category": int(
            (project_methods["category_mapping_status"] != "mapped").sum()
        ),
    }
