# Power BI injects the input query as a pandas DataFrame named `dataset`.

from __future__ import annotations

from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from xgboost import XGBRegressor
except ImportError as exc:
    raise RuntimeError(
        "Power BI Python requires xgboost 3.2.0 for segmented forecasting."
    ) from exc


PRODUCT_SEGMENTS = ("Non-Combustible", "Combustible")
CATEGORY_SEGMENTS = ("Data Centres", "Education", "Apartments/Housing", "Other")
XGB_WEIGHT = 0.75
SEASONAL_WEIGHT = 0.25
FORECAST_HORIZON_MONTHS = 15
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

INPUT_COLUMNS = (
    "month_start",
    "product_segment",
    "category_segment",
    "actual_weighted_enquiry_value",
)


def _prepare_monthly_actuals(raw_dataset: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(raw_dataset, pd.DataFrame):
        raise TypeError("Power BI must provide 'dataset' as a pandas DataFrame.")

    monthly = raw_dataset.copy()
    monthly.columns = [str(column).strip().lower() for column in monthly.columns]
    if monthly.columns.duplicated().any():
        duplicates = monthly.columns[monthly.columns.duplicated()].tolist()
        raise ValueError(f"Input contains duplicate columns: {duplicates}")

    missing = sorted(set(INPUT_COLUMNS).difference(monthly.columns))
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")
    monthly = monthly.loc[:, INPUT_COLUMNS].copy()

    parsed_months = pd.to_datetime(monthly["month_start"], errors="coerce")
    values = pd.to_numeric(
        monthly["actual_weighted_enquiry_value"], errors="coerce"
    )
    if parsed_months.isna().any():
        invalid_months = monthly.loc[parsed_months.isna(), "month_start"]
        invalid_samples = [
            f"{value!r} ({type(value).__name__})"
            for value in invalid_months.head(5)
        ]
        raise ValueError(
            "Input contains invalid or missing month_start values: "
            f"count={len(invalid_months)}, samples={invalid_samples}."
        )
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError("Input contains invalid or non-finite actual values.")
    if (values < 0).any():
        raise ValueError("Input actual values must be non-negative.")

    normalized_months = parsed_months.dt.to_period("M").dt.to_timestamp()
    if not parsed_months.eq(normalized_months).all():
        raise ValueError("Every month_start must be the first day of its month.")

    monthly["month_start"] = normalized_months
    monthly["actual_weighted_enquiry_value"] = values.astype(float)
    for column in ("product_segment", "category_segment"):
        monthly[column] = monthly[column].astype("string").str.strip()

    invalid_products = sorted(
        set(monthly["product_segment"].dropna()).difference(PRODUCT_SEGMENTS)
    )
    invalid_categories = sorted(
        set(monthly["category_segment"].dropna()).difference(CATEGORY_SEGMENTS)
    )
    if monthly[["product_segment", "category_segment"]].isna().any().any():
        raise ValueError("Input contains missing product or category segments.")
    if invalid_products:
        raise ValueError(f"Input contains invalid product segments: {invalid_products}")
    if invalid_categories:
        raise ValueError(f"Input contains invalid category segments: {invalid_categories}")

    key_columns = ["month_start", "product_segment", "category_segment"]
    duplicate_count = int(monthly.duplicated(key_columns).sum())
    if duplicate_count:
        raise ValueError(f"Input contains {duplicate_count} duplicate leaf-month rows.")

    actual_months = pd.DatetimeIndex(sorted(monthly["month_start"].unique()))
    if actual_months.empty:
        raise ValueError("Input contains no monthly actual rows.")
    expected_months = pd.date_range(
        start=actual_months.min(),
        end=actual_months.max(),
        freq="MS",
    )
    if not actual_months.equals(expected_months):
        raise ValueError("Input month history must be continuous.")

    expected_keys = pd.MultiIndex.from_product(
        [expected_months, PRODUCT_SEGMENTS, CATEGORY_SEGMENTS],
        names=key_columns,
    )
    actual_keys = pd.MultiIndex.from_frame(monthly[key_columns])
    missing_keys = expected_keys.difference(actual_keys)
    unexpected_keys = actual_keys.difference(expected_keys)
    if len(missing_keys) or len(unexpected_keys):
        raise ValueError(
            "Input must contain exactly eight leaves per month; "
            f"missing={len(missing_keys)}, unexpected={len(unexpected_keys)}."
        )

    output_columns = [
        "product_segment",
        "category_segment",
        "month_start",
        "actual_weighted_enquiry_value",
    ]
    return monthly.sort_values(key_columns).loc[:, output_columns].reset_index(drop=True)


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
        frame[f"roll_std_{window}"] = (
            shifted.rolling(window).std(ddof=0).fillna(0.0)
        )
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
    lags = tuple(sorted({int(value) for value in config["lags"]}))
    working = (
        pd.Series(
            np.log1p(np.clip(train.values, 0.0, None)),
            index=train.index,
        )
        if config["use_log"]
        else train.astype(float)
    )
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

    values = (
        np.expm1(np.asarray(predictions))
        if config["use_log"]
        else np.asarray(predictions)
    )
    return _non_negative(pd.Series(values, index=future_index))


def _forecast_series(
    train: pd.Series,
    future_index: pd.DatetimeIndex,
) -> tuple[pd.Series, pd.Series, pd.Series, str, str]:
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
        except ValueError as exc:
            boosted = seasonal.copy()
            model_used = "seasonal_average_fallback_features"
            fallback_reason = str(exc)
    forecast = _non_negative((XGB_WEIGHT * boosted) + (SEASONAL_WEIGHT * seasonal))
    return forecast, boosted, seasonal, model_used, fallback_reason


def _forecast_monthly_leaves(
    monthly_actuals: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    report_parts: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    history_end = monthly_actuals["month_start"].max()

    for product_segment in PRODUCT_SEGMENTS:
        for category_segment in CATEGORY_SEGMENTS:
            leaf = monthly_actuals.loc[
                (monthly_actuals["product_segment"] == product_segment)
                & (monthly_actuals["category_segment"] == category_segment)
            ].sort_values("month_start")
            train = leaf.set_index("month_start")[
                "actual_weighted_enquiry_value"
            ].astype(float)
            future_index = pd.date_range(
                start=train.index.max() + pd.offsets.MonthBegin(1),
                periods=FORECAST_HORIZON_MONTHS,
                freq="MS",
            )
            nonzero_months = int((train > 0).sum())
            forecast, xgb, seasonal, model_used, fallback_reason = _forecast_series(
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
                    "forecast_horizon_months": FORECAST_HORIZON_MONTHS,
                    "forecast_start": future_index.min(),
                    "forecast_end": future_index.max(),
                    "xgb_weight": XGB_WEIGHT,
                    "seasonal_weight": SEASONAL_WEIGHT,
                }
            )

    report = pd.concat(report_parts, ignore_index=True).sort_values(
        ["product_segment", "category_segment", "month_start", "series_type"]
    )
    report["forecast_horizon_months"] = FORECAST_HORIZON_MONTHS
    report["history_end"] = history_end
    return report.reset_index(drop=True), pd.DataFrame(summaries)


def _build_overall_benchmark(monthly_actuals: pd.DataFrame) -> pd.DataFrame:
    train = (
        monthly_actuals.groupby("month_start")["actual_weighted_enquiry_value"]
        .sum()
        .sort_index()
        .astype(float)
    )
    future_index = pd.date_range(
        start=train.index.max() + pd.offsets.MonthBegin(1),
        periods=FORECAST_HORIZON_MONTHS,
        freq="MS",
    )
    forecast, xgb, seasonal, model_used, fallback_reason = _forecast_series(
        train,
        future_index,
    )
    actual_rows = pd.DataFrame(
        {
            "month_start": train.index,
            "actual_weighted_enquiry_value": train.values,
            "forecast_weighted_enquiry_value": np.nan,
            "xgboost_forecast": np.nan,
            "seasonal_forecast": np.nan,
            "series_type": "Actual",
            "model": "",
            "fallback_reason": "",
        }
    )
    bridge_rows = pd.DataFrame(
        {
            "month_start": [train.index.max()],
            "actual_weighted_enquiry_value": [np.nan],
            "forecast_weighted_enquiry_value": [float(train.iloc[-1])],
            "xgboost_forecast": [float(train.iloc[-1])],
            "seasonal_forecast": [float(train.iloc[-1])],
            "series_type": ["Bridge"],
            "model": [model_used],
            "fallback_reason": [fallback_reason],
        }
    )
    forecast_rows = pd.DataFrame(
        {
            "month_start": future_index,
            "actual_weighted_enquiry_value": np.nan,
            "forecast_weighted_enquiry_value": forecast.values,
            "xgboost_forecast": xgb.values,
            "seasonal_forecast": seasonal.values,
            "series_type": "Forecast",
            "model": model_used,
            "fallback_reason": fallback_reason,
        }
    )
    report = pd.concat(
        [actual_rows, bridge_rows, forecast_rows],
        ignore_index=True,
    ).sort_values(["month_start", "series_type"])
    report["forecast_horizon_months"] = FORECAST_HORIZON_MONTHS
    report["history_end"] = train.index.max()
    report["benchmark_only"] = True
    return report.reset_index(drop=True)


def _run_pipeline(
    raw_dataset: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    monthly_actuals = _prepare_monthly_actuals(raw_dataset)
    report, summary = _forecast_monthly_leaves(monthly_actuals)
    benchmark = _build_overall_benchmark(monthly_actuals)
    return report, summary, benchmark


_input_dataset = globals().get("dataset")
if not isinstance(_input_dataset, pd.DataFrame):
    raise RuntimeError("Power BI did not provide the required pandas DataFrame 'dataset'.")

forecast_report, segment_model_summary, overall_benchmark_report = _run_pipeline(
    _input_dataset
)
globals().pop("dataset", None)
del _input_dataset