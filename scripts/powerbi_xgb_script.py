# 'dataset' holds the input data for this script

import pandas as pd
import numpy as np
from xgboost import XGBRegressor

monthly = dataset.copy()
monthly.columns = [str(c).strip().lower() for c in monthly.columns]

debug_input = pd.DataFrame({
    "row_count": [len(monthly)],
    "columns": [", ".join(monthly.columns)],
})

if "enquiry_month" not in monthly.columns:
    raise ValueError(f"Expected column 'enquiry_month'. Found: {list(monthly.columns)}")

if "weighted_enquiry_value" not in monthly.columns:
    raise ValueError(f"Expected column 'weighted_enquiry_value'. Found: {list(monthly.columns)}")

monthly["enquiry_month"] = pd.to_datetime(
    monthly["enquiry_month"].astype(str),
    format="%Y-%m-%d",
    errors="coerce",
)

monthly["weighted_enquiry_value"] = pd.to_numeric(
    monthly["weighted_enquiry_value"],
    errors="coerce",
).fillna(0.0)

monthly = monthly.dropna(subset=["enquiry_month"])

if monthly.empty:
    raise ValueError("No valid rows after parsing enquiry_month.")

monthly = (
    monthly.sort_values("enquiry_month")
    .groupby("enquiry_month", as_index=False)
    .agg(weighted_enquiry_value=("weighted_enquiry_value", "sum"))
)

value_series = (
    monthly
    .set_index("enquiry_month")["weighted_enquiry_value"]
    .asfreq("MS")
    .fillna(0.0)
    .astype(float)
)

if value_series.empty or pd.isna(value_series.index.max()):
    raise ValueError("Forecast history is empty. Cannot create future forecast dates.")

FORECAST_HORIZON = 12
XGB_WEIGHT = 0.75
SEASONAL_WEIGHT = 0.25

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


def non_negative(series):
    return pd.Series(np.clip(np.asarray(series, dtype=float), 0.0, None), index=series.index)


def safe_log1p(x):
    return np.log1p(np.clip(x, 0.0, None))


def seasonal_average_forecast(train, future_index, lookback=36):
    train = train.astype(float)
    working = train.iloc[-lookback:] if lookback else train
    month_means = working.groupby(working.index.month).mean()
    global_mean = float(working.mean()) if len(working) else 0.0
    values = [month_means.get(ts.month, global_mean) for ts in future_index]
    return non_negative(pd.Series(values, index=future_index, dtype=float))


def build_xgb_feature_matrix(series, lags, rolling_windows=(), add_month_features=False):
    frame = pd.DataFrame(index=series.index)

    for lag in sorted({int(lag) for lag in lags}):
        frame[f"lag_{lag}"] = series.shift(lag)

    shifted = series.shift(1)
    for window in sorted({int(window) for window in rolling_windows}):
        frame[f"roll_mean_{window}"] = shifted.rolling(window).mean()
        frame[f"roll_std_{window}"] = shifted.rolling(window).std(ddof=0).fillna(0.0)

    if add_month_features:
        frame["month_sin"] = np.sin(2 * np.pi * series.index.month / 12.0)
        frame["month_cos"] = np.cos(2 * np.pi * series.index.month / 12.0)

    valid = frame.notna().all(axis=1)
    return frame.loc[valid], series.loc[valid]


def build_xgb_feature_row(history, ts, lags, rolling_windows=(), add_month_features=False):
    row = {}

    for lag in sorted({int(lag) for lag in lags}):
        lag_ts = ts - pd.DateOffset(months=lag)
        row[f"lag_{lag}"] = float(history.loc[lag_ts])

    for window in sorted({int(window) for window in rolling_windows}):
        values = [
            float(history.loc[ts - pd.DateOffset(months=step)])
            for step in range(1, window + 1)
        ]
        row[f"roll_mean_{window}"] = float(np.mean(values))
        row[f"roll_std_{window}"] = float(np.std(values, ddof=0))

    if add_month_features:
        row["month_sin"] = float(np.sin(2 * np.pi * ts.month / 12.0))
        row["month_cos"] = float(np.cos(2 * np.pi * ts.month / 12.0))

    return row


def xgboost_forecast(train, future_index, lags, rolling_windows, max_depth,
                     n_estimators, learning_rate, subsample, colsample_bytree,
                     min_child_weight, reg_lambda, reg_alpha,
                     add_month_features, use_log):
    train = train.astype(float)
    lags = tuple(sorted({int(lag) for lag in lags}))

    if len(train) <= max(lags):
        return seasonal_average_forecast(train, future_index)

    working_series = pd.Series(safe_log1p(train.values), index=train.index) if use_log else train

    X_train, y_train = build_xgb_feature_matrix(
        working_series,
        lags=lags,
        rolling_windows=rolling_windows,
        add_month_features=add_month_features,
    )

    if len(X_train) < 12:
        return seasonal_average_forecast(train, future_index)

    feature_columns = X_train.columns.tolist()

    model = XGBRegressor(
        objective="reg:squarederror",
        max_depth=max_depth,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        min_child_weight=min_child_weight,
        reg_lambda=reg_lambda,
        reg_alpha=reg_alpha,
        random_state=42,
        n_jobs=1,
    )

    model.fit(X_train, y_train)

    history = working_series.copy()
    preds = []

    for ts in future_index:
        row = build_xgb_feature_row(
            history,
            ts,
            lags=lags,
            rolling_windows=rolling_windows,
            add_month_features=add_month_features,
        )
        step_frame = pd.DataFrame([row], index=[ts]).reindex(columns=feature_columns, fill_value=0.0)
        yhat = float(model.predict(step_frame)[0])
        history.loc[ts] = yhat
        preds.append(yhat)

    pred_values = np.expm1(np.asarray(preds)) if use_log else np.asarray(preds)
    return non_negative(pd.Series(pred_values, index=future_index))


future_index = pd.date_range(
    value_series.index.max() + pd.offsets.MonthBegin(1),
    periods=FORECAST_HORIZON,
    freq="MS",
)

xgb_forecast = xgboost_forecast(
    value_series,
    future_index,
    **WEIGHTED_VALUE_XGB_CONFIG,
)

seasonal_forecast = seasonal_average_forecast(
    value_series,
    future_index,
    lookback=24,
)

forecast_series = non_negative(
    (XGB_WEIGHT * xgb_forecast) + (SEASONAL_WEIGHT * seasonal_forecast)
)

last_actual_month = value_series.index.max()
last_actual_value = float(value_series.loc[last_actual_month])

forecast_bridge = pd.Series(
    [last_actual_value],
    index=pd.DatetimeIndex([last_actual_month]),
)

forecast_series_bridged = pd.concat([
    forecast_bridge,
    forecast_series,
])

xgb_forecast_bridged = np.concatenate([
    [last_actual_value],
    xgb_forecast.values,
])

seasonal_forecast_bridged = np.concatenate([
    [last_actual_value],
    seasonal_forecast.values,
])

actual_rows = pd.DataFrame({
    "month_start": value_series.index,
    "actual_weighted_enquiry_value": value_series.values,
    "forecast_weighted_enquiry_value": np.nan,
    "xgboost_forecast": np.nan,
    "seasonal_forecast": np.nan,
    "series_type": "Actual",
    "model": "",
})

forecast_rows = pd.DataFrame({
    "month_start": forecast_series_bridged.index,
    "actual_weighted_enquiry_value": np.nan,
    "forecast_weighted_enquiry_value": forecast_series_bridged.values,
    "xgboost_forecast": xgb_forecast_bridged,
    "seasonal_forecast": seasonal_forecast_bridged,
    "series_type": "Forecast",
    "model": "xgboost_tuned_75pct_seasonal_25pct",
})

forecast_report = pd.concat([actual_rows, forecast_rows], ignore_index=True)

model_summary = pd.DataFrame([{
    "target": "weighted_enquiry_value",
    "model": "xgboost_tuned_75pct_seasonal_25pct",
    "forecast_horizon": FORECAST_HORIZON,
    "xgb_weight": XGB_WEIGHT,
    "seasonal_weight": SEASONAL_WEIGHT,
    "history_start": value_series.index.min(),
    "history_end": value_series.index.max(),
    "history_months": len(value_series),
}])



# ==================================================================
# One off Use - Power BI Python Script - vw_weighted_enquiry_value_monthly_oct2027
# ==================================================================

# 'dataset' holds the input data for this script

import pandas as pd
import numpy as np
from xgboost import XGBRegressor

monthly = dataset.copy()
monthly.columns = [str(c).strip().lower() for c in monthly.columns]

debug_input = pd.DataFrame({
    "row_count": [len(monthly)],
    "columns": [", ".join(monthly.columns)],
})

if "enquiry_month" not in monthly.columns:
    raise ValueError(f"Expected column 'enquiry_month'. Found: {list(monthly.columns)}")

if "weighted_enquiry_value" not in monthly.columns:
    raise ValueError(f"Expected column 'weighted_enquiry_value'. Found: {list(monthly.columns)}")

monthly["enquiry_month"] = pd.to_datetime(
    monthly["enquiry_month"].astype(str),
    format="%Y-%m-%d",
    errors="coerce",
)

monthly["weighted_enquiry_value"] = pd.to_numeric(
    monthly["weighted_enquiry_value"],
    errors="coerce",
).fillna(0.0)

monthly = monthly.dropna(subset=["enquiry_month"])

if monthly.empty:
    raise ValueError("No valid rows after parsing enquiry_month.")

monthly = (
    monthly.sort_values("enquiry_month")
    .groupby("enquiry_month", as_index=False)
    .agg(weighted_enquiry_value=("weighted_enquiry_value", "sum"))
)

value_series = (
    monthly
    .set_index("enquiry_month")["weighted_enquiry_value"]
    .asfreq("MS")
    .fillna(0.0)
    .astype(float)
)

if value_series.empty or pd.isna(value_series.index.max()):
    raise ValueError("Forecast history is empty. Cannot create future forecast dates.")

# One-off stakeholder request:
# Generate forecast months up to and including October 2027.
FORECAST_END_MONTH = pd.Timestamp("2027-10-01")

XGB_WEIGHT = 0.75
SEASONAL_WEIGHT = 0.25

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


def non_negative(series):
    return pd.Series(
        np.clip(np.asarray(series, dtype=float), 0.0, None),
        index=series.index,
    )


def safe_log1p(x):
    return np.log1p(np.clip(x, 0.0, None))


def seasonal_average_forecast(train, future_index, lookback=36):
    train = train.astype(float)
    working = train.iloc[-lookback:] if lookback else train
    month_means = working.groupby(working.index.month).mean()
    global_mean = float(working.mean()) if len(working) else 0.0
    values = [month_means.get(ts.month, global_mean) for ts in future_index]
    return non_negative(pd.Series(values, index=future_index, dtype=float))


def build_xgb_feature_matrix(series, lags, rolling_windows=(), add_month_features=False):
    frame = pd.DataFrame(index=series.index)

    for lag in sorted({int(lag) for lag in lags}):
        frame[f"lag_{lag}"] = series.shift(lag)

    shifted = series.shift(1)
    for window in sorted({int(window) for window in rolling_windows}):
        frame[f"roll_mean_{window}"] = shifted.rolling(window).mean()
        frame[f"roll_std_{window}"] = shifted.rolling(window).std(ddof=0).fillna(0.0)

    if add_month_features:
        frame["month_sin"] = np.sin(2 * np.pi * series.index.month / 12.0)
        frame["month_cos"] = np.cos(2 * np.pi * series.index.month / 12.0)

    valid = frame.notna().all(axis=1)
    return frame.loc[valid], series.loc[valid]


def build_xgb_feature_row(history, ts, lags, rolling_windows=(), add_month_features=False):
    row = {}

    for lag in sorted({int(lag) for lag in lags}):
        lag_ts = ts - pd.DateOffset(months=lag)
        row[f"lag_{lag}"] = float(history.loc[lag_ts])

    for window in sorted({int(window) for window in rolling_windows}):
        values = [
            float(history.loc[ts - pd.DateOffset(months=step)])
            for step in range(1, window + 1)
        ]
        row[f"roll_mean_{window}"] = float(np.mean(values))
        row[f"roll_std_{window}"] = float(np.std(values, ddof=0))

    if add_month_features:
        row["month_sin"] = float(np.sin(2 * np.pi * ts.month / 12.0))
        row["month_cos"] = float(np.cos(2 * np.pi * ts.month / 12.0))

    return row


def xgboost_forecast(train, future_index, lags, rolling_windows, max_depth,
                     n_estimators, learning_rate, subsample, colsample_bytree,
                     min_child_weight, reg_lambda, reg_alpha,
                     add_month_features, use_log):
    train = train.astype(float)
    lags = tuple(sorted({int(lag) for lag in lags}))

    if len(train) <= max(lags):
        return seasonal_average_forecast(train, future_index)

    working_series = pd.Series(
        safe_log1p(train.values),
        index=train.index,
    ) if use_log else train

    X_train, y_train = build_xgb_feature_matrix(
        working_series,
        lags=lags,
        rolling_windows=rolling_windows,
        add_month_features=add_month_features,
    )

    if len(X_train) < 12:
        return seasonal_average_forecast(train, future_index)

    feature_columns = X_train.columns.tolist()

    model = XGBRegressor(
        objective="reg:squarederror",
        max_depth=max_depth,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        min_child_weight=min_child_weight,
        reg_lambda=reg_lambda,
        reg_alpha=reg_alpha,
        random_state=42,
        n_jobs=1,
    )

    model.fit(X_train, y_train)

    history = working_series.copy()
    preds = []

    for ts in future_index:
        row = build_xgb_feature_row(
            history,
            ts,
            lags=lags,
            rolling_windows=rolling_windows,
            add_month_features=add_month_features,
        )
        step_frame = pd.DataFrame([row], index=[ts]).reindex(
            columns=feature_columns,
            fill_value=0.0,
        )
        yhat = float(model.predict(step_frame)[0])
        history.loc[ts] = yhat
        preds.append(yhat)

    pred_values = np.expm1(np.asarray(preds)) if use_log else np.asarray(preds)
    return non_negative(pd.Series(pred_values, index=future_index))


future_start = value_series.index.max() + pd.offsets.MonthBegin(1)

future_index = pd.date_range(
    start=future_start,
    end=FORECAST_END_MONTH,
    freq="MS",
)

FORECAST_HORIZON = len(future_index)

if FORECAST_HORIZON <= 0:
    raise ValueError(
        "Forecast end month must be after the latest actual month. "
        f"Latest actual month: {value_series.index.max().date()}, "
        f"forecast end month: {FORECAST_END_MONTH.date()}."
    )

xgb_forecast = xgboost_forecast(
    value_series,
    future_index,
    **WEIGHTED_VALUE_XGB_CONFIG,
)

seasonal_forecast = seasonal_average_forecast(
    value_series,
    future_index,
    lookback=24,
)

forecast_series = non_negative(
    (XGB_WEIGHT * xgb_forecast) + (SEASONAL_WEIGHT * seasonal_forecast)
)

last_actual_month = value_series.index.max()
last_actual_value = float(value_series.loc[last_actual_month])

forecast_bridge = pd.Series(
    [last_actual_value],
    index=pd.DatetimeIndex([last_actual_month]),
)

forecast_series_bridged = pd.concat([
    forecast_bridge,
    forecast_series,
])

xgb_forecast_bridged = np.concatenate([
    [last_actual_value],
    xgb_forecast.values,
])

seasonal_forecast_bridged = np.concatenate([
    [last_actual_value],
    seasonal_forecast.values,
])

actual_rows = pd.DataFrame({
    "month_start": value_series.index,
    "actual_weighted_enquiry_value": value_series.values,
    "forecast_weighted_enquiry_value": np.nan,
    "xgboost_forecast": np.nan,
    "seasonal_forecast": np.nan,
    "series_type": "Actual",
    "model": "",
})

forecast_rows = pd.DataFrame({
    "month_start": forecast_series_bridged.index,
    "actual_weighted_enquiry_value": np.nan,
    "forecast_weighted_enquiry_value": forecast_series_bridged.values,
    "xgboost_forecast": xgb_forecast_bridged,
    "seasonal_forecast": seasonal_forecast_bridged,
    "series_type": "Forecast",
    "model": "xgboost_tuned_75pct_seasonal_25pct",
})

forecast_report = pd.concat([actual_rows, forecast_rows], ignore_index=True)

model_summary = pd.DataFrame([{
    "target": "weighted_enquiry_value",
    "model": "xgboost_tuned_75pct_seasonal_25pct",
    "forecast_horizon": FORECAST_HORIZON,
    "forecast_end_month": FORECAST_END_MONTH,
    "xgb_weight": XGB_WEIGHT,
    "seasonal_weight": SEASONAL_WEIGHT,
    "history_start": value_series.index.min(),
    "history_end": value_series.index.max(),
    "history_months": len(value_series),
}])