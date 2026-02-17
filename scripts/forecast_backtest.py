from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import psycopg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "forecast"

ACTUALS_COUNT_SQL = """
WITH actuals AS (
    SELECT
        s.parent_monday_id AS project_id,
        DATE_TRUNC('month', s.invoice_date)::DATE AS forecast_month,
        SUM(COALESCE(NULLIF(s.amount_invoiced, 0), 0))::NUMERIC(14,2) AS actual_value
    FROM subitems s
    WHERE s.parent_monday_id IS NOT NULL
      AND s.invoice_date >= (DATE_TRUNC('month', CURRENT_DATE) - MAKE_INTERVAL(months => %s))
      AND s.invoice_date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1, 2
)
SELECT COUNT(*)
FROM actuals
WHERE actual_value >= %s;
"""

BACKTEST_SQL = """
WITH actuals AS (
    SELECT
        s.parent_monday_id AS project_id,
        DATE_TRUNC('month', s.invoice_date)::DATE AS forecast_month,
        SUM(COALESCE(NULLIF(s.amount_invoiced, 0), 0))::NUMERIC(14,2) AS actual_value
    FROM subitems s
    WHERE s.parent_monday_id IS NOT NULL
      AND s.invoice_date >= (DATE_TRUNC('month', CURRENT_DATE) - MAKE_INTERVAL(months => %s))
      AND s.invoice_date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1, 2
),
actuals_filtered AS (
    SELECT *
    FROM actuals
    WHERE actual_value >= %s
),
joined AS (
    SELECT
        a.project_id,
        a.forecast_month,
        a.actual_value,
        f.snapshot_date,
        f.stage_bucket,
        f.contract_value,
        f.probability,
        f.committed_value,
        f.expected_value,
        f.best_case_value,
        f.worst_case_value,
        ROW_NUMBER() OVER (
            PARTITION BY a.project_id, a.forecast_month
            ORDER BY f.snapshot_date DESC
        ) AS rn
    FROM actuals_filtered a
    LEFT JOIN pipeline_forecast_snapshot f
        ON f.project_id = a.project_id
       AND f.forecast_month = a.forecast_month
       AND f.snapshot_date <= (a.forecast_month - (%s::INT * INTERVAL '1 day'))::DATE
)
SELECT
    project_id,
    forecast_month,
    snapshot_date,
    stage_bucket,
    actual_value::FLOAT8 AS actual_value,
    contract_value::FLOAT8 AS contract_value,
    committed_value::FLOAT8 AS committed_value,
    expected_value::FLOAT8 AS expected_value,
    best_case_value::FLOAT8 AS best_case_value,
    worst_case_value::FLOAT8 AS worst_case_value
FROM joined
WHERE rn = 1
  AND snapshot_date IS NOT NULL
ORDER BY forecast_month, project_id;
"""


@dataclass
class BacktestSummary:
    total_rows: int
    actual_population_rows: int
    matched_actual_ratio: float
    total_actual: float
    total_expected: float
    total_committed: float
    expected_wape: float
    committed_wape: float
    expected_bias_ratio: float
    committed_bias_ratio: float
    coverage_rate: float
    monotonicity_violations: int


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL environment variable is required")
    return dsn


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def _fetch_scalar(
    conn: psycopg.Connection[Any], sql: str, params: Sequence[object]
) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return int(row[0] if row and row[0] is not None else 0)


def _fetch_df(
    conn: psycopg.Connection[Any], sql: str, params: Sequence[object]
) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        if not cur.description:
            return pd.DataFrame()
        columns = [getattr(c, "name", c[0]) for c in cur.description]
    return pd.DataFrame(rows, columns=columns)


def _compute_summary(df: pd.DataFrame, actual_population_rows: int) -> BacktestSummary:
    rows = df.copy()

    numeric_cols = [
        "actual_value",
        "contract_value",
        "committed_value",
        "expected_value",
        "best_case_value",
        "worst_case_value",
    ]
    for col in numeric_cols:
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)

    rows["expected_error"] = rows["expected_value"] - rows["actual_value"]
    rows["committed_error"] = rows["committed_value"] - rows["actual_value"]

    total_actual = float(rows["actual_value"].sum())
    total_expected = float(rows["expected_value"].sum())
    total_committed = float(rows["committed_value"].sum())

    expected_wape = _safe_div(rows["expected_error"].abs().sum(), total_actual)
    committed_wape = _safe_div(rows["committed_error"].abs().sum(), total_actual)
    expected_bias_ratio = _safe_div(total_expected - total_actual, total_actual)
    committed_bias_ratio = _safe_div(total_committed - total_actual, total_actual)

    coverage_mask = (
        (rows["actual_value"] >= rows["worst_case_value"])
        & (rows["actual_value"] <= rows["best_case_value"])
    )
    coverage_rate = float(coverage_mask.mean()) if len(rows) else 0.0

    monotonicity_violations = int(
        (
            (rows["worst_case_value"] > rows["expected_value"])
            | (rows["expected_value"] > rows["best_case_value"])
        ).sum()
    )

    return BacktestSummary(
        total_rows=int(len(rows)),
        actual_population_rows=int(actual_population_rows),
        matched_actual_ratio=_safe_div(len(rows), actual_population_rows),
        total_actual=total_actual,
        total_expected=total_expected,
        total_committed=total_committed,
        expected_wape=float(expected_wape),
        committed_wape=float(committed_wape),
        expected_bias_ratio=float(expected_bias_ratio),
        committed_bias_ratio=float(committed_bias_ratio),
        coverage_rate=float(coverage_rate),
        monotonicity_violations=monotonicity_violations,
    )


def _monthly_rollup(df: pd.DataFrame) -> pd.DataFrame:
    rows = df.copy()
    for col in ["actual_value", "expected_value", "committed_value", "best_case_value", "worst_case_value"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)

    monthly = (
        rows.groupby("forecast_month", as_index=False)
        .agg(
            projects=("project_id", "count"),
            actual_value=("actual_value", "sum"),
            expected_value=("expected_value", "sum"),
            committed_value=("committed_value", "sum"),
            best_case_value=("best_case_value", "sum"),
            worst_case_value=("worst_case_value", "sum"),
        )
        .sort_values("forecast_month")
    )

    monthly["expected_bias_pct"] = (
        (monthly["expected_value"] - monthly["actual_value"])
        .div(monthly["actual_value"].replace({0: pd.NA}))
        .fillna(0.0)
        * 100.0
    ).round(2)

    return monthly


def _recommendations(summary: BacktestSummary) -> list[str]:
    recs: list[str] = []

    if summary.monotonicity_violations > 0:
        recs.append(
            "Fix SQL band monotonicity immediately (worst <= expected <= best must always hold)."
        )

    if summary.coverage_rate < 0.70:
        recs.append(
            "Prediction bands are too narrow (coverage < 70%). Increase probability spread fallback and/or reduce confidence tightening."
        )
    elif summary.coverage_rate > 0.95:
        recs.append(
            "Prediction bands are likely too wide (coverage > 95%). Tighten spread fallback to improve decision usefulness."
        )

    if summary.expected_bias_ratio > 0.10:
        recs.append(
            "Expected forecast is over actuals by more than 10%. Reduce open-stage fallback probabilities or tighten optimistic assumptions."
        )
    elif summary.expected_bias_ratio < -0.10:
        recs.append(
            "Expected forecast is under actuals by more than 10%. Increase open-stage fallback probabilities where confidence is missing."
        )

    if summary.matched_actual_ratio < 0.90:
        recs.append(
            "Snapshot-to-actual match rate is below 90%. Verify daily snapshot job timing and snapshot completeness."
        )

    if not recs:
        recs.append("No material calibration adjustment suggested at current thresholds.")

    return recs


def _fmt_money(value: float) -> str:
    return f"{value:,.2f}"


def _fmt_pct(value: float) -> str:
    return f"{value * 100.0:.2f}%"


def _md_table(headers: list[str], rows: list[list[object]]) -> str:
    rows_as_str = [[str(cell) for cell in row] for row in rows]
    widths = [len(h) for h in headers]

    for row in rows_as_str:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def render(row: list[str]) -> str:
        padded = [row[i].ljust(widths[i]) for i in range(len(row))]
        return "| " + " | ".join(padded) + " |"

    header_line = render(headers)
    sep_line = "|-" + "-|-".join("-" * w for w in widths) + "-|"
    data_lines = [render(row) for row in rows_as_str]
    return "\n".join([header_line, sep_line, *data_lines])


def _render_markdown(
    summary: BacktestSummary,
    monthly: pd.DataFrame,
    recommendations: list[str],
    months_back: int,
    min_actual_value: float,
    snapshot_lag_days: int,
) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    summary_rows = [
        ["Generated at (UTC)", generated_at],
        ["Lookback months", months_back],
        ["Minimum actual value filter", _fmt_money(min_actual_value)],
        ["Snapshot lag days", snapshot_lag_days],
        ["Matched project-month rows", summary.total_rows],
        ["All actual project-month rows", summary.actual_population_rows],
        ["Snapshot match rate", _fmt_pct(summary.matched_actual_ratio)],
        ["Total actual value", _fmt_money(summary.total_actual)],
        ["Total expected value", _fmt_money(summary.total_expected)],
        ["Total committed value", _fmt_money(summary.total_committed)],
        ["Expected WAPE", _fmt_pct(summary.expected_wape)],
        ["Committed WAPE", _fmt_pct(summary.committed_wape)],
        ["Expected bias", _fmt_pct(summary.expected_bias_ratio)],
        ["Committed bias", _fmt_pct(summary.committed_bias_ratio)],
        ["Band coverage rate", _fmt_pct(summary.coverage_rate)],
        ["Monotonicity violations", summary.monotonicity_violations],
    ]

    monthly_rows: list[list[object]] = []
    for _, row in monthly.iterrows():
        forecast_month = row["forecast_month"]
        if hasattr(forecast_month, "date"):
            forecast_month = forecast_month.date().isoformat()
        monthly_rows.append(
            [
                forecast_month,
                int(row["projects"]),
                _fmt_money(float(row["actual_value"])),
                _fmt_money(float(row["expected_value"])),
                _fmt_money(float(row["committed_value"])),
                _fmt_money(float(row["worst_case_value"])),
                _fmt_money(float(row["best_case_value"])),
                f"{float(row['expected_bias_pct']):.2f}%",
            ]
        )

    lines: list[str] = []
    lines.append("# Forecast Backtest Report")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(_md_table(["Metric", "Value"], summary_rows))
    lines.append("")
    lines.append("## Calibration Adjustments")
    lines.append("")
    for rec in recommendations:
        lines.append(f"- {rec}")
    lines.append("")
    lines.append("## Monthly Rollup")
    lines.append("")
    if monthly_rows:
        lines.append(
            _md_table(
                [
                    "Forecast Month",
                    "Projects",
                    "Actual",
                    "Expected",
                    "Committed",
                    "Worst",
                    "Best",
                    "Expected Bias %",
                ],
                monthly_rows,
            )
        )
    else:
        lines.append("_No monthly rollup rows available._")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest forecast snapshots against invoiced actuals."
    )
    parser.add_argument(
        "--months-back",
        type=int,
        default=6,
        help="How many completed months to backtest (default: 6).",
    )
    parser.add_argument(
        "--min-actual-value",
        type=float,
        default=1.0,
        help="Minimum monthly actual value to include (default: 1.0).",
    )
    parser.add_argument(
        "--snapshot-lag-days",
        type=int,
        default=1,
        help="Use snapshots on or before forecast_month - lag_days (default: 1).",
    )
    parser.add_argument(
        "--output-markdown",
        type=str,
        default="",
        help="Optional markdown output path.",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default="",
        help="Optional CSV output path for matched rows.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.months_back < 1:
        raise ValueError("--months-back must be >= 1")
    if args.snapshot_lag_days < 0:
        raise ValueError("--snapshot-lag-days must be >= 0")
    if args.min_actual_value < 0:
        raise ValueError("--min-actual-value must be >= 0")

    dsn = _get_dsn()

    with psycopg.connect(dsn) as conn:
        actual_population_rows = _fetch_scalar(
            conn,
            ACTUALS_COUNT_SQL,
            (args.months_back, args.min_actual_value),
        )
        df = _fetch_df(
            conn,
            BACKTEST_SQL,
            (args.months_back, args.min_actual_value, args.snapshot_lag_days),
        )

    if df.empty:
        print("No matched snapshot rows for the selected backtest window.")
        print(
            "Check that invoiced actuals exist and snapshots were created before target months."
        )
        return 0

    summary = _compute_summary(df, actual_population_rows)
    monthly = _monthly_rollup(df)
    recommendations = _recommendations(summary)

    report_markdown = _render_markdown(
        summary=summary,
        monthly=monthly,
        recommendations=recommendations,
        months_back=args.months_back,
        min_actual_value=args.min_actual_value,
        snapshot_lag_days=args.snapshot_lag_days,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_md = (
        Path(args.output_markdown)
        if args.output_markdown
        else DEFAULT_OUTPUT_DIR / f"forecast_backtest_{timestamp}.md"
    )
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(report_markdown, encoding="utf-8")

    if args.output_csv:
        output_csv = Path(args.output_csv)
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, index=False)
        print(f"Matched backtest rows written to: {output_csv}")

    print(f"Backtest report written to: {output_md}")
    print(
        f"Rows matched: {summary.total_rows}/{summary.actual_population_rows} "
        f"({summary.matched_actual_ratio * 100.0:.2f}%)"
    )
    print(f"Expected WAPE: {summary.expected_wape * 100.0:.2f}%")
    print(f"Band coverage: {summary.coverage_rate * 100.0:.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())