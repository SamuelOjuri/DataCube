"""Export and validate the live eight-leaf weighted-enquiry forecast inputs.

The production PBIX reads the SQL views directly. This utility provides offline
audit exports, model/backtest evidence, and an optional legacy-allocator parity
check. The database transaction is explicitly read-only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.segmented_weighted_enquiry_forecast import (
    DEFAULT_FORECAST_HORIZON_MONTHS,
    allocate_projects_to_leaves,
    backtest_monthly_leaves,
    forecast_monthly_leaves,
)


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "segmented_weighted_enquiry"

ALLOCATION_VIEW_SQL = """
SELECT
    project_id,
    enquiry_month,
    product_segment,
    category_segment,
    allocation_share,
    project_weighted_enquiry_value,
    allocated_weighted_enquiry_value,
    product_allocation_method,
    category_allocation_method,
    product_mapping_status,
    category_mapping_status,
    subitem_source_value_total
FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
ORDER BY project_id, product_segment, category_segment
"""

MONTHLY_VIEW_SQL = """
SELECT
    month_start,
    product_segment,
    category_segment,
    actual_weighted_enquiry_value
FROM public.vw_weighted_enquiry_leaf_monthly_v1
ORDER BY month_start, product_segment, category_segment
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export and validate live segmented weighted-enquiry SQL views."
    )
    parser.add_argument(
        "--forecast-horizon-months",
        type=int,
        default=DEFAULT_FORECAST_HORIZON_MONTHS,
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--backtest-months", type=int, default=12)
    parser.add_argument(
        "--allocation-input",
        type=Path,
        help="Use a prior SQL allocation-view CSV instead of querying Supabase.",
    )
    parser.add_argument(
        "--monthly-input",
        type=Path,
        help="Use a prior SQL monthly-view CSV instead of querying Supabase.",
    )
    parser.add_argument(
        "--raw-extract",
        type=Path,
        help=(
            "Optionally compare the legacy Python allocator with the SQL allocation "
            "view. This input is never used as the production forecast source."
        ),
    )
    return parser.parse_args()


def _get_dsn() -> str:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
    for name in ("SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DATABASE_URL"):
        if os.getenv(name):
            return str(os.getenv(name))
    raise RuntimeError("Set SUPABASE_DB_URL, SUPABASE_DATABASE_URL, or DATABASE_URL.")


def _fetch_views_read_only(dsn: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    import psycopg

    frames: list[pd.DataFrame] = []
    with psycopg.connect(dsn) as connection:
        with connection.transaction():
            connection.execute("SET TRANSACTION READ ONLY")
            read_only = connection.execute("SHOW transaction_read_only").fetchone()
            if not read_only or read_only[0] != "on":
                raise RuntimeError("Database transaction is not read-only; aborting.")
            for query in (ALLOCATION_VIEW_SQL, MONTHLY_VIEW_SQL):
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    columns = [column.name for column in cursor.description or ()]
                    frames.append(pd.DataFrame(cursor.fetchall(), columns=columns))
    return frames[0], frames[1]


def main() -> int:
    args = parse_args()
    if args.forecast_horizon_months <= 0:
        raise ValueError("--forecast-horizon-months must be positive.")
    if bool(args.allocation_input) != bool(args.monthly_input):
        raise ValueError("--allocation-input and --monthly-input must be used together.")

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.allocation_input:
        allocations = pd.read_csv(args.allocation_input.resolve())
        monthly_actuals = pd.read_csv(args.monthly_input.resolve())
        database_transaction = "not applicable (validated SQL-view CSV replay)"
        forecast_input_mode = "sql_view_export_replay"
    else:
        allocations, monthly_actuals = _fetch_views_read_only(_get_dsn())
        database_transaction = "READ ONLY"
        forecast_input_mode = "live_supabase_sql_views"
    if allocations.empty or monthly_actuals.empty:
        raise RuntimeError("The SQL-view audit input returned no rows.")

    allocations["enquiry_month"] = pd.to_datetime(
        allocations["enquiry_month"], errors="raise"
    )
    monthly_actuals["month_start"] = pd.to_datetime(
        monthly_actuals["month_start"], errors="raise"
    )
    duplicate_monthly_keys = monthly_actuals.duplicated(
        ["month_start", "product_segment", "category_segment"]
    )
    rows_per_month = monthly_actuals.groupby("month_start").size()
    if duplicate_monthly_keys.any() or not rows_per_month.eq(8).all():
        raise AssertionError("The monthly SQL view is not a unique eight-leaf grid.")

    share_totals = allocations.groupby("project_id")["allocation_share"].sum()
    if not share_totals.sub(1.0).abs().le(1e-9).all():
        raise AssertionError("One or more SQL project allocation shares do not sum to 1.")
    project_reconciliation = allocations.groupby("project_id").agg(
        source=("project_weighted_enquiry_value", "first"),
        allocated=("allocated_weighted_enquiry_value", "sum"),
    )
    max_project_delta = float(
        (project_reconciliation["source"] - project_reconciliation["allocated"])
        .abs()
        .max()
    )
    if max_project_delta > 0.01:
        raise AssertionError("SQL project allocations do not reconcile within GBP 0.01.")

    oracle_validated = False
    if args.raw_extract:
        projects = pd.read_csv(
            args.raw_extract.resolve(),
            converters={"subitem_allocations": json.loads},
        )
        oracle_allocations = allocate_projects_to_leaves(projects)
        columns = list(allocations.columns)
        sql_comparable = allocations.loc[:, columns].sort_values(
            ["project_id", "product_segment", "category_segment"]
        )
        oracle_comparable = oracle_allocations.sort_values(
            ["project_id", "product_segment", "category_segment"]
        )
        pd.testing.assert_frame_equal(
            sql_comparable.reset_index(drop=True),
            oracle_comparable.reset_index(drop=True),
            check_dtype=False,
            rtol=0.0,
            atol=1e-9,
        )
        oracle_validated = True

    report, model_summary = forecast_monthly_leaves(
        monthly_actuals,
        forecast_horizon_months=args.forecast_horizon_months,
    )
    backtest_predictions, backtest_metrics = backtest_monthly_leaves(
        monthly_actuals,
        holdout_months=args.backtest_months,
    )
    overall_backtest = backtest_metrics.loc[backtest_metrics["level"] == "overall"].set_index("model")
    quality = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "database_transaction": database_transaction,
        "forecast_input_mode": forecast_input_mode,
        "legacy_allocator_role": "optional parity oracle only",
        "legacy_allocator_validated": oracle_validated,
        "allocation_rows": len(allocations),
        "project_count": int(allocations["project_id"].nunique()),
        "monthly_rows": len(monthly_actuals),
        "monthly_count": int(monthly_actuals["month_start"].nunique()),
        "leaf_count": int(
            monthly_actuals[["product_segment", "category_segment"]]
            .drop_duplicates()
            .shape[0]
        ),
        "max_project_reconciliation_delta": max_project_delta,
        "forecast_horizon_months": args.forecast_horizon_months,
        "backtest_months": args.backtest_months,
        "overall_backtest_wape": float(overall_backtest.loc["forecast", "wape"]),
        "overall_backtest_bias_ratio": float(
            overall_backtest.loc["forecast", "bias_ratio"]
        ),
        "seasonal_naive_backtest_wape": float(
            overall_backtest.loc["seasonal_naive", "wape"]
        ),
        "seasonal_naive_backtest_bias_ratio": float(
            overall_backtest.loc["seasonal_naive", "bias_ratio"]
        ),
    }

    allocations.to_csv(output_dir / "project_leaf_allocations.csv", index=False)
    monthly_actuals.to_csv(output_dir / "monthly_leaf_actuals.csv", index=False)
    report.to_csv(output_dir / "segmented_weighted_enquiry_forecast.csv", index=False)
    model_summary.to_csv(output_dir / "segment_model_summary.csv", index=False)
    backtest_predictions.to_csv(output_dir / "backtest_predictions.csv", index=False)
    backtest_metrics.to_csv(output_dir / "backtest_metrics.csv", index=False)
    (output_dir / "data_quality_summary.json").write_text(
        json.dumps(quality, indent=2, default=str),
        encoding="utf-8",
    )

    print(json.dumps(quality, indent=2, default=str))
    print(f"Output directory: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
