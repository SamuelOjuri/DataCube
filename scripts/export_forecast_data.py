"""
Export the raw project-level enquiry training dataset.

The raw CSV is consumed by both the gross enquiry-value forecasting notebook
and the weighted enquiry-value forecasting notebook. The weighted target is
produced here as ``weighted_enquiry_value = gross_enquiry_value *
expected_conversion_rate`` using the latest per-project analysis (same
convention as ``vw_actual_enquiry_monthly_v1.actual_pipeline_value``).

Usage:
python scripts\\export_forecast_data.py --start-date 2022-01-01
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.normalization import compute_product_key, normalize_category_value

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

RAW_EXPORT_SQL = """
WITH latest_analysis AS (
    SELECT
        ar.project_id,
        ar.expected_conversion_rate,
        ar.conversion_confidence,
        ar.analysis_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY ar.project_id
            ORDER BY ar.analysis_timestamp DESC NULLS LAST, ar.id DESC
        ) AS rn
    FROM analysis_results ar
)
SELECT
    p.monday_id AS project_id,
    p.item_name,
    p.project_name,
    p.date_created::date AS date_created,
    DATE_TRUNC('month', p.date_created)::date AS enquiry_month,
    COALESCE(p.new_enquiry_value, 0)::float8 AS new_enquiry_value,
    la.expected_conversion_rate::float8 AS expected_conversion_rate,
    la.conversion_confidence::float8 AS conversion_confidence,
    la.analysis_timestamp AS analysis_timestamp,
    NULLIF(TRIM(p.account), '') AS account,
    NULLIF(TRIM(p.type), '') AS type,
    p.category AS category_raw,
    p.product_type AS product_type_raw,
    p.product_key AS product_key_raw,
    NULLIF(TRIM(p.value_band), '') AS value_band,
    NULLIF(TRIM(p.pipeline_stage), '') AS pipeline_stage,
    NULLIF(TRIM(p.status_category), '') AS status_category,
    NULLIF(TRIM(p.zip_code), '') AS zip_code,
    NULLIF(TRIM(p.sales_representative), '') AS sales_representative,
    NULLIF(TRIM(p.funding), '') AS funding,
    p.created_at,
    p.updated_at
FROM projects p
LEFT JOIN latest_analysis la
    ON la.project_id = p.monday_id
   AND la.rn = 1
WHERE p.date_created IS NOT NULL
  AND p.date_created >= %s::date
ORDER BY p.date_created, p.monday_id;
"""

AUDIT_VIEW_SQL = """
SELECT
    enquiry_month,
    project_count,
    actual_enquiry_value,
    actual_pipeline_value
FROM vw_actual_enquiry_monthly_v1
ORDER BY enquiry_month;
"""


def _parse_date(value: str) -> str:
    try:
        return pd.Timestamp(value).date().isoformat()
    except Exception as exc:  # noqa: BLE001
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Use YYYY-MM-DD."
        ) from exc


def _get_dsn() -> str:
    for env_name in ("SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DATABASE_URL"):
        value = os.getenv(env_name)
        if value:
            return value
    raise RuntimeError(
        "Set SUPABASE_DB_URL (or SUPABASE_DATABASE_URL / DATABASE_URL) before running."
    )


def _fetch_df(conn: psycopg.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        if not cur.description:
            return pd.DataFrame()
        columns = [getattr(col, "name", col[0]) for col in cur.description]
    return pd.DataFrame(rows, columns=columns)


def _clean_text(value: object, default: str = "Unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _canonical_category(raw_value: object) -> str:
    normalized = normalize_category_value(raw_value)
    return normalized or _clean_text(raw_value)


def _canonical_product_key(raw_db_key: object, raw_product_type: object) -> str:
    db_key = compute_product_key(raw_db_key)
    if db_key and db_key != "unknown":
        return db_key

    type_key = compute_product_key(raw_product_type)
    if type_key:
        return type_key

    return db_key or "unknown"


def _prepare_export(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    df["date_created"] = pd.to_datetime(df["date_created"], errors="coerce").dt.date
    df["enquiry_month"] = pd.to_datetime(df["enquiry_month"], errors="coerce").dt.date
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["analysis_timestamp"] = pd.to_datetime(
        df["analysis_timestamp"], errors="coerce"
    )

    df["new_enquiry_value"] = pd.to_numeric(
        df["new_enquiry_value"], errors="coerce"
    ).fillna(0.0)
    df["gross_enquiry_value"] = df["new_enquiry_value"].clip(lower=0.0)
    df["has_positive_value"] = (df["gross_enquiry_value"] > 0).astype(int)

    # Weighted enquiry value = gross * expected_conversion_rate, mirroring
    # vw_actual_enquiry_monthly_v1.actual_pipeline_value. Projects without an
    # LLM analysis get rate 0 (i.e. zero weighted contribution), so we expose
    # has_analysis separately for downstream coverage diagnostics.
    raw_rate = pd.to_numeric(df["expected_conversion_rate"], errors="coerce")
    df["has_analysis"] = raw_rate.notna().astype(int)
    df["expected_conversion_rate"] = raw_rate.fillna(0.0).clip(lower=0.0, upper=1.0)
    df["conversion_confidence"] = (
        pd.to_numeric(df["conversion_confidence"], errors="coerce")
        .clip(lower=0.0, upper=1.0)
    )
    df["weighted_enquiry_value"] = (
        df["gross_enquiry_value"] * df["expected_conversion_rate"]
    ).clip(lower=0.0)
    df["has_positive_weighted_value"] = (df["weighted_enquiry_value"] > 0).astype(int)

    for col in [
        "account",
        "type",
        "value_band",
        "pipeline_stage",
        "status_category",
        "zip_code",
        "sales_representative",
        "funding",
    ]:
        df[col] = df[col].map(_clean_text)

    df["category_raw"] = df["category_raw"].map(lambda x: _clean_text(x, default=""))
    df["product_type_raw"] = df["product_type_raw"].map(
        lambda x: _clean_text(x, default="")
    )
    df["product_key_raw"] = df["product_key_raw"].map(
        lambda x: _clean_text(x, default="")
    )

    df["category"] = df["category_raw"].map(_canonical_category)
    df["product_key"] = [
        _canonical_product_key(raw_db_key, raw_product_type)
        for raw_db_key, raw_product_type in zip(
            df["product_key_raw"], df["product_type_raw"]
        )
    ]

    export_cols = [
        "project_id",
        "item_name",
        "project_name",
        "date_created",
        "enquiry_month",
        "new_enquiry_value",
        "gross_enquiry_value",
        "has_positive_value",
        "expected_conversion_rate",
        "conversion_confidence",
        "has_analysis",
        "weighted_enquiry_value",
        "has_positive_weighted_value",
        "account",
        "type",
        "category",
        "category_raw",
        "product_key",
        "product_key_raw",
        "product_type_raw",
        "value_band",
        "pipeline_stage",
        "status_category",
        "zip_code",
        "sales_representative",
        "funding",
        "analysis_timestamp",
        "created_at",
        "updated_at",
    ]

    return (
        df[export_cols]
        .sort_values(["date_created", "project_id"])
        .reset_index(drop=True)
    )


def _build_overall_monthly(df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        df.groupby("enquiry_month", as_index=False)
        .agg(
            enquiry_count_all=("project_id", "count"),
            enquiry_count_valued=("has_positive_value", "sum"),
            enquiry_count_analysed=("has_analysis", "sum"),
            gross_enquiry_value=("gross_enquiry_value", "sum"),
            weighted_enquiry_value=("weighted_enquiry_value", "sum"),
        )
        .sort_values("enquiry_month")
    )
    monthly["gross_enquiry_value"] = monthly["gross_enquiry_value"].round(2)
    monthly["weighted_enquiry_value"] = monthly["weighted_enquiry_value"].round(2)
    # Useful for gating early months that pre-date full LLM analysis coverage.
    monthly["analysis_coverage"] = (
        monthly["enquiry_count_analysed"] / monthly["enquiry_count_all"].replace(0, pd.NA)
    ).astype(float).round(4)
    return monthly


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export the raw project-level enquiry training dataset for Colab."
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default="2021-01-01",
        help="Earliest project date_created to export (YYYY-MM-DD). Default: 2021-01-01.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional output path. Default: data/processed/enquiry_training_raw_<timestamp>.csv",
    )
    parser.add_argument(
        "--also-write-monthly",
        action="store_true",
        help="Also write an overall monthly summary CSV beside the raw file.",
    )
    parser.add_argument(
        "--also-write-view-audit",
        action="store_true",
        help="Also export vw_actual_enquiry_monthly_v1 for comparison.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_csv = args.output_csv or (
        DEFAULT_OUTPUT_DIR / f"enquiry_training_raw_{timestamp}.csv"
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    dsn = _get_dsn()

    with psycopg.connect(dsn) as conn:
        raw_df = _fetch_df(conn, RAW_EXPORT_SQL, (args.start_date,))
        export_df = _prepare_export(raw_df)
        export_df.to_csv(output_csv, index=False)

        print(f"Raw training CSV written to: {output_csv}")
        print(f"Rows exported: {len(export_df)}")

        if not export_df.empty:
            print(
                f"Date range: {export_df['date_created'].min()} -> "
                f"{export_df['date_created'].max()}"
            )
            print(f"Positive-value rows: {int(export_df['has_positive_value'].sum())}")
            print(
                "Analysed rows: "
                f"{int(export_df['has_analysis'].sum())} / {len(export_df)} "
                f"({export_df['has_analysis'].mean():.1%})"
            )
            print(
                "Positive weighted-value rows: "
                f"{int(export_df['has_positive_weighted_value'].sum())}"
            )
            gross_sum = float(export_df["gross_enquiry_value"].sum())
            weighted_sum = float(export_df["weighted_enquiry_value"].sum())
            ratio = (weighted_sum / gross_sum) if gross_sum else 0.0
            print(
                f"Gross enquiry value:    {gross_sum:,.2f}\n"
                f"Weighted enquiry value: {weighted_sum:,.2f} "
                f"(weighted/gross = {ratio:.3f})"
            )
            print(f"Unique accounts: {export_df['account'].nunique()}")
            print(f"Unique types: {export_df['type'].nunique()}")
            print(f"Unique categories: {export_df['category'].nunique()}")
            print(f"Unique product_keys: {export_df['product_key'].nunique()}")

            print("\nTop product_key values:")
            print(export_df["product_key"].value_counts().head(10).to_string())

        if args.also_write_monthly:
            monthly_df = _build_overall_monthly(export_df)
            monthly_path = output_csv.with_name(
                output_csv.stem + "_overall_monthly.csv"
            )
            monthly_df.to_csv(monthly_path, index=False)
            print(f"\nOverall monthly summary written to: {monthly_path}")

        if args.also_write_view_audit:
            audit_df = _fetch_df(conn, AUDIT_VIEW_SQL)
            if not audit_df.empty:
                audit_df["enquiry_month"] = pd.to_datetime(
                    audit_df["enquiry_month"], errors="coerce"
                ).dt.date
            audit_path = output_csv.with_name(output_csv.stem + "_view_audit.csv")
            audit_df.to_csv(audit_path, index=False)
            print(f"View audit CSV written to: {audit_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())