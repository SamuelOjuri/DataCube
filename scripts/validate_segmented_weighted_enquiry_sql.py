from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BASELINE_DIR = (
    PROJECT_ROOT / "outputs" / "segmented_weighted_enquiry_baseline_frozen"
)

load_dotenv(PROJECT_ROOT / ".env")

REQUIRED_RELATIONS = (
    "public.vw_weighted_enquiry_project_leaf_allocation_v1",
    "public.vw_weighted_enquiry_leaf_monthly_v1",
    "public.vw_weighted_enquiry_value_monthly_v1",
)

CHECKS: dict[str, str] = {
    "project_allocation_share_violations": """
        SELECT COUNT(*)
        FROM (
            SELECT project_id
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY project_id
            HAVING ABS(SUM(allocation_share) - 1.0) > 1e-9
        ) invalid;
    """,
    "project_value_reconciliation_violations": """
        SELECT COUNT(*)
        FROM (
            SELECT project_id
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY project_id
            HAVING ABS(
                MAX(project_weighted_enquiry_value)
                - SUM(allocated_weighted_enquiry_value)
            ) > 0.01
        ) invalid;
    """,
    "project_weighted_value_source_mismatches": """
        WITH latest_analysis AS (
            SELECT
                ar.project_id,
                ar.expected_conversion_rate,
                ROW_NUMBER() OVER (
                    PARTITION BY ar.project_id
                    ORDER BY ar.analysis_timestamp DESC NULLS LAST, ar.id DESC
                ) AS rn
            FROM public.analysis_results ar
        ),
        expected AS (
            SELECT
                p.monday_id AS project_id,
                (
                    GREATEST(COALESCE(p.new_enquiry_value, 0), 0)
                    * LEAST(
                        GREATEST(COALESCE(la.expected_conversion_rate, 0), 0),
                        1
                    )
                )::FLOAT8 AS weighted_value
            FROM public.projects p
            LEFT JOIN latest_analysis la
                ON la.project_id = p.monday_id
               AND la.rn = 1
            WHERE p.date_created IS NOT NULL
              AND p.date_created >= DATE '2022-01-01'
              AND DATE_TRUNC('month', p.date_created)::DATE
                    < DATE_TRUNC('month', CURRENT_DATE)::DATE
        ),
        actual AS (
            SELECT project_id, MAX(project_weighted_enquiry_value) AS weighted_value
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY project_id
        )
        SELECT COUNT(*)
        FROM expected
        FULL OUTER JOIN actual USING (project_id)
        WHERE expected.project_id IS NULL
           OR actual.project_id IS NULL
           OR ABS(expected.weighted_value - actual.weighted_value) > 0.01;
    """,
    "product_allocation_method_mismatches": """
        WITH actual AS (
            SELECT project_id, MIN(product_allocation_method) AS allocation_method
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY project_id
        ),
        expected AS (
            SELECT
                actual.project_id,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM public.subitems s
                        CROSS JOIN LATERAL public.weighted_enquiry_products(
                            s.product_type
                        ) product
                        WHERE s.parent_monday_id = actual.project_id
                          AND GREATEST(COALESCE(s.new_enquiry_value, 0), 0) > 0
                          AND product.identity <> 'missing'
                    ) THEN 'subitem_value_weighted'
                    WHEN (
                        SELECT COUNT(*)
                        FROM public.projects p
                        CROSS JOIN LATERAL public.weighted_enquiry_products(
                            p.product_type
                        ) product
                        WHERE p.monday_id = actual.project_id
                    ) > 1 THEN 'equal_product_split'
                    ELSE 'default_mapping'
                END AS allocation_method
            FROM actual
        )
        SELECT COUNT(*)
        FROM actual
        JOIN expected USING (project_id)
        WHERE actual.allocation_method <> expected.allocation_method;
    """,
    "allocation_domain_or_numeric_violations": """
        SELECT COUNT(*)
        FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
        WHERE product_segment NOT IN ('Non-Combustible', 'Combustible')
           OR category_segment NOT IN (
                'Data Centres', 'Education', 'Apartments/Housing', 'Other'
           )
           OR product_allocation_method NOT IN (
                'subitem_value_weighted', 'equal_product_split', 'default_mapping'
           )
           OR category_allocation_method NOT IN (
                'equal_category_segment_split', 'default_mapping'
           )
           OR product_mapping_status NOT IN (
                'mapped', 'contains_unmapped', 'missing'
           )
           OR category_mapping_status NOT IN (
                'mapped', 'contains_unmapped', 'missing'
           )
           OR allocation_share < 0
           OR allocation_share > 1
           OR allocated_weighted_enquiry_value < 0
           OR allocation_share::TEXT IN ('NaN', 'Infinity', '-Infinity')
           OR project_weighted_enquiry_value::TEXT IN (
                'NaN', 'Infinity', '-Infinity'
           )
           OR allocated_weighted_enquiry_value::TEXT IN (
                'NaN', 'Infinity', '-Infinity'
           );
    """,
    "monthly_duplicate_leaf_keys": """
        SELECT COUNT(*)
        FROM (
            SELECT month_start, product_segment, category_segment
            FROM public.vw_weighted_enquiry_leaf_monthly_v1
            GROUP BY month_start, product_segment, category_segment
            HAVING COUNT(*) > 1
        ) duplicates;
    """,
    "monthly_incomplete_leaf_grids": """
        SELECT COUNT(*)
        FROM (
            SELECT month_start
            FROM public.vw_weighted_enquiry_leaf_monthly_v1
            GROUP BY month_start
            HAVING COUNT(*) <> 8
               OR COUNT(DISTINCT (product_segment, category_segment)) <> 8
        ) invalid;
    """,
    "monthly_missing_or_unexpected_grid_rows": """
        WITH expected AS (
            SELECT
                month_start::DATE,
                product_segment,
                category_segment
            FROM generate_series(
                DATE '2022-01-01',
                (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::DATE,
                INTERVAL '1 month'
            ) month_start
            CROSS JOIN (
                VALUES ('Non-Combustible'), ('Combustible')
            ) products(product_segment)
            CROSS JOIN (
                VALUES
                    ('Data Centres'),
                    ('Education'),
                    ('Apartments/Housing'),
                    ('Other')
            ) categories(category_segment)
        ),
        differences AS (
            (
                SELECT month_start, product_segment, category_segment FROM expected
                EXCEPT
                SELECT month_start, product_segment, category_segment
                FROM public.vw_weighted_enquiry_leaf_monthly_v1
            )
            UNION ALL
            (
                SELECT month_start, product_segment, category_segment
                FROM public.vw_weighted_enquiry_leaf_monthly_v1
                EXCEPT
                SELECT month_start, product_segment, category_segment FROM expected
            )
        )
        SELECT COUNT(*) FROM differences;
    """,
    "monthly_invalid_values": """
        SELECT COUNT(*)
        FROM public.vw_weighted_enquiry_leaf_monthly_v1
        WHERE actual_weighted_enquiry_value < 0
           OR actual_weighted_enquiry_value::TEXT IN (
                'NaN', 'Infinity', '-Infinity'
           );
    """,
    "monthly_allocation_reconciliation_mismatches": """
        WITH allocated AS (
            SELECT
                enquiry_month AS month_start,
                product_segment,
                category_segment,
                SUM(allocated_weighted_enquiry_value) AS expected_value
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY enquiry_month, product_segment, category_segment
        )
        SELECT COUNT(*)
        FROM public.vw_weighted_enquiry_leaf_monthly_v1 monthly
        LEFT JOIN allocated
          USING (month_start, product_segment, category_segment)
        WHERE ABS(
            monthly.actual_weighted_enquiry_value
            - COALESCE(allocated.expected_value, 0)
        ) > 0.01;
    """,
    "monthly_overall_view_reconciliation_mismatches": """
        WITH leaf_totals AS (
            SELECT month_start, SUM(actual_weighted_enquiry_value) AS weighted_value
            FROM public.vw_weighted_enquiry_leaf_monthly_v1
            GROUP BY month_start
        ),
        comparison AS (
            SELECT
                COALESCE(leaf.month_start, overall.enquiry_month) AS month_start,
                leaf.weighted_value AS leaf_value,
                overall.weighted_enquiry_value AS overall_value
            FROM leaf_totals leaf
            FULL OUTER JOIN public.vw_weighted_enquiry_value_monthly_v1 overall
                ON overall.enquiry_month = leaf.month_start
        )
        SELECT COUNT(*)
        FROM comparison
        WHERE leaf_value IS NULL
           OR overall_value IS NULL
           OR ABS(leaf_value - overall_value) > 0.01;
    """,
}

ALLOCATION_KEY = [
    "project_id",
    "enquiry_month",
    "product_segment",
    "category_segment",
]
ALLOCATION_TEXT_COLUMNS = [
    "product_allocation_method",
    "category_allocation_method",
    "product_mapping_status",
    "category_mapping_status",
]
ALLOCATION_NUMERIC_TOLERANCES = {
    "allocation_share": 1e-9,
    "project_weighted_enquiry_value": 0.01,
    "allocated_weighted_enquiry_value": 0.01,
    "subitem_source_value_total": 0.01,
}
MONTHLY_KEY = ["month_start", "product_segment", "category_segment"]


@dataclass(frozen=True)
class ValidationResult:
    name: str
    value: int
    passed: bool
    detail: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate segmented weighted-enquiry SQL invariants and frozen "
            "Python-allocation parity using SELECT queries only."
        )
    )
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        default=DEFAULT_BASELINE_DIR,
        help="Frozen Phase 1 baseline directory.",
    )
    parser.add_argument(
        "--skip-baseline",
        action="store_true",
        help="Run live SQL invariants without comparing the mutable source to the snapshot.",
    )
    parser.add_argument(
        "--monthly-output",
        type=Path,
        help=(
            "Write the validated public.vw_weighted_enquiry_leaf_monthly_v1 "
            "rows to this CSV after every requested check passes."
        ),
    )
    return parser.parse_args()


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL environment variable is required")
    return dsn


def _scalar(
    cursor: psycopg.Cursor[Any], sql: str, params: Sequence[object] = ()
) -> int:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return int(row[0] or 0) if row else 0


def _frame(cursor: psycopg.Cursor[Any], sql: str) -> pd.DataFrame:
    cursor.execute(sql)
    rows = cursor.fetchall()
    columns = [column.name for column in cursor.description or ()]
    return pd.DataFrame(rows, columns=columns)


def _live_results(cursor: psycopg.Cursor[Any]) -> list[ValidationResult]:
    results: list[ValidationResult] = []
    for relation_name in REQUIRED_RELATIONS:
        missing = int(
            _scalar(cursor, "SELECT (to_regclass(%s) IS NULL)::INT;", (relation_name,))
        )
        results.append(
            ValidationResult(
                name=f"required_relation:{relation_name}",
                value=missing,
                passed=missing == 0,
            )
        )
    if any(not result.passed for result in results):
        return results

    for name, sql in CHECKS.items():
        value = _scalar(cursor, sql)
        results.append(ValidationResult(name=name, value=value, passed=value == 0))
    return results


def _prepare_allocation_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    prepared["project_id"] = prepared["project_id"].astype("string")
    prepared["enquiry_month"] = pd.to_datetime(prepared["enquiry_month"])
    return prepared


def _prepare_monthly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    prepared["month_start"] = pd.to_datetime(prepared["month_start"])
    return prepared


def _key_results(
    merged: pd.DataFrame,
    prefix: str,
) -> list[ValidationResult]:
    missing = int((merged["_merge"] == "left_only").sum())
    extra = int((merged["_merge"] == "right_only").sum())
    return [
        ValidationResult(
            name=f"{prefix}_missing_sql_rows",
            value=missing,
            passed=missing == 0,
        ),
        ValidationResult(
            name=f"{prefix}_unexpected_sql_rows",
            value=extra,
            passed=extra == 0,
        ),
    ]


def _baseline_allocation_results(
    cursor: psycopg.Cursor[Any], baseline_dir: Path
) -> list[ValidationResult]:
    baseline_path = baseline_dir / "project_leaf_allocations.csv"
    if not baseline_path.is_file():
        return [
            ValidationResult(
                name="baseline_project_allocations_file_missing",
                value=1,
                passed=False,
                detail=str(baseline_path),
            )
        ]

    baseline = _prepare_allocation_frame(
        pd.read_csv(baseline_path, dtype={"project_id": "string"})
    )
    sql = _prepare_allocation_frame(
        _frame(
            cursor,
            """
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
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1;
            """,
        )
    )
    comparison_columns = (
        ALLOCATION_KEY
        + ALLOCATION_TEXT_COLUMNS
        + list(ALLOCATION_NUMERIC_TOLERANCES)
    )
    try:
        merged = baseline[comparison_columns].merge(
            sql[comparison_columns],
            on=ALLOCATION_KEY,
            how="outer",
            suffixes=("_baseline", "_sql"),
            indicator=True,
            validate="one_to_one",
        )
    except pd.errors.MergeError as exc:
        return [
            ValidationResult(
                name="baseline_project_allocation_duplicate_keys",
                value=1,
                passed=False,
                detail=str(exc),
            )
        ]

    results = _key_results(merged, "baseline_project_allocations")
    matched = merged.loc[merged["_merge"] == "both"]
    for column in ALLOCATION_TEXT_COLUMNS:
        baseline_values = matched[f"{column}_baseline"].astype("string").fillna("<NULL>")
        sql_values = matched[f"{column}_sql"].astype("string").fillna("<NULL>")
        mismatches = int((baseline_values != sql_values).sum())
        results.append(
            ValidationResult(
                name=f"baseline_project_allocations_{column}_mismatches",
                value=mismatches,
                passed=mismatches == 0,
            )
        )
    for column, tolerance in ALLOCATION_NUMERIC_TOLERANCES.items():
        baseline_values = pd.to_numeric(matched[f"{column}_baseline"], errors="coerce")
        sql_values = pd.to_numeric(matched[f"{column}_sql"], errors="coerce")
        matches = np.isclose(
            baseline_values,
            sql_values,
            rtol=0.0,
            atol=tolerance,
            equal_nan=True,
        )
        mismatches = int((~matches).sum())
        results.append(
            ValidationResult(
                name=f"baseline_project_allocations_{column}_mismatches",
                value=mismatches,
                passed=mismatches == 0,
                detail=f"absolute tolerance={tolerance}",
            )
        )
    return results


def _baseline_monthly_results(
    cursor: psycopg.Cursor[Any], baseline_dir: Path
) -> list[ValidationResult]:
    baseline_path = baseline_dir / "monthly_leaf_actuals.csv"
    if not baseline_path.is_file():
        return [
            ValidationResult(
                name="baseline_monthly_actuals_file_missing",
                value=1,
                passed=False,
                detail=str(baseline_path),
            )
        ]

    baseline = _prepare_monthly_frame(pd.read_csv(baseline_path))
    sql = _prepare_monthly_frame(
        _frame(
            cursor,
            """
            SELECT
                month_start,
                product_segment,
                category_segment,
                actual_weighted_enquiry_value
            FROM public.vw_weighted_enquiry_leaf_monthly_v1;
            """,
        )
    )
    try:
        merged = baseline.merge(
            sql,
            on=MONTHLY_KEY,
            how="outer",
            suffixes=("_baseline", "_sql"),
            indicator=True,
            validate="one_to_one",
        )
    except pd.errors.MergeError as exc:
        return [
            ValidationResult(
                name="baseline_monthly_actuals_duplicate_keys",
                value=1,
                passed=False,
                detail=str(exc),
            )
        ]

    results = _key_results(merged, "baseline_monthly_actuals")
    matched = merged.loc[merged["_merge"] == "both"]
    values_match = np.isclose(
        pd.to_numeric(
            matched["actual_weighted_enquiry_value_baseline"], errors="coerce"
        ),
        pd.to_numeric(matched["actual_weighted_enquiry_value_sql"], errors="coerce"),
        rtol=0.0,
        atol=0.01,
        equal_nan=True,
    )
    mismatches = int((~values_match).sum())
    results.append(
        ValidationResult(
            name="baseline_monthly_actual_value_mismatches",
            value=mismatches,
            passed=mismatches == 0,
            detail="absolute tolerance=0.01",
        )
    )
    return results


def _print_results(results: list[ValidationResult]) -> None:
    print(f"{'CHECK':70} {'VALUE':>10}  STATUS")
    print("-" * 95)
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{result.name:70} {result.value:10d}  {status}")
        if result.detail and not result.passed:
            print(f"  {result.detail}")


def main() -> int:
    args = parse_args()
    results: list[ValidationResult] = []
    monthly_export: pd.DataFrame | None = None

    with psycopg.connect(_get_dsn(), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
            live_results = _live_results(cursor)
            results.extend(live_results)
            if all(result.passed for result in live_results) and not args.skip_baseline:
                baseline_dir = args.baseline_dir.resolve()
                results.extend(_baseline_allocation_results(cursor, baseline_dir))
                results.extend(_baseline_monthly_results(cursor, baseline_dir))
            if all(result.passed for result in live_results) and args.monthly_output:
                monthly_export = _prepare_monthly_frame(
                    _frame(
                        cursor,
                        """
                        SELECT
                            month_start,
                            product_segment,
                            category_segment,
                            actual_weighted_enquiry_value
                        FROM public.vw_weighted_enquiry_leaf_monthly_v1
                        ORDER BY month_start, product_segment, category_segment;
                        """,
                    )
                )

    _print_results(results)
    failed = [result for result in results if not result.passed]
    if failed:
        print(f"\nValidation failed: {len(failed)} check(s) did not pass.")
        return 1

    if monthly_export is not None:
        monthly_output = args.monthly_output.resolve()
        monthly_output.parent.mkdir(parents=True, exist_ok=True)
        monthly_export.to_csv(monthly_output, index=False)
        print(f"\nValidated monthly SQL rows written to: {monthly_output}")

    if args.skip_baseline:
        print("\nAll live segmented weighted-enquiry SQL validations passed.")
    else:
        print(
            "\nAll live SQL validations and frozen Python-baseline parity checks passed."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())