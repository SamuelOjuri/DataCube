from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Sequence

import psycopg

CHECKS: dict[str, str] = {
    "stage_bucket_domain_invalid_rows": """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE stage_bucket NOT IN ('Committed', 'Open', 'Lost');
    """,
    "stage_bucket_mapping_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE stage_bucket <> CASE
            WHEN pipeline_stage IN (
                'Won - Closed (Invoiced)',
                'Won - Open (Order Received)',
                'Won Via Other Ref'
            ) THEN 'Committed'
            WHEN pipeline_stage = 'Lost' THEN 'Lost'
            ELSE 'Open'
        END;
    """,
    "contract_value_fallback_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE contract_value <> COALESCE(
            NULLIF(total_order_value, 0),
            NULLIF(new_enquiry_value, 0),
            0
        )::NUMERIC(12,2);
    """,
    "monthly_window_out_of_range_rows": """
        WITH bounds AS (
            SELECT
                DATE_TRUNC('month', CURRENT_DATE)::DATE AS window_start,
                (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '12 months')::DATE AS window_end
        )
        SELECT COUNT(*)
        FROM mv_pipeline_forecast_monthly_12m_v1 mv
        CROSS JOIN bounds b
        WHERE mv.forecast_month < b.window_start
           OR mv.forecast_month >= b.window_end;
    """,
    "project_band_monotonicity_violations": """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE worst_case_value > expected_value
           OR expected_value > best_case_value;
    """,
    "monthly_band_monotonicity_violations": """
        SELECT COUNT(*)
        FROM mv_pipeline_forecast_monthly_12m_v1
        WHERE worst_case_value > expected_value
           OR expected_value > best_case_value;
    """,
}


@dataclass
class ValidationResult:
    name: str
    value: int
    passed: bool


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL environment variable is required")
    return dsn


def _scalar(cur: psycopg.Cursor, sql: str, params: Sequence[object] = ()) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def main() -> int:
    dsn = _get_dsn()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            relation_check_sql = """
                SELECT (
                    to_regclass('public.vw_pipeline_forecast_project_v1') IS NOT NULL
                    AND to_regclass('public.mv_pipeline_forecast_monthly_12m_v1') IS NOT NULL
                    AND to_regclass('public.pipeline_forecast_snapshot') IS NOT NULL
                )::int;
            """
            if not _scalar(cur, relation_check_sql):
                print("Missing one or more required forecast relations.")
                return 2

            results: list[ValidationResult] = []
            for name, sql in CHECKS.items():
                value = _scalar(cur, sql)
                results.append(ValidationResult(name=name, value=value, passed=(value == 0)))

    print(f"{'CHECK':45} {'VALUE':>10}  STATUS")
    print("-" * 70)
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{result.name:45} {result.value:10d}  {status}")

    failed = [r for r in results if not r.passed]
    if failed:
        print(f"\nValidation failed: {len(failed)} check(s) have non-zero violations.")
        return 1

    print("\nAll forecast SQL validations passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())