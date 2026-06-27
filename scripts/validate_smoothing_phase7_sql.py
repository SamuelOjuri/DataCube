from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Sequence

import psycopg
from dotenv import load_dotenv

load_dotenv()

CHECKS: dict[str, str] = {
    "project_invoice_spread_generated_column_violations": """
        SELECT COUNT(*)
        FROM projects
        WHERE invoicing_spread_days < 0
           OR (
                first_date_invoiced IS NOT NULL
                AND last_date_invoiced IS NOT NULL
                AND invoicing_spread_days IS DISTINCT FROM GREATEST(
                    last_date_invoiced - first_date_invoiced,
                    0
                )
           )
           OR (
                (first_date_invoiced IS NULL OR last_date_invoiced IS NULL)
                AND invoicing_spread_days IS NOT NULL
           );
    """,
    "project_invoice_rollup_subitem_mismatches": """
        WITH subitem_rollups AS (
            SELECT
                parent_monday_id,
                MIN(invoice_date)::DATE AS expected_first_date_invoiced,
                MAX(invoice_date)::DATE AS expected_last_date_invoiced
            FROM subitems
            WHERE parent_monday_id IS NOT NULL
              AND invoice_date IS NOT NULL
            GROUP BY parent_monday_id
        )
        SELECT COUNT(*)
        FROM subitem_rollups r
        INNER JOIN projects p
            ON p.monday_id = r.parent_monday_id
        WHERE p.first_date_invoiced IS DISTINCT FROM r.expected_first_date_invoiced
           OR p.last_date_invoiced IS DISTINCT FROM r.expected_last_date_invoiced
           OR p.invoicing_spread_days IS DISTINCT FROM GREATEST(
                r.expected_last_date_invoiced - r.expected_first_date_invoiced,
                0
           );
    """,
    "smoothing_unsplit_product_account_tokens": """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_signal_members_v1
        WHERE dimension IN ('product', 'account')
          AND (
              group_key LIKE '%%,%%'
              OR group_display LIKE '%%,%%'
          );
    """,
    "smoothing_duplicate_product_account_memberships": """
        SELECT COUNT(*)
        FROM (
            SELECT project_id, dimension, group_key
            FROM vw_invoice_smoothing_signal_members_v1
            WHERE dimension IN ('product', 'account')
            GROUP BY project_id, dimension, group_key
            HAVING COUNT(*) > 1
        ) duplicates;
    """,
    "pipeline_smoothing_fallback_flag_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE category_signal_fallback_used IS DISTINCT FROM (
                category_candidate_token_count = 0
                OR category_matched_signal_count < category_candidate_token_count
            )
           OR type_signal_fallback_used IS DISTINCT FROM (
                type_candidate_token_count = 0
                OR type_matched_signal_count < type_candidate_token_count
            )
           OR product_signal_fallback_used IS DISTINCT FROM (
                product_candidate_token_count = 0
                OR product_matched_signal_count < product_candidate_token_count
            )
           OR account_signal_fallback_used IS DISTINCT FROM (
                account_candidate_token_count = 0
                OR account_matched_signal_count < account_candidate_token_count
            );
    """,
    "latest_smoothing_snapshot_project_sumback_mismatches": """
        WITH latest_snapshot AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM pipeline_smoothing_forecast_snapshot
        ),
        per_project AS (
            SELECT
                s.project_id,
                MAX(s.expected_value) AS expected_value,
                SUM(s.allocated_expected_value) AS allocated_expected_value
            FROM pipeline_smoothing_forecast_snapshot s
            CROSS JOIN latest_snapshot l
            WHERE s.snapshot_date = l.snapshot_date
            GROUP BY s.project_id
        )
        SELECT COUNT(*)
        FROM per_project
        WHERE ABS(expected_value - allocated_expected_value) > GREATEST(
            0.25::NUMERIC,
            ABS(expected_value) * 0.000001::NUMERIC
        );
    """,
    "latest_smoothing_snapshot_missing_project_fields": """
        WITH latest_snapshot AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM pipeline_smoothing_forecast_snapshot
        )
        SELECT COUNT(*)
        FROM pipeline_smoothing_forecast_snapshot s
        CROSS JOIN latest_snapshot l
        WHERE s.snapshot_date = l.snapshot_date
          AND (
              s.base_forecast_month IS NULL
              OR s.forecast_date IS NULL
              OR s.combined_smoothed_probability IS NULL
              OR s.expected_spread_days IS NULL
              OR s.source_view <> 'vw_pipeline_smoothing_score_v1'
          );
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
    results: list[ValidationResult] = []
    with psycopg.connect(_get_dsn()) as conn:
        with conn.cursor() as cur:
            for name, sql in CHECKS.items():
                value = _scalar(cur, sql)
                results.append(ValidationResult(name=name, value=value, passed=(value == 0)))

    print(f"{'CHECK':55} {'VALUE':>10}  STATUS")
    print("-" * 82)
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{result.name:55} {result.value:10d}  {status}")

    failed = [result for result in results if not result.passed]
    if failed:
        print(f"\nValidation failed: {len(failed)} check(s) have non-zero violations.")
        return 1

    print("\nAll Phase 7 smoothing SQL validations passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())