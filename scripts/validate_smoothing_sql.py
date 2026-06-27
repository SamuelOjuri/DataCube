from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date
from typing import Sequence

import psycopg
from dotenv import load_dotenv

load_dotenv()

REQUIRED_RELATIONS = (
    "public.vw_invoice_smoothing_training_v1",
    "public.vw_invoice_smoothing_signal_members_v1",
    "public.mv_invoice_smoothing_signal_v1",
    "public.vw_invoice_smoothing_global_signal_v1",
    "public.vw_pipeline_smoothing_score_v1",
    "public.mv_pipeline_smoothed_revenue_monthly_12m_v1",
    "public.pipeline_smoothing_forecast_snapshot",
)

REQUIRED_FUNCTIONS = (
    "public.invoice_smoothing_as_of_date()",
    "public.smoothing_normalize_token(text)",
    "public.invoice_smoothing_training_rows(date)",
    "public.invoice_smoothing_signal_members(date)",
    "public.invoice_smoothing_signal_rows(date)",
    "public.refresh_invoice_smoothing_signal_v1(date)",
    "public.create_pipeline_smoothing_forecast_snapshot(date)",
    "public.cleanup_old_pipeline_smoothing_forecast_snapshots(integer)",
)

CHECKS: dict[str, str] = {
    "training_negative_spread_rows": """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_training_v1
        WHERE invoicing_spread_days < 0
           OR last_date_invoiced < first_date_invoiced;
    """,
    "training_smoothing_definition_mismatches": """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_training_v1
        WHERE is_smoothed IS DISTINCT FROM (last_date_invoiced > first_date_invoiced);
    """,
    "training_maturity_definition_mismatches": """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_training_v1
        WHERE is_mature IS DISTINCT FROM (first_date_invoiced <= mature_cutoff_date);
    """,
    "membership_duplicate_project_tokens": """
        SELECT COUNT(*)
        FROM (
            SELECT project_id, dimension, group_key
            FROM vw_invoice_smoothing_signal_members_v1
            GROUP BY project_id, dimension, group_key
            HAVING COUNT(*) > 1
        ) duplicates;
    """,
    "membership_invalid_dimensions": """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_signal_members_v1
        WHERE dimension NOT IN ('category', 'type', 'category_type', 'product', 'account');
    """,
    "membership_comma_token_expansion_mismatches": """
        WITH product_expected AS (
            SELECT
                t.project_id,
                COUNT(DISTINCT public.smoothing_normalize_token(product_token.token)) AS expected_count
            FROM vw_invoice_smoothing_training_v1 t
            CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
                COALESCE(t.raw_product_mirror, ''),
                '[[:space:]]*,[[:space:]]*'
            ) AS product_token(token)
            WHERE t.raw_product_mirror LIKE '%%,%%'
              AND public.smoothing_normalize_token(product_token.token) IS NOT NULL
            GROUP BY t.project_id
        ),
        account_expected AS (
            SELECT
                t.project_id,
                COUNT(DISTINCT public.smoothing_normalize_token(account_token.token)) AS expected_count
            FROM vw_invoice_smoothing_training_v1 t
            CROSS JOIN LATERAL REGEXP_SPLIT_TO_TABLE(
                COALESCE(t.raw_account_mirror, ''),
                '[[:space:]]*,[[:space:]]*'
            ) AS account_token(token)
            WHERE t.raw_account_mirror LIKE '%%,%%'
              AND public.smoothing_normalize_token(account_token.token) IS NOT NULL
            GROUP BY t.project_id
        ),
        product_actual AS (
            SELECT project_id, COUNT(*) AS actual_count
            FROM vw_invoice_smoothing_signal_members_v1
            WHERE dimension = 'product'
            GROUP BY project_id
        ),
        account_actual AS (
            SELECT project_id, COUNT(*) AS actual_count
            FROM vw_invoice_smoothing_signal_members_v1
            WHERE dimension = 'account'
            GROUP BY project_id
        ),
        mismatches AS (
            SELECT pe.project_id, 'product' AS dimension
            FROM product_expected pe
            LEFT JOIN product_actual pa
                ON pa.project_id = pe.project_id
            WHERE COALESCE(pa.actual_count, 0) < pe.expected_count

            UNION ALL

            SELECT ae.project_id, 'account' AS dimension
            FROM account_expected ae
            LEFT JOIN account_actual aa
                ON aa.project_id = ae.project_id
            WHERE COALESCE(aa.actual_count, 0) < ae.expected_count
        )
        SELECT COUNT(*)
        FROM mismatches;
    """,
    "signal_invalid_dimensions": """
        SELECT COUNT(*)
        FROM mv_invoice_smoothing_signal_v1
        WHERE dimension NOT IN ('global', 'category', 'type', 'category_type', 'product', 'account');
    """,
    "signal_probability_bounds_violations": """
        SELECT COUNT(*)
        FROM mv_invoice_smoothing_signal_v1
        WHERE all_smoothed_pct NOT BETWEEN 0 AND 1
           OR mature_smoothed_pct NOT BETWEEN 0 AND 1
           OR shrunk_mature_smoothed_pct NOT BETWEEN 0 AND 1
           OR rate_90_plus_days NOT BETWEEN 0 AND 1
           OR rate_180_plus_days NOT BETWEEN 0 AND 1;
    """,
    "signal_negative_metric_rows": """
        SELECT COUNT(*)
        FROM mv_invoice_smoothing_signal_v1
        WHERE all_project_count < 0
           OR all_smoothed_count < 0
           OR mature_project_count < 0
           OR mature_smoothed_count < 0
           OR mature_spread_days_sum < 0
           OR mature_avg_spread_days < 0
           OR mature_expected_spread_days < 0
           OR avg_spread_if_smoothed < 0
           OR median_smoothed_days < 0
           OR p75_smoothed_days < 0;
    """,
    "signal_invalid_confidence_rows": """
        SELECT COUNT(*)
        FROM mv_invoice_smoothing_signal_v1
        WHERE confidence NOT IN ('High', 'Medium', 'Low', 'Very low');
    """,
    "global_signal_row_count_mismatch": """
        SELECT CASE WHEN COUNT(*) = 1 THEN 0 ELSE COUNT(*) END
        FROM mv_invoice_smoothing_signal_v1
        WHERE dimension = 'global'
          AND group_key = '__global__';
    """,
    "shrunk_rate_formula_mismatches": """
        WITH signal_as_of AS (
            SELECT as_of_date
            FROM public.mv_invoice_smoothing_signal_v1
            WHERE dimension = 'global'
              AND group_key = '__global__'
        ),
        global_stats AS (
            SELECT
                COUNT(*) FILTER (WHERE t.is_mature) AS global_mature_project_count,
                COUNT(*) FILTER (WHERE t.is_mature AND t.is_smoothed) AS global_mature_smoothed_count
            FROM public.invoice_smoothing_training_rows((SELECT as_of_date FROM signal_as_of)::DATE) t
        ),
        global_rates AS (
            SELECT
                CASE
                    WHEN gs.global_mature_project_count > 0 THEN
                        gs.global_mature_smoothed_count::NUMERIC / gs.global_mature_project_count
                    ELSE 0::NUMERIC
                END AS global_mature_rate
            FROM global_stats gs
        )
        SELECT COUNT(*)
        FROM public.mv_invoice_smoothing_signal_v1 s
        CROSS JOIN global_rates gr
        WHERE s.dimension <> 'global'
          AND s.shrunk_mature_smoothed_pct IS DISTINCT FROM ROUND(
              (
                  s.mature_smoothed_count::NUMERIC
                  + s.shrinkage_k::NUMERIC * gr.global_mature_rate
              ) / (s.mature_project_count::NUMERIC + s.shrinkage_k::NUMERIC),
              6
          )::NUMERIC(8,6);
    """,
    "shrunk_spread_formula_mismatches": """
        WITH signal_as_of AS (
            SELECT as_of_date
            FROM public.mv_invoice_smoothing_signal_v1
            WHERE dimension = 'global'
              AND group_key = '__global__'
        ),
        global_stats AS (
            SELECT
                COUNT(*) FILTER (WHERE t.is_mature) AS global_mature_project_count,
                COALESCE(
                    SUM(t.invoicing_spread_days) FILTER (WHERE t.is_mature),
                    0
                )::NUMERIC AS global_mature_spread_days_sum
            FROM public.invoice_smoothing_training_rows((SELECT as_of_date FROM signal_as_of)::DATE) t
        ),
        global_rates AS (
            SELECT
                CASE
                    WHEN gs.global_mature_project_count > 0 THEN
                        gs.global_mature_spread_days_sum / gs.global_mature_project_count
                    ELSE 0::NUMERIC
                END AS global_mature_avg_spread_days
            FROM global_stats gs
        )
        SELECT COUNT(*)
        FROM public.mv_invoice_smoothing_signal_v1 s
        CROSS JOIN global_rates gr
        WHERE s.dimension <> 'global'
          AND s.mature_expected_spread_days IS DISTINCT FROM ROUND(
              (
                  s.mature_spread_days_sum
                  + s.shrinkage_k::NUMERIC * gr.global_mature_avg_spread_days
              ) / (s.mature_project_count::NUMERIC + s.shrinkage_k::NUMERIC),
              2
          )::NUMERIC(10,2);
    """,
    "pipeline_smoothing_probability_bounds_violations": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE combined_smoothed_probability NOT BETWEEN 0 AND 1
           OR category_rate NOT BETWEEN 0 AND 1
           OR type_rate NOT BETWEEN 0 AND 1
           OR product_rate NOT BETWEEN 0 AND 1
           OR account_rate NOT BETWEEN 0 AND 1;
    """,
    "pipeline_smoothing_negative_metric_rows": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE expected_spread_days < 0
           OR category_spread_days < 0
           OR type_spread_days < 0
           OR product_spread_days < 0
           OR account_spread_days < 0
           OR smoothed_expected_value < 0
           OR unsmoothed_expected_value < 0;
    """,
    "pipeline_smoothing_invalid_confidence_rows": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE category_confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR type_confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR product_confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR account_confidence NOT IN ('High', 'Medium', 'Low', 'Very low');
    """,
    "pipeline_smoothing_weighted_probability_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE combined_smoothed_probability IS DISTINCT FROM ROUND(
            (
                category_rate * 0.30::NUMERIC +
                type_rate * 0.10::NUMERIC +
                product_rate * 0.30::NUMERIC +
                account_rate * 0.30::NUMERIC
            ),
            6
        )::NUMERIC(8,6);
    """,
    "pipeline_smoothing_weighted_spread_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE expected_spread_days IS DISTINCT FROM ROUND(
            (
                category_spread_days * 0.30::NUMERIC +
                type_spread_days * 0.10::NUMERIC +
                product_spread_days * 0.30::NUMERIC +
                account_spread_days * 0.30::NUMERIC
            ),
            2
        )::NUMERIC(10,2);
    """,
    "pipeline_smoothing_risk_band_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE risk_band <> CASE
            WHEN combined_smoothed_probability >= 0.55::NUMERIC THEN 'Very High'
            WHEN combined_smoothed_probability >= 0.45::NUMERIC THEN 'High'
            WHEN combined_smoothed_probability >= 0.35::NUMERIC THEN 'Moderate'
            ELSE 'Low'
        END;
    """,
    "pipeline_smoothing_treatment_mismatches": """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE workbook_suggested_treatment <> CASE risk_band
            WHEN 'Very High' THEN 'Adopt smoothing by default'
            WHEN 'High' THEN 'Model smoothing scenario'
            WHEN 'Moderate' THEN 'Commercial review / light smoothing'
            ELSE 'No default smoothing'
        END
           OR default_smoothing_recommended IS DISTINCT FROM (
                risk_band = 'Very High'
                OR (
                    expected_spread_days >= 120::NUMERIC
                    AND risk_band IN ('High', 'Moderate')
                )
           );
    """,
    "pipeline_smoothing_missing_token_fallback_mismatches": """
        WITH global_signal AS (
            SELECT shrunk_mature_smoothed_pct, mature_expected_spread_days
            FROM public.mv_invoice_smoothing_signal_v1
            WHERE dimension = 'global'
              AND group_key = '__global__'
        )
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1 s
        CROSS JOIN global_signal g
        WHERE (
                s.category_candidate_token_count = 0
                AND (
                    s.category_signal_fallback_used IS DISTINCT FROM TRUE
                    OR s.category_rate IS DISTINCT FROM g.shrunk_mature_smoothed_pct
                    OR s.category_spread_days IS DISTINCT FROM g.mature_expected_spread_days
                )
            )
           OR (
                s.type_candidate_token_count = 0
                AND (
                    s.type_signal_fallback_used IS DISTINCT FROM TRUE
                    OR s.type_rate IS DISTINCT FROM g.shrunk_mature_smoothed_pct
                    OR s.type_spread_days IS DISTINCT FROM g.mature_expected_spread_days
                )
            )
           OR (
                s.product_candidate_token_count = 0
                AND (
                    s.product_signal_fallback_used IS DISTINCT FROM TRUE
                    OR s.product_rate IS DISTINCT FROM g.shrunk_mature_smoothed_pct
                    OR s.product_spread_days IS DISTINCT FROM g.mature_expected_spread_days
                )
            )
           OR (
                s.account_candidate_token_count = 0
                AND (
                    s.account_signal_fallback_used IS DISTINCT FROM TRUE
                    OR s.account_rate IS DISTINCT FROM g.shrunk_mature_smoothed_pct
                    OR s.account_spread_days IS DISTINCT FROM g.mature_expected_spread_days
                )
            );
    """,
    "smoothed_monthly_window_out_of_range_rows": """
        WITH bounds AS (
            SELECT
                DATE_TRUNC('month', CURRENT_DATE)::DATE AS window_start,
                (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '12 months')::DATE AS window_end
        )
        SELECT COUNT(*)
        FROM mv_pipeline_smoothed_revenue_monthly_12m_v1 mv
        CROSS JOIN bounds b
        WHERE mv.forecast_month < b.window_start
           OR mv.forecast_month >= b.window_end;
    """,
    "smoothed_monthly_negative_metric_rows": """
        SELECT COUNT(*)
        FROM mv_pipeline_smoothed_revenue_monthly_12m_v1
        WHERE project_count < 0
           OR unsmoothed_project_count < 0
           OR smoothed_project_count < 0
           OR unsmoothed_expected_value < 0
           OR smoothed_expected_value < 0
           OR allocated_expected_value < 0;
    """,
    "smoothed_monthly_component_mismatches": """
        SELECT COUNT(*)
        FROM mv_pipeline_smoothed_revenue_monthly_12m_v1
        WHERE ABS(
            allocated_expected_value
            - (unsmoothed_expected_value + smoothed_expected_value)
        ) > 0.02::NUMERIC;
    """,
    "smoothed_monthly_stage_total_mismatches": """
        WITH bounds AS (
            SELECT
                DATE_TRUNC('month', CURRENT_DATE)::DATE AS window_start,
                (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '12 months')::DATE AS window_end
        ),
        expected_by_stage AS (
            SELECT
                s.stage_bucket,
                ROUND(SUM(COALESCE(s.expected_value, 0::NUMERIC)), 2)::NUMERIC(14,2) AS expected_value
            FROM vw_pipeline_smoothing_score_v1 s
            CROSS JOIN bounds b
            WHERE s.forecast_month >= b.window_start
              AND s.forecast_month < b.window_end
            GROUP BY s.stage_bucket
        ),
        allocated_by_stage AS (
            SELECT
                mv.stage_bucket,
                ROUND(SUM(COALESCE(mv.allocated_expected_value, 0::NUMERIC)), 2)::NUMERIC(14,2)
                    AS allocated_expected_value
            FROM mv_pipeline_smoothed_revenue_monthly_12m_v1 mv
            GROUP BY mv.stage_bucket
        )
        SELECT COUNT(*)
        FROM expected_by_stage e
        FULL OUTER JOIN allocated_by_stage a
            ON a.stage_bucket = e.stage_bucket
        WHERE ABS(
            COALESCE(e.expected_value, 0::NUMERIC)
            - COALESCE(a.allocated_expected_value, 0::NUMERIC)
        ) > GREATEST(
            0.25::NUMERIC,
            ABS(COALESCE(e.expected_value, 0::NUMERIC)) * 0.000001::NUMERIC
        );
    """,
    "smoothing_snapshot_latest_invalid_rows": """
        WITH latest_snapshot AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM public.pipeline_smoothing_forecast_snapshot
        )
        SELECT COUNT(*)
        FROM public.pipeline_smoothing_forecast_snapshot s
        CROSS JOIN latest_snapshot latest
        WHERE latest.snapshot_date IS NOT NULL
          AND s.snapshot_date = latest.snapshot_date
          AND (
              s.project_id LIKE 'monthly:%%'
              OR s.forecast_month < DATE_TRUNC('month', s.snapshot_date)::DATE
              OR s.forecast_month >= (DATE_TRUNC('month', s.snapshot_date) + INTERVAL '12 months')::DATE
              OR s.forecast_month < s.base_forecast_month
              OR s.forecast_date IS NULL
              OR s.stage_bucket NOT IN ('Committed', 'Open', 'Lost')
              OR s.combined_smoothed_probability NOT BETWEEN 0 AND 1
              OR s.expected_spread_days < 0
              OR COALESCE(s.risk_band, 'Low') NOT IN ('Very High', 'High', 'Moderate', 'Low')
              OR s.expected_value < 0
              OR s.unsmoothed_allocated_value < 0
              OR s.smoothed_allocated_value < 0
              OR s.allocated_expected_value < 0
              OR ABS(
                    s.allocated_expected_value
                    - (s.unsmoothed_allocated_value + s.smoothed_allocated_value)
                 ) > 0.02::NUMERIC
          );
    """,
    "smoothing_snapshot_latest_monthly_total_mismatches": """
        WITH latest_snapshot AS (
            SELECT MAX(snapshot_date) AS snapshot_date
            FROM public.pipeline_smoothing_forecast_snapshot
        ),
        snapshot_totals AS (
            SELECT
                s.stage_bucket,
                ROUND(SUM(s.allocated_expected_value), 2)::NUMERIC(14,2) AS allocated_expected_value
            FROM public.pipeline_smoothing_forecast_snapshot s
            CROSS JOIN latest_snapshot latest
            WHERE latest.snapshot_date IS NOT NULL
              AND s.snapshot_date = latest.snapshot_date
            GROUP BY s.stage_bucket
        ),
        monthly_totals AS (
            SELECT
                mv.stage_bucket,
                ROUND(SUM(mv.allocated_expected_value), 2)::NUMERIC(14,2) AS allocated_expected_value
            FROM public.mv_pipeline_smoothed_revenue_monthly_12m_v1 mv
            GROUP BY mv.stage_bucket
        )
        SELECT COUNT(*)
        FROM snapshot_totals s
        FULL OUTER JOIN monthly_totals m
            ON m.stage_bucket = s.stage_bucket
        WHERE ABS(
            COALESCE(s.allocated_expected_value, 0::NUMERIC)
            - COALESCE(m.allocated_expected_value, 0::NUMERIC)
        ) > GREATEST(
            0.25::NUMERIC,
            ABS(COALESCE(m.allocated_expected_value, 0::NUMERIC)) * 0.000001::NUMERIC
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


def _scalar(
    cur: psycopg.Cursor, sql: str, params: Sequence[object] = ()
) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def _relation_exists(cur: psycopg.Cursor, relation_name: str) -> bool:
    return bool(_scalar(cur, "SELECT (to_regclass(%s) IS NOT NULL)::int;", (relation_name,)))


def _function_exists(cur: psycopg.Cursor, function_signature: str) -> bool:
    return bool(
        _scalar(
            cur,
            "SELECT (to_regprocedure(%s) IS NOT NULL)::int;",
            (function_signature,),
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate invoice smoothing SQL artifacts")
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=None,
        help="Optional as-of date used when refreshing the smoothing signal materialized view",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh mv_invoice_smoothing_signal_v1 before running checks",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    dsn = _get_dsn()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            missing_relations = [
                relation for relation in REQUIRED_RELATIONS if not _relation_exists(cur, relation)
            ]
            missing_functions = [
                function for function in REQUIRED_FUNCTIONS if not _function_exists(cur, function)
            ]
            if missing_relations or missing_functions:
                if missing_relations:
                    print("Missing required smoothing relations:")
                    for relation in missing_relations:
                        print(f"  - {relation}")
                if missing_functions:
                    print("Missing required smoothing functions:")
                    for function in missing_functions:
                        print(f"  - {function}")
                return 2

            if args.refresh:
                cur.execute(
                    "SELECT refresh_invoice_smoothing_signal_v1(%s::date);",
                    (args.as_of_date,),
                )
                refreshed_as_of = cur.fetchone()[0]
                cur.execute(
                    "REFRESH MATERIALIZED VIEW public.mv_pipeline_smoothed_revenue_monthly_12m_v1;"
                )
                if _function_exists(cur, "public.create_pipeline_smoothing_forecast_snapshot(date)"):
                    cur.execute(
                        "SELECT create_pipeline_smoothing_forecast_snapshot(%s::date);",
                        (refreshed_as_of,),
                    )
                conn.commit()
                print(f"Refreshed mv_invoice_smoothing_signal_v1 for as_of_date={refreshed_as_of}")
                print("Refreshed mv_pipeline_smoothed_revenue_monthly_12m_v1")

            results: list[ValidationResult] = []
            for name, sql in CHECKS.items():
                value = _scalar(cur, sql)
                results.append(ValidationResult(name=name, value=value, passed=(value == 0)))

    print(f"{'CHECK':45} {'VALUE':>10}  STATUS")
    print("-" * 70)
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{result.name:45} {result.value:10d}  {status}")

    failed = [result for result in results if not result.passed]
    if failed:
        print(f"\nValidation failed: {len(failed)} check(s) have non-zero violations.")
        return 1

    print("\nAll smoothing SQL validations passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())