from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any, Sequence

import psycopg
import pytest

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


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        pytest.skip("SUPABASE_DB_URL is not set; skipping smoothing SQL validation tests")
    return dsn


@pytest.fixture(scope="module")
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_get_dsn()) as connection:
        yield connection


def _scalar(
    conn: psycopg.Connection[Any], sql: str, params: Sequence[Any] = ()
) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    assert row is not None, "Expected query to return one row"
    return row[0]


@pytest.mark.parametrize("relation_name", REQUIRED_RELATIONS)
def test_smoothing_relations_exist(
    conn: psycopg.Connection[Any], relation_name: str
) -> None:
    relation = _scalar(conn, "SELECT to_regclass(%s);", (relation_name,))
    assert relation is not None, f"Missing relation: {relation_name}"


@pytest.mark.parametrize("function_signature", REQUIRED_FUNCTIONS)
def test_smoothing_functions_exist(
    conn: psycopg.Connection[Any], function_signature: str
) -> None:
    function = _scalar(conn, "SELECT to_regprocedure(%s);", (function_signature,))
    assert function is not None, f"Missing function: {function_signature}"


def test_training_view_definitions_are_consistent(conn: psycopg.Connection[Any]) -> None:
    mismatches = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_training_v1
        WHERE invoicing_spread_days < 0
           OR last_date_invoiced < first_date_invoiced
           OR is_smoothed IS DISTINCT FROM (last_date_invoiced > first_date_invoiced)
           OR is_mature IS DISTINCT FROM (first_date_invoiced <= mature_cutoff_date);
        """,
    )
    assert mismatches == 0, f"Found {mismatches} inconsistent training rows"


def test_as_of_date_function_is_configurable(conn: psycopg.Connection[Any]) -> None:
    mismatches = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM invoice_smoothing_training_rows('2026-04-28'::date)
        WHERE as_of_date <> '2026-04-28'::date
           OR mature_cutoff_date <> '2025-10-30'::date;
        """,
    )
    assert mismatches == 0, "Explicit smoothing as_of_date did not produce the expected cutoff"


def test_signal_membership_dimensions_and_uniqueness(conn: psycopg.Connection[Any]) -> None:
    invalid_dimensions = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_signal_members_v1
        WHERE dimension NOT IN ('category', 'type', 'category_type', 'product', 'account');
        """,
    )
    assert invalid_dimensions == 0, f"Found {invalid_dimensions} invalid membership dimensions"

    duplicate_tokens = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT project_id, dimension, group_key
            FROM vw_invoice_smoothing_signal_members_v1
            GROUP BY project_id, dimension, group_key
            HAVING COUNT(*) > 1
        ) duplicates;
        """,
    )
    assert duplicate_tokens == 0, f"Found {duplicate_tokens} duplicate project-token memberships"


def test_product_and_account_comma_tokens_are_expanded_once(
    conn: psycopg.Connection[Any],
) -> None:
    violations = _scalar(
        conn,
        """
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
    )
    assert violations == 0, f"Found {violations} comma-token expansion mismatches"


def test_signal_bounds_and_global_row(conn: psycopg.Connection[Any]) -> None:
    global_rows = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM mv_invoice_smoothing_signal_v1
        WHERE dimension = 'global'
          AND group_key = '__global__';
        """,
    )
    assert global_rows == 1, f"Expected one global smoothing signal row, found {global_rows}"

    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM mv_invoice_smoothing_signal_v1
        WHERE dimension NOT IN ('global', 'category', 'type', 'category_type', 'product', 'account')
           OR all_smoothed_pct NOT BETWEEN 0 AND 1
           OR mature_smoothed_pct NOT BETWEEN 0 AND 1
           OR shrunk_mature_smoothed_pct NOT BETWEEN 0 AND 1
           OR rate_90_plus_days NOT BETWEEN 0 AND 1
           OR rate_180_plus_days NOT BETWEEN 0 AND 1
           OR confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR mature_expected_spread_days < 0;
        """,
    )
    assert violations == 0, f"Found {violations} smoothing signal bounds violations"


def test_shrunk_signal_formulas(conn: psycopg.Connection[Any]) -> None:
    rate_mismatches = _scalar(
        conn,
        """
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
    )
    assert rate_mismatches == 0, f"Found {rate_mismatches} shrunk-rate formula mismatches"

    spread_mismatches = _scalar(
        conn,
        """
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
    )
    assert spread_mismatches == 0, f"Found {spread_mismatches} shrunk-spread formula mismatches"


def test_pipeline_smoothing_score_bounds(conn: psycopg.Connection[Any]) -> None:
    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE combined_smoothed_probability NOT BETWEEN 0 AND 1
           OR expected_spread_days < 0
           OR smoothed_expected_value < 0
           OR unsmoothed_expected_value < 0
           OR risk_band NOT IN ('Very High', 'High', 'Moderate', 'Low')
           OR category_rate NOT BETWEEN 0 AND 1
           OR type_rate NOT BETWEEN 0 AND 1
           OR product_rate NOT BETWEEN 0 AND 1
           OR account_rate NOT BETWEEN 0 AND 1
           OR category_spread_days < 0
           OR type_spread_days < 0
           OR product_spread_days < 0
           OR account_spread_days < 0
           OR category_confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR type_confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR product_confidence NOT IN ('High', 'Medium', 'Low', 'Very low')
           OR account_confidence NOT IN ('High', 'Medium', 'Low', 'Very low');
        """,
    )
    assert violations == 0, f"Found {violations} pipeline smoothing score bounds violations"


def test_pipeline_smoothing_score_weighted_formulas(conn: psycopg.Connection[Any]) -> None:
    probability_mismatches = _scalar(
        conn,
        """
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
    )
    assert probability_mismatches == 0, (
        f"Found {probability_mismatches} smoothing probability formula mismatches"
    )

    spread_mismatches = _scalar(
        conn,
        """
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
    )
    assert spread_mismatches == 0, f"Found {spread_mismatches} smoothing spread formula mismatches"


def test_pipeline_smoothing_score_risk_bands_and_treatments(
    conn: psycopg.Connection[Any],
) -> None:
    risk_band_mismatches = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_pipeline_smoothing_score_v1
        WHERE risk_band <> CASE
            WHEN combined_smoothed_probability >= 0.55::NUMERIC THEN 'Very High'
            WHEN combined_smoothed_probability >= 0.45::NUMERIC THEN 'High'
            WHEN combined_smoothed_probability >= 0.35::NUMERIC THEN 'Moderate'
            ELSE 'Low'
        END;
        """,
    )
    assert risk_band_mismatches == 0, (
        f"Found {risk_band_mismatches} smoothing risk-band mismatches"
    )

    treatment_mismatches = _scalar(
        conn,
        """
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
    )
    assert treatment_mismatches == 0, (
        f"Found {treatment_mismatches} smoothing treatment mismatches"
    )


def test_pipeline_smoothing_score_global_fallback(conn: psycopg.Connection[Any]) -> None:
    missing_fallback_rows = _scalar(
        conn,
        """
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
    )
    assert missing_fallback_rows == 0, (
        f"Found {missing_fallback_rows} missing-token global fallback mismatches"
    )


def test_smoothed_monthly_revenue_window_and_bounds(
    conn: psycopg.Connection[Any],
) -> None:
    out_of_window = _scalar(
        conn,
        """
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
    )
    assert out_of_window == 0, f"Found {out_of_window} smoothed monthly rows outside 12-month window"

    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM mv_pipeline_smoothed_revenue_monthly_12m_v1
        WHERE project_count < 0
           OR unsmoothed_project_count < 0
           OR smoothed_project_count < 0
           OR unsmoothed_expected_value < 0
           OR smoothed_expected_value < 0
           OR allocated_expected_value < 0
           OR ABS(
                allocated_expected_value
                - (unsmoothed_expected_value + smoothed_expected_value)
           ) > 0.02::NUMERIC;
        """,
    )
    assert violations == 0, f"Found {violations} smoothed monthly bounds violations"


def test_smoothed_monthly_revenue_sums_to_project_expected_value(
    conn: psycopg.Connection[Any],
) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0, (
        f"Found {mismatches} stage totals where smoothed monthly allocation does not sum back"
    )


def test_smoothing_snapshot_latest_rows_are_project_level_and_bounded(
    conn: psycopg.Connection[Any],
) -> None:
    violations = _scalar(
        conn,
        """
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
    )
    assert violations == 0, f"Found {violations} invalid latest smoothing snapshot rows"


def test_smoothing_snapshot_latest_totals_match_monthly_artifact(
    conn: psycopg.Connection[Any],
) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0, (
        f"Found {mismatches} stage totals where latest snapshot differs from monthly artifact"
    )