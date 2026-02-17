# pytest tests/test_pipeline_forecast_service.py -q

from __future__ import annotations

import os
from typing import Any, Sequence

import psycopg
import pytest

REQUIRED_RELATIONS = (
    "public.vw_pipeline_forecast_project_v1",
    "public.mv_pipeline_forecast_monthly_12m_v1",
    "public.pipeline_forecast_snapshot",
)


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        pytest.skip(
            "SUPABASE_DB_URL is not set; skipping forecast SQL validation tests"
        )
    return dsn


@pytest.fixture(scope="module")
def conn() -> psycopg.Connection[Any]:
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
def test_forecast_relations_exist(
    conn: psycopg.Connection[Any], relation_name: str
) -> None:
    relation = _scalar(conn, "SELECT to_regclass(%s);", (relation_name,))
    assert relation is not None, f"Missing relation: {relation_name}"


def test_stage_bucket_domain_is_valid(conn: psycopg.Connection[Any]) -> None:
    unknown_bucket_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE stage_bucket NOT IN ('Committed', 'Open', 'Lost');
        """,
    )
    assert (
        unknown_bucket_count == 0
    ), f"Found {unknown_bucket_count} rows with invalid stage_bucket"


def test_stage_bucket_mapping_correctness(conn: psycopg.Connection[Any]) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0, f"Found {mismatches} stage-bucket mapping mismatches"


def test_contract_value_practical_fallback(conn: psycopg.Connection[Any]) -> None:
    mismatches = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE contract_value <> COALESCE(
            NULLIF(total_order_value, 0),
            NULLIF(new_enquiry_value, 0),
            0
        )::NUMERIC(12,2);
        """,
    )
    assert (
        mismatches == 0
    ), f"Found {mismatches} contract-value practical fallback mismatches"


def test_monthly_12m_window_clamping(conn: psycopg.Connection[Any]) -> None:
    out_of_window = _scalar(
        conn,
        """
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
    )
    assert out_of_window == 0, f"Found {out_of_window} rows outside 12-month window"


def test_project_band_monotonicity(conn: psycopg.Connection[Any]) -> None:
    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_pipeline_forecast_project_v1
        WHERE worst_case_value > expected_value
           OR expected_value > best_case_value;
        """,
    )
    assert violations == 0, f"Found {violations} project-level band monotonicity violations"


def test_monthly_band_monotonicity(conn: psycopg.Connection[Any]) -> None:
    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM mv_pipeline_forecast_monthly_12m_v1
        WHERE worst_case_value > expected_value
           OR expected_value > best_case_value;
        """,
    )
    assert violations == 0, f"Found {violations} monthly band monotonicity violations"