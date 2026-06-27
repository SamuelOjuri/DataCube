from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any, Sequence

import psycopg
import pytest


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        pytest.skip("SUPABASE_DB_URL is not set; skipping Phase 7 smoothing SQL tests")
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


def test_project_invoice_spread_generated_column_consistency(
    conn: psycopg.Connection[Any],
) -> None:
    violations = _scalar(
        conn,
        """
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
    )
    assert violations == 0, f"Found {violations} invoice spread generated-column violations"


def test_project_invoice_rollups_match_persisted_subitem_invoice_dates(
    conn: psycopg.Connection[Any],
) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0, f"Found {mismatches} project invoice rollups out of sync with subitems"


def test_smoothing_product_and_account_tokens_are_split_to_single_memberships(
    conn: psycopg.Connection[Any],
) -> None:
    unsplit_tokens = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM vw_invoice_smoothing_signal_members_v1
        WHERE dimension IN ('product', 'account')
          AND (
              group_key LIKE '%%,%%'
              OR group_display LIKE '%%,%%'
          );
        """,
    )
    assert unsplit_tokens == 0, f"Found {unsplit_tokens} unsplit product/account smoothing tokens"

    duplicate_memberships = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT project_id, dimension, group_key
            FROM vw_invoice_smoothing_signal_members_v1
            WHERE dimension IN ('product', 'account')
            GROUP BY project_id, dimension, group_key
            HAVING COUNT(*) > 1
        ) duplicates;
        """,
    )
    assert duplicate_memberships == 0, (
        f"Found {duplicate_memberships} duplicate product/account memberships"
    )


def test_pipeline_smoothing_fallback_flags_match_candidate_signal_coverage(
    conn: psycopg.Connection[Any],
) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0, f"Found {mismatches} smoothing fallback flag mismatches"


def test_latest_smoothing_snapshot_project_month_rows_sum_back_to_project_values(
    conn: psycopg.Connection[Any],
) -> None:
    latest_snapshot_date = _scalar(
        conn,
        """
        SELECT MAX(snapshot_date)
        FROM pipeline_smoothing_forecast_snapshot;
        """,
    )
    if latest_snapshot_date is None:
        pytest.skip("No smoothing snapshot rows exist yet")

    mismatches = _scalar(
        conn,
        """
        WITH per_project AS (
            SELECT
                project_id,
                MAX(expected_value) AS expected_value,
                SUM(allocated_expected_value) AS allocated_expected_value
            FROM pipeline_smoothing_forecast_snapshot
            WHERE snapshot_date = %s::DATE
            GROUP BY project_id
        )
        SELECT COUNT(*)
        FROM per_project
        WHERE ABS(expected_value - allocated_expected_value) > GREATEST(
            0.25::NUMERIC,
            ABS(expected_value) * 0.000001::NUMERIC
        );
        """,
        (latest_snapshot_date,),
    )
    assert mismatches == 0, (
        f"Found {mismatches} smoothing snapshot projects that do not sum back"
    )


def test_latest_smoothing_snapshot_has_project_level_explanation_fields(
    conn: psycopg.Connection[Any],
) -> None:
    latest_snapshot_date = _scalar(
        conn,
        """
        SELECT MAX(snapshot_date)
        FROM pipeline_smoothing_forecast_snapshot;
        """,
    )
    if latest_snapshot_date is None:
        pytest.skip("No smoothing snapshot rows exist yet")

    missing_fields = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM pipeline_smoothing_forecast_snapshot
        WHERE snapshot_date = %s::DATE
          AND (
              base_forecast_month IS NULL
              OR forecast_date IS NULL
              OR combined_smoothed_probability IS NULL
              OR expected_spread_days IS NULL
              OR source_view <> 'vw_pipeline_smoothing_score_v1'
          );
        """,
        (latest_snapshot_date,),
    )
    assert missing_fields == 0, (
        f"Found {missing_fields} smoothing snapshot rows missing project-level explanation fields"
    )