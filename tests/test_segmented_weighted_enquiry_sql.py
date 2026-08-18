from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any, Sequence

import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv()

REQUIRED_RELATIONS = (
    "public.vw_weighted_enquiry_project_leaf_allocation_v1",
    "public.vw_weighted_enquiry_leaf_monthly_v1",
)

REQUIRED_FUNCTIONS = (
    "public.weighted_enquiry_normalize_text_key(text)",
    "public.weighted_enquiry_product_key(text)",
    "public.weighted_enquiry_products(text)",
    "public.weighted_enquiry_category_label(text)",
    "public.weighted_enquiry_category_segment(text)",
    "public.weighted_enquiry_category_segments(text)",
)


def _get_dsn() -> str:
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        pytest.skip(
            "SUPABASE_DB_URL is not set; skipping segmented weighted-enquiry SQL tests"
        )
    return dsn


@pytest.fixture(scope="module")
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_get_dsn(), autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET default_transaction_read_only = on")
        yield connection


def _scalar(
    conn: psycopg.Connection[Any], sql: str, params: Sequence[Any] = ()
) -> Any:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
    assert row is not None, "Expected query to return one row"
    return row[0]


def _rows(
    conn: psycopg.Connection[Any], sql: str, params: Sequence[Any] = ()
) -> list[tuple[Any, ...]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


@pytest.mark.parametrize("relation_name", REQUIRED_RELATIONS)
def test_required_relations_exist(
    conn: psycopg.Connection[Any], relation_name: str
) -> None:
    assert _scalar(conn, "SELECT to_regclass(%s);", (relation_name,)) is not None


@pytest.mark.parametrize("function_signature", REQUIRED_FUNCTIONS)
def test_required_functions_exist(
    conn: psycopg.Connection[Any], function_signature: str
) -> None:
    assert _scalar(conn, "SELECT to_regprocedure(%s);", (function_signature,)) is not None


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    (
        ("  ROCKWOOL  HardRock Multi - Fix ( DD )  ", "rockwool hardrock multi-fix (dd)"),
        ("EPS / PIR Tissue Faced", "eps/pir tissue faced"),
        (None, ""),
    ),
)
def test_text_normalization_matches_python_contract(
    conn: psycopg.Connection[Any], raw_value: str | None, expected: str
) -> None:
    actual = _scalar(
        conn,
        "SELECT public.weighted_enquiry_normalize_text_key(%s, TRUE);",
        (raw_value,),
    )
    assert actual == expected


def test_product_aliases_are_canonicalized_and_deduplicated(
    conn: psycopg.Connection[Any],
) -> None:
    rows = _rows(
        conn,
        """
        SELECT identity, reporting_segment, mapping_status
        FROM public.weighted_enquiry_products(%s)
        ORDER BY identity;
        """,
        (
            "ROCKWOOL HardRock Multi-Fix (DD), HardRock, ROCKDeck, "
            "EPS 150 (SPR), T3+",
        ),
    )
    assert rows == [
        ("eps_standard", "Combustible", "mapped"),
        ("hardrock", "Non-Combustible", "mapped"),
        ("rockdeck", "Non-Combustible", "mapped"),
        ("t3_system", "Non-Combustible", "mapped"),
    ]


def test_distinct_unmapped_products_remain_auditable(
    conn: psycopg.Connection[Any],
) -> None:
    rows = _rows(
        conn,
        """
        SELECT identity, reporting_segment, mapping_status
        FROM public.weighted_enquiry_products(%s)
        ORDER BY identity;
        """,
        ("Mystery Board, mystery board, Novel Board",),
    )
    assert rows == [
        ("unmapped:mystery board", "Combustible", "unmapped"),
        ("unmapped:novel board", "Combustible", "unmapped"),
    ]


def test_missing_product_defaults_to_combustible(
    conn: psycopg.Connection[Any],
) -> None:
    rows = _rows(
        conn,
        """
        SELECT identity, reporting_segment, mapping_status
        FROM public.weighted_enquiry_products(NULL);
        """,
    )
    assert rows == [("missing", "Combustible", "missing")]


def test_categories_are_deduplicated_after_reporting_mapping(
    conn: psycopg.Connection[Any],
) -> None:
    rows = _rows(
        conn,
        """
        SELECT segment, mapping_status
        FROM public.weighted_enquiry_category_segments(%s)
        ORDER BY segment;
        """,
        ("House, Apartments, House",),
    )
    assert rows == [("Apartments/Housing", "mapped")]


def test_numeric_and_text_category_aliases_map_to_reporting_segments(
    conn: psycopg.Connection[Any],
) -> None:
    rows = _rows(
        conn,
        """
        SELECT segment, mapping_status
        FROM public.weighted_enquiry_category_segments(%s)
        ORDER BY segment;
        """,
        ("13, Education, Unknown Category",),
    )
    assert rows == [
        ("Data Centres", "mapped"),
        ("Education", "mapped"),
        ("Other", "unmapped"),
    ]


def test_project_allocation_shares_sum_to_one(
    conn: psycopg.Connection[Any],
) -> None:
    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT project_id
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY project_id
            HAVING ABS(SUM(allocation_share) - 1.0) > 1e-9
        ) invalid_projects;
        """,
    )
    assert violations == 0


def test_allocated_values_reconcile_to_project_weighted_values(
    conn: psycopg.Connection[Any],
) -> None:
    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT project_id
            FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
            GROUP BY project_id
            HAVING ABS(
                MAX(project_weighted_enquiry_value)
                - SUM(allocated_weighted_enquiry_value)
            ) > 0.01
        ) invalid_projects;
        """,
    )
    assert violations == 0


def test_allocation_domains_and_numeric_values_are_valid(
    conn: psycopg.Connection[Any],
) -> None:
    violations = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM public.vw_weighted_enquiry_project_leaf_allocation_v1
        WHERE product_segment NOT IN ('Non-Combustible', 'Combustible')
           OR category_segment NOT IN (
                'Data Centres', 'Education', 'Apartments/Housing', 'Other'
           )
           OR allocation_share < 0
           OR allocation_share > 1
           OR allocated_weighted_enquiry_value < 0
           OR allocation_share::TEXT IN ('NaN', 'Infinity', '-Infinity')
           OR project_weighted_enquiry_value::TEXT IN ('NaN', 'Infinity', '-Infinity')
           OR allocated_weighted_enquiry_value::TEXT IN ('NaN', 'Infinity', '-Infinity');
        """,
    )
    assert violations == 0


def test_monthly_view_is_a_complete_unique_eight_leaf_grid(
    conn: psycopg.Connection[Any],
) -> None:
    invalid_months = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT month_start
            FROM public.vw_weighted_enquiry_leaf_monthly_v1
            GROUP BY month_start
            HAVING COUNT(*) <> 8
               OR COUNT(DISTINCT (product_segment, category_segment)) <> 8
        ) invalid;
        """,
    )
    assert invalid_months == 0

    duplicate_keys = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT month_start, product_segment, category_segment
            FROM public.vw_weighted_enquiry_leaf_monthly_v1
            GROUP BY month_start, product_segment, category_segment
            HAVING COUNT(*) > 1
        ) duplicates;
        """,
    )
    assert duplicate_keys == 0


def test_monthly_view_has_expected_bounds_and_non_negative_values(
    conn: psycopg.Connection[Any],
) -> None:
    bounds = _rows(
        conn,
        """
        SELECT MIN(month_start), MAX(month_start), COUNT(DISTINCT month_start)
        FROM public.vw_weighted_enquiry_leaf_monthly_v1;
        """,
    )[0]
    expected_months = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM generate_series(
            DATE '2022-01-01',
            (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::DATE,
            INTERVAL '1 month'
        );
        """,
    )
    assert bounds == (
        _scalar(conn, "SELECT DATE '2022-01-01';"),
        _scalar(
            conn,
            "SELECT (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::DATE;",
        ),
        expected_months,
    )

    invalid_values = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM public.vw_weighted_enquiry_leaf_monthly_v1
        WHERE actual_weighted_enquiry_value < 0
              OR actual_weighted_enquiry_value::TEXT IN ('NaN', 'Infinity', '-Infinity');
        """,
    )
    assert invalid_values == 0


def test_monthly_leaf_values_reconcile_to_allocation_view(
    conn: psycopg.Connection[Any],
) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0


def test_monthly_leaf_totals_reconcile_to_existing_overall_view(
    conn: psycopg.Connection[Any],
) -> None:
    mismatches = _scalar(
        conn,
        """
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
    )
    assert mismatches == 0