#!/usr/bin/env python

from __future__ import annotations

"""
Mirror validation and optional rehydration helper.

Usage
-----
python scripts/validate_mirror_inputs.py
python scripts/validate_mirror_inputs.py --auto-rehydrate
python scripts/validate_mirror_inputs.py --limit 10
"""

from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

import argparse
import os
import subprocess
import sys
from pprint import pprint
from typing import Dict, Iterable, Tuple

import psycopg

SQL_CHECKS: Dict[str, str] = {
    "missing_subitem_mirrors": """
        SELECT COUNT(*) AS missing_rows
        FROM subitems
        WHERE COALESCE(account, '') = '' OR COALESCE(product_type, '') = '';
    """,
    "projects_without_gestation": """
        SELECT COUNT(*) AS zero_or_null_gestation
        FROM projects
        WHERE COALESCE(gestation_period, 0) = 0;
    """,
    "numeric_nulls": """
        SELECT
            COUNT(*) FILTER (WHERE p.new_enquiry_value IS NULL) AS projects_null_nev,
            COUNT(*) FILTER (WHERE s.new_enquiry_value IS NULL) AS subitems_null_nev,
            COUNT(*) FILTER (WHERE s.quote_amount IS NULL)       AS subitems_null_quote
        FROM projects p
        LEFT JOIN subitems s ON s.parent_monday_id = p.monday_id;
    """,
}

SPOT_CHECK_TEMPLATE = """



    SELECT

        p.monday_id,

        p.item_name,

        p.last_synced_at,

        p.gestation_period,

        p.new_enquiry_value              AS project_new_enquiry_value,

        COALESCE(SUM(s.new_enquiry_value), 0) AS subitem_rollup_new_enquiry,

        MAX(s.account)                   AS sample_account,

        MAX(s.product_type)              AS sample_product_type

    FROM projects p

    LEFT JOIN subitems s ON s.parent_monday_id = p.monday_id

    WHERE p.last_synced_at IS NOT NULL

    GROUP BY

        p.monday_id,

        p.item_name,

        p.last_synced_at,

        p.gestation_period,

        p.new_enquiry_value

    ORDER BY p.last_synced_at DESC

    LIMIT %(limit)s;

"""


def get_connection_dsn() -> str:
    for key in ("SUPABASE_DB_URL", "SUPABASE_DATABASE_URL", "DATABASE_URL"):
        dsn = os.getenv(key)
        if not dsn:
            continue
        if not dsn.startswith(("postgres://", "postgresql://")):
            raise SystemExit(
                f"{key} is set but does not look like a Postgres URI. "
                "Copy the connection string from Supabase Project Settings → Database → Connection string → URI."
            )
        parsed = urlparse(dsn)
        if not parsed.hostname:
            raise SystemExit(
                f"{key} is set but the hostname part is empty. "
                "Expected something like db.<project-id>.supabase.co."
            )
        return dsn
    raise SystemExit(
        "Set SUPABASE_DB_URL (or SUPABASE_DATABASE_URL / DATABASE_URL) with your service-role Postgres connection string."
    )


def run_queries(conn: psycopg.Connection, queries: Dict[str, str]) -> Dict[str, Tuple]:
    results: Dict[str, Tuple] = {}
    with conn.cursor() as cur:
        for name, sql in queries.items():
            cur.execute(sql)
            results[name] = cur.fetchall()
    return results


def print_results(results: Dict[str, Iterable[Tuple]]) -> None:
    print("\n=== Mirror Validation Results ===")
    for name, rows in results.items():
        print(f"\n{name}:")
        for row in rows:
            pprint(dict(row._asdict() if hasattr(row, "_asdict") else zip(range(len(row)), row)))


def spot_check_projects(conn: psycopg.Connection, limit: int) -> None:
    with conn.cursor() as cur:
        cur.execute(SPOT_CHECK_TEMPLATE, {"limit": limit})
        rows = cur.fetchall()
        print(f"\n=== Spot Check (latest {limit}) ===")
        for row in rows:
            pprint(dict(row._asdict() if hasattr(row, "_asdict") else zip(range(len(row)), row)))


def maybe_run_rehydrate(auto: bool, mirror_results: Dict[str, Tuple]) -> None:
    if not auto:
        return
    missing_rows = mirror_results.get("missing_subitem_mirrors", [(0,)])[0][0]
    if missing_rows == 0:
        print("\nNo missing mirrors detected; skipping rehydrate.")
        return
    print(f"\nDetected {missing_rows} rows with missing mirrors. Launching rehydrate...")
    subprocess.run(
        [sys.executable, "scripts/rehydrate_sync.py", "--mode", "rehydrate"],
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate mirror inputs before automation.")
    parser.add_argument("--limit", type=int, default=5, help="Number of recent projects to display in spot check.")
    parser.add_argument("--auto-rehydrate", action="store_true", help="Run rehydrate script if mirrors are missing.")
    args = parser.parse_args()

    dsn = get_connection_dsn()
    with psycopg.connect(dsn, row_factory=psycopg.rows.namedtuple_row) as conn:
        check_results = run_queries(conn, SQL_CHECKS)
        print_results(check_results)
        spot_check_projects(conn, args.limit)

    maybe_run_rehydrate(args.auto_rehydrate, check_results)

    print("\nValidation complete.")


if __name__ == "__main__":
    main()