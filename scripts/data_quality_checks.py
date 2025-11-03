"""Quick data quality checks for webhook-triggered analysis inputs.

Run via `python scripts/data_quality_checks.py` after syncing data.

Requires Supabase environment variables (see `.env`). Uses the service key to
pull lightweight aggregates so we can confirm the fields consumed by
`AnalysisService` are populated before relying on webhooks.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Tuple

from rich import box
from rich.console import Console
from rich.table import Table


# Make project modules importable when executed as a script
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient  # noqa: E402


console = Console()


def _count_null_or_blank(client, table: str, column: str) -> Tuple[int, int]:
    """Return matching count and total rows for a column."""

    total_resp = (
        client.table(table)
        .select("monday_id", count="exact", head=True)
        .execute()
    )
    total = total_resp.count or 0

    null_resp = (
        client.table(table)
        .select("monday_id", count="exact", head=True)
        .or_(f"{column}.is.null,{column}.eq.\"\"")
        .execute()
    )
    return null_resp.count or 0, total


def _count_numeric_null(client, table: str, column: str) -> Tuple[int, int]:
    """Return count of rows where a numeric column is null."""

    total_resp = (
        client.table(table)
        .select("monday_id", count="exact", head=True)
        .execute()
    )
    total = total_resp.count or 0

    null_resp = (
        client.table(table)
        .select("monday_id", count="exact", head=True)
        .is_(column, "null")
        .execute()
    )
    return null_resp.count or 0, total


def run_checks():
    supabase = SupabaseClient()
    client = supabase.client

    console.rule("Mirror Completeness (subitems)")
    mirror_table = Table(box=box.SIMPLE_HEAVY)
    mirror_table.add_column("Field")
    mirror_table.add_column("Missing")
    mirror_table.add_column("Total")
    mirror_table.add_column("Coverage")

    for label, column in (
        ("account", "account"),
        ("product_type", "product_type"),
    ):
        missing, total = _count_null_or_blank(client, "subitems", column)
        coverage = 0.0 if total == 0 else 100 * (1 - missing / total)
        mirror_table.add_row(
            label,
            f"{missing:,}",
            f"{total:,}",
            f"{coverage:.2f}%",
        )
    console.print(mirror_table)

    console.rule("Numeric Field Sanity")
    numeric_table = Table(box=box.SIMPLE_HEAVY)
    numeric_table.add_column("Table")
    numeric_table.add_column("Column")
    numeric_table.add_column("Null Rows")
    numeric_table.add_column("Total")
    numeric_table.add_column("Coverage")

    for table, column in (
        ("subitems", "new_enquiry_value"),
        ("projects", "new_enquiry_value"),
        ("projects", "gestation_period"),
    ):
        missing, total = _count_numeric_null(client, table, column)
        coverage = 0.0 if total == 0 else 100 * (1 - missing / total)
        numeric_table.add_row(
            table,
            column,
            f"{missing:,}",
            f"{total:,}",
            f"{coverage:.2f}%",
        )
    console.print(numeric_table)

    console.rule("conversion_metrics sample")
    sample = (
        client.table("conversion_metrics")
        .select("account,type,category,product_type,win_rate,median_gestation")
        .limit(5)
        .execute()
    )
    if sample.data:
        sample_table = Table(show_edge=False, header_style="bold")
        sample_table.add_column("account")
        sample_table.add_column("type")
        sample_table.add_column("category")
        sample_table.add_column("product_type")
        sample_table.add_column("win_rate")
        sample_table.add_column("median_gestation")
        for row in sample.data:
            sample_table.add_row(
                str(row.get("account") or ""),
                str(row.get("type") or ""),
                str(row.get("category") or ""),
                str(row.get("product_type") or ""),
                f"{row.get('win_rate') or 0:.3f}",
                str(row.get("median_gestation") or ""),
            )
        console.print(sample_table)
    else:
        console.print("No rows in conversion_metrics (did you refresh the view?).")


if __name__ == "__main__":
    try:
        run_checks()
    except Exception as exc:  # pragma: no cover - CLI diagnostics
        console.print(f"[red]Data quality checks failed:[/red] {exc}")

