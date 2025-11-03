"""Utilities to verify webhook-triggered analysis for a specific project.

Usage:
    python scripts/verify_webhook_analysis.py --monday-id <pulse_id>

Outputs the most recent `webhook_events` entries, highlights any warnings, and
prints the latest row from `analysis_results` so you can confirm the pipeline
ran end-to-end.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table


PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database.supabase_client import SupabaseClient  # noqa: E402


console = Console()


def dump_webhook_events(client, monday_id: str, limit: int = 5) -> None:
    response = (
        client.table('webhook_events')
        .select('id, event_type, status, error_message, received_at, processed_at')
        .eq('item_id', monday_id)
        .order('received_at', desc=True)
        .limit(limit)
        .execute()
    )
    rows = response.data or []

    if not rows:
        console.print(f"[yellow]No webhook_events found for item {monday_id}[/yellow]")
        return

    table = Table(title=f"Recent webhook events for {monday_id}", header_style="bold magenta")
    table.add_column("id")
    table.add_column("event")
    table.add_column("status")
    table.add_column("received_at")
    table.add_column("processed_at")
    table.add_column("error")

    for row in rows:
        table.add_row(
            str(row.get('id')),
            row.get('event_type') or "",
            row.get('status') or "",
            (row.get('received_at') or '')[:19],
            (row.get('processed_at') or '')[:19],
            (row.get('error_message') or '')[:80],
        )

    console.print(table)


def dump_latest_analysis(client, monday_id: str) -> None:
    response = (
        client.table('analysis_results')
        .select('*')
        .eq('project_id', monday_id)
        .order('analysis_timestamp', desc=True)
        .limit(1)
        .execute()
    )
    row = (response.data or [None])[0]
    if not row:
        console.print(f"[yellow]No analysis_results found for project {monday_id}[/yellow]")
        return

    table = Table(title=f"Latest analysis for {monday_id}", header_style="bold green")
    for key in (
        'analysis_timestamp',
        'expected_gestation_days',
        'gestation_confidence',
        'expected_conversion_rate',
        'conversion_confidence',
        'rating_score',
        'analysis_version',
        'llm_model',
    ):
        table.add_row(key, str(row.get(key)))

    console.print(table)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect webhook + analysis state")
    parser.add_argument("--monday-id", required=True, help="Monday pulse/project ID")
    parser.add_argument("--webhook-limit", type=int, default=5, help="Rows to fetch from webhook_events")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    supabase = SupabaseClient()
    client = supabase.client

    dump_webhook_events(client, args.monday_id, args.webhook_limit)
    console.rule()
    dump_latest_analysis(client, args.monday_id)


if __name__ == "__main__":
    main()

