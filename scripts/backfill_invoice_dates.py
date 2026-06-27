"""
Backfill project invoice date ranges from persisted subitems.

Usage:
    python -m scripts.backfill_invoice_dates --dry-run
    python -m scripts.backfill_invoice_dates

The script recomputes projects.first_date_invoiced, projects.last_date_invoiced,
and projects.invoicing_spread_days from subitems.invoice_date. Projects with no
subitem invoice dates are cleared by default so stale rollup dates do not remain.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.supabase_client import SupabaseClient

PAGE_SIZE = 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _parse_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _fetch_all_projects(db: SupabaseClient) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    start = 0
    while True:
        result = (
            db.client.table("projects")
            .select("monday_id, first_date_invoiced, last_date_invoiced, invoicing_spread_days")
            .range(start, start + PAGE_SIZE - 1)
            .execute()
        )
        page = result.data or []
        rows.extend(page)
        logger.info("Fetched %d project rows", len(rows))
        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return rows


def _fetch_invoice_ranges(db: SupabaseClient) -> Dict[str, Tuple[str, str, int, int]]:
    first_dates: Dict[str, datetime] = {}
    last_dates: Dict[str, datetime] = {}
    invoice_counts: Dict[str, int] = {}

    start = 0
    fetched = 0
    while True:
        result = (
            db.client.table("subitems")
            .select("parent_monday_id, invoice_date")
            .range(start, start + PAGE_SIZE - 1)
            .execute()
        )
        page = result.data or []
        fetched += len(page)

        for row in page:
            parent_id = str(row.get("parent_monday_id") or "").strip()
            if not parent_id:
                continue
            invoice_dt = _parse_date(row.get("invoice_date"))
            if not invoice_dt:
                continue
            if parent_id not in first_dates or invoice_dt < first_dates[parent_id]:
                first_dates[parent_id] = invoice_dt
            if parent_id not in last_dates or invoice_dt > last_dates[parent_id]:
                last_dates[parent_id] = invoice_dt
            invoice_counts[parent_id] = invoice_counts.get(parent_id, 0) + 1

        logger.info("Fetched %d subitem rows", fetched)
        if len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE

    ranges: Dict[str, Tuple[str, str, int, int]] = {}
    for parent_id, first_dt in first_dates.items():
        last_dt = last_dates[parent_id]
        spread_days = max(0, (last_dt.date() - first_dt.date()).days)
        ranges[parent_id] = (
            first_dt.date().isoformat(),
            last_dt.date().isoformat(),
            int(spread_days),
            invoice_counts.get(parent_id, 0),
        )
    return ranges


def _normalize_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _build_updates(
    projects: List[Dict[str, Any]],
    invoice_ranges: Dict[str, Tuple[str, str, int, int]],
    clear_missing: bool,
) -> List[Dict[str, Any]]:
    updates: List[Dict[str, Any]] = []
    now_iso = datetime.now().isoformat()

    for project in projects:
        project_id = str(project.get("monday_id") or "").strip()
        if not project_id:
            continue

        invoice_range = invoice_ranges.get(project_id)
        if invoice_range:
            first_invoice, last_invoice, spread_days, _invoice_count = invoice_range
            payload = {
                "monday_id": project_id,
                "first_date_invoiced": first_invoice,
                "last_date_invoiced": last_invoice,
                "invoicing_spread_days": spread_days,
                "last_synced_at": now_iso,
            }
        elif clear_missing:
            payload = {
                "monday_id": project_id,
                "first_date_invoiced": None,
                "last_date_invoiced": None,
                "invoicing_spread_days": None,
                "last_synced_at": now_iso,
            }
        else:
            continue

        changed = (
            project.get("first_date_invoiced") != payload["first_date_invoiced"]
            or project.get("last_date_invoiced") != payload["last_date_invoiced"]
            or _normalize_int(project.get("invoicing_spread_days"))
            != payload["invoicing_spread_days"]
        )
        if changed:
            updates.append(payload)

    return updates


def backfill(batch_size: int = 500, dry_run: bool = False, clear_missing: bool = True) -> None:
    db = SupabaseClient()

    logger.info("Fetching projects and subitem invoice dates")
    projects = _fetch_all_projects(db)
    invoice_ranges = _fetch_invoice_ranges(db)

    logger.info(
        "Computed invoice ranges for %d projects from %d project rows",
        len(invoice_ranges),
        len(projects),
    )

    updates = _build_updates(projects, invoice_ranges, clear_missing=clear_missing)
    logger.info("Prepared %d project invoice date updates", len(updates))

    if updates:
        with_dates = sum(1 for row in updates if row.get("first_date_invoiced"))
        clearing = len(updates) - with_dates
        logger.info("Updates with invoice dates: %d; clearing stale dates: %d", with_dates, clearing)

    if dry_run:
        logger.info("[DRY RUN] No changes written")
        for row in updates[:20]:
            logger.info("  %s", row)
        if len(updates) > 20:
            logger.info("  ... and %d more", len(updates) - 20)
        return

    updated = 0
    errors = 0
    for start in range(0, len(updates), batch_size):
        batch = updates[start : start + batch_size]
        for row in batch:
            project_id = row["monday_id"]
            payload = {
                "first_date_invoiced": row["first_date_invoiced"],
                "last_date_invoiced": row["last_date_invoiced"],
                "last_synced_at": row["last_synced_at"],
            }
            try:
                db.client.table("projects").update(payload).eq("monday_id", project_id).execute()
                updated += 1
            except Exception as exc:
                errors += 1
                logger.error("Failed updating project %s: %s", project_id, exc)

        logger.info(
            "Batch %d/%d complete: updated=%d errors=%d",
            (start // batch_size) + 1,
            ((len(updates) - 1) // batch_size) + 1,
            updated,
            errors,
        )

    logger.info("Invoice date backfill complete: updated=%d errors=%d", updated, errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill project invoice date ranges from subitems")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--no-clear-missing",
        action="store_true",
        help="Leave project invoice date fields unchanged when no subitem invoice dates exist",
    )
    args = parser.parse_args()
    backfill(
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        clear_missing=not args.no_clear_missing,
    )
