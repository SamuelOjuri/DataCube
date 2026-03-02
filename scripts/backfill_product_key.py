"""
Backfill: compute product_key for all existing projects rows.

Usage:
    python -m scripts.backfill_product_key [--batch-size 500] [--dry-run] [--all]
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.supabase_client import SupabaseClient
from src.database.sync_service import DataSyncService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill(batch_size: int = 500, dry_run: bool = False, recompute_all: bool = False) -> None:
    db = SupabaseClient()
    svc = DataSyncService()

    # ── Fetch rows ──────────────────────────────────────────────
    if recompute_all:
        logger.info("Fetching ALL projects for full recompute...")
        all_rows = []
        offset = 0
        page_size = 1000
        while True:
            page = (
                db.client.table("projects")
                .select("monday_id, product_type, product_key")
                .range(offset, offset + page_size - 1)
                .execute()
                .data
            )
            if not page:
                break
            all_rows.extend(page)
            offset += page_size
            logger.info(f"  Fetched {len(all_rows)} rows so far...")
            if len(page) < page_size:
                break
        rows = all_rows
    else:
        logger.info("Fetching projects with missing/unknown product_key...")
        all_rows = []
        offset = 0
        page_size = 1000
        while True:
            page = (
                db.client.table("projects")
                .select("monday_id, product_type, product_key")
                .or_("product_key.is.null,product_key.eq.unknown,product_key.eq.")
                .range(offset, offset + page_size - 1)
                .execute()
                .data
            )
            if not page:
                break
            all_rows.extend(page)
            offset += page_size
            logger.info(f"  Fetched {len(all_rows)} rows so far...")
            if len(page) < page_size:
                break
        rows = all_rows

    if not rows:
        logger.info("Nothing to backfill — all rows already have a product_key.")
        return

    logger.info(f"Found {len(rows)} rows to evaluate.")

    # ── Compute updates ─────────────────────────────────────────
    updates = []
    for row in rows:
        raw = row.get("product_type") or ""
        new_key = svc._compute_product_key(raw)
        current_key = row.get("product_key") or ""
        if new_key != current_key:
            updates.append({"monday_id": row["monday_id"], "product_key": new_key})

    logger.info(f"{len(updates)} rows need updating (out of {len(rows)} evaluated).")

    if not updates:
        logger.info("Nothing to update.")
        return

    # ── Show distribution ────────────────────────────────────────
    from collections import Counter
    dist = Counter(u["product_key"] for u in updates)
    logger.info("Proposed product_key distribution:")
    for key, count in dist.most_common(20):
        logger.info(f"  {key}: {count}")
    remaining = len(dist) - 20
    if remaining > 0:
        logger.info(f"  ... and {remaining} more unique keys")

    if dry_run:
        logger.info("[DRY RUN] No changes written.")
        for u in updates[:20]:
            logger.info(f"  {u['monday_id']} → product_key='{u['product_key']}'")
        if len(updates) > 20:
            logger.info(f"  ... and {len(updates) - 20} more")
        return

    # ── Batch update ─────────────────────────────────────────────
    updated = 0
    errors = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        for row in batch:
            try:
                db.client.table("projects").update(
                    {"product_key": row["product_key"]}
                ).eq("monday_id", row["monday_id"]).execute()
                updated += 1
            except Exception as e:
                errors += 1
                logger.error(f"Error updating monday_id={row['monday_id']}: {e}")

        batch_num = i // batch_size + 1
        total_batches = (len(updates) + batch_size - 1) // batch_size
        logger.info(f"  Batch {batch_num}/{total_batches} — updated: {updated}, errors: {errors}")

    logger.info(f"Backfill complete: {updated} rows updated, {errors} errors.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill product_key column")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all", action="store_true", help="Recompute all rows, not just unknown/null")
    args = parser.parse_args()
    backfill(batch_size=args.batch_size, dry_run=args.dry_run, recompute_all=args.all)