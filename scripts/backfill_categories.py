# scripts/backfill_categories.py
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.core.monday_client import MondayClient
from src.config import PARENT_COLUMNS, CATEGORY_LABELS

logger = logging.getLogger("backfill_categories")

CATEGORY_COL_ID = PARENT_COLUMNS.get('category', 'dropdown__1')
SUPABASE_UPDATE_BATCH_SIZE = 1000
MONDAY_PER_CALL_LIMIT = 100  # Monday get_items_by_ids per-call chunk size


def _parse_category_label(column_values: List[Dict]) -> Optional[str]:
    """
    Extract category label from Monday column_values.
    Prefer mapping using dropdown ids (canonical labels); fall back to text if needed.
    """
    if not column_values:
        return None

    for c in column_values:
        if c and c.get('id') == CATEGORY_COL_ID:
            raw_value = c.get('value')
            text_value = (c.get('text') or '').strip() or None

            # Try id-based mapping first (canonical)
            if raw_value:
                try:
                    data = json.loads(raw_value)
                    ids = data.get('ids') or []
                    if ids:
                        key = str(ids[0])
                        mapped = CATEGORY_LABELS.get(key)
                        if mapped:
                            return mapped
                except (json.JSONDecodeError, TypeError):
                    pass

            # Fallback to Monday's text label
            return text_value
    return None


def fetch_monday_categories_for_ids(mc: MondayClient, ids: List[str]) -> Dict[str, Optional[str]]:
    """
    Fetch Monday items by ids and return a mapping: monday_id -> category_label (or None).
    """
    items = mc.get_items_by_ids(item_ids=ids, column_ids=[CATEGORY_COL_ID], per_call_limit=MONDAY_PER_CALL_LIMIT)
    result: Dict[str, Optional[str]] = {}
    for item in items or []:
        mid = str(item.get('id'))
        cols = item.get('column_values') or []
        result[mid] = _parse_category_label(cols)
    return result


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.info("Starting category backfill")

    supabase = SupabaseClient()
    monday = MondayClient()

    # Limit scope to projects that already exist in Supabase
    existing_ids = supabase.get_existing_project_ids()
    if not existing_ids:
        logger.info("No existing projects found in Supabase. Exiting.")
        return

    logger.info(f"Found {len(existing_ids)} existing projects in Supabase")

    # Fetch category values from Monday for those ids
    monday_map = fetch_monday_categories_for_ids(monday, sorted(existing_ids))
    logger.info(f"Fetched categories for {len(monday_map)} Monday items")

    # Build update payloads (update-only)
    now_iso = datetime.now().isoformat()
    updates: List[Dict] = []
    for mid, cat in monday_map.items():
        updates.append({
            "monday_id": mid,
            "category": cat,  # None -> NULL in DB if unset
            "last_synced_at": now_iso,
        })

    if not updates:
        logger.info("No updates to apply. Exiting.")
        return

    # Apply per-row updates (different values per row)
    total = len(updates)
    done = 0
    fail = 0
    failed_ids: List[str] = []

    logger.info(f"Updating {total} rows in {((total - 1)//SUPABASE_UPDATE_BATCH_SIZE) + 1} batches (batch_size={SUPABASE_UPDATE_BATCH_SIZE})...")

    for i in range(0, total, SUPABASE_UPDATE_BATCH_SIZE):
        batch_idx = i // SUPABASE_UPDATE_BATCH_SIZE + 1
        batch = updates[i:i + SUPABASE_UPDATE_BATCH_SIZE]
        logger.info(f"[Batch {batch_idx}] size={len(batch)}")

        for j, row in enumerate(batch, 1):
            try:
                payload = {
                    "category": row["category"],
                    "last_synced_at": row["last_synced_at"],
                }
                supabase.client.table("projects")\
                    .update(payload)\
                    .eq("monday_id", row["monday_id"])\
                    .execute()
                done += 1
                if j % 250 == 0:
                    logger.info(f"[Batch {batch_idx}] progress {j}/{len(batch)} (global {done}/{total})")
            except Exception as e:
                fail += 1
                failed_ids.append(str(row.get("monday_id")))
                logger.error(f"[Batch {batch_idx}] update failed for {row.get('monday_id')}: {e}", exc_info=True)

    logger.info(f"Category backfill complete. Success={done} Fail={fail}")
    if failed_ids:
        logger.error(f"Failed monday_ids: {failed_ids}")


if __name__ == "__main__":
    main()