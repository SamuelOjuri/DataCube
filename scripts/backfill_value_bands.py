# scripts/backfill_value_bands.py
import asyncio
import sys
import logging
from datetime import datetime
from typing import Dict, List
from pathlib import Path
import math

import pandas as pd

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.core.data_processor import HierarchicalSegmentation

PAGE_SIZE = 1000
logger = logging.getLogger("backfill_value_bands")


def get_projects_count(client: SupabaseClient) -> int:
    try:
        return client.client.table("projects").select("*", count="exact", head=True).execute().count
    except Exception:
        return None


def fetch_all_projects(client: SupabaseClient) -> List[Dict]:
    rows: List[Dict] = []
    start = 0
    total = get_projects_count(client)
    if total:
        logger.info(f"Total projects to scan: {total}")
    else:
        logger.info("Total projects to scan: unknown")

    while True:
        resp = (
            client.client.table("projects")
            .select("*")
            .range(start, start + PAGE_SIZE - 1)
            .execute()
        )
        data = resp.data or []
        rows.extend(data)
        start += len(data)

        if total:
            pct = (len(rows) / total) * 100
            logger.info(f"Fetched {len(rows)}/{total} ({pct:.1f}%)")
        else:
            logger.info(f"Fetched {len(rows)} rows so far")

        if len(data) < PAGE_SIZE:
            break
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s", datefmt="%H:%M:%S")
    supabase = SupabaseClient()
    logger.info("Starting value band backfill")

    records = fetch_all_projects(supabase)

    if not records:
        logger.info("No projects found. Exiting.")
        return

    df = pd.DataFrame(records)
    if "new_enquiry_value" not in df.columns:
        df["new_enquiry_value"] = 0.0
    df["new_enquiry_value"] = df["new_enquiry_value"].fillna(0.0).astype(float)

    non_zero = int((df["new_enquiry_value"] > 0).sum())
    logger.info(f"Prepared DataFrame: rows={len(df)} | non_zero_new_enquiry={non_zero}")

    segmentation = HierarchicalSegmentation()
    df = segmentation.create_value_bands(df)

    band_counts = df["value_band"].value_counts(dropna=False)
    logger.info(f"value_band distribution: {band_counts.to_dict()}")

    updates: List[Dict] = []
    now_iso = datetime.now().isoformat()
    for original, (_, row) in zip(records, df.iterrows()):
        new_value = round(float(row["new_enquiry_value"]), 2)
        new_band = str(row["value_band"]) if pd.notna(row["value_band"]) else "Unknown"

        changed = (
            original.get("value_band") != new_band
            or round(float(original.get("new_enquiry_value") or 0.0), 2) != new_value
        )
        if changed:
            updates.append(
                {
                    "monday_id": original.get("monday_id"),
                    "new_enquiry_value": new_value,
                    "value_band": new_band,
                    "last_synced_at": now_iso,
                }
            )

    logger.info(f"Computed updates: {len(updates)} of {len(records)} ({(len(updates)/len(records))*100:.1f}%)")

    if not updates:
        logger.info("No updates required.")
        return

    # Update only existing records (no inserts)
    existing_ids = supabase.get_existing_project_ids()
    updates_in_db = [u for u in updates if str(u.get("monday_id") or "") in existing_ids]

    logger.info(f"Applying update-only mode: {len(updates_in_db)}/{len(updates)} rows exist in DB")

    if not updates_in_db:
        logger.info("No existing rows to update. Exiting.")
        return

    # Chunked per-row updates (each row has different values)
    BATCH_SIZE = 1000
    total = len(updates_in_db)
    done = 0
    fail = 0
    failed_ids: List[str] = []
    failed_rows: List[Dict] = []

    logger.info(f"Updating {total} rows in {((total - 1)//BATCH_SIZE) + 1} batches (batch_size={BATCH_SIZE})...")
    for i in range(0, total, BATCH_SIZE):
        batch_idx = i // BATCH_SIZE + 1
        batch = updates_in_db[i:i + BATCH_SIZE]
        logger.info(f"[Batch {batch_idx}] size={len(batch)}")

        for j, row in enumerate(batch, 1):
            try:
                payload = {
                    "new_enquiry_value": row["new_enquiry_value"],
                    "value_band": row["value_band"],
                    "last_synced_at": row["last_synced_at"],
                }
                supabase.client.table("projects")\
                    .update(payload)\
                    .eq("monday_id", row["monday_id"])\
                    .execute()
                done += 1
                if j % 100 == 0:
                    logger.info(f"[Batch {batch_idx}] progress {j}/{len(batch)} (global {done}/{total})")
            except Exception as e:
                fail += 1
                failed_ids.append(str(row.get("monday_id")))
                failed_rows.append({
                    "monday_id": row.get("monday_id"),
                    "value_band": row.get("value_band"),
                    "new_enquiry_value": row.get("new_enquiry_value"),
                    "last_synced_at": row.get("last_synced_at"),
                    "batch": batch_idx,
                })
                logger.error(f"[Batch {batch_idx}] update failed for {row.get('monday_id')}: {e}", exc_info=True)

    logger.info(f"Update-only backfill complete. Success={done} Fail={fail}")

    if failed_ids:
        logger.error(f"Failed monday_ids: {failed_ids}")



if __name__ == "__main__":
    main()
