# scripts_new/rehydrate_sync.py

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.core.monday_client import MondayClient
from src.database.sync_service import DataSyncService
from src.config import (
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_COLUMNS,
    SUBITEM_COLUMNS,
    HIDDEN_ITEMS_COLUMNS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class IntelligentSyncManager:
    """Rehydration manager with robust retries, cursor resume, and progress logs"""

    def __init__(self):
        self.supabase = SupabaseClient()
        self.monday = MondayClient()
        self.sync_service = DataSyncService()

        # Reuse intelligent defaults
        self.CHUNK_SIZE = 100
        self.SYNC_BATCH = 1000
        self.RATE_LIMIT_DELAY = 2.5
        self.MAX_RETRIES = 5
        self.BATCH_SIZE = 1000
        self.BATCH_DELAY = 3

    async def _retry(self, fn, *args, **kwargs):
        """Generic retry wrapper for Monday API calls (no cursor)."""
        attempt = 0
        while attempt < self.MAX_RETRIES:
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                msg = str(e).lower()
                attempt += 1
                if "rate limit" in msg or "429" in msg:
                    wait = 60
                elif "504" in msg or "timeout" in msg:
                    wait = 30
                elif "internal server" in msg or "503" in msg:
                    wait = 5
                else:
                    if attempt >= self.MAX_RETRIES:
                        raise
                    wait = 5
                logger.warning(f"Retry {attempt}/{self.MAX_RETRIES} after error: {e} (waiting {wait}s)")
                await asyncio.sleep(wait)
        raise RuntimeError("Max retries exceeded")

    async def _retry_supabase(self, fn, *args, **kwargs):
        attempt = 0
        while attempt < self.MAX_RETRIES:
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                msg = str(e).lower()
                attempt += 1
                transient = any(t in msg for t in [
                    "getaddrinfo failed", "timed out", "timeout", "10060", "10054",
                    "connecterror", "connection aborted", "temporarily unavailable"
                ])
                if not transient or attempt >= self.MAX_RETRIES:
                    raise
                wait = min(60, 2 ** attempt)  # simple backoff
                logger.warning(f"Supabase transient error: {e} (retry {attempt}/{self.MAX_RETRIES} in {wait}s)")
                await asyncio.sleep(wait)

    async def _fast_forward_to_known_ids(self, board_id: str, known_ids: List[str], max_hops: int = 500) -> Optional[str]:
        """Scan IDs-only pages to jump near the last processed region after reseed."""
        if not known_ids:
            return None
        cursor = None
        hops = 0
        while hops < max_hops:
            page = (self.monday.get_next_item_ids_page(cursor, self.CHUNK_SIZE)
                    if cursor else
                    self.monday.get_item_ids_page(board_id, self.CHUNK_SIZE, None))
            items = page.get('items') or []
            if not items:
                return None
            ids_on_page = {str(it.get('id')) for it in items}
            if any(k in ids_on_page for k in known_ids):
                return page.get('next_cursor')  # resume AFTER matched page
            cursor = page.get('next_cursor')
            if not cursor:
                return None
            hops += 1
            await asyncio.sleep(self.RATE_LIMIT_DELAY)
        return None

    async def _rehydrate_hidden_and_subitems(self) -> Dict:
        """IDs-first, details-second for hidden and subitems to reduce per-call payloads."""
        stats = {"hidden_updated": 0, "subitems_updated": 0}
        log_id = self.supabase.log_sync_operation("rehydrate", SUBITEM_BOARD_ID, "Rehydrate Hidden + Subitems")

        try:
            # Cumulative progress tracking
            start = datetime.now()
            hidden_pages = 0
            hidden_processed = 0
            sub_pages = 0
            sub_processed = 0

            # Resume support
            from collections import deque
            hidden_last_seen = deque(maxlen=10)
            sub_last_seen = deque(maxlen=10)
            hidden_cursor: Optional[str] = None
            sub_cursor: Optional[str] = None
            try:
                prev = (
                    self.supabase.client.table("sync_log")
                    .select("metadata, started_at")
                    .eq("board_id", SUBITEM_BOARD_ID)
                    .eq("board_name", "Rehydrate Hidden + Subitems")
                    .order("started_at", desc=True)
                    .limit(1)
                    .execute()
                )
                if prev.data:
                    meta = (prev.data[0].get("metadata") or {})
                    phase = meta.get("phase")
                    saved = meta.get("last_cursor")
                    ring = meta.get("last_seen_ring") or []
                    if phase == "hidden" and saved:
                        hidden_cursor = saved
                        try:
                            hidden_last_seen.extend([str(x) for x in ring])
                        except Exception:
                            pass
                    elif phase == "subitems" and saved:
                        sub_cursor = saved
                        try:
                            sub_last_seen.extend([str(x) for x in ring])
                        except Exception:
                            pass
            except Exception:
                pass

            # Hidden items → IDs-only pages, then details for each page
            # NOTE: do not reset hidden_cursor here; we may have restored it from metadata above
            while True:
                try:
                    if hidden_cursor:
                        page = self.monday.get_next_item_ids_page(hidden_cursor, 100)
                    else:
                        page = self.monday.get_item_ids_page(HIDDEN_ITEMS_BOARD_ID, 100, None)
                except Exception as e:
                    msg = str(e).lower()
                    if "cursorexpirederror" in msg:
                        try:
                            self.supabase.save_cursor_checkpoint(
                                log_id, hidden_cursor,
                                {"phase": "hidden", "pages": hidden_pages, "processed_total": hidden_processed, "last_seen_ring": list(hidden_last_seen)}
                            )
                        except Exception:
                            pass
                        seed = self.monday.get_item_ids_page(HIDDEN_ITEMS_BOARD_ID, 1, None)
                        hidden_cursor = seed.get("next_cursor")
                        # Fast-forward near previous position
                        try:
                            ff = await self._fast_forward_to_known_ids(HIDDEN_ITEMS_BOARD_ID, list(hidden_last_seen), max_hops=500)
                            if ff:
                                logger.info(f"[rehydrate][hidden] fast-forward matched recent IDs (ring={len(hidden_last_seen)}); resuming near previous position")
                                try:
                                    self.supabase._merge_log_metadata(log_id, {"hidden_fast_forward_match": True})
                                except Exception:
                                    pass
                                hidden_cursor = ff
                            else:
                                logger.info(f"[rehydrate][hidden] fast-forward found no recent IDs (ring={len(hidden_last_seen)}); resuming from seed cursor (may reprocess)")
                                try:
                                    self.supabase._merge_log_metadata(log_id, {"hidden_fast_forward_match": False})
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                        continue
                    if "rate limit" in msg or "429" in msg:
                        retry_sec = 60
                        try:
                            import re
                            m = re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(e))
                            if m:
                                retry_sec = max(1, int(m.group(1)))
                        except Exception:
                            pass
                        await asyncio.sleep(retry_sec)
                        continue
                    if "internal server error" in msg or "internal server" in msg or "503" in msg:
                        await asyncio.sleep(5)
                        continue
                    if "504" in msg or "timeout" in msg:
                        await asyncio.sleep(30)
                        continue
                    logger.error(f"[rehydrate][hidden] fatal page error: {e}")
                    break

                items = page.get("items") or []
                ids = [str(it.get("id")) for it in items if it.get("id") is not None]
                if not ids:
                    break

                # Track last-seen ID for fast-forward
                try:
                    last_id = str(items[-1].get('id')) if items else None
                    if last_id:
                        hidden_last_seen.append(last_id)
                except Exception:
                    pass

                raw_hidden = await self._retry(
                    self.monday.get_items_by_ids, ids, list(HIDDEN_ITEMS_COLUMNS.values())
                )
                hidden_data = self.sync_service._transform_for_hidden_table(raw_hidden)
                updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
                stats["hidden_updated"] += updated

                hidden_pages += 1
                hidden_processed += len(hidden_data)
                elapsed = (datetime.now() - start).total_seconds() or 1
                logger.info(
                    f"[rehydrate][hidden] pages={hidden_pages} processed_total={hidden_processed} "
                    f"updated_total={stats['hidden_updated']} rate={hidden_processed/elapsed:.1f}/s"
                )

                hidden_cursor = page.get("next_cursor")
                try:
                    self.supabase.save_cursor_checkpoint(
                        log_id, hidden_cursor,
                        {"phase": "hidden", "pages": hidden_pages, "processed_total": hidden_processed, "last_seen_ring": list(hidden_last_seen)}
                    )
                except Exception:
                    pass
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
                if not hidden_cursor:
                    break

            # Subitems → IDs-only pages, then details (include parent reference)
            sub_cursor: Optional[str] = None
            while True:
                try:
                    if sub_cursor:
                        page = self.monday.get_next_item_ids_page(sub_cursor, 100)
                    else:
                        page = self.monday.get_item_ids_page(SUBITEM_BOARD_ID, 100, None)
                except Exception as e:
                    msg = str(e).lower()
                    if "cursorexpirederror" in msg:
                        try:
                            self.supabase.save_cursor_checkpoint(
                                log_id, sub_cursor,
                                {"phase": "subitems", "pages": sub_pages, "processed_total": sub_processed, "last_seen_ring": list(sub_last_seen)}
                            )
                        except Exception:
                            pass
                        seed = self.monday.get_item_ids_page(SUBITEM_BOARD_ID, 1, None)
                        sub_cursor = seed.get("next_cursor")
                        # Fast-forward near previous position
                        try:
                            ff = await self._fast_forward_to_known_ids(SUBITEM_BOARD_ID, list(sub_last_seen), max_hops=500)
                            if ff:
                                logger.info(f"[rehydrate][subitems] fast-forward matched recent IDs (ring={len(sub_last_seen)}); resuming near previous position")
                                try:
                                    self.supabase._merge_log_metadata(log_id, {"subitems_fast_forward_match": True})
                                except Exception:
                                    pass
                                sub_cursor = ff
                            else:
                                logger.info(f"[rehydrate][subitems] fast-forward found no recent IDs (ring={len(sub_last_seen)}); resuming from seed cursor (may reprocess)")
                                try:
                                    self.supabase._merge_log_metadata(log_id, {"subitems_fast_forward_match": False})
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        await asyncio.sleep(1)
                        continue
                    if "rate limit" in msg or "429" in msg:
                        retry_sec = 60
                        try:
                            import re
                            m = re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(e))
                            if m:
                                retry_sec = max(1, int(m.group(1)))
                        except Exception:
                            pass
                        await asyncio.sleep(retry_sec)
                        continue
                    if "internal server error" in msg or "internal server" in msg or "503" in msg:
                        await asyncio.sleep(5)
                        continue
                    if "504" in msg or "timeout" in msg:
                        await asyncio.sleep(30)
                        continue
                    logger.error(f"[rehydrate][subitems] fatal page error: {e}")
                    break

                items = page.get("items") or []
                ids = [str(it.get("id")) for it in items if it.get("id") is not None]
                if not ids:
                    break

                # Track last-seen ID for fast-forward
                try:
                    last_id = str(items[-1].get('id')) if items else None
                    if last_id:
                        sub_last_seen.append(last_id)
                except Exception:
                    pass

                raw_subs = await self._retry(
                    self.monday.get_items_by_ids, ids, list(SUBITEM_COLUMNS.values()), include_parent=True
                )
                subitems_data = self.sync_service._transform_for_subitems_table(raw_subs)
                updated = await self.sync_service._batch_upsert_subitems(subitems_data)
                stats["subitems_updated"] += updated

                sub_pages += 1
                sub_processed += len(subitems_data)
                elapsed = (datetime.now() - start).total_seconds() or 1
                logger.info(
                    f"[rehydrate][subitems] pages={sub_pages} processed_total={sub_processed} "
                    f"updated_total={stats['subitems_updated']} rate={sub_processed/elapsed:.1f}/s"
                )

                sub_cursor = page.get("next_cursor")
                try:
                    self.supabase.save_cursor_checkpoint(
                        log_id, sub_cursor,
                        {"phase": "subitems", "pages": sub_pages, "processed_total": sub_processed, "last_seen_ring": list(sub_last_seen)}
                    )
                except Exception:
                    pass
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
                if not sub_cursor:
                    break

            duration = (datetime.now() - start).total_seconds()
            logger.info(
                f"[rehydrate] Phase A complete in {duration:.1f}s: "
                f"hidden_updated={stats['hidden_updated']} subitems_updated={stats['subitems_updated']}"
            )
            self.supabase.update_sync_log(log_id, "completed", stats)
            return {"success": True, **stats}
        except Exception as e:
            self.supabase.update_sync_log(log_id, "failed", error=str(e))
            logger.error(f"Rehydrate hidden/subitems failed: {e}")
            return {"success": False, "error": str(e)}

    async def _get_projects_for_rehydrate(self, page_size: int = 1000) -> List[str]:
        """Read monday_ids for ALL projects (full rehydrate required) using keyset pagination."""
        ids, last = [], None
        while True:
            q = self.supabase.client.table("projects").select("monday_id").order("monday_id")
            if last is not None:
                q = q.gt("monday_id", last)
            resp = await self._retry_supabase(lambda: q.limit(page_size).execute())
            rows = resp.data or []
            if not rows:
                break
            ids.extend(str(r["monday_id"]) for r in rows if r.get("monday_id"))
            last = rows[-1]["monday_id"]
            if len(ids) % (page_size * 5) == 0:
                logger.info(f"[rehydrate] queued {len(ids)} projects")
        return ids

    async def _rehydrate_projects_by_ids(self, ids: List[str], chunk: int = 100) -> Dict:
        """Fetch parents and related subitems, resolve mirrors, transform and upsert."""
        stats = {"batches": 0, "updated": 0, "errors": 0}
        log_id = self.supabase.log_sync_operation("rehydrate", PARENT_BOARD_ID, "Rehydrate Projects")

        try:
            # Determine resume index from the most recent run (if any)
            resume_index = 0
            try:
                prev = (
                    self.supabase.client.table("sync_log")
                    .select("id, metadata, status, started_at")
                    .eq("board_id", PARENT_BOARD_ID)
                    .eq("board_name", "Rehydrate Projects")
                    .order("started_at", desc=True)
                    .limit(1)
                    .execute()
                )
                if prev.data:
                    meta = (prev.data[0].get("metadata") or {})
                    ri = meta.get("resume_index")
                    if isinstance(ri, int) and 0 <= ri < len(ids):
                        resume_index = ri
            except Exception:
                pass

            if resume_index > 0:
                logger.info(f"[rehydrate][projects] resuming from index {resume_index} of {len(ids)}")

            # Seed log metadata for this session
            try:
                self.supabase._merge_log_metadata(log_id, {"resume_index": resume_index, "total_ids": len(ids), "chunk": chunk})
            except Exception:
                pass

            start = datetime.now()
            processed_in_batch = 0
            for i in range(resume_index, len(ids), chunk):
                batch_ids = ids[i : i + chunk]

                # Parents (retry wrapper)
                raw_items = await self._retry(
                    self.monday.get_items_by_ids, batch_ids, list(PARENT_COLUMNS.values())
                )
                parents = [self.sync_service._normalize_monday_item(it) for it in raw_items]

                # Related subitems (retry wrapper)
                raw_subs = await self._retry(
                    self.monday.get_subitems_for_parents, batch_ids, list(SUBITEM_COLUMNS.values())
                )
                # Preserve parent_item for mirror aggregation
                subs = raw_subs

                # NEW: transform subitems and compute gestation fallback map from hidden+subitems
                subitems_data = self.sync_service._transform_for_subitems_table(subs)
                gmap = self.sync_service._compute_gestation_fallback_from_subitems(subitems_data)

                # Resolve mirrors (hidden list not required for gestation since we use gmap),
                # then transform projects
                processed = self.sync_service._process_and_resolve_mirrors(parents, subs, [])
                projects = self.sync_service._transform_for_projects_table(processed["projects"])

                # NEW: apply gestation fallback where current value is missing/zero
                self.sync_service._apply_project_gestation_fallback(projects, gmap)

                # Upsert
                updated = await self.sync_service._batch_upsert_projects(projects)

                stats["batches"] += 1
                stats["updated"] += updated
                processed_in_batch += len(batch_ids)

                # Checkpoint resume index after each successful batch
                try:
                    self.supabase._merge_log_metadata(log_id, {
                        "resume_index": i + len(batch_ids),
                        "updated_total": stats["updated"],
                        "batches": stats["batches"]
                    })
                except Exception:
                    pass

                if stats["batches"] % 5 == 0:
                    elapsed = (datetime.now() - start).total_seconds() or 1
                    rate = (i + len(batch_ids)) / elapsed
                    logger.info(
                        f"[rehydrate] batches={stats['batches']} processed={i+len(batch_ids)} "
                        f"updated={stats['updated']} rate={rate:.1f}/s"
                    )

                # Fine-grained delay per chunk plus batch pause like intelligent flow
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
                if processed_in_batch >= self.BATCH_SIZE:
                    logger.info(f"[rehydrate] batch pause {self.BATCH_DELAY}s after {processed_in_batch} items")
                    await asyncio.sleep(self.BATCH_DELAY)
                    processed_in_batch = 0

            # Finalize: mark resume_index as completed
            try:
                self.supabase._merge_log_metadata(log_id, {"resume_index": len(ids)})
            except Exception:
                pass

            self.supabase.update_sync_log(log_id, "completed", stats)
            return {"success": True, **stats}
        except Exception as e:
            stats["errors"] += 1
            # Leave the last saved resume_index as-is for proper resume
            self.supabase.update_sync_log(log_id, "failed", stats, str(e))
            logger.error(f"Rehydrate projects failed: {e}")
            return {"success": False, "error": str(e), **stats}

    async def run_rehydration(self):
        """End-to-end rehydration: hidden → subitems → projects (targets rows with nulls)."""
        print("=" * 70)
        print("REHYDRATION RUN (hidden → subitems → projects)")
        print("=" * 70)

        # Phase A: Rehydrate hidden + subitems (uses robust extractor with cursor resume)
        print("\n📌 Phase A: Hidden + Subitems")
        hs = await self._rehydrate_hidden_and_subitems()
        if not hs.get("success"):
            print(f"  ❌ Failed: {hs.get('error')}")
            return
        print(f"  ✅ Hidden updated: {hs.get('hidden_updated', 0)}")
        print(f"  ✅ Subitems updated: {hs.get('subitems_updated', 0)}")

        # Phase B: Collect projects needing rehydrate
        print("\n📌 Phase B: Selecting projects needing update")
        ids = await self._get_projects_for_rehydrate(page_size=1000)
        print(f"  ▶ Found {len(ids):,} projects to rehydrate")

        if not ids:
            print("  ✅ Nothing to rehydrate for projects")
            return

        # Phase C: Rehydrate projects in batches (fetch parents+subs, resolve, upsert)
        print("\n📌 Phase C: Rehydrating projects")
        pr = await self._rehydrate_projects_by_ids(ids, chunk=self.CHUNK_SIZE)
        if pr.get("success"):
            print(f"  ✅ Batches: {pr.get('batches', 0)}; Updated rows: {pr.get('updated', 0)}")
        else:
            print(f"  ❌ Rehydrate projects failed: {pr.get('error')}")

async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Intelligent rehydration runner")
    parser.add_argument("--mode", choices=["rehydrate", "full"], default="rehydrate")
    args = parser.parse_args()

    manager = IntelligentSyncManager()
    if args.mode == "rehydrate":
        await manager.run_rehydration()
    else:
        # Keep your existing full sync if needed
        await manager.run_rehydration()

if __name__ == "__main__":
    asyncio.run(main())