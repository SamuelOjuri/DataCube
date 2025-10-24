# scripts/rehydrate_recent.py

import asyncio
import logging
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import (
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_COLUMNS,
    SUBITEM_COLUMNS,
    PARENT_COLUMNS,
)
from src.database.supabase_client import SupabaseClient
from src.database.sync_service import DataSyncService
from src.core.monday_client import MondayClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("rehydrate_recent")


class RecentRehydrationManager:
    """
    Targeted rehydration manager that only syncs projects created on or after a given
    date in Supabase. Mirrors the hydration behaviour of `rehydrate_sync.py`, but
    scopes extraction and enrichment to a filtered subset of Monday items.

    Key behaviours:
      * Queries Supabase `projects` table for candidate `monday_id` values.
      * Derives hidden-item prefixes from parent names (e.g. “17421_…” pattern, see SampleBoardData.txt).
      * Primes hidden-item caches before processing subitems so broken `connect_boards8__1`
        links can be reconstructed via name/prefix lookups.
      * Processes parents + subitems in chunks with the same retry / checkpoint semantics
        as the full rehydrate pipeline.
    """

    def __init__(self, since: datetime, chunk_size: int = 100):
        self.since = since
        self.chunk_size = chunk_size
        self.supabase = SupabaseClient()
        self.sync_service = DataSyncService()
        self.monday = MondayClient()

        # copied retry settings from the full pipeline
        self.MAX_RETRIES = 5
        self.RATE_LIMIT_DELAY = 2.5
        self.BATCH_DELAY = 3
        self.BATCH_SIZE = 1000

        self.hidden_pages = 0
        self.hidden_ids_scanned = 0
        self.hidden_ids_matched = 0
        self.hidden_rows_fetched = 0
        self.hidden_rows_upserted = 0
        self.subitem_batches = 0
        self.project_batches = 0
        self.project_rows_upserted = 0
        self.subitem_rows_upserted = 0

    async def run(self) -> None:
        logger.info("Starting recent rehydrate run (since=%s)", self.since.date())

        candidates = self._load_candidate_projects()
        if not candidates:
            logger.info("No Supabase projects found with date_created >= %s", self.since.date())
            return

        prefixes = {c.prefix for c in candidates if c.prefix}
        logger.info("Supabase returned %d projects (%d unique prefixes)", len(candidates), len(prefixes))

        await self._warm_hidden_cache(prefixes)
        await self._rehydrate_batches(candidates)

        logger.info(
            (
                "Recent rehydrate complete | candidates=%d project_rows_upserted=%d "
                "subitem_rows_upserted=%d project_batches=%d subitem_batches=%d "
                "hidden_pages=%d hidden_ids_scanned=%d hidden_ids_matched=%d hidden_rows_upserted=%d"
            ),
            len(candidates),
            self.project_rows_upserted,
            self.subitem_rows_upserted,
            self.project_batches,
            self.subitem_batches,
            self.hidden_pages,
            self.hidden_ids_scanned,
            self.hidden_ids_matched,
            self.hidden_rows_upserted,
        )

        logger.info("Recent rehydrate completed successfully")

    async def _warm_hidden_cache(self, prefixes: Set[str]) -> None:
        """
        Stage 1: prime hidden-item caches so later subitem transforms can recover
        quote/reason metadata even when board relations are broken.

        * Uses IDs-only pagination (cheap) to discover hidden rows whose names start
          with the target prefixes (SampleBoardData.txt demonstrates the <parent>_<suffix> pattern).
        * Fetches detailed column data only for matched IDs, transforms, and upserts.
        """
        if not prefixes:
            logger.info("No prefixes supplied; skipping hidden warm-up")
            return

        logger.info("Priming hidden cache for %d prefixes", len(prefixes))

        log_id = self.supabase.log_sync_operation("rehydrate", HIDDEN_ITEMS_BOARD_ID, "Recent Hidden Warmup")
        self.hidden_rows_fetched = 0
        self.hidden_rows_upserted = 0

        try:
            stats = await self._collect_hidden_ids(prefixes, log_id)
            matched_ids = set(stats.get("matched_ids") or [])
            self.hidden_pages = stats.get("pages", 0)
            self.hidden_ids_scanned = stats.get("processed_total", 0)
            self.hidden_ids_matched = len(matched_ids)
            if not matched_ids:
                logger.warning(
                    "No hidden items matched requested prefixes; subitem fallback will rely on full cache"
                )
                if log_id:
                    self.supabase.update_sync_log(log_id, "completed", stats)
                return

            logger.info("Matched %d hidden items; fetching full payloads", len(matched_ids))
            hidden_rows = await self._fetch_hidden_details(matched_ids)
            if not hidden_rows:
                logger.warning("Failed to fetch detailed hidden items")
                if log_id:
                    self.supabase.update_sync_log(log_id, "completed", stats)
                return
            self.hidden_rows_fetched = len(hidden_rows)

            hidden_data = self.sync_service._transform_for_hidden_table(hidden_rows)
            if not hidden_data:
                logger.warning("Hidden transform yielded no rows")
                if log_id:
                    self.supabase.update_sync_log(log_id, "completed", stats)
                return

            updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
            self.hidden_rows_upserted = updated
            stats["hidden_rows_upserted"] = updated
            logger.info(
                "Hidden warm-up complete | matched=%d fetched=%d upserted=%d pages=%d scanned=%d",
                len(matched_ids),
                self.hidden_rows_fetched,
                self.hidden_rows_upserted,
                self.hidden_pages,
                self.hidden_ids_scanned,
            )

            if log_id:
                self.supabase.update_sync_log(log_id, "completed", stats)
        except Exception as exc:
            logger.error("Hidden warm-up failed: %s", exc)
            if log_id:
                self.supabase.update_sync_log(log_id, "failed", error=str(exc))
            raise

    async def _collect_hidden_ids(self, prefixes: Set[str], log_id: Optional[str]) -> Dict[str, Any]:
        """
        Scan hidden board ID pages until all prefixes are matched or pages run out.
        Resiliently handles common Monday API failures (expired cursors, 429s, server errors, etc).
        """
        from collections import deque
        from typing import Deque, Optional
        from datetime import datetime, timedelta

        pending = set(prefixes)
        collected: Set[str] = set()

        cursor: Optional[str] = None
        hidden_last_seen: Deque[str] = deque(maxlen=10)
        ID_PAGE_SIZE = 100
        hidden_pages = 0
        hidden_processed = 0
        start = datetime.utcnow()
        last_progress = start
        MAX_CONSECUTIVE_502 = 10
        STALL_WARNING_INTERVAL = timedelta(minutes=5)
        consecutive_502 = 0

        try:
            prev = (
                self.supabase.client.table("sync_log")
                .select("metadata, started_at")
                .eq("board_id", HIDDEN_ITEMS_BOARD_ID)
                .eq("board_name", "Recent Hidden Warmup")
                .order("started_at", desc=True)
                .limit(1)
                .execute()
            )
            if prev.data:
                meta = (prev.data[0].get("metadata") or {})
                if meta.get("phase") == "hidden" and meta.get("last_cursor"):
                    cursor = meta.get("last_cursor")
                    hidden_pages = int(meta.get("pages") or 0)
                    hidden_processed = int(meta.get("processed_total") or 0)
                    for value in meta.get("last_seen_ring") or []:
                        try:
                            hidden_last_seen.append(str(value))
                        except Exception:
                            pass
        except Exception:
            pass

        column_ids = ["name"]  # pull names so prefix matching works

        while True:
            try:
                if cursor:
                    page = self.monday.get_next_items_page(cursor, column_ids, limit=ID_PAGE_SIZE)
                else:
                    page = self.monday.get_items_page(HIDDEN_ITEMS_BOARD_ID, column_ids, limit=ID_PAGE_SIZE, cursor=None)
            except Exception as exc:
                msg = str(exc).lower()

                if "cursor" in msg and "expire" in msg:
                    if log_id:
                        try:
                            self.supabase.save_cursor_checkpoint(
                                log_id,
                                cursor,
                                {
                                    "phase": "hidden",
                                    "pages": hidden_pages,
                                    "processed_total": hidden_processed,
                                    "last_seen_ring": list(hidden_last_seen),
                                },
                            )
                        except Exception:
                            pass

                    seed = self.monday.get_items_page(HIDDEN_ITEMS_BOARD_ID, column_ids, limit=1, cursor=None)
                    cursor = seed.get("next_cursor")

                    try:
                        ff = await self._fast_forward_to_known_ids(
                            HIDDEN_ITEMS_BOARD_ID,
                            list(hidden_last_seen),
                            max_hops=500,
                        )
                        if ff:
                            logger.info(
                                "[rehydrate][hidden] fast-forward matched recent IDs (ring=%d); resuming near previous position",
                                len(hidden_last_seen),
                            )
                            cursor = ff
                        else:
                            logger.info(
                                "[rehydrate][hidden] fast-forward found no recent IDs (ring=%d); resuming from seed cursor (may reprocess)",
                                len(hidden_last_seen),
                            )
                    except Exception:
                        pass

                    await asyncio.sleep(1)
                    continue

                if "rate limit" in msg or "429" in msg:
                    retry_sec = 60
                    try:
                        import re
                        match = re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(exc))
                        if match:
                            retry_sec = max(1, int(match.group(1)))
                    except Exception:
                        pass
                    await asyncio.sleep(retry_sec)
                    continue

                if "bad gateway" in msg or "502" in msg:
                    consecutive_502 += 1
                    wait_for = 10 * min(consecutive_502, 6)
                    logger.warning(
                        "[rehydrate][hidden] 502 Bad Gateway (streak=%d) — sleeping %ds before retry",
                        consecutive_502,
                        wait_for,
                    )
                    if datetime.utcnow() - last_progress > STALL_WARNING_INTERVAL:
                        logger.warning(
                            "[rehydrate][hidden] still waiting for hidden page (> %s without progress)",
                            STALL_WARNING_INTERVAL,
                        )
                    if consecutive_502 >= MAX_CONSECUTIVE_502:
                        raise RuntimeError(
                            f"Aborting hidden pagination after {consecutive_502} consecutive 502 errors"
                        )
                    await asyncio.sleep(wait_for)
                    continue

                if "internal server" in msg or "503" in msg:
                    await asyncio.sleep(5)
                    continue

                if "504" in msg or "timeout" in msg:
                    await asyncio.sleep(30)
                    continue

                logger.error("[rehydrate][hidden] fatal page error: %s", exc)
                break

            items = page.get("items") or []
            if not items:
                break

            consecutive_502 = 0

            for entry in items:
                name = (entry.get("name") or "").strip()
                hid = str(entry.get("id") or "")
                if not name or not hid:
                    continue

                prefix = self._leading_digits(name)
                if prefix and prefix in pending:
                    collected.add(hid)
                    pending.discard(prefix)

                try:
                    hidden_last_seen.append(hid)
                except Exception:
                    pass

            hidden_pages += 1
            hidden_processed += len(items)
            last_progress = datetime.utcnow()
            elapsed = (datetime.utcnow() - start).total_seconds() or 1
            logger.info(
                "[rehydrate][hidden] pages=%d processed_total=%d matched=%d unmatched=%d rate=%.1f/s",
                hidden_pages,
                hidden_processed,
                len(collected),
                len(pending),
                hidden_processed / elapsed,
            )
            self.hidden_pages = hidden_pages
            self.hidden_ids_scanned = hidden_processed

            cursor = page.get("next_cursor")
            if log_id:
                try:
                    self.supabase.save_cursor_checkpoint(
                        log_id,
                        cursor,
                        {
                            "phase": "hidden",
                            "pages": hidden_pages,
                            "processed_total": hidden_processed,
                            "last_seen_ring": list(hidden_last_seen),
                        },
                    )
                except Exception:
                    pass

            if not cursor or not pending:
                break

            await asyncio.sleep(self.RATE_LIMIT_DELAY)

        logger.info(
            "Hidden ID scan finished: matched=%d, unmatched_prefixes=%d",
            len(collected),
            len(pending),
        )

        return {
            "matched_ids": list(collected),
            "pages": hidden_pages,
            "processed_total": hidden_processed,
            "unmatched_prefixes": len(pending),
        }

    async def _fast_forward_to_known_ids(self, board_id: str, known_ids: List[str], max_hops: int = 500) -> Optional[str]:
        if not known_ids:
            return None

        cursor: Optional[str] = None
        hops = 0

        while hops < max_hops:
            page = (
                self.monday.get_next_item_ids_page(cursor, self.chunk_size)
                if cursor
                else self.monday.get_item_ids_page(board_id, self.chunk_size, None)
            )
            items = page.get("items") or []
            if not items:
                return None

            ids_on_page = {str(it.get("id")) for it in items}
            if any(k in ids_on_page for k in known_ids):
                return page.get("next_cursor")

            cursor = page.get("next_cursor")
            if not cursor:
                return None

            hops += 1
            await asyncio.sleep(self.RATE_LIMIT_DELAY)

        return None

    async def _fetch_hidden_details(self, hidden_ids: Set[str]) -> List[Dict]:
        ids = list(hidden_ids)
        results: List[Dict] = []
        column_ids = list(HIDDEN_ITEMS_COLUMNS.values())

        for offset in range(0, len(ids), self.chunk_size):
            batch = ids[offset: offset + self.chunk_size]
            try:
                data = await self._retry(
                    self.monday.get_items_by_ids,
                    batch,
                    column_ids,
                )
                results.extend(data)
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
            except Exception as exc:
                logger.error("Hidden detail fetch failed for %s: %s", batch, exc)

        return results

    async def _rehydrate_batches(self, candidates: List["ProjectCandidate"]) -> None:
        """
        Stage 2/3: fetch subitems + parents for the selected Monday IDs, resolve mirrors,
        transform, and upsert. Mirrors the structure of `_rehydrate_projects_by_ids` but
        operates on the filtered candidate list and reuses the warmed hidden caches.
        """
        column_parent = list(PARENT_COLUMNS.values())
        column_sub = list(SUBITEM_COLUMNS.values())

        resume_index = 0
        processed_in_batch = 0
        total_processed = 0
        start_time = datetime.utcnow()

        while resume_index < len(candidates):
            chunk = candidates[resume_index: resume_index + self.chunk_size]
            chunk_ids = [c.monday_id for c in chunk]
            chunk_start = resume_index
            chunk_end = resume_index + len(chunk)

            logger.info(
                "Processing chunk %d-%d (%d projects)",
                chunk_start,
                chunk_end,
                len(chunk),
            )

            parent_items_raw = await self._retry(
                self.monday.get_items_by_ids,
                chunk_ids,
                column_parent,
            )
            parents = [
                self.sync_service._normalize_monday_item(parent)
                for parent in parent_items_raw
            ]

            subitems = await self._retry(
                self.monday.get_subitems_for_parents,
                chunk_ids,
                column_sub,
            )

            hidden_stub = []
            for item in parents:
                name = (item.get("name") or "").strip()
                prefix = self._leading_digits(name)
                if not prefix:
                    continue
                if prefix in self.sync_service._hidden_lookup_by_name:
                    hidden_stub.append(self.sync_service._hidden_lookup_by_name[prefix])

            processed = self.sync_service._process_and_resolve_mirrors(
                parents,
                subitems,
                hidden_stub,
            )

            subitems_data = self.sync_service._transform_for_subitems_table(processed["subitems"])
            gestation_map = self.sync_service._compute_gestation_fallback_from_subitems(subitems_data)
            rollup_map = self.sync_service._rollup_new_enquiry_from_subitems(subitems_data)

            projects_data = self.sync_service._transform_for_projects_table(processed["projects"])
            self.sync_service._apply_project_new_enquiry_rollup(projects_data, rollup_map)
            self.sync_service._apply_project_gestation_fallback(projects_data, gestation_map)

            subitems_written = await self.sync_service._batch_upsert_subitems(subitems_data)
            self.subitem_batches += 1
            self.subitem_rows_upserted += subitems_written

            projects_written = await self.sync_service._batch_upsert_projects(projects_data)
            self.project_batches += 1
            self.project_rows_upserted += projects_written

            total_processed += len(chunk_ids)
            elapsed = (datetime.utcnow() - start_time).total_seconds() or 1
            logger.info(
                (
                    "Chunk committed | range=%d-%d projects=%d subitems=%d "
                    "proj_rows=%d sub_rows=%d total_proj_rows=%d total_sub_rows=%d "
                    "project_batches=%d subitem_batches=%d rate=%.1f/s"
                ),
                chunk_start,
                chunk_end,
                len(projects_data),
                len(subitems_data),
                projects_written,
                subitems_written,
                self.project_rows_upserted,
                self.subitem_rows_upserted,
                self.project_batches,
                self.subitem_batches,
                total_processed / elapsed,
            )

            processed_in_batch += len(chunk_ids)
            resume_index = chunk_end

            await asyncio.sleep(self.RATE_LIMIT_DELAY)
            if processed_in_batch >= self.BATCH_SIZE:
                logger.info("Batch throttle pause (%ds)", self.BATCH_DELAY)
                await asyncio.sleep(self.BATCH_DELAY)
                processed_in_batch = 0

        logger.info("Processed %d candidate projects", len(candidates))

    def _load_candidate_projects(self) -> List["ProjectCandidate"]:
        """
        Query Supabase for monday_id + name of projects whose `date_created` is >= since date.
        """
        try:
            rows = (
                self.supabase.client.table("projects")
                .select("monday_id, item_name, project_name, date_created")
                .gte("date_created", self.since.date().isoformat())
                .order("date_created", desc=False)
                .execute()
                .data
                or []
            )
        except Exception as exc:
            logger.error("Failed to query Supabase projects: %s", exc)
            return []

        candidates = []
        for row in rows:
            monday_id = str(row.get("monday_id") or "").strip()
            if not monday_id:
                continue
            name = (row.get("item_name") or row.get("project_name") or "").strip()
            prefix = self._leading_digits(name)
            candidates.append(ProjectCandidate(monday_id, name, prefix))

        return candidates

    async def _retry(self, fn, *args, **kwargs):
        attempt = 0
        while attempt < self.MAX_RETRIES:
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                attempt += 1
                msg = str(exc).lower()
                if attempt >= self.MAX_RETRIES:
                    raise

                if "rate limit" in msg or "429" in msg:
                    wait = 60
                elif "504" in msg or "timeout" in msg:
                    wait = 30
                elif "internal server" in msg or "503" in msg:
                    wait = 5
                else:
                    wait = 5

                logger.warning("Retry %d/%d after error: %s (sleep=%ds)", attempt, self.MAX_RETRIES, exc, wait)
                await asyncio.sleep(wait)

        raise RuntimeError("Exceeded retries")

    @staticmethod
    def _leading_digits(value: str) -> Optional[str]:
        """Return leading numeric token used as the shared prefix (SampleBoardData.txt pattern)."""
        value = (value or "").strip()
        if not value:
            return None
        token = []
        for ch in value:
            if ch.isdigit():
                token.append(ch)
            else:
                break
        return "".join(token) or None


class ProjectCandidate:
    __slots__ = ("monday_id", "name", "prefix")

    def __init__(self, monday_id: str, name: str, prefix: Optional[str]):
        self.monday_id = monday_id
        self.name = name
        self.prefix = prefix


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Rehydrate recent projects only")
    parser.add_argument(
        "--since",
        type=str,
        help="ISO date (YYYY-MM-DD). Rehydrate projects with Supabase date_created >= this value",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="If --since not provided, rehydrate projects created within the past N days (default 7)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of parent IDs to process per chunk",
    )
    args = parser.parse_args()

    if args.since:
        since = datetime.fromisoformat(args.since)
    else:
        since = datetime.utcnow() - timedelta(days=args.days_back)

    manager = RecentRehydrationManager(since=since, chunk_size=args.chunk_size)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())