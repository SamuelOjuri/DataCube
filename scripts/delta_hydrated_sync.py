# scripts/delta_hydrated_sync.py

import asyncio
import logging
import sys
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set

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
logger = logging.getLogger("delta_rehydrate")


class DeltaRehydrationManager:
    """
    Create-only rehydration for projects created in Monday after a cutoff date
    that are still missing from Supabase. The flow mirrors the full rehydrate
    pipeline but scopes work to new Monday items, maintaining the hidden →
    subitems → parent enrichment despite the broken board relation.
    """

    def __init__(self, since: datetime, chunk_size: int = 100):
        self.since = since
        self.chunk_size = chunk_size
        self.supabase = SupabaseClient()
        self.sync_service = DataSyncService()
        self.monday = MondayClient()

        self.MAX_RETRIES = 5
        self.RATE_LIMIT_DELAY = 2.5
        self.BATCH_DELAY = 3
        self.BATCH_SIZE = 1000

        self.hidden_pages = 0
        self.hidden_ids_scanned = 0
        self.subitem_batches = 0
        self.project_batches = 0
        self.projects_inserted = 0
        self.project_seed_batches = 0
        self.project_placeholders_seeded = 0
        self._hidden_rows_by_prefix: Dict[str, List[Dict]] = {}

    async def run(self) -> None:
        logger.info("Starting delta rehydrate (since=%s)", self.since.date())

        supabase_ids = self._load_existing_supabase_ids()
        logger.info("Supabase already has %d projects on/after cutoff", len(supabase_ids))

        candidates = await self._discover_new_monday_projects(supabase_ids)
        if not candidates:
            logger.info("No new Monday projects found after %s", self.since.date())
            return

        prefixes = {c.prefix for c in candidates if c.prefix}
        logger.info(
            "Discovered %d new Monday projects (%d unique prefixes)",
            len(candidates),
            len(prefixes),
        )

        await self._warm_hidden_cache(prefixes)
        await self._rehydrate_batches(candidates)

        logger.info(
            (
                "Delta rehydrate complete | new_projects=%d placeholders_seeded=%d "
                "hidden_pages=%d subitem_batches=%d project_batches=%d seed_batches=%d"
            ),
            self.projects_inserted,
            self.project_placeholders_seeded,
            self.hidden_pages,
            self.subitem_batches,
            self.project_batches,
            self.project_seed_batches,
        )

    def _load_existing_supabase_ids(self) -> Set[str]:
        """
        Get monday_id values already present in Supabase for projects on/after the cutoff.
        """
        try:
            rows = (
                self.supabase.client.table("projects")
                .select("monday_id")
                .gte("date_created", self.since.date().isoformat())
                .execute()
                .data
                or []
            )
        except Exception as exc:
            logger.error("Failed to query Supabase projects: %s", exc)
            return set()

        ids = {str(row.get("monday_id")) for row in rows if row.get("monday_id")}
        return ids

    async def _discover_new_monday_projects(self, supabase_ids: Set[str]) -> List["ProjectCandidate"]:
        """
        Query the parent board for projects created on/after the cutoff date whose
        monday_id is not already present in Supabase.
        """
        candidates: List[ProjectCandidate] = []
        seen: Set[str] = set()

        column_ids = list(PARENT_COLUMNS.values())
        cutoff_iso = self.since.date().isoformat()
        cursor: Optional[str] = None
        ID_PAGE_SIZE = 100
        page_idx = 0

        while True:
            try:
                page = self.monday.get_items_page_since_date(
                    PARENT_BOARD_ID,
                    column_ids,
                    cutoff_iso,
                    limit=ID_PAGE_SIZE,
                    cursor=cursor,
                )
            except Exception as exc:
                msg = str(exc).lower()
                if "rate limit" in msg or "429" in msg:
                    retry_sec = 60
                    try:
                        import re
                        m = re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(exc))
                        if m:
                            retry_sec = max(1, int(m.group(1)))
                    except Exception:
                        pass
                    logger.info("Rate limit hit on parent board; sleeping %ds", retry_sec)
                    await asyncio.sleep(retry_sec)
                    continue
                if "bad gateway" in msg or "502" in msg:
                    logger.warning("Monday returned 502 Bad Gateway for parent page; retrying in 10s")
                    await asyncio.sleep(10)
                    continue
                if "internal server" in msg or "503" in msg:
                    await asyncio.sleep(5)
                    continue
                if "504" in msg or "timeout" in msg:
                    await asyncio.sleep(30)
                    continue

                logger.error("Fatal parent page error: %s", exc)
                break

            items = page.get("items") or []
            if not items:
                break

            page_idx += 1
            logger.info("Parent page %d fetched (%d items)", page_idx, len(items))

            for item in items:
                monday_id = str(item.get("id") or "")
                if not monday_id or monday_id in seen or monday_id in supabase_ids:
                    continue

                normalized = self.sync_service._normalize_monday_item(item)
                date_created = self._parse_date(normalized.get("date_created"))
                if not date_created or date_created < self.since.date():
                    continue

                name = normalized.get("name") or item.get("name") or ""
                prefix = self._leading_digits(name)
                candidates.append(ProjectCandidate(monday_id, name, prefix))
                seen.add(monday_id)

            cursor = page.get("next_cursor")
            if not cursor:
                break

            await asyncio.sleep(self.RATE_LIMIT_DELAY)

        logger.info("Parent discovery finished: %d candidates", len(candidates))
        return candidates

    async def _warm_hidden_cache(self, prefixes: Set[str]) -> None:
        """
        Preload hidden items needed for the target prefixes. This primes the
        DataSyncService caches so subitems can recover hidden metadata even when
        the board relation is broken.
        """
        if not prefixes:
            logger.info("No prefixes supplied; skipping hidden warm-up")
            return

        logger.info("Priming hidden cache for %d prefixes", len(prefixes))
        logger.info("Hidden prefix targets: %s", ", ".join(sorted(prefixes)))

        log_id = self.supabase.log_sync_operation("rehydrate", HIDDEN_ITEMS_BOARD_ID, "Delta Hidden Warmup")

        self._hidden_rows_by_prefix.clear()
        try:
            stats = await self._collect_hidden_ids(prefixes, log_id)
            matched_ids = set(stats.get("matched_ids") or [])
            if not matched_ids:
                logger.warning("No hidden items matched requested prefixes; subitem fallback will rely on full cache")
                self._hidden_rows_by_prefix.clear()
                self.supabase.update_sync_log(log_id, "completed", stats)
                return

            logger.info("Matched %d hidden items; fetching full payloads", len(matched_ids))
            hidden_rows = await self._fetch_hidden_details(matched_ids)
            if not hidden_rows:
                logger.warning("Failed to fetch detailed hidden items")
                self.supabase.update_sync_log(log_id, "completed", stats)
                return

            prefix_buckets: Dict[str, List[Dict]] = {}
            for row in hidden_rows:
                name = (row.get("name") or "").strip()
                prefix = self._leading_digits(name)
                if prefix:
                    prefix_buckets.setdefault(prefix, []).append(row)
            self._hidden_rows_by_prefix = prefix_buckets

            hidden_data = self.sync_service._transform_for_hidden_table(hidden_rows)
            if not hidden_data:
                logger.warning("Hidden transform yielded no rows")
                self.supabase.update_sync_log(log_id, "completed", stats)
                return

            updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
            stats["hidden_rows_upserted"] = updated
            logger.info(
                "Hidden warm-up complete | upserted=%d rows matched=%d pages=%d processed=%d",
                updated,
                len(matched_ids),
                stats.get("pages", 0),
                stats.get("processed_total", 0),
            )
            self.supabase.update_sync_log(log_id, "completed", stats)
        except Exception as exc:
            logger.error("Hidden warm-up failed: %s", exc)
            self.supabase.update_sync_log(log_id, "failed", error=str(exc))
            raise

    async def _collect_hidden_ids(self, prefixes: Set[str], log_id: Optional[str]) -> Dict[str, Any]:
        """
        Walk the hidden board (IDs + names), matching prefixes with full items_page
        responses so the prefix check can succeed. Includes stall detection and
        capped retries to avoid infinite loops.
        """
        pending = set(prefixes)
        collected: Set[str] = set()

        column_ids = ["name"]  # capturing names is all we need for prefix extraction
        cursor: Optional[str] = None
        hidden_pages = 0
        hidden_processed = 0
        hidden_last_seen: Deque[str] = deque(maxlen=10)
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
                .eq("board_name", "Delta Hidden Warmup")
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

        while True:
            try:
                if cursor:
                    page = self.monday.get_next_items_page(cursor, column_ids, limit=100)
                else:
                    page = self.monday.get_items_page(HIDDEN_ITEMS_BOARD_ID, column_ids, limit=100, cursor=None)
            except Exception as exc:
                msg = str(exc).lower()

                if "cursorexpirederror" in msg or ("cursor" in msg and "expire" in msg):
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
                                "[delta][hidden] fast-forward matched recent IDs (ring=%d); resuming near previous position",
                                len(hidden_last_seen),
                            )
                            cursor = ff
                        else:
                            logger.info(
                                "[delta][hidden] fast-forward found no recent IDs (ring=%d); resuming from seed cursor (may reprocess)",
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
                    logger.info("Hidden pagination hit rate limit; sleeping %ds", retry_sec)
                    await asyncio.sleep(retry_sec)
                    continue

                if "bad gateway" in msg or "502" in msg:
                    consecutive_502 += 1
                    wait_for = 10 * min(consecutive_502, 6)
                    logger.warning(
                        "[delta][hidden] 502 Bad Gateway (streak=%d) — sleeping %ds before retry",
                        consecutive_502,
                        wait_for,
                    )
                    if datetime.utcnow() - last_progress > STALL_WARNING_INTERVAL:
                        logger.warning(
                            "[delta][hidden] still waiting for hidden page (> %s without progress)",
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

                logger.error("[delta][hidden] fatal page error: %s", exc)
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
            self.hidden_pages = hidden_pages
            self.hidden_ids_scanned = hidden_processed
            last_progress = datetime.utcnow()

            elapsed = (datetime.utcnow() - start).total_seconds() or 1
            logger.info(
                "[delta][hidden] pages=%d processed_total=%d matched=%d unmatched=%d rate=%.1f/s",
                hidden_pages,
                hidden_processed,
                len(collected),
                len(pending),
                hidden_processed / elapsed,
            )

            cursor = page.get("next_cursor")
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

        return {
            "matched_ids": list(collected),
            "pages": hidden_pages,
            "processed_total": hidden_processed,
            "unmatched_prefixes": len(pending),
        }

    async def _fast_forward_to_known_ids(self, board_id: str, known_ids: List[str], max_hops: int = 500) -> Optional[str]:
        if not known_ids:
            return None
        cursor = None
        hops = 0
        while hops < max_hops:
            page = (
                self.monday.get_next_item_ids_page(cursor, self.chunk_size)
                if cursor else
                self.monday.get_item_ids_page(board_id, self.chunk_size, None)
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
        column_parent = list(PARENT_COLUMNS.values())
        column_sub = list(SUBITEM_COLUMNS.values())

        resume_index = 0
        processed_in_batch = 0

        while resume_index < len(candidates):
            chunk = candidates[resume_index: resume_index + self.chunk_size]
            chunk_ids = [c.monday_id for c in chunk]

            logger.info(
                "Processing chunk %d-%d (%d projects)",
                resume_index,
                resume_index + len(chunk),
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

            placeholders = self._prepare_project_placeholders(
                parents,
                already_normalized=True,
            )
            if placeholders:
                self.project_seed_batches += 1
                self.project_placeholders_seeded += len(placeholders)
                await self.sync_service._batch_upsert_projects(placeholders)
                logger.info(
                    "Seeded parent placeholders | projects=%d range=%d-%d",
                    len(placeholders),
                    resume_index,
                    resume_index + len(chunk),
                )

            subitems = await self._retry(
                self.monday.get_subitems_for_parents,
                chunk_ids,
                column_sub,
            )

            hidden_stub = []
            hidden_ids_used: Set[str] = set()
            for item in parents:
                name = (item.get("name") or "").strip()
                prefix = self._leading_digits(name)
                if not prefix:
                    continue
                for hidden_row in self._hidden_rows_by_prefix.get(prefix, []):
                    hid = str(hidden_row.get("id") or hidden_row.get("monday_id") or "")
                    if hid and hid in hidden_ids_used:
                        continue
                    hidden_stub.append(hidden_row)
                    if hid:
                        hidden_ids_used.add(hid)

            processed = self.sync_service._process_and_resolve_mirrors(
                parents,
                subitems,
                hidden_stub,
            )

            subitems_data = self.sync_service._transform_for_subitems_table(processed["subitems"])
            rollup_map = self.sync_service._rollup_new_enquiry_from_subitems(subitems_data)
            gestation_map = self.sync_service._compute_gestation_fallback_from_subitems(subitems_data)

            projects_data = self.sync_service._transform_for_projects_table(processed["projects"])
            self.sync_service._apply_project_new_enquiry_rollup(projects_data, rollup_map)
            self.sync_service._apply_project_gestation_fallback(projects_data, gestation_map)

            self.subitem_batches += 1
            await self.sync_service._batch_upsert_subitems(subitems_data)

            self.project_batches += 1
            updated = await self.sync_service._batch_upsert_projects(projects_data)
            self.projects_inserted += len(projects_data)
            logger.info(
                "Chunk inserted | projects=%d subitems=%d new_project_rows=%d",
                len(projects_data),
                len(subitems_data),
                len(projects_data),
            )

            processed_in_batch += len(chunk_ids)
            resume_index += len(chunk)

            await asyncio.sleep(self.RATE_LIMIT_DELAY)
            if processed_in_batch >= self.BATCH_SIZE:
                logger.info("Batch throttle pause (%ds)", self.BATCH_DELAY)
                await asyncio.sleep(self.BATCH_DELAY)
                processed_in_batch = 0

        logger.info("Inserted %d new projects total", self.projects_inserted)

    def _prepare_project_placeholders(
        self,
        parents: List[Dict],
        already_normalized: bool = False,
    ) -> List[Dict]:
        """Transform parent items into minimal project rows so Supabase FKs succeed before enrichment."""
        if not parents:
            return []

        normalized = parents if already_normalized else [
            self.sync_service._normalize_monday_item(parent) for parent in parents
        ]
        placeholders = self.sync_service._transform_for_projects_table(normalized)

        for project in placeholders:
            if project.get("new_enquiry_value") is None:
                project["new_enquiry_value"] = 0.0

        return placeholders

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

    def _parse_date(self, value: Optional[str]) -> Optional[datetime.date]:
        if not value:
            return None
        try:
            if isinstance(value, str):
                return datetime.strptime(value, "%Y-%m-%d").date()
        except Exception:
            return None
        return None

    @staticmethod
    def _leading_digits(value: str) -> Optional[str]:
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

    parser = argparse.ArgumentParser(description="Delta rehydrate (create-only) recently created Monday projects")
    parser.add_argument(
        "--since",
        type=str,
        help="ISO date (YYYY-MM-DD). Include Monday projects with date_created >= this value",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="If --since not provided, include Monday projects created within the past N days (default 7)",
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

    manager = DeltaRehydrationManager(since=since, chunk_size=args.chunk_size)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())