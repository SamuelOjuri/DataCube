"""Reusable automation tasks extracted from CLI scripts."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from ..config import (
    ANALYSIS_LOOKBACK_DAYS,
    GEMINI_API_KEY,
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_COLUMNS,
    SUBITEM_COLUMNS,
    PARENT_COLUMNS,
)
from ..database.supabase_client import SupabaseClient
from ..database.sync_service import DataSyncService
from ..core.monday_client import MondayClient
from ..services.analysis_service import AnalysisService
from ..services.monday_update_service import MondayUpdateService


logger = logging.getLogger(__name__)


@dataclass
class ProjectCandidate:
    """Project metadata used during rehydration flows."""

    monday_id: str
    name: str
    prefix: Optional[str]


class DeltaRehydrationManager:
    """Create-only rehydration for Monday projects absent in Supabase."""

    def __init__(
        self,
        since: datetime,
        chunk_size: int = 100,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.since = since
        self.chunk_size = chunk_size
        self.supabase = SupabaseClient()
        self.sync_service = DataSyncService()
        self.monday = MondayClient()
        self.logger = logger or logging.getLogger(f"{__name__}.delta_rehydrate")

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

    async def _warm_hidden_cache_targeted(self, prefixes: Set[str]) -> None:
        """
        Targeted hidden warm-up for small prefix sets (create-only flow).
        Uses contains_text query instead of crawling the entire hidden board.
        """
        if not prefixes:
            self.logger.info("No prefixes supplied; skipping targeted hidden warm-up")
            return

        self.logger.info(
            "Using targeted hidden lookup for %d prefixes: %s",
            len(prefixes),
            ", ".join(sorted(prefixes)),
        )

        column_ids = list(HIDDEN_ITEMS_COLUMNS.values())

        try:
            hidden_rows = self.monday.get_hidden_items_by_prefix(
                board_id=HIDDEN_ITEMS_BOARD_ID,
                prefixes=list(prefixes),
                column_ids=column_ids,
                limit=50,
            )

            if not hidden_rows:
                self.logger.warning("No hidden items found for prefixes: %s", prefixes)
                return

            # Build prefix buckets for later use in _rehydrate_batches
            prefix_buckets: Dict[str, List[Dict]] = {}
            for row in hidden_rows:
                name = (row.get("name") or "").strip()
                prefix = self._leading_digits(name)
                if prefix:
                    prefix_buckets.setdefault(prefix, []).append(row)
            self._hidden_rows_by_prefix = prefix_buckets

            # Transform + upsert so Supabase stays in sync
            hidden_data = self.sync_service._transform_for_hidden_table(hidden_rows)
            if not hidden_data:
                self.logger.warning("Hidden transform yielded no rows for prefixes: %s", prefixes)
                return

            updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
            self.logger.info(
                "Targeted hidden warm-up complete | prefixes=%d fetched=%d upserted=%d",
                len(prefixes),
                len(hidden_rows),
                updated,
            )
        except Exception as exc:
            self.logger.error("Targeted hidden warm-up failed: %s", exc)
            # Fall back to the full crawl so the run can still proceed
            await self._warm_hidden_cache(prefixes)

    async def run(self) -> None:
        self.logger.info("Starting delta rehydrate (since=%s)", self.since.date())

        supabase_ids = self._load_existing_supabase_ids()
        self.logger.info("Supabase already has %d projects on/after cutoff", len(supabase_ids))

        candidates = await self._discover_new_monday_projects(supabase_ids)
        if not candidates:
            self.logger.info("No new Monday projects found after %s", self.since.date())
            return

        prefixes = {c.prefix for c in candidates if c.prefix}

        self.logger.info(
            "Discovered %d new Monday projects (%d unique prefixes)",
            len(candidates),
            len(prefixes),
        )

        if prefixes:
            if len(prefixes) <= 50:
                await self._warm_hidden_cache_targeted(prefixes)
            else:
                await self._warm_hidden_cache(prefixes)

        await self._rehydrate_batches(candidates)

        self.logger.info(
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
            self.logger.error("Failed to query Supabase projects: %s", exc)
            return set()

        return {str(row.get("monday_id")) for row in rows if row.get("monday_id")}

    async def _discover_new_monday_projects(self, supabase_ids: Set[str]) -> List[ProjectCandidate]:
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

                        match = re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(exc))
                        if match:
                            retry_sec = max(1, int(match.group(1)))
                    except Exception:
                        pass
                    self.logger.info("Rate limit hit on parent board; sleeping %ds", retry_sec)
                    await asyncio.sleep(retry_sec)
                    continue
                if "bad gateway" in msg or "502" in msg:
                    self.logger.warning("Monday returned 502 Bad Gateway for parent page; retrying in 10s")
                    await asyncio.sleep(10)
                    continue
                if "internal server" in msg or "503" in msg:
                    await asyncio.sleep(5)
                    continue
                if "504" in msg or "timeout" in msg:
                    await asyncio.sleep(30)
                    continue

                self.logger.error("Fatal parent page error: %s", exc)
                break

            items = page.get("items") or []
            if not items:
                break

            page_idx += 1
            self.logger.info("Parent page %d fetched (%d items)", page_idx, len(items))

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

        self.logger.info("Parent discovery finished: %d candidates", len(candidates))
        return candidates

    async def _warm_hidden_cache(self, prefixes: Set[str]) -> None:
        if not prefixes:
            self.logger.info("No prefixes supplied; skipping hidden warm-up")
            return

        self.logger.info("Priming hidden cache for %d prefixes", len(prefixes))
        self.logger.info("Hidden prefix targets: %s", ", ".join(sorted(prefixes)))

        log_id = self.supabase.log_sync_operation("rehydrate", HIDDEN_ITEMS_BOARD_ID, "Delta Hidden Warmup")
        self._hidden_rows_by_prefix.clear()
        try:
            stats = await self._collect_hidden_ids(prefixes, log_id)
            matched_ids = set(stats.get("matched_ids") or [])
            if stats.get("unmatched_prefix_values"):
                self.logger.warning(
                    "Hidden warm-up finished with unmatched prefixes: %s",
                    ", ".join(stats["unmatched_prefix_values"]),
                )
            stats.setdefault("unmatched_prefix_values", [])
            if not matched_ids:
                self.logger.warning(
                    "No hidden items matched requested prefixes; subitem fallback will rely on full cache"
                )
                self._hidden_rows_by_prefix.clear()
                self.supabase.update_sync_log(log_id, "completed", stats)
                return

            self.logger.info("Matched %d hidden items; fetching full payloads", len(matched_ids))
            hidden_rows = await self._fetch_hidden_details(matched_ids)
            if not hidden_rows:
                self.logger.warning("Failed to fetch detailed hidden items")
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
                self.logger.warning("Hidden transform yielded no rows")
                self.supabase.update_sync_log(log_id, "completed", stats)
                return

            updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
            stats["hidden_rows_upserted"] = updated
            self.logger.info(
                "Hidden warm-up complete | upserted=%d rows matched=%d pages=%d processed=%d",
                updated,
                len(matched_ids),
                stats.get("pages", 0),
                stats.get("processed_total", 0),
            )
            self.supabase.update_sync_log(log_id, "completed", stats)
        except Exception as exc:
            self.logger.error("Hidden warm-up failed: %s", exc)
            self.supabase.update_sync_log(log_id, "failed", error=str(exc))
            raise

    async def _collect_hidden_ids(self, prefixes: Set[str], log_id: Optional[str]) -> Dict[str, Any]:
        from collections import deque

        pending = set(prefixes)
        collected: Set[str] = set()

        column_ids = ["name"]
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
                            self.logger.info(
                                "[delta][hidden] fast-forward matched recent IDs (ring=%d); resuming near previous position",
                                len(hidden_last_seen),
                            )
                            cursor = ff
                        else:
                            self.logger.info(
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
                    self.logger.info("Hidden pagination hit rate limit; sleeping %ds", retry_sec)
                    await asyncio.sleep(retry_sec)
                    continue

                if "bad gateway" in msg or "502" in msg:
                    consecutive_502 += 1
                    wait_for = 10 * min(consecutive_502, 6)
                    self.logger.warning(
                        "[delta][hidden] 502 Bad Gateway (streak=%d) — sleeping %ds before retry",
                        consecutive_502,
                        wait_for,
                    )
                    if datetime.utcnow() - last_progress > STALL_WARNING_INTERVAL:
                        self.logger.warning(
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

                self.logger.error("[delta][hidden] fatal page error: %s", exc)
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
            self.logger.info(
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
            "unmatched_prefix_values": sorted(pending),
        }

    async def _fast_forward_to_known_ids(
        self, board_id: str, known_ids: List[str], max_hops: int = 500
    ) -> Optional[str]:
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
            batch = ids[offset : offset + self.chunk_size]
            try:
                data = await self._retry(
                    self.monday.get_items_by_ids,
                    batch,
                    column_ids,
                )
                results.extend(data)
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
            except Exception as exc:
                self.logger.error("Hidden detail fetch failed for %s: %s", batch, exc)

        return results

    async def _rehydrate_batches(self, candidates: List[ProjectCandidate]) -> None:
        column_parent = list(PARENT_COLUMNS.values())
        column_sub = list(SUBITEM_COLUMNS.values())
        resume_index = 0
        processed_in_batch = 0

        while resume_index < len(candidates):
            chunk = candidates[resume_index : resume_index + self.chunk_size]
            chunk_ids = [c.monday_id for c in chunk]

            self.logger.info(
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

            placeholders = self._prepare_project_placeholders(parents, already_normalized=True)
            if placeholders:
                self.project_seed_batches += 1
                self.project_placeholders_seeded += len(placeholders)
                await self.sync_service._batch_upsert_projects(placeholders)
                self.logger.info(
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

            hidden_stub: List[Dict] = []
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

            processed = self.sync_service._process_and_resolve_mirrors(parents, subitems, hidden_stub)

            subitems_data = self.sync_service._transform_for_subitems_table(processed["subitems"])
            rollup_map = self.sync_service._rollup_new_enquiry_from_subitems(subitems_data)
            gestation_map = self.sync_service._compute_gestation_fallback_from_subitems(subitems_data)
            order_total_map, order_date_map = self.sync_service._rollup_order_values_from_subitems(
                subitems_data
            )

            projects_data = self.sync_service._transform_for_projects_table(processed["projects"])
            self.sync_service._apply_project_new_enquiry_rollup(projects_data, rollup_map)
            self.sync_service._apply_project_gestation_fallback(projects_data, gestation_map)
            self.sync_service._apply_project_order_rollup(
                projects_data, order_total_map, order_date_map
            )

            self.subitem_batches += 1
            await self.sync_service._batch_upsert_subitems(subitems_data)

            self.project_batches += 1
            await self.sync_service._batch_upsert_projects(projects_data)
            self.projects_inserted += len(projects_data)
            self.logger.info(
                "Chunk inserted | projects=%d subitems=%d new_project_rows=%d",
                len(projects_data),
                len(subitems_data),
                len(projects_data),
            )

            processed_in_batch += len(chunk_ids)
            resume_index += len(chunk)

            await asyncio.sleep(self.RATE_LIMIT_DELAY)
            if processed_in_batch >= self.BATCH_SIZE:
                self.logger.info("Batch throttle pause (%ds)", self.BATCH_DELAY)
                await asyncio.sleep(self.BATCH_DELAY)
                processed_in_batch = 0

        self.logger.info("Inserted %d new projects total", self.projects_inserted)

    def _prepare_project_placeholders(
        self,
        parents: List[Dict],
        *,
        already_normalized: bool = False,
    ) -> List[Dict]:
        if not parents:
            return []

        normalized = (
            parents
            if already_normalized
            else [self.sync_service._normalize_monday_item(parent) for parent in parents]
        )
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

                self.logger.warning(
                    "Retry %d/%d after error: %s (sleep=%ds)", attempt, self.MAX_RETRIES, exc, wait
                )
                await asyncio.sleep(wait)

        raise RuntimeError("Exceeded retries")

    def _parse_date(self, value: Optional[str]) -> Optional[date]:
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
        token: List[str] = []
        for ch in value:
            if ch.isdigit():
                token.append(ch)
            else:
                break
        return "".join(token) or None


class RecentRehydrationManager:
    """Targeted rehydrate for Supabase projects created after a cutoff."""

    def __init__(
        self,
        since: datetime,
        chunk_size: int = 100,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.since = since
        self.chunk_size = chunk_size
        self.supabase = SupabaseClient()
        self.sync_service = DataSyncService()
        self.monday = MondayClient()
        self.logger = logger or logging.getLogger(f"{__name__}.rehydrate_recent")

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
        self.logger.info("Starting recent rehydrate run (since=%s)", self.since.date())

        candidates = self._load_candidate_projects()
        if not candidates:
            self.logger.info(
                "No Supabase projects found with date_created >= %s", self.since.date()
            )
            return

        prefixes = {c.prefix for c in candidates if c.prefix}
        self.logger.info(
            "Supabase returned %d projects (%d unique prefixes)",
            len(candidates),
            len(prefixes),
        )

        # await self._warm_hidden_cache(prefixes)
        # await self._rehydrate_batches(candidates)

        # Use targeted lookup for small batches; fall back to full crawl otherwise
        if prefixes:
            if len(prefixes) <= 50:
                await self._warm_hidden_cache_targeted(prefixes)
            else:
                await self._warm_hidden_cache(prefixes)

        await self._rehydrate_batches(candidates)

        self.logger.info(
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

        self.logger.info("Recent rehydrate completed successfully")

    async def _warm_hidden_cache(self, prefixes: Set[str]) -> None:
        if not prefixes:
            self.logger.info("No prefixes supplied; skipping hidden warm-up")
            return

        self.logger.info("Priming hidden cache for %d prefixes", len(prefixes))

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
                self.logger.warning(
                    "No hidden items matched requested prefixes; subitem fallback will rely on full cache"
                )
                if log_id:
                    self.supabase.update_sync_log(log_id, "completed", stats)
                return

            self.logger.info("Matched %d hidden items; fetching full payloads", len(matched_ids))
            hidden_rows = await self._fetch_hidden_details(matched_ids)
            if not hidden_rows:
                self.logger.warning("Failed to fetch detailed hidden items")
                if log_id:
                    self.supabase.update_sync_log(log_id, "completed", stats)
                return
            self.hidden_rows_fetched = len(hidden_rows)

            hidden_data = self.sync_service._transform_for_hidden_table(hidden_rows)
            if not hidden_data:
                self.logger.warning("Hidden transform yielded no rows")
                if log_id:
                    self.supabase.update_sync_log(log_id, "completed", stats)
                return

            updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
            self.hidden_rows_upserted = updated
            stats["hidden_rows_upserted"] = updated
            if stats.get("unmatched_prefix_values"):
                self.logger.warning(
                    "Hidden warm-up complete with unmatched prefixes: %s",
                    ", ".join(stats["unmatched_prefix_values"]),
                )
            self.logger.info(
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
            self.logger.error("Hidden warm-up failed: %s", exc)
            if log_id:
                self.supabase.update_sync_log(log_id, "failed", error=str(exc))
            raise

    async def _warm_hidden_cache_targeted(self, prefixes: Set[str]) -> None:
        """
        Targeted hidden cache warm-up for small prefix sets (webhook triggers).
        Uses filtered GraphQL query instead of full board scan.
        """
        if not prefixes:
            self.logger.info("No prefixes supplied; skipping targeted hidden warm-up")
            return

        self.logger.info("Using targeted hidden lookup for %d prefixes: %s", len(prefixes), prefixes)

        column_ids = list(HIDDEN_ITEMS_COLUMNS.values())

        try:
            # Query hidden items directly by prefix using contains_text
            hidden_rows = self.monday.get_hidden_items_by_prefix(
                board_id=HIDDEN_ITEMS_BOARD_ID,
                prefixes=list(prefixes),
                column_ids=column_ids,
                limit=50,
            )

            if not hidden_rows:
                self.logger.warning("No hidden items found for prefixes: %s", prefixes)
                return

            self.hidden_rows_fetched = len(hidden_rows)
            self.logger.info("Fetched %d hidden items for prefixes", len(hidden_rows))

            # Transform and populate caches
            hidden_data = self.sync_service._transform_for_hidden_table(hidden_rows)
            if hidden_data:
                updated = await self.sync_service._batch_upsert_hidden_items(hidden_data)
                self.hidden_rows_upserted = updated
                self.logger.info(
                    "Targeted hidden warm-up complete | prefixes=%d fetched=%d upserted=%d",
                    len(prefixes),
                    self.hidden_rows_fetched,
                    updated,
                )
            else:
                self.logger.warning("Hidden transform yielded no rows for prefixes: %s", prefixes)

        except Exception as exc:
            self.logger.error("Targeted hidden warm-up failed: %s", exc)
            # Fall back to full scan on error
            self.logger.info("Falling back to full hidden board scan")
            await self._warm_hidden_cache(prefixes)

    async def _collect_hidden_ids(self, prefixes: Set[str], log_id: Optional[str]) -> Dict[str, Any]:
        from collections import deque

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

        column_ids = ["name"]

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
                            self.logger.info(
                                "[rehydrate][hidden] fast-forward matched recent IDs (ring=%d); resuming near previous position",
                                len(hidden_last_seen),
                            )
                            cursor = ff
                        else:
                            self.logger.info(
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
                    self.logger.warning(
                        "[rehydrate][hidden] 502 Bad Gateway (streak=%d) — sleeping %ds before retry",
                        consecutive_502,
                        wait_for,
                    )
                    if datetime.utcnow() - last_progress > STALL_WARNING_INTERVAL:
                        self.logger.warning(
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

                self.logger.error("[rehydrate][hidden] fatal page error: %s", exc)
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
            self.logger.info(
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

        self.logger.info(
            "Hidden ID scan finished: matched=%d, unmatched_prefixes=%d",
            len(collected),
            len(pending),
        )

        return {
            "matched_ids": list(collected),
            "pages": hidden_pages,
            "processed_total": hidden_processed,
            "unmatched_prefixes": len(pending),
            "unmatched_prefix_values": sorted(pending),
        }

    async def _fast_forward_to_known_ids(
        self, board_id: str, known_ids: List[str], max_hops: int = 500
    ) -> Optional[str]:
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
            batch = ids[offset : offset + self.chunk_size]
            try:
                data = await self._retry(
                    self.monday.get_items_by_ids,
                    batch,
                    column_ids,
                )
                results.extend(data)
                await asyncio.sleep(self.RATE_LIMIT_DELAY)
            except Exception as exc:
                self.logger.error("Hidden detail fetch failed for %s: %s", batch, exc)

        return results

    async def _rehydrate_batches(self, candidates: List[ProjectCandidate]) -> None:
        column_parent = list(PARENT_COLUMNS.values())
        column_sub = list(SUBITEM_COLUMNS.values())

        resume_index = 0
        processed_in_batch = 0
        total_processed = 0
        start_time = datetime.utcnow()

        while resume_index < len(candidates):
            chunk = candidates[resume_index : resume_index + self.chunk_size]
            chunk_ids = [c.monday_id for c in chunk]
            chunk_start = resume_index
            chunk_end = resume_index + len(chunk)

            self.logger.info(
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

            hidden_stub: List[Dict] = []
            for item in parents:
                name = (item.get("name") or "").strip()
                prefix = self._leading_digits(name)
                if not prefix:
                    continue
                if prefix in self.sync_service._hidden_lookup_by_name:
                    hidden_stub.append(self.sync_service._hidden_lookup_by_name[prefix])

            processed = self.sync_service._process_and_resolve_mirrors(parents, subitems, hidden_stub)

            subitems_data = self.sync_service._transform_for_subitems_table(processed["subitems"])
            gestation_map = self.sync_service._compute_gestation_fallback_from_subitems(subitems_data)
            rollup_map = self.sync_service._rollup_new_enquiry_from_subitems(subitems_data)
            order_total_map, order_date_map = self.sync_service._rollup_order_values_from_subitems(
                subitems_data
            )

            projects_data = self.sync_service._transform_for_projects_table(processed["projects"])
            self.sync_service._apply_project_new_enquiry_rollup(projects_data, rollup_map)
            self.sync_service._apply_project_gestation_fallback(projects_data, gestation_map)
            self.sync_service._apply_project_order_rollup(
                projects_data, order_total_map, order_date_map
            )

            subitems_written = await self.sync_service._batch_upsert_subitems(subitems_data)
            self.subitem_batches += 1
            self.subitem_rows_upserted += subitems_written

            projects_written = await self.sync_service._batch_upsert_projects(projects_data)
            self.project_batches += 1
            self.project_rows_upserted += projects_written

            total_processed += len(chunk_ids)
            elapsed = (datetime.utcnow() - start_time).total_seconds() or 1
            self.logger.info(
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
                self.logger.info("Batch throttle pause (%ds)", self.BATCH_DELAY)
                await asyncio.sleep(self.BATCH_DELAY)
                processed_in_batch = 0

        self.logger.info("Processed %d candidate projects", len(candidates))

    def _load_candidate_projects(self) -> List[ProjectCandidate]:
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
            self.logger.error("Failed to query Supabase projects: %s", exc)
            return []

        candidates: List[ProjectCandidate] = []
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

                self.logger.warning(
                    "Retry %d/%d after error: %s (sleep=%ds)", attempt, self.MAX_RETRIES, exc, wait
                )
                await asyncio.sleep(wait)

        raise RuntimeError("Exceeded retries")

    @staticmethod
    def _leading_digits(value: str) -> Optional[str]:
        value = (value or "").strip()
        if not value:
            return None
        token: List[str] = []
        for ch in value:
            if ch.isdigit():
                token.append(ch)
            else:
                break
        return "".join(token) or None


async def rehydrate_delta(
    *,
    since: Optional[datetime] = None,
    days_back: int = 3,
    chunk_size: int = 100,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Run the delta rehydrate flow for newly created Monday projects."""

    target_since = since or datetime.utcnow() - timedelta(days=days_back)
    manager = DeltaRehydrationManager(target_since, chunk_size=chunk_size, logger=logger)
    await manager.run()


async def rehydrate_recent(
    *,
    since: Optional[datetime] = None,
    days_back: int = 3,
    chunk_size: int = 100,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Run the recent rehydrate flow for Supabase projects after a cutoff."""

    target_since = since or datetime.utcnow() - timedelta(days=days_back)
    manager = RecentRehydrationManager(target_since, chunk_size=chunk_size, logger=logger)
    await manager.run()


async def rehydrate_projects_by_ids(
    project_ids: Sequence[str],
    *,
    chunk_size: int = 100,
    logger: Optional[logging.Logger] = None,
) -> None:
    if not project_ids:
        return

    log = logger or logging.getLogger(f"{__name__}.rehydrate_by_ids")
    supabase = SupabaseClient()
    rows = (
        supabase.client.table("projects")
        .select("monday_id, item_name, project_name")
        .in_("monday_id", list(project_ids))
        .execute()
        .data
        or []
    )

    candidates: List[ProjectCandidate] = []
    seen_ids: Set[str] = set()
    for row in rows:
        project_id = str(row.get("monday_id") or "").strip()
        if not project_id:
            continue
        name = (row.get("item_name") or row.get("project_name") or "").strip()
        prefix = DeltaRehydrationManager._leading_digits(name)  # type: ignore[attr-defined]
        candidates.append(ProjectCandidate(project_id, name, prefix))
        seen_ids.add(project_id)

    missing_ids = [pid for pid in project_ids if pid not in seen_ids]
    if missing_ids:
        monday = MondayClient()
        items = monday.get_items_by_ids(missing_ids, ["name"])
        for item in items or []:
            project_id = str(item.get("id") or "")
            if not project_id:
                continue
            name = (item.get("name") or "").strip()
            prefix = DeltaRehydrationManager._leading_digits(name)  # type: ignore[attr-defined]
            candidates.append(ProjectCandidate(project_id, name, prefix))

    if not candidates:
        log.warning("rehydrate_projects_by_ids called with no resolvable candidates: %s", project_ids)
        return

    manager = RecentRehydrationManager(
        since=datetime.utcnow(),
        chunk_size=max(1, min(chunk_size, len(candidates))),
        logger=log,
    )
    prefixes = {c.prefix for c in candidates if c.prefix}
    if prefixes:
        # Use targeted lookup for small sets (typical webhook triggers)
        if len(prefixes) <= 50:
            await manager._warm_hidden_cache_targeted(prefixes)
        else:
            await manager._warm_hidden_cache(prefixes)
    await manager._rehydrate_batches(candidates)
    # prefixes = {c.prefix for c in candidates if c.prefix}
    # if prefixes:
    #     await manager._warm_hidden_cache(prefixes)
    # await manager._rehydrate_batches(candidates)



def backfill_llm(
    *,
    cutoff: date | str,
    batch_size: int = 100,
    limit: Optional[int] = None,
    resume_after: Optional[str] = None,
    throttle: float = 0.35,
    lookback_days: Optional[int] = None,
    skip_existing_after: Optional[date | str] = None,
    force: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, int]:
    """Backfill analysis_results with Gemini reasoning for projects after cutoff."""

    log = logger or logging.getLogger(f"{__name__}.backfill_llm")

    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY is required to run LLM backfill")

    db = SupabaseClient()
    svc = AnalysisService(db_client=db, lookback_days=lookback_days or ANALYSIS_LOOKBACK_DAYS)

    stats = {"total": 0, "updated": 0, "skipped": 0, "missing": 0, "errors": 0}
    cutoff_iso = _normalise_date_string(cutoff)
    skip_after_dt = _parse_optional_date(skip_existing_after)

    log.info(
        "Starting LLM backfill | cutoff=%s | batch_size=%s | throttle=%.2fs | limit=%s | force=%s",
        cutoff_iso,
        batch_size,
        throttle,
        limit,
        force,
    )

    started_at = time.time()
    for batch_index, batch in enumerate(
        _iter_projects(db, cutoff_iso, batch_size=max(1, batch_size), resume_after=resume_after, limit=limit),
        start=1,
    ):
        log.info("Processing batch %s (%s projects)", batch_index, len(batch))
        _process_projects(
            svc,
            db,
            batch,
            throttle=max(0.0, throttle),
            skip_existing_after=skip_after_dt,
            force=force,
            stats=stats,
            logger=log,
        )

    elapsed = time.time() - started_at
    log.info(
        "Finished | total=%s | updated=%s | skipped=%s | missing=%s | errors=%s | runtime=%.1fs",
        stats["total"],
        stats["updated"],
        stats["skipped"],
        stats["missing"],
        stats["errors"],
        elapsed,
    )

    return stats


def sync_projects_to_monday(
    *,
    since: str = "2023-01-01",
    batch_size: int = 100,
    limit: Optional[int] = None,
    sleep_interval: float = 0.5,
    dry_run: bool = False,
    include_updates: bool = True,
    throttle: bool = True,
    logger: Optional[logging.Logger] = None,
    project_ids: Optional[Sequence[str]] = None,
) -> Dict[str, int]:
    """Push Supabase analysis results back into Monday columns and updates."""

    log = logger or logging.getLogger(f"{__name__}.sync_monday")
    db = SupabaseClient()
    service = MondayUpdateService(db_client=db)

    stats = {
        "total": 0,
        "synced": 0,
        "dry_run": 0,
        "missing": 0,
        "errors": 0,
    }
    failed_projects: List[str] = []

    cleaned_ids: List[str] = []
    if project_ids is not None:
        cleaned_ids = [str(pid).strip() for pid in project_ids if str(pid).strip()]
        if not cleaned_ids:
            log.info("No project IDs supplied for Monday sync; nothing to do")
            return stats

        step = max(1, batch_size)

        def _iter_explicit_batches() -> Iterable[Tuple[int, List[str]]]:
            for idx, start in enumerate(range(0, len(cleaned_ids), step), start=1):
                subset = cleaned_ids[start : start + step]
                log.info("Processing explicit batch %s with %s project(s)", idx, len(subset))
                yield idx, subset

        batch_iterator = _iter_explicit_batches()
    else:
        def _iter_paginated_batches() -> Iterable[Tuple[int, List[str]]]:
            for batch_index, batch in enumerate(
                _iter_projects_paginated(db, since, max(1, batch_size), limit),
                start=1,
            ):
                log.info("Fetched batch %s with %s project(s)", batch_index, len(batch))
                ids: List[str] = []
                for row in batch:
                    pid = str(row.get("monday_id") or "").strip()
                    if pid:
                        ids.append(pid)
                    else:
                        stats["missing"] += 1
                        log.warning("Skipping project with missing monday_id: %s", row)
                if ids:
                    yield batch_index, ids

        batch_iterator = _iter_paginated_batches()

    for batch_index, id_batch in batch_iterator:
        for project_id in id_batch:
            stats["total"] += 1

            analysis = db.get_latest_analysis_result(project_id)
            if not analysis:
                stats["missing"] += 1
                log.warning("No analysis_results row for project_id=%s", project_id)
                failed_projects.append(project_id)
                continue

            if dry_run:
                stats["dry_run"] += 1
                log.info(
                    "Dry-run: would sync project %s | rating=%s | conversion=%.3f | gestation=%s",
                    project_id,
                    analysis.get("rating_score"),
                    analysis.get("expected_conversion_rate") or 0.0,
                    analysis.get("expected_gestation_days"),
                )
                continue

            try:
                result = service.sync_project(
                    project_id=project_id,
                    analysis=analysis,
                    include_update=include_updates,
                    throttle=throttle,
                )
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                log.exception("Error syncing project %s: %s", project_id, exc)
                failed_projects.append(project_id)
                continue

            if result.get("success"):
                stats["synced"] += 1
                log.info(
                    "Synced project %s | columns=%s | update_action=%s",
                    project_id,
                    result.get("columns"),
                    result.get("update_action"),
                )
            else:
                stats["errors"] += 1
                log.warning(
                    "Failed to sync project %s | error=%s",
                    project_id,
                    result.get("error") or "unknown",
                )
                failed_projects.append(project_id)

            if sleep_interval > 0:
                time.sleep(sleep_interval)

    if failed_projects:
        unique_failed = sorted(set(failed_projects))
        log.warning(
            "Failed project IDs (%s): %s",
            len(unique_failed),
            ", ".join(unique_failed),
        )

    return stats


def _normalise_date_string(value: date | str) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _parse_optional_date(value: Optional[date | str]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return datetime.fromisoformat(str(value))


def _iter_projects(
    db: SupabaseClient,
    cutoff: str,
    *,
    batch_size: int,
    resume_after: Optional[str],
    limit: Optional[int],
) -> Iterator[List[Dict[str, object]]]:
    cursor = resume_after
    processed = 0

    while True:
        query = (
            db.client.table("projects")
            .select("monday_id,date_created")
            .gte("date_created", cutoff)
            .order("monday_id", desc=False)
            .limit(batch_size)
        )
        if cursor:
            query = query.gt("monday_id", cursor)

        response = query.execute()
        rows: List[Dict[str, object]] = response.data or []
        if not rows:
            break

        if limit is not None and processed + len(rows) > limit:
            rows = rows[: max(0, limit - processed)]

        if not rows:
            break

        yield rows
        processed += len(rows)
        cursor = str(rows[-1]["monday_id"])

        if limit is not None and processed >= limit:
            break


def _has_llm_result(
    db: SupabaseClient,
    project_id: str,
    *,
    min_timestamp: Optional[datetime],
) -> bool:
    row = db.get_latest_analysis_result(project_id)
    if not row:
        return False

    reasoning = row.get("reasoning")
    if not isinstance(reasoning, dict):
        return False
    if not {"gestation", "conversion", "rating"} <= set(reasoning):
        return False

    if min_timestamp and row.get("analysis_timestamp"):
        try:
            existing_ts = datetime.fromisoformat(row["analysis_timestamp"])
            if existing_ts >= min_timestamp:
                return True
        except ValueError:
            return False
        return False

    return True


def _process_projects(
    svc: AnalysisService,
    db: SupabaseClient,
    rows: Sequence[Dict[str, object]],
    *,
    throttle: float,
    skip_existing_after: Optional[datetime],
    force: bool,
    stats: Dict[str, int],
    logger: logging.Logger,
) -> None:
    for row in rows:
        project_id = str(row.get("monday_id") or "").strip()
        if not project_id:
            stats["missing"] += 1
            logger.warning("Skipping row with missing monday_id: %s", row)
            continue

        stats["total"] += 1

        if not force and _has_llm_result(db, project_id, min_timestamp=skip_existing_after):
            stats["skipped"] += 1
            logger.info("Skipping %s: existing LLM analysis detected", project_id)
            continue

        try:
            result = svc.analyze_and_store(project_id, with_llm=True)
        except Exception as exc:  # noqa: BLE001
            stats["errors"] += 1
            logger.exception("LLM analysis failed for %s: %s", project_id, exc)
            continue

        if result.get("success"):
            stats["updated"] += 1
            logger.info(
                "Stored analysis for %s | rating=%s | conversion=%.3f | gestation=%s",
                project_id,
                result["result"].get("rating_score"),
                result["result"].get("expected_conversion_rate") or 0.0,
                result["result"].get("expected_gestation_days"),
            )
        else:
            stats["errors"] += 1
            logger.warning(
                "Analysis service returned error for %s: %s",
                project_id,
                result.get("error"),
            )

        if throttle > 0:
            time.sleep(throttle)


def _iter_projects_paginated(
    db: SupabaseClient,
    since: str,
    batch_size: int,
    limit: Optional[int],
) -> Iterable[List[Dict]]:
    offset = 0
    processed = 0

    while True:
        batch = (
            db.client.table("projects")
            .select("monday_id,date_created")
            .gte("date_created", since)
            .order("monday_id", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
            .data
            or []
        )

        if not batch:
            break

        if limit is not None and processed + len(batch) > limit:
            batch = batch[: max(0, limit - processed)]

        if not batch:
            break

        yield batch
        processed += len(batch)

        if limit is not None and processed >= limit:
            break
        if len(batch) < batch_size:
            break

        offset += len(batch)


__all__ = [
    "ProjectCandidate",
    "DeltaRehydrationManager",
    "RecentRehydrationManager",
    "rehydrate_delta",
    "rehydrate_recent",
    "rehydrate_projects_by_ids",
    "backfill_llm",
    "sync_projects_to_monday",
]