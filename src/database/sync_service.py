import asyncio

import json
import logging
import re
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
import pandas as pd

from ..config import (
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_COLUMNS,
    SUBITEM_COLUMNS,
    HIDDEN_ITEMS_COLUMNS,
    SYNC_BATCH_SIZE,
    PIPELINE_STAGE_LABELS,
    TYPE_LABELS,
    CATEGORY_LABELS,
    CANONICAL_PRODUCT_KEYS,
    PRODUCT_TYPE_ALIASES,
)
from ..core.monday_client import MondayClient
from ..core.data_processor import LabelNormalizer, EnhancedMirrorResolver, HierarchicalSegmentation
from .supabase_client import SupabaseClient
from ..core.enhanced_extractor import EnhancedColumnExtractor, EnhancedMondayExtractor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Precompiled regexes for product-key normalisation
_RE_SLASH = re.compile(r"\s*/\s*")
_RE_HYPHEN = re.compile(r"\s*-\s*")
_RE_WS = re.compile(r"\s+")
_RE_CSV = re.compile(r"\s*,\s*")
_RE_LPAREN = re.compile(r"\s*\(\s*")
_RE_RPAREN = re.compile(r"\s*\)\s*")
_RE_PAREN_SP = re.compile(r"(?<!\s)\(")

class DataSyncService:
    """Enhanced service for syncing data between Monday and Supabase"""

    def __init__(self):
        self.monday_client = MondayClient()
        self.supabase_client = SupabaseClient()
        self.label_normalizer = LabelNormalizer()
        self.mirror_resolver = EnhancedMirrorResolver()
        self.segmentation = HierarchicalSegmentation()
        self.enhanced_extractor = EnhancedMondayExtractor(self.monday_client)
        self._hidden_lookup_by_id: Dict[str, Dict] = {}
        self._hidden_lookup_by_name: Dict[str, Dict] = {}
        self.max_duplicate_chunks = 10
        # Lazy-built alias map cache
        self._product_alias_map: Optional[Dict[str, str]] = None

    # -----------------------------------------------------------------
    # Product-key normalisation helpers
    # -----------------------------------------------------------------

    def _normalize_text_key(self, value: Any) -> str:
        """
        Segmentation-key normaliser:
        - stringify, lowercase, trim
        - collapse slash spacing: 'EPS / PIR' -> 'eps/pir'
        - collapse hyphen spacing: 'multi - fix' -> 'multi-fix'
        - normalise parentheses spacing
        - collapse whitespace
        """
        if value is None:
            return ""
        s = str(value).strip().lower()
        if not s:
            return ""
        s = _RE_SLASH.sub("/", s)
        s = _RE_HYPHEN.sub("-", s)
        s = _RE_LPAREN.sub("(", s)
        s = _RE_RPAREN.sub(")", s)
        s = _RE_PAREN_SP.sub(" (", s)
        s = _RE_WS.sub(" ", s)
        return s

    def _get_product_alias_map(self) -> Dict[str, str]:
        """Build alias lookup once per service instance, validating against canonical keys."""
        if self._product_alias_map is not None:
            return self._product_alias_map
        m: Dict[str, str] = {}
        for raw_k, raw_v in PRODUCT_TYPE_ALIASES.items():
            nk = self._normalize_text_key(raw_k)
            nv = self._normalize_text_key(raw_v)
            if not nk or not nv:
                continue
            if nv not in CANONICAL_PRODUCT_KEYS:
                logger.warning(
                    f"Alias value '{raw_v}' (normalised: '{nv}') is not in "
                    f"CANONICAL_PRODUCT_KEYS — skipping alias '{raw_k}'"
                )
                continue
            m[nk] = nv
        self._product_alias_map = m
        return m

    def _compute_product_key(self, product_type_raw: Any) -> str:
        """
        Derive a canonical product_key from a raw product_type string.
        - Splits CSV tokens
        - Normalises each token (lowercase, slash/hyphen/paren collapse)
        - Maps via alias table → canonical key
        - Deduplicates, sorts, joins
        - Unmapped tokens are silently dropped
        - Returns 'unknown' when product_type is non-empty but nothing maps
        - Returns '' (empty) when product_type is empty/None
        """
        if product_type_raw is None:
            return ""
        raw = str(product_type_raw).strip()
        if not raw:
            return ""
        alias_map = self._get_product_alias_map()
        keys: Set[str] = set()
        for part in _RE_CSV.split(raw):
            if not part:
                continue
            token = self._normalize_text_key(part)
            if not token:
                continue
            canonical = alias_map.get(token)
            if canonical:
                keys.add(canonical)
            # Unmapped tokens are dropped to prevent combinatorial explosion
        if not keys:
            return "unknown"
        return ", ".join(sorted(keys))


    async def perform_full_sync(self) -> Dict[str, Any]:
        logger.info("Starting full sync from Monday to Supabase")
        parent_log_id = self.supabase_client.log_sync_operation(
            'full', PARENT_BOARD_ID, 'Tapered Enquiry Maintenance'
        )
        try:
            parent_info = self.monday_client.get_board_info(PARENT_BOARD_ID)
            subitem_info = self.monday_client.get_board_info(SUBITEM_BOARD_ID)
            hidden_info = self.monday_client.get_board_info(HIDDEN_ITEMS_BOARD_ID)
            logger.info(f"Boards to sync - Parent: {parent_info['items_count']}, "
                        f"Subitems: {subitem_info['items_count']}, "
                        f"Hidden: {hidden_info['items_count']}")
            parent_items = await self._extract_all_items(
                PARENT_BOARD_ID,
                list(PARENT_COLUMNS.values()),
                parent_info['items_count']
            )
            subitems = await self._extract_all_items(
                SUBITEM_BOARD_ID,
                list(SUBITEM_COLUMNS.values()),
                subitem_info['items_count']
            )
            hidden_items = await self._extract_all_items(
                HIDDEN_ITEMS_BOARD_ID,
                list(HIDDEN_ITEMS_COLUMNS.values()),
                hidden_info['items_count']
            )
            processed_data = self._process_and_resolve_mirrors(
                parent_items, subitems, hidden_items
            )
            hidden_data = self._transform_for_hidden_table(processed_data['hidden'])
            subitems_data = self._transform_for_subitems_table(processed_data['subitems'])
            rollup_map = self._rollup_new_enquiry_from_subitems(subitems_data)
            gmap = self._compute_gestation_fallback_from_subitems(subitems_data)
            order_total_map, order_date_map = self._rollup_order_values_from_subitems(subitems_data)
            design_date_map, invoice_date_map = self._rollup_design_invoice_dates_from_subitems(
                subitems_data
            )
            projects_data = self._transform_for_projects_table(processed_data['projects'])
            self._apply_project_new_enquiry_rollup(projects_data, rollup_map)
            self._apply_project_gestation_fallback(projects_data, gmap)
            self._apply_project_order_rollup(projects_data, order_total_map, order_date_map)
            self._apply_project_design_invoice_rollup(
                projects_data, design_date_map, invoice_date_map
            )
            stats = {
                'processed': len(projects_data) + len(subitems_data) + len(hidden_data),
                'created': 0,
                'updated': 0,
                'errors': 0
            }
            stats['updated'] += await self._batch_upsert_hidden_items(hidden_data)
            stats['updated'] += await self._batch_upsert_projects(projects_data)
            stats['updated'] += await self._batch_upsert_subitems(subitems_data)
            self.supabase_client.update_sync_log(parent_log_id, 'completed', stats)
            logger.info(f"Full sync completed. Stats: {stats}")
            return {"success": True, "stats": stats}
        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            self.supabase_client.update_sync_log(
                parent_log_id, 'failed', error=str(e)
            )
            return {"success": False, "error": str(e)}


    def _transform_for_projects_table(self, projects: List[Dict]) -> List[Dict]:
        """Enhanced transform function with proper value handling"""

        transformed = []

        for item in projects:
            try:
                # Fallbacks from hidden-items cache by project prefix (e.g., "17403")
                prefix = (item.get('name') or item.get('item_name') or '').strip()
                hidden_vals = list(self._hidden_lookup_by_name.values()) if hasattr(self, '_hidden_lookup_by_name') else []
                related_hidden = [h for h in hidden_vals if isinstance(h.get('item_name'), str) and h.get('item_name', '').startswith(prefix + '_')] if prefix else []

                # Compute fallback new_enquiry_value (sum of quotes with "New Enquiry")
                fallback_nev = None
                if related_hidden:
                    total = 0.0
                    for h in related_hidden:
                        if (h.get('reason_for_change') or '').strip() == 'New Enquiry':
                            qa = self._parse_numeric_value(h.get('quote_amount'))
                            if qa:
                                total += qa
                    fallback_nev = total if total > 0 else None

                # Compute fallback gestation from hidden items dates
                fallback_gestation = None
                if related_hidden:
                    try:
                        design_dates = [d for d in (h.get('date_design_completed') for h in related_hidden) if d]
                        invoice_dates = [d for d in (h.get('invoice_date') for h in related_hidden) if d]
                        if design_dates and invoice_dates:
                            earliest_design = min(design_dates)
                            earliest_invoice = min(invoice_dates)
                            d_dt = datetime.strptime(earliest_design, '%Y-%m-%d')
                            i_dt = datetime.strptime(earliest_invoice, '%Y-%m-%d')
                            days_diff = (i_dt - d_dt).days
                            fallback_gestation = 0 if days_diff > 500000 or days_diff < 0 else days_diff
                        elif design_dates and not invoice_dates:
                            fallback_gestation = 0
                    except Exception:
                        pass

                # Fallback project_name from hidden items if missing
                project_name = item.get('project_name', '')
                if (not project_name or not str(project_name).strip()) and related_hidden:
                    try:
                        names = [h.get('project_name') for h in related_hidden if h.get('project_name')]
                        if names:
                            project_name = max(set(names), key=names.count)
                    except Exception:
                        pass

                # Dedup for mirror fallbacks (preserve raw product_type)
                account_val = item.get('account') or item.get('account_mirror') or ''
                product_val = item.get('product_type') or item.get('product_mirror') or ''
                account_val = self._dedup_csv_labels(account_val)
                product_val = self._dedup_csv_labels(product_val)

                # Derive canonical segmentation key from raw product_type
                product_key = self._compute_product_key(product_val)

                # Primary project build with corrected gestation preference order
                project = {
                    'monday_id': item.get('monday_id') or item.get('id'),
                    'item_name': item.get('name', ''),
                    'project_name': project_name or '',
                    # Prefer resolver-computed values first, then mirrors (deduped)
                    'account': account_val,
                    'product_type': product_val,       # raw (deduped) — preserved as-is
                    'product_key': product_key,         # canonical segmentation key
                    'new_enquiry_value': (
                        self._parse_numeric_value(item.get('new_enquiry_value'))
                        or self._parse_numeric_value(item.get('new_enq_value_mirror'))
                        or fallback_nev
                        or 0.0
                    ),
                    # Standard fields
                    'pipeline_stage': self._normalize_pipeline_stage(item.get('pipeline_stage')),
                    'type': self._normalize_type(item.get('type')),
                    'category': self._normalize_category(item.get('category')),
                    'zip_code': item.get('zip_code', ''),
                    'sales_representative': item.get('sales_representative', ''),
                    'funding': item.get('funding', ''),
                    'feedback': item.get('feedback', ''),
                    'lost_to_who_or_why': item.get('lost_to_who_or_why', ''),
                    # Calculate gestation period
                    'gestation_period': (
                        self._parse_gestation_period(
                            self._calculate_gestation_period(item) or item.get('gestation_period')
                        )
                        if (self._calculate_gestation_period(item) or item.get('gestation_period')) is not None
                        else self._parse_gestation_period(fallback_gestation)
                    ),
                    # Numeric fields
                    'project_value': self._parse_numeric_value(
                        item.get('project_value') or item.get('overall_project_value')
                    ),
                    'weighted_pipeline': self._parse_numeric_value(
                        item.get('weighted_pipeline')
                    ),
                    'probability_percent': self._parse_probability(
                        item.get('probability_percent', 0)
                    ),
                    # Date fields
                    'date_created': self._parse_date_value(item.get('date_created')),
                    'expected_start_date': self._parse_date_value(item.get('expected_start_date')),
                    'follow_up_date': self._parse_date_value(item.get('follow_up_date')),
                    'first_date_designed': self._parse_date_value(item.get('first_date_designed')),
                    'last_date_designed': self._parse_date_value(item.get('last_date_designed')),
                    'first_date_invoiced': self._parse_date_value(item.get('first_date_invoiced')),
                    # Metadata
                    'last_synced_at': datetime.now().isoformat()
                }

                project['new_enquiry_value'] = round(float(project['new_enquiry_value'] or 0.0), 2)
                transformed.append(project)

            except Exception as e:
                logger.error(f"Error transforming project {item.get('monday_id')}: {e}")
                continue

        if transformed:
            df = pd.DataFrame(transformed)
            df = self.segmentation.create_value_bands(df)
            bands = df.get('value_band')
            if bands is not None:
                for project, band in zip(transformed, bands):
                    project['value_band'] = str(band) if pd.notna(band) else 'Unknown'

        return transformed

    async def perform_delta_sync(self, hours_back: int = 24) -> Dict[str, Any]:
        """Perform delta sync for recent changes"""
        logger.info(f"Starting delta sync for last {hours_back} hours")

        last_sync = self.supabase_client.get_last_sync_time(PARENT_BOARD_ID)
        if not last_sync:
            last_sync = datetime.now() - timedelta(hours=hours_back)

        log_id = self.supabase_client.log_sync_operation(
            "delta", PARENT_BOARD_ID, "Delta Sync"
        )

        try:
            updated_items = await self._get_updated_items_since(last_sync)

            if not any(updated_items.values()):
                logger.info("No items to sync")
                self.supabase_client.update_sync_log(
                    log_id, "completed", {"processed": 0}
                )
                return {"success": True, "stats": {"processed": 0}}

            stats = await self._sync_updated_items(updated_items)

            self.supabase_client.update_sync_log(log_id, "completed", stats)
            logger.info(f"Delta sync completed. Stats: {stats}")

            return {"success": True, "stats": stats}

        except Exception as e:
            logger.error(f"Delta sync failed: {e}")
            self.supabase_client.update_sync_log(
                log_id, "failed", error=str(e)
            )
            return {"success": False, "error": str(e)}

    async def _get_updated_items_since(self, last_sync: datetime) -> Dict[str, List[Dict]]:
        """Get items updated since last sync time"""
        logger.info(f"Fetching items updated since {last_sync}")

        updated_items = {
            "projects": [],
            "subitems": [],
            "hidden": [],
        }

        try:
            parent_items = await self._extract_recent_items(
                PARENT_BOARD_ID,
                list(PARENT_COLUMNS.values()),
                limit=1000,
            )
            updated_items["projects"] = self._filter_items_by_update_time(parent_items, last_sync)

            subitems = await self._extract_recent_items(
                SUBITEM_BOARD_ID,
                list(SUBITEM_COLUMNS.values()),
                limit=1000,
            )
            updated_items["subitems"] = self._filter_items_by_update_time(subitems, last_sync)

            hidden_items = await self._extract_recent_items(
                HIDDEN_ITEMS_BOARD_ID,
                list(HIDDEN_ITEMS_COLUMNS.values()),
                limit=1000,
            )
            updated_items["hidden"] = self._filter_items_by_update_time(hidden_items, last_sync)

            logger.info(
                f"Found {len(updated_items['projects'])} updated projects, "
                f"{len(updated_items['subitems'])} updated subitems, "
                f"{len(updated_items['hidden'])} updated hidden items"
            )

        except Exception as e:
            logger.error(f"Error fetching updated items: {e}")
            raise

        return updated_items

    async def _extract_recent_items(
        self,
        board_id: str,
        column_ids: List[str],
        limit: int = 1000
    ) -> List[Dict]:
        """Extract recent items from a board"""
        items = []
        cursor = None
        fetched = 0
        max_retries = 3
        retry_count = 0

        board_batch_sizes = {
            PARENT_BOARD_ID: 100,
            SUBITEM_BOARD_ID: 200,
            HIDDEN_ITEMS_BOARD_ID: 500,
        }
        base_batch_size = board_batch_sizes.get(board_id, 500)

        while fetched < limit and retry_count < max_retries:
            try:
                batch_limit = min(base_batch_size, limit - fetched)

                if cursor:
                    batch = self.monday_client.get_next_items_page(
                        cursor=cursor,
                        column_ids=column_ids,
                        limit=batch_limit,
                    )
                else:
                    if board_id == SUBITEM_BOARD_ID:
                        batch = self.monday_client.get_subitems_page(
                            board_id, column_ids, limit=batch_limit, cursor=None
                        )
                    else:
                        batch = self.monday_client.get_items_page(
                            board_id, column_ids, limit=batch_limit, cursor=None
                        )

                if not batch["items"]:
                    break

                items.extend(batch["items"])
                fetched += len(batch["items"])

                cursor = batch.get("next_cursor")
                if not cursor:
                    break

                await asyncio.sleep(0.5)
                retry_count = 0

            except Exception as e:
                retry_count += 1
                logger.warning(
                    f"Error extracting batch from {board_id} "
                    f"(attempt {retry_count}/{max_retries}): {e}"
                )

                if retry_count >= max_retries:
                    logger.error(f"Failed to extract from board {board_id} after {max_retries} attempts")
                    break

                wait_time = 2 ** retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)

        return items

    def _filter_items_by_update_time(self, items: List[Dict], since: datetime) -> List[Dict]:
        """
        Monday payloads often don't include reliable updated_at by default;
        keep current behavior and rely on upsert idempotency.
        """
        return items

    async def _sync_updated_items(self, updated_items: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Sync updated items to Supabase"""
        stats = {
            "processed": 0,
            "updated": 0,
            "errors": 0,
        }

        try:
            if updated_items["projects"] or updated_items["subitems"] or updated_items["hidden"]:
                processed_data = self._process_and_resolve_mirrors(
                    updated_items["projects"],
                    updated_items["subitems"],
                    updated_items["hidden"],
                )

                hidden_data: List[Dict] = []
                subitems_data: List[Dict] = []
                projects_data: List[Dict] = []

                rollup_map: Dict[str, float] = {}
                gmap: Dict[str, int] = {}
                order_total_map: Dict[str, float] = {}
                order_date_map: Dict[str, str] = {}
                design_date_map: Dict[str, str] = {}
                invoice_date_map: Dict[str, str] = {}

                if processed_data["hidden"]:
                    hidden_data = self._transform_for_hidden_table(processed_data["hidden"])
                    stats["updated"] += await self._batch_upsert_hidden_items(hidden_data)
                    stats["processed"] += len(hidden_data)

                if processed_data["subitems"]:
                    subitems_data = self._transform_for_subitems_table(processed_data["subitems"])
                    rollup_map = self._rollup_new_enquiry_from_subitems(subitems_data)
                    gmap = self._compute_gestation_fallback_from_subitems(subitems_data)
                    order_total_map, order_date_map = self._rollup_order_values_from_subitems(subitems_data)
                    design_date_map, invoice_date_map = self._rollup_design_invoice_dates_from_subitems(
                        subitems_data
                    )
                    stats["updated"] += await self._batch_upsert_subitems(subitems_data)
                    stats["processed"] += len(subitems_data)

                if processed_data["projects"]:
                    projects_data = self._transform_for_projects_table(processed_data["projects"])
                    self._apply_project_new_enquiry_rollup(projects_data, rollup_map)
                    self._apply_project_gestation_fallback(projects_data, gmap)
                    self._apply_project_order_rollup(projects_data, order_total_map, order_date_map)
                    self._apply_project_design_invoice_rollup(
                        projects_data, design_date_map, invoice_date_map
                    )
                    stats["updated"] += await self._batch_upsert_projects(projects_data)
                    stats["processed"] += len(projects_data)
                elif rollup_map or gmap or order_total_map or order_date_map or design_date_map or invoice_date_map:
                    patch = []
                    if rollup_map:
                        patch.extend(
                            {
                                "monday_id": pid,
                                "new_enquiry_value": val,
                                "last_synced_at": datetime.now().isoformat(),
                            }
                            for pid, val in rollup_map.items()
                            if val is not None
                        )
                    if gmap:
                        patch.extend(
                            {
                                "monday_id": pid,
                                "gestation_period": val,
                                "last_synced_at": datetime.now().isoformat(),
                            }
                            for pid, val in gmap.items()
                            if val is not None and val > 0
                        )
                    if order_total_map:
                        patch.extend(
                            {
                                "monday_id": pid,
                                "total_order_value": val,
                                "last_synced_at": datetime.now().isoformat(),
                            }
                            for pid, val in order_total_map.items()
                            if val is not None
                        )
                    if order_date_map:
                        patch.extend(
                            {
                                "monday_id": pid,
                                "date_order_received": val,
                                "last_synced_at": datetime.now().isoformat(),
                            }
                            for pid, val in order_date_map.items()
                            if val
                        )
                    if design_date_map:
                        patch.extend(
                            {
                                "monday_id": pid,
                                "first_date_designed": val,
                                "last_synced_at": datetime.now().isoformat(),
                            }
                            for pid, val in design_date_map.items()
                            if val
                        )
                    if invoice_date_map:
                        patch.extend(
                            {
                                "monday_id": pid,
                                "first_date_invoiced": val,
                                "last_synced_at": datetime.now().isoformat(),
                            }
                            for pid, val in invoice_date_map.items()
                            if val
                        )

                    if patch:
                        existing_ids = self.supabase_client.get_existing_project_ids()
                        patch_in_db = [p for p in patch if str(p.get("monday_id") or "") in existing_ids]

                        updated = 0
                        for row in patch_in_db:
                            try:
                                payload = {
                                    k: v for k, v in row.items()
                                    if k in (
                                        "new_enquiry_value",
                                        "gestation_period",
                                        "total_order_value",
                                        "date_order_received",
                                        "first_date_designed",
                                        "first_date_invoiced",
                                        "last_synced_at",
                                    )
                                }
                                self.supabase_client.client.table("projects") \
                                    .update(payload) \
                                    .eq("monday_id", row["monday_id"]) \
                                    .execute()
                                updated += 1
                            except Exception as e:
                                logger.error(f"Update-only patch failed for {row.get('monday_id')}: {e}")

                        stats["updated"] += updated

        except Exception as e:
            logger.error(f"Error syncing updated items: {e}")
            stats["errors"] += 1
            raise

        return stats

    async def _extract_all_items(
        self,
        board_id: str,
        column_ids: List[str],
        total_count: int
    ) -> List[Dict]:
        """Extract all items from a board with progress tracking"""
        items = []
        cursor = self.supabase_client.get_last_cursor_checkpoint(board_id) or None

        logger.info(f"Extracting {total_count} items from board {board_id}")

        from collections import deque
        last_seen_ring = deque(maxlen=10)
        chunks_completed = 0
        total_processed = 0
        start_time = datetime.now()

        BATCH_DELAY = 1.5
        THROTTLE_BATCH = 1000
        board_batch_size = self._get_board_batch_size(board_id)

        while True:
            try:
                cursor_preview = "start" if not cursor else f"{cursor[:8]}...{cursor[-8:]}"
                logger.info(f"Fetching page (cursor: {cursor_preview})")

                if cursor:
                    if board_id == SUBITEM_BOARD_ID:
                        batch = self.monday_client.get_subitems_page(
                            board_id, column_ids, limit=board_batch_size, cursor=cursor
                        )
                    else:
                        batch = self.monday_client.get_next_items_page(
                            cursor=cursor, column_ids=column_ids, limit=board_batch_size
                        )
                else:
                    if board_id == SUBITEM_BOARD_ID:
                        batch = self.monday_client.get_subitems_page(
                            board_id, column_ids, limit=board_batch_size, cursor=None
                        )
                    else:
                        batch = self.monday_client.get_items_page(
                            board_id, column_ids, limit=board_batch_size, cursor=None
                        )

                if batch["items"]:
                    items.extend(batch["items"])
                    try:
                        last_id = str(batch["items"][-1].get("id")) if batch["items"] else None
                        if last_id:
                            last_seen_ring.append(last_id)
                    except Exception:
                        pass

                cursor = batch.get("next_cursor")
                if not cursor:
                    break

                self.supabase_client.save_cursor_checkpoint_for_board(board_id, cursor)

                chunks_completed += 1
                total_processed += len(batch["items"] or [])
                if chunks_completed % 5 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds() or 1
                    rate = total_processed / elapsed
                    logger.info(
                        f"[{board_id}] pages={chunks_completed} items={total_processed} "
                        f"rate={rate:.1f}/s cursor={cursor_preview}"
                    )

                await asyncio.sleep(0.5)
                if total_processed > 0 and (total_processed % THROTTLE_BATCH == 0):
                    logger.info(f"[{board_id}] batch throttle pause {BATCH_DELAY}s after {total_processed} items")
                    await asyncio.sleep(BATCH_DELAY)

            except Exception as e:
                if "CursorExpiredError" in str(e):
                    try:
                        self.supabase_client.save_cursor_checkpoint_for_board(board_id, cursor)
                    except Exception:
                        pass

                    logger.warning(
                        f"Cursor expired for board {board_id} - reseeding and continuing with next_items_page"
                    )

                    seed = self.monday_client.get_items_page(
                        board_id, column_ids, limit=1, cursor=None
                    )
                    cursor = seed.get("next_cursor")

                    try:
                        if last_seen_ring:
                            ff_cursor = None
                            hops = 0
                            max_hops = 500
                            while hops < max_hops:
                                page_ids = (
                                    self.monday_client.get_next_item_ids_page(ff_cursor, board_batch_size)
                                    if ff_cursor
                                    else self.monday_client.get_item_ids_page(board_id, board_batch_size, None)
                                )
                                ids_on_page = {str(it.get("id")) for it in (page_ids.get("items") or [])}
                                if any(k in ids_on_page for k in list(last_seen_ring)):
                                    cursor = page_ids.get("next_cursor")
                                    break
                                ff_cursor = page_ids.get("next_cursor")
                                if not ff_cursor:
                                    break
                                hops += 1
                                await asyncio.sleep(0.2)
                    except Exception:
                        pass

                    await asyncio.sleep(1)
                    continue

                msg = str(e).lower()
                if "504" in msg or "timeout" in msg:
                    logger.warning("API timeout - waiting 30s before retry...")
                    await asyncio.sleep(30)
                    continue
                if "rate limit" in msg or "429" in msg:
                    retry_sec = 60
                    try:
                        import re as _re
                        m = _re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(e))
                        if m:
                            retry_sec = max(1, int(m.group(1)))
                    except Exception:
                        pass
                    logger.warning(f"Rate limit hit - waiting {retry_sec}s...")
                    await asyncio.sleep(retry_sec)
                    continue
                if "internal server error" in msg or "internal server" in msg or "503" in msg:
                    logger.warning("Internal server error - retrying in 5s...")
                    await asyncio.sleep(5)
                    continue

                logger.error(f"Error extracting batch from {board_id}: {e}")
                break

        logger.info(f"Extracted {len(items)} items from board {board_id}")
        return items

    def _calculate_gestation_period(self, item: Dict) -> Optional[int]:
        """Calculate gestation period from dates"""
        try:
            fdi = item.get("first_date_invoiced")
            fdd = item.get("first_date_designed")

            if not fdd:
                return None
            if not fdi:
                return 0

            if isinstance(fdi, str):
                fdi_date = datetime.strptime(fdi, "%Y-%m-%d")
            else:
                return 0

            if isinstance(fdd, str):
                fdd_date = datetime.strptime(fdd, "%Y-%m-%d")
            else:
                return None

            days = (fdi_date - fdd_date).days
            if days > 500000:
                return 0

            return max(0, days)

        except Exception as e:
            logger.debug(f"Could not calculate gestation period: {e}")
            return None

    def _transform_for_subitems_table(self, subitems: List[Dict]) -> List[Dict]:
        """Enhanced transform function for subitems with parent relationships"""
        transformed = []
        skipped_count = 0
        exception_count = 0
        skipped_ids = []

        logger.info(f"Starting transformation of {len(subitems)} subitems")

        for item in subitems:
            try:
                normalized = self._normalize_monday_item(item)
                nm = normalized.get("name") or normalized.get("item_name") or item.get("name")

                parent_id = normalized.get("parent_monday_id") or (item.get("parent_item") or {}).get("id")
                if not parent_id:
                    logger.warning(
                        f"Skipping subitem without parent reference: "
                        f"{normalized.get('monday_id') or normalized.get('id') or item.get('id')}"
                    )
                    skipped_ids.append(normalized.get("monday_id") or normalized.get("id") or item.get("id"))
                    skipped_count += 1
                    continue

                reason = (normalized.get("reason_for_change") or "").strip()
                if not reason and nm and nm in self._hidden_lookup_by_name:
                    r = self._hidden_lookup_by_name[nm].get("reason_for_change")
                    reason = (r or "").strip() if r is not None else ""

                quote_val = self._parse_numeric_value(normalized.get("quote_amount"))
                if quote_val is None and nm and nm in self._hidden_lookup_by_name:
                    quote_val = self._hidden_lookup_by_name[nm].get("quote_amount")
                    if quote_val is not None:
                        logger.info(f"Enriched {nm} quote_amount: {quote_val}")

                account_fallback = self.mirror_resolver._extract_column_value(item, "mirror_12__1") or ""
                product_fallback = self.mirror_resolver._extract_column_value(item, "mirror875__1") or ""

                new_enq_val = self._parse_numeric_value(normalized.get("new_enquiry_value"))
                if new_enq_val is None:
                    direct_nev = self.mirror_resolver._extract_numeric_value(item, "formula_mkqa31kh")
                    if direct_nev is not None:
                        new_enq_val = direct_nev
                    else:
                        if reason == "New Enquiry":
                            new_enq_val = quote_val if quote_val is not None else 0.0
                        else:
                            new_enq_val = 0.0

                hidden_id = normalized.get("hidden_item_id")
                if not hidden_id and nm and nm in self._hidden_lookup_by_name:
                    candidate = self._hidden_lookup_by_name[nm].get("monday_id")
                    if candidate:
                        hidden_id = candidate

                if (not reason) and hidden_id:
                    hid = str(hidden_id)
                    if hid and hid in self._hidden_lookup_by_id:
                        r = self._hidden_lookup_by_id[hid].get("reason_for_change")
                        reason = (r or "").strip() if r is not None else reason

                date_design_completed = self._parse_date_value(
                    normalized.get("date_design_completed_mirror") or normalized.get("date_design_completed")
                )
                if not date_design_completed and nm and nm in self._hidden_lookup_by_name:
                    date_design_completed = self._hidden_lookup_by_name[nm].get("date_design_completed")
                if not date_design_completed and hidden_id:
                    hid = str(hidden_id)
                    if hid in self._hidden_lookup_by_id:
                        date_design_completed = self._hidden_lookup_by_id[hid].get("date_design_completed")
                date_design_completed = self._parse_date_value(date_design_completed)

                invoice_date = self._parse_date_value(normalized.get("invoice_date"))
                if not invoice_date and nm and nm in self._hidden_lookup_by_name:
                    invoice_date = self._hidden_lookup_by_name[nm].get("invoice_date")
                if not invoice_date and hidden_id:
                    hid = str(hidden_id)
                    if hid in self._hidden_lookup_by_id:
                        invoice_date = self._hidden_lookup_by_id[hid].get("invoice_date")
                invoice_date = self._parse_date_value(invoice_date)

                date_order_received = self._parse_date_value(normalized.get("date_order_received"))
                if not date_order_received and nm and nm in self._hidden_lookup_by_name:
                    date_order_received = self._hidden_lookup_by_name[nm].get("date_order_received")
                if not date_order_received and hidden_id:
                    hid = str(hidden_id)
                    if hid in self._hidden_lookup_by_id:
                        date_order_received = self._hidden_lookup_by_id[hid].get("date_order_received")
                date_order_received = self._parse_date_value(date_order_received)

                cust_order_value_material = self._parse_numeric_value(
                    normalized.get("cust_order_value_material")
                )
                if cust_order_value_material is None and nm and nm in self._hidden_lookup_by_name:
                    cust_order_value_material = self._hidden_lookup_by_name[nm].get("cust_order_value_material")
                if cust_order_value_material is None and hidden_id:
                    hid = str(hidden_id)
                    if hid in self._hidden_lookup_by_id:
                        cust_order_value_material = self._hidden_lookup_by_id[hid].get("cust_order_value_material")

                subitem = {
                    "monday_id": normalized.get("monday_id") or normalized.get("id") or item.get("id"),
                    "parent_monday_id": parent_id,
                    "item_name": normalized.get("name", "") or item.get("name", ""),
                    "account": normalized.get("account") or account_fallback,
                    "product_type": normalized.get("product_type") or product_fallback,
                    "new_enquiry_value": new_enq_val,
                    "reason_for_change": reason,
                    "hidden_item_id": hidden_id,
                    "designer": normalized.get("designer", ""),
                    "surveyor": normalized.get("surveyor", ""),
                    "area": self._parse_numeric_value(normalized.get("area")),
                    "fall": normalized.get("fall", ""),
                    "deck_type": normalized.get("deck_type", ""),
                    "layering": normalized.get("layering", ""),
                    "time_taken": self._parse_numeric_value(normalized.get("time_taken")),
                    "min_thickness": self._parse_numeric_value(normalized.get("min_thickness")),
                    "max_thickness": self._parse_numeric_value(normalized.get("max_thickness")),
                    "u_value": self._parse_numeric_value(normalized.get("u_value")),
                    "m2_rate": self._parse_numeric_value(normalized.get("m2_rate")),
                    "material_value": self._parse_numeric_value(normalized.get("material_value")),
                    "transport_cost": self._parse_numeric_value(normalized.get("transport_cost")),
                    "order_status": normalized.get("order_status", ""),
                    "date_order_received": date_order_received,
                    "cust_order_value_material": cust_order_value_material,
                    "customer_po": normalized.get("customer_po", ""),
                    "supplier1": normalized.get("supplier1", ""),
                    "supplier2": normalized.get("supplier2", ""),
                    "requested_delivery_date": self._parse_date_value(normalized.get("requested_delivery_date")),
                    "final_delivery_date": self._parse_date_value(normalized.get("final_delivery_date")),
                    "delivery_address": normalized.get("delivery_address", ""),
                    "date_received": self._parse_date_value(normalized.get("date_received")),
                    "date_design_completed": date_design_completed,
                    "invoice_number": normalized.get("invoice_number", ""),
                    "invoice_date": invoice_date,
                    "quote_amount": quote_val,
                    "amount_invoiced": self._parse_numeric_value(normalized.get("amount_invoiced")),
                    "last_synced_at": datetime.now().isoformat(),
                }

                transformed.append(subitem)

            except Exception as e:
                logger.error(
                    f"Error transforming subitem {item.get('monday_id') or item.get('id')}: {e}"
                )
                exception_count += 1
                continue

        if skipped_ids:
            logger.info(
                f"Skipped subitems (no parent): {skipped_ids[:20]}"
                f"{'...' if len(skipped_ids) > 20 else ''}"
            )
        logger.info(
            f"Transformation complete: {len(transformed)} transformed, "
            f"{skipped_count} skipped, {exception_count} exceptions"
        )
        return transformed

    def _transform_for_hidden_table(self, hidden_items: List[Dict]) -> List[Dict]:
        """Enhanced transform function for hidden items"""
        transformed = []

        for item in hidden_items:
            try:
                normalized_item = self._normalize_monday_item(item)

                hidden = {
                    "monday_id": normalized_item.get("id"),
                    "item_name": normalized_item.get("name", ""),
                    "status": normalized_item.get("status", ""),
                    "project_attachments": self._parse_json_field(normalized_item.get("project_attachments")),
                    "prefix": normalized_item.get("prefix", ""),
                    "revision": normalized_item.get("revision", ""),
                    "project_name": normalized_item.get("project_name", ""),
                    "reason_for_change": normalized_item.get("reason_for_change", ""),
                    "urgency": normalized_item.get("urgency", ""),
                    "approx_bonding": normalized_item.get("approx_bonding", ""),
                    "volume_m3": self._parse_numeric_value(normalized_item.get("volume_m3")),
                    "wastage_percent": self._parse_numeric_value(normalized_item.get("wastage_percent")),
                    "min_thickness": self._parse_numeric_value(normalized_item.get("min_thickness")),
                    "max_thickness": self._parse_numeric_value(normalized_item.get("max_thickness")),
                    "time_taken": self._parse_numeric_value(normalized_item.get("time_taken")),
                    "date_received": self._parse_date_value(normalized_item.get("date_received")),
                    "date_design_completed": self._parse_date_value(normalized_item.get("date_design_completed")),
                    "invoice_date": self._parse_date_value(normalized_item.get("invoice_date")),
                    "date_order_received": self._parse_date_value(normalized_item.get("date_order_received")),
                    "cust_order_value_material": self._parse_numeric_value(
                        normalized_item.get("cust_order_value_material")
                    ),
                    "date_quoted": self._parse_date_value(normalized_item.get("date_quoted")),
                    "date_project_won": self._parse_date_value(normalized_item.get("date_project_won")),
                    "date_project_closed": self._parse_date_value(normalized_item.get("date_project_closed")),
                    "quote_amount": self._parse_numeric_value(normalized_item.get("quote_amount", 0)),
                    "material_value": self._parse_numeric_value(normalized_item.get("material_value")),
                    "transport_cost": self._parse_numeric_value(normalized_item.get("transport_cost")),
                    "target_gpm": self._parse_numeric_value(normalized_item.get("target_gpm")),
                    "tp_margin": self._parse_numeric_value(normalized_item.get("tp_margin")),
                    "commission": self._parse_numeric_value(normalized_item.get("commission")),
                    "distributor_margin": self._parse_numeric_value(normalized_item.get("distributor_margin")),
                    "account_id": normalized_item.get("account_id", ""),
                    "contact_id": normalized_item.get("contact_id", ""),
                    "last_synced_at": datetime.now().isoformat(),
                }

                try:
                    if hidden.get("monday_id") is not None:
                        self._hidden_lookup_by_id[str(hidden["monday_id"])] = hidden
                    if hidden.get("item_name"):
                        self._hidden_lookup_by_name[hidden["item_name"]] = hidden
                except Exception as e:
                    logger.warning(f"Failed to update hidden lookup: {e}")

                transformed.append({k: v for k, v in hidden.items() if v is not None})

            except Exception as e:
                logger.warning(f"Failed to transform hidden item {item.get('id')}: {e}")

        return transformed

    def _normalize_monday_item(self, item: Dict) -> Dict:
        """Normalize Monday item using existing label normalization logic"""
        normalized: Dict[str, Any] = {}

        normalized["id"] = item.get("id")
        normalized["name"] = item.get("name", "")

        if isinstance(item.get("parent_item"), dict):
            normalized["parent_item"] = item.get("parent_item")

        for column_value in item.get("column_values", []):
            column_id = column_value.get("id")
            if column_id:
                normalized_value = self.label_normalizer.normalize_column_value(
                    column_value, column_id
                )
                if normalized_value is not None:
                    field_name = self._map_column_to_field(column_id)
                    if field_name:
                        normalized[field_name] = normalized_value

        return normalized

    def _map_column_to_field(self, column_id: str) -> Optional[str]:
        reverse_mapping = {}
        for field, col in PARENT_COLUMNS.items():
            reverse_mapping[col] = field
        for field, col in SUBITEM_COLUMNS.items():
            reverse_mapping[col] = field
        for field, col in HIDDEN_ITEMS_COLUMNS.items():
            reverse_mapping[col] = field
        return reverse_mapping.get(column_id)

    def _normalize_pipeline_stage(self, stage_value: Any) -> str:
        if isinstance(stage_value, str) and stage_value.isdigit():
            return PIPELINE_STAGE_LABELS.get(stage_value, stage_value)
        return str(stage_value) if stage_value else ""

    def _normalize_type(self, type_value: Any) -> str:
        if isinstance(type_value, str) and type_value.isdigit():
            return TYPE_LABELS.get(type_value, type_value)
        return str(type_value) if type_value else ""

    def _normalize_category(self, category_value: Any) -> str:
        if isinstance(category_value, str) and category_value.isdigit():
            return CATEGORY_LABELS.get(category_value, category_value)
        return str(category_value) if category_value else ""

    def _parse_numeric_value(self, value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            if isinstance(value, str):
                clean_value = value.replace("£", "").replace(",", "").strip()
                return float(clean_value) if clean_value else None
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_gestation_period(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            gestation = int(float(value))
            if 0 <= gestation <= 500000:
                return gestation
            logger.warning(f"Gestation period out of range: {gestation}")
            return None
        except (ValueError, TypeError):
            return None

    def _parse_probability(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            prob = int(float(value))
            if 0 <= prob <= 100:
                return prob
            logger.warning(f"Probability out of range: {prob}")
            return None
        except (ValueError, TypeError):
            return None

    def _parse_date_value(self, value: Any) -> Optional[str]:
        if not value:
            return None
        try:
            if isinstance(value, str):
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        return datetime.strptime(value, fmt).date().isoformat()
                    except ValueError:
                        continue
            return str(value)
        except Exception:
            return None

    def _parse_json_field(self, value: Any) -> Optional[Dict]:
        if not value:
            return None
        try:
            if isinstance(value, str):
                return json.loads(value)
            if isinstance(value, dict):
                return value
            return None
        except (json.JSONDecodeError, TypeError):
            return None

    def _dedup_csv_labels(self, v: Any) -> str:
        if not v:
            return ""
        parts = [p.strip() for p in str(v).split(",") if p and p.strip()]
        seen = set()
        uniq = []
        for p in parts:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return ", ".join(uniq)

    def _rollup_new_enquiry_from_subitems(self, subitems_data: List[Dict]) -> Dict[str, float]:
        rollup: Dict[str, float] = {}
        for s in subitems_data or []:
            pid = s.get("parent_monday_id")
            if not pid:
                continue
            val = self._parse_numeric_value(s.get("new_enquiry_value"))
            if val is None:
                if (s.get("reason_for_change") or "").strip() == "New Enquiry":
                    val = self._parse_numeric_value(s.get("quote_amount")) or 0.0
                else:
                    val = 0.0
            rollup[pid] = round(rollup.get(pid, 0.0) + float(val), 2)
        return rollup

    def _rollup_order_values_from_subitems(
        self,
        subitems_data: List[Dict]
    ) -> Tuple[Dict[str, float], Dict[str, str]]:
        totals: Dict[str, float] = {}
        min_dates: Dict[str, datetime] = {}

        def _to_dt(value: Any) -> Optional[datetime]:
            parsed = self._parse_date_value(value)
            if not parsed:
                return None
            try:
                return datetime.strptime(parsed, "%Y-%m-%d")
            except Exception:
                return None

        for s in subitems_data or []:
            pid = s.get("parent_monday_id")
            if not pid:
                continue

            val = self._parse_numeric_value(s.get("cust_order_value_material"))
            if val is not None:
                totals[pid] = round(totals.get(pid, 0.0) + float(val), 2)

            d_dt = _to_dt(s.get("date_order_received"))
            if d_dt and (pid not in min_dates or d_dt < min_dates[pid]):
                min_dates[pid] = d_dt

        min_date_map = {pid: dt.date().isoformat() for pid, dt in min_dates.items()}
        return totals, min_date_map

    def _rollup_design_invoice_dates_from_subitems(
        self,
        subitems_data: List[Dict]
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        design_min: Dict[str, datetime] = {}
        invoice_min: Dict[str, datetime] = {}

        def _to_dt(value: Any) -> Optional[datetime]:
            parsed = self._parse_date_value(value)
            if not parsed:
                return None
            try:
                return datetime.strptime(parsed, "%Y-%m-%d")
            except Exception:
                return None

        for s in subitems_data or []:
            pid = s.get("parent_monday_id")
            if not pid:
                continue

            d_dt = _to_dt(s.get("date_design_completed"))
            if d_dt and (pid not in design_min or d_dt < design_min[pid]):
                design_min[pid] = d_dt

            i_dt = _to_dt(s.get("invoice_date"))
            if i_dt and (pid not in invoice_min or i_dt < invoice_min[pid]):
                invoice_min[pid] = i_dt

        design_map = {pid: dt.date().isoformat() for pid, dt in design_min.items()}
        invoice_map = {pid: dt.date().isoformat() for pid, dt in invoice_min.items()}
        return design_map, invoice_map

    def _apply_project_design_invoice_rollup(
        self,
        projects_data: List[Dict],
        design_min_map: Dict[str, str],
        invoice_min_map: Dict[str, str]
    ) -> None:
        if not projects_data or (not design_min_map and not invoice_min_map):
            return

        for p in projects_data:
            pid = str(p.get("monday_id") or p.get("id") or "")
            if not pid:
                continue

            rolled_design = design_min_map.get(pid)
            if rolled_design:
                current_design = self._parse_date_value(p.get("first_date_designed"))
                if not current_design:
                    p["first_date_designed"] = rolled_design
                else:
                    try:
                        cur_dt = datetime.strptime(current_design, "%Y-%m-%d")
                        new_dt = datetime.strptime(rolled_design, "%Y-%m-%d")
                        if new_dt < cur_dt:
                            p["first_date_designed"] = rolled_design
                    except Exception:
                        p["first_date_designed"] = rolled_design

            rolled_invoice = invoice_min_map.get(pid)
            if rolled_invoice:
                current_invoice = self._parse_date_value(p.get("first_date_invoiced"))
                if not current_invoice:
                    p["first_date_invoiced"] = rolled_invoice
                else:
                    try:
                        cur_dt = datetime.strptime(current_invoice, "%Y-%m-%d")
                        new_dt = datetime.strptime(rolled_invoice, "%Y-%m-%d")
                        if new_dt < cur_dt:
                            p["first_date_invoiced"] = rolled_invoice
                    except Exception:
                        p["first_date_invoiced"] = rolled_invoice

    def _apply_project_order_rollup(
        self,
        projects_data: List[Dict],
        total_map: Dict[str, float],
        min_date_map: Dict[str, str]
    ) -> None:
        if not projects_data or (not total_map and not min_date_map):
            return

        for p in projects_data:
            pid = str(p.get("monday_id") or p.get("id") or "")
            if not pid:
                continue

            rolled_total = total_map.get(pid)
            if rolled_total is not None:
                current_total = self._parse_numeric_value(p.get("total_order_value"))
                if current_total is None or float(current_total) <= 0:
                    p["total_order_value"] = rolled_total

            rolled_date = min_date_map.get(pid)
            if rolled_date:
                current_date = self._parse_date_value(p.get("date_order_received"))
                if not current_date:
                    p["date_order_received"] = rolled_date
                else:
                    try:
                        cur_dt = datetime.strptime(current_date, "%Y-%m-%d")
                        new_dt = datetime.strptime(rolled_date, "%Y-%m-%d")
                        if new_dt < cur_dt:
                            p["date_order_received"] = rolled_date
                    except Exception:
                        p["date_order_received"] = rolled_date

    def _apply_project_new_enquiry_rollup(self, projects_data: List[Dict], rollup_map: Dict[str, float]) -> None:
        if not projects_data or not rollup_map:
            return
        for p in projects_data:
            pid = str(p.get("monday_id") or p.get("id") or "")
            if not pid:
                continue
            current = self._parse_numeric_value(p.get("new_enquiry_value"))
            rolled = rollup_map.get(pid)
            if rolled is not None and (current is None or float(current) <= 0):
                p["new_enquiry_value"] = rolled

    def _compute_gestation_fallback_from_subitems(self, subitems_data: List[Dict]) -> Dict[str, int]:
        earliest_design: Dict[str, datetime] = {}
        earliest_invoice: Dict[str, datetime] = {}

        def _get_hidden_for_sub(s: Dict) -> Optional[Dict]:
            hid = s.get("hidden_item_id")
            if hid and str(hid) in self._hidden_lookup_by_id:
                return self._hidden_lookup_by_id[str(hid)]
            nm = s.get("item_name") or s.get("name")
            if nm and nm in self._hidden_lookup_by_name:
                return self._hidden_lookup_by_name[nm]
            return None

        for s in subitems_data or []:
            pid = s.get("parent_monday_id")
            if not pid:
                continue

            hidden = _get_hidden_for_sub(s)
            d = (hidden or {}).get("date_design_completed") or s.get("date_design_completed")
            i = (hidden or {}).get("invoice_date") or s.get("invoice_date")

            try:
                if d:
                    d_dt = datetime.strptime(d, "%Y-%m-%d")
                    if pid not in earliest_design or d_dt < earliest_design[pid]:
                        earliest_design[pid] = d_dt
                if i:
                    i_dt = datetime.strptime(i, "%Y-%m-%d")
                    if pid not in earliest_invoice or i_dt < earliest_invoice[pid]:
                        earliest_invoice[pid] = i_dt
            except Exception:
                continue

        out: Dict[str, int] = {}
        for pid, d_dt in earliest_design.items():
            i_dt = earliest_invoice.get(pid)
            if not i_dt:
                out[pid] = 0
                continue
            days = (i_dt - d_dt).days
            out[pid] = 0 if (days < 0 or days > 500000) else int(days)

        return out

    def _apply_project_gestation_fallback(self, projects_data: List[Dict], gmap: Dict[str, int]) -> None:
        if not projects_data or not gmap:
            return

        for p in projects_data:
            pid = str(p.get("monday_id") or p.get("id") or "")
            if not pid:
                continue
            current = self._parse_gestation_period(p.get("gestation_period"))
            rolled = gmap.get(pid)
            if rolled is not None and rolled > 0 and (current is None or int(current) <= 0):
                p["gestation_period"] = rolled

    async def _batch_upsert_projects(self, projects_data: List[Dict]) -> int:
        total_updated = 0

        for i in range(0, len(projects_data), SYNC_BATCH_SIZE):
            batch = projects_data[i:i + SYNC_BATCH_SIZE]
            try:
                result = self.supabase_client.upsert_projects(batch)
                if result["success"]:
                    inserts = result.get("count", 0)
                    updates = result.get("updates", 0)
                    affected = result.get("total_affected", 0)
                    total_updated += updates
                    logger.info(
                        f"[upsert projects] batch={i // SYNC_BATCH_SIZE + 1} "
                        f"size={len(batch)} inserts={inserts} updates={updates} affected={affected}"
                    )
                else:
                    logger.error(f"Failed to upsert projects batch: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error upserting projects batch: {e}")

        return total_updated

    async def _batch_upsert_subitems(self, subitems_data: List[Dict]) -> int:
        total_updated = 0

        for i in range(0, len(subitems_data), SYNC_BATCH_SIZE):
            batch = subitems_data[i:i + SYNC_BATCH_SIZE]
            try:
                result = self.supabase_client.upsert_subitems(batch)
                if result["success"]:
                    total_updated += result["count"]
                    logger.info(
                        f"[upsert subitems] batch={i // SYNC_BATCH_SIZE + 1} "
                        f"size={len(batch)} updated={result['count']} total={total_updated}"
                    )
                else:
                    logger.error(f"Failed to upsert subitems batch: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error upserting subitems batch: {e}")

        return total_updated

    async def _batch_upsert_hidden_items(self, hidden_data: List[Dict]) -> int:
        total_updated = 0

        for i in range(0, len(hidden_data), SYNC_BATCH_SIZE):
            batch = hidden_data[i:i + SYNC_BATCH_SIZE]
            try:
                result = self.supabase_client.upsert_hidden_items(batch)
                if result["success"]:
                    total_updated += result["count"]
                    logger.info(
                        f"[upsert hidden] batch={i // SYNC_BATCH_SIZE + 1} "
                        f"size={len(batch)} updated={result['count']} total={total_updated}"
                    )
                else:
                    logger.error(f"Failed to upsert hidden items batch: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error upserting hidden items batch: {e}")

        return total_updated

    def _process_and_resolve_mirrors(
        self,
        parent_items: List[Dict],
        subitems: List[Dict],
        hidden_items: List[Dict]
    ) -> Dict[str, List[Dict]]:
        """Process and resolve mirror columns"""
        try:
            processed_projects = self.mirror_resolver.resolve_mirrors_with_hidden_items(
                parent_items, subitems, hidden_items
            )

            return {
                "projects": processed_projects,
                "subitems": subitems,
                "hidden": hidden_items,
            }
        except Exception as e:
            logger.error(f"Error resolving mirrors: {e}")
            return {
                "projects": parent_items,
                "subitems": subitems,
                "hidden": hidden_items,
            }

    def _get_board_batch_size(self, board_id: str) -> int:
        board_batch_sizes = {
            PARENT_BOARD_ID: 100,
            SUBITEM_BOARD_ID: 100,
            HIDDEN_ITEMS_BOARD_ID: 100,
        }
        return board_batch_sizes.get(board_id, 100)

