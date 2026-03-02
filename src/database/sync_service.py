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

