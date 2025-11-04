import asyncio
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
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
    CATEGORY_LABELS
)
from ..core.monday_client import MondayClient
from ..core.data_processor import LabelNormalizer, EnhancedMirrorResolver, HierarchicalSegmentation
from .supabase_client import SupabaseClient
from ..core.enhanced_extractor import EnhancedColumnExtractor, EnhancedMondayExtractor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataSyncService:
    """Enhanced service for syncing data between Monday and Supabase"""
    
    def __init__(self):
        self.monday_client = MondayClient()
        self.supabase_client = SupabaseClient()
        self.label_normalizer = LabelNormalizer()
        self.mirror_resolver = EnhancedMirrorResolver()
        self.segmentation = HierarchicalSegmentation()
        # Add enhanced extractor
        self.enhanced_extractor = EnhancedMondayExtractor(self.monday_client)
        # Fallback caches for enrichment from hidden items (needed due to broken board connections)
        self._hidden_lookup_by_id: Dict[str, Dict] = {}
        self._hidden_lookup_by_name: Dict[str, Dict] = {}
        self.max_duplicate_chunks = 10
       
    async def perform_full_sync(self) -> Dict[str, Any]:
        """Perform full data sync from Monday to Supabase"""
        logger.info("Starting full sync from Monday to Supabase")
        
        # Create sync log entries
        parent_log_id = self.supabase_client.log_sync_operation(
            'full', PARENT_BOARD_ID, 'Tapered Enquiry Maintenance'
        )
        
        try:
            # Step 1: Get board info
            parent_info = self.monday_client.get_board_info(PARENT_BOARD_ID)
            subitem_info = self.monday_client.get_board_info(SUBITEM_BOARD_ID)
            hidden_info = self.monday_client.get_board_info(HIDDEN_ITEMS_BOARD_ID)
            
            logger.info(f"Boards to sync - Parent: {parent_info['items_count']}, "
                       f"Subitems: {subitem_info['items_count']}, "
                       f"Hidden: {hidden_info['items_count']}")
            
            # Step 2: Extract all data from Monday
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
            
            # Step 3: Resolve mirrors and process data
            processed_data = self._process_and_resolve_mirrors(
                parent_items, subitems, hidden_items
            )
            
            # Step 4: Transform for Supabase with enhanced normalization (hidden → subitems → projects)
            hidden_data = self._transform_for_hidden_table(processed_data['hidden'])
            subitems_data = self._transform_for_subitems_table(processed_data['subitems'])

            # Roll up "New Enquiry Value" per project from transformed subitems
            rollup_map = self._rollup_new_enquiry_from_subitems(subitems_data)

            projects_data = self._transform_for_projects_table(processed_data['projects'])

            # Apply rollup fallback to projects where missing/zero
            self._apply_project_new_enquiry_rollup(projects_data, rollup_map)

            # NEW: compute and apply gestation fallback from subitems+hidden_items
            gmap = self._compute_gestation_fallback_from_subitems(subitems_data)
            self._apply_project_gestation_fallback(projects_data, gmap)

            # Step 5: Batch upload to Supabase
            stats = {
                'processed': len(projects_data) + len(subitems_data) + len(hidden_data),
                'created': 0,
                'updated': 0,
                'errors': 0
            }

            # Upload in batches with error tracking
            stats['updated'] += await self._batch_upsert_hidden_items(hidden_data)
            stats['updated'] += await self._batch_upsert_projects(projects_data)
            stats['updated'] += await self._batch_upsert_subitems(subitems_data)
            
            # Update sync log
            self.supabase_client.update_sync_log(parent_log_id, 'completed', stats)
            
            logger.info(f"Full sync completed. Stats: {stats}")
            return {"success": True, "stats": stats}
            
        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            self.supabase_client.update_sync_log(
                parent_log_id, 'failed', error=str(e)
            )
            return {"success": False, "error": str(e)}
    
    async def perform_delta_sync(self, hours_back: int = 24) -> Dict[str, Any]:
        """Perform delta sync for recent changes"""
        logger.info(f"Starting delta sync for last {hours_back} hours")
        
        # Get last sync time or use hours_back
        last_sync = self.supabase_client.get_last_sync_time(PARENT_BOARD_ID)
        if not last_sync:
            last_sync = datetime.now() - timedelta(hours=hours_back)
        
        log_id = self.supabase_client.log_sync_operation(
            'delta', PARENT_BOARD_ID, 'Delta Sync'
        )
        
        try:
            # Query Monday for items updated since last sync
            updated_items = await self._get_updated_items_since(last_sync)
            
            if not any(updated_items.values()):
                logger.info("No items to sync")
                self.supabase_client.update_sync_log(
                    log_id, 'completed', {'processed': 0}
                )
                return {"success": True, "stats": {'processed': 0}}
            
            # Process and sync updated items
            stats = await self._sync_updated_items(updated_items)
            
            self.supabase_client.update_sync_log(log_id, 'completed', stats)
            logger.info(f"Delta sync completed. Stats: {stats}")
            
            return {"success": True, "stats": stats}
            
        except Exception as e:
            logger.error(f"Delta sync failed: {e}")
            self.supabase_client.update_sync_log(
                log_id, 'failed', error=str(e)
            )
            return {"success": False, "error": str(e)}
    
    async def perform_chunked_projects_sync(self, chunk_size: int = 1000, start_offset: int = 0, max_items: int = None) -> Dict[str, Any]:
        """Perform chunked sync for large projects board to avoid timeouts"""
        logger.info(f"Starting chunked projects sync with chunk size: {chunk_size}, offset: {start_offset}")
        
        # Create sync log entry
        log_id = self.supabase_client.log_sync_operation(
            'full', PARENT_BOARD_ID, 'Chunked Projects Sync'
        )
        
        try:
            # Get total count
            board_info = self.monday_client.get_board_info(PARENT_BOARD_ID)
            total_items = board_info['items_count']
            
            logger.info(f"Syncing {total_items} projects in chunks of {chunk_size}")
            
            stats = {
                'processed': 0,
                'updated': 0,
                'errors': 0,
                'chunks_completed': 0,
                'chunks_failed': 0
            }
            # Load existing IDs once; skip already-synced before upsert
            existing_ids = self.supabase_client.get_existing_project_ids()
            last_offset_processed = start_offset
            
            # Calculate actual range
            end_offset = total_items if max_items is None else min(start_offset + max_items, total_items)
            
            # Process in chunks (start from start_offset)
            for offset in range(start_offset, end_offset, chunk_size):
                chunk_end = min(offset + chunk_size, end_offset)
                
                try:
                    logger.info(f"Processing chunk {offset}-{chunk_end} of {total_items}")
                    
                    # Extract chunk
                    chunk = await self._extract_items_chunk(
                        PARENT_BOARD_ID,
                        list(PARENT_COLUMNS.values()),
                        limit=chunk_size,
                        offset=offset
                    )
                    
                    if not chunk:
                        stats['chunks_failed'] += 1
                        stats['errors'] += 1
                        continue

                    # Process and filter out already-synced IDs
                    processed_chunk = await self._process_projects_chunk(chunk)
                    new_items = [
                        p for p in processed_chunk
                        if str(p.get('monday_id')) not in existing_ids
                    ]
                    if not new_items:
                        stats['processed'] += len(chunk)
                        stats['chunks_completed'] += 1
                        last_offset_processed = chunk_end
                        logger.info(f"✓ Chunk {offset}-{chunk_end} completed: 0 new items (all already synced)")
                        await asyncio.sleep(5)
                        continue

                    uploaded_count = await self._batch_upsert_projects(new_items)
                    
                    stats['processed'] += len(chunk)
                    stats['updated'] += uploaded_count
                    stats['chunks_completed'] += 1
                    last_offset_processed = chunk_end
                    existing_ids.update(
                        str(p['monday_id']) for p in new_items if p.get('monday_id') is not None
                    )
                    
                    logger.info(f"✓ Chunk {offset}-{chunk_end} completed: {len(new_items)} new items")
                    
                    # Rate limiting between chunks
                    await asyncio.sleep(5)
                    
                except Exception as e:
                    stats['errors'] += 1
                    stats['chunks_failed'] += 1
                    logger.warning(f"✗ Chunk {offset}-{chunk_end} failed: {e}")
                    continue
            
            # Add resume checkpoint
            stats['last_offset'] = last_offset_processed

            # Update sync log
            self.supabase_client.update_sync_log(log_id, 'completed', stats)
            
            logger.info(f"Chunked projects sync completed. Stats: {stats}")
            return {"success": True, "stats": stats}

        except Exception as e:
            logger.error(f"Chunked projects sync failed: {e}")
            self.supabase_client.update_sync_log(log_id, 'failed', error=str(e))
            return {"success": False, "error": str(e)}
    
    async def perform_smart_projects_sync(self, chunk_size: int = 50, max_items: int = 500) -> Dict[str, Any]:
        """Perform smart sync using set-based deduplication instead of ID comparison"""
        logger.info(f"Starting smart projects sync with chunk size: {chunk_size}")
        
        # Create sync log entry
        log_id = self.supabase_client.log_sync_operation(
            'full', PARENT_BOARD_ID, 'Smart Projects Sync'
        )
        
        try:
            # Get ALL existing Monday IDs as a set for O(1) lookup
            existing_ids = self.supabase_client.get_existing_project_ids()
            logger.info(f"Found {len(existing_ids)} existing projects in database")
            
            # Get board info
            board_info = self.monday_client.get_board_info(PARENT_BOARD_ID)
            total_items = board_info['items_count']
            
            stats = {
                'processed': 0,
                'updated': 0,
                'errors': 0,
                'chunks_completed': 0,
                'chunks_failed': 0,
                'existing_count': len(existing_ids)
            }
            
            # Fetch items and filter for truly new ones
            all_items = await self._extract_all_items_with_dedup(
                PARENT_BOARD_ID,
                list(PARENT_COLUMNS.values()),
                existing_ids,  # Pass the set of existing IDs
                max_items
            )
            
            if not all_items:
                logger.info("No new items found to sync")
                self.supabase_client.update_sync_log(log_id, 'completed', stats)
                return {"success": True, "stats": stats}
            
            logger.info(f"Found {len(all_items)} new items to sync")
            
            # Process in chunks
            for i in range(0, len(all_items), chunk_size):
                chunk = all_items[i:i + chunk_size]
                
                try:
                    logger.info(f"Processing chunk {i//chunk_size + 1}: {len(chunk)} items")
                    
                    # Process and sync chunk
                    processed_chunk = await self._process_projects_chunk(chunk)
                    uploaded_count = await self._batch_upsert_projects(processed_chunk)
                    
                    stats['processed'] += len(chunk)
                    stats['updated'] += uploaded_count
                    stats['chunks_completed'] += 1
                    
                    logger.info(f"✓ Chunk completed: {uploaded_count} new items synced")
                    
                    # Rate limiting
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    stats['errors'] += 1
                    stats['chunks_failed'] += 1
                    logger.warning(f"✗ Chunk failed: {e}")
                    continue
            
            # Update stats with final max Monday ID
            if all_items:
                final_max = max(item.get('id', '0') for item in all_items)
                stats['max_monday_id_after'] = final_max
            
            # Update sync log
            self.supabase_client.update_sync_log(log_id, 'completed', stats)
            
            logger.info(f"Smart projects sync completed. Stats: {stats}")
            return {"success": True, "stats": stats}
            
        except Exception as e:
            logger.error(f"Smart projects sync failed: {e}")
            self.supabase_client.update_sync_log(log_id, 'failed', error=str(e))
            return {"success": False, "error": str(e)}

    async def _extract_all_items_smart(
        self, 
        board_id: str, 
        column_ids: List[str],
        min_monday_id: str = None,
        max_items: int = None
    ) -> List[Dict]:
        """Extract items filtering by Monday ID to avoid duplicates"""
        items = []
        cursor = None
        fetched = 0
        max_retries = 3
        
        logger.info(f"Extracting items with monday_id > {min_monday_id}")
        
        while True:
            retry_count = 0
            batch_limit = min(self._get_board_batch_size(board_id), max_items - fetched if max_items else self._get_board_batch_size(board_id))
            
            if batch_limit <= 0:
                break
            
            while retry_count < max_retries:
                try:
                    # Add timeout and retry logic
                    batch = self.monday_client.get_items_page(
                        board_id, column_ids, 
                        limit=batch_limit, cursor=cursor
                    )
                    
                    if not batch['items']:
                        logger.info("No more items to fetch")
                        return items
                    
                    # Filter items by Monday ID
                    new_items = []
                    for item in batch['items']:
                        item_id = item.get('id', '0')
                        # Convert to integers for proper comparison
                        if min_monday_id is None or int(item_id) > int(min_monday_id):
                            new_items.append(item)
                    
                    items.extend(new_items)
                    fetched += len(batch['items'])
                    
                    logger.info(f"Fetched {len(batch['items'])}, filtered to {len(new_items)} new items")
                    
                    # If we got fewer new items than the batch size, we're likely past new data
                    if len(new_items) < len(batch['items']) * 0.1:  # Less than 10% are new
                        logger.info("Most items are duplicates, stopping extraction")
                        return items
                    
                    cursor = batch.get('next_cursor')
                    if not cursor:
                        return items
                    
                    # Success - break retry loop
                    break
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"Failed after {max_retries} retries: {e}")
                        return items  # Return what we have so far
                    
                    wait_time = 2 ** retry_count  # Exponential backoff
                    logger.warning(f"Attempt {retry_count} failed: {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
            
            # Rate limiting between successful batches
            await asyncio.sleep(2)
        
        logger.info(f"Smart extraction completed: {len(items)} new items found")
        return items
    
    async def _get_updated_items_since(self, last_sync: datetime) -> Dict[str, List[Dict]]:
        """Get items updated since last sync time"""
        logger.info(f"Fetching items updated since {last_sync}")
        
        # Convert to Monday.com format (ISO string)
        since_date = last_sync.isoformat()
        
        updated_items = {
            'projects': [],
            'subitems': [],
            'hidden': []
        }
        
        try:
            # Query each board for recently updated items
            # Note: Monday.com doesn't have a direct "updated_since" filter in GraphQL
            # We'll fetch recent items and filter by updated_at on our side
            
            # Get recent parent items
            parent_items = await self._extract_recent_items(
                PARENT_BOARD_ID, 
                list(PARENT_COLUMNS.values()),
                limit=1000
            )
            updated_items['projects'] = self._filter_items_by_update_time(parent_items, last_sync)
            
            # Get recent subitems
            subitems = await self._extract_recent_items(
                SUBITEM_BOARD_ID,
                list(SUBITEM_COLUMNS.values()),
                limit=1000
            )
            updated_items['subitems'] = self._filter_items_by_update_time(subitems, last_sync)
            
            # Get recent hidden items
            hidden_items = await self._extract_recent_items(
                HIDDEN_ITEMS_BOARD_ID,
                list(HIDDEN_ITEMS_COLUMNS.values()),
                limit=1000
            )
            updated_items['hidden'] = self._filter_items_by_update_time(hidden_items, last_sync)
            
            logger.info(f"Found {len(updated_items['projects'])} updated projects, "
                       f"{len(updated_items['subitems'])} updated subitems, "
                       f"{len(updated_items['hidden'])} updated hidden items")
            
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
        """Extract recent items from a board (most recently created/updated first)"""
        items = []
        cursor = None
        fetched = 0
        max_retries = 3
        retry_count = 0
        
        # Board-specific batch sizes based on complexity
        board_batch_sizes = {
            PARENT_BOARD_ID: 100,      # Main board - complex queries
            SUBITEM_BOARD_ID: 200,     # Subitems - moderate complexity  
            HIDDEN_ITEMS_BOARD_ID: 500 # Hidden items - simple queries
        }
        
        base_batch_size = board_batch_sizes.get(board_id, 500)
        
        while fetched < limit and retry_count < max_retries:
            try:
                batch_limit = min(base_batch_size, limit - fetched)
                # Use items_page for first page; next_items_page for subsequent
                if cursor:
                    batch = self.monday_client.get_next_items_page(
                        cursor=cursor,
                        column_ids=column_ids,
                        limit=batch_limit
                    )
                else:
                    batch = self.monday_client.get_items_page(
                        board_id, column_ids, 
                        limit=batch_limit, cursor=None
                    )
                
                if not batch['items']:
                    break
                
                items.extend(batch['items'])
                fetched += len(batch['items'])
                
                cursor = batch.get('next_cursor')
                if not cursor:
                    break
                await asyncio.sleep(0.5)  # Increase rate limiting delay
                retry_count = 0  # Reset retry count on success
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Error extracting batch from {board_id} (attempt {retry_count}/{max_retries}): {e}")
                
                if retry_count >= max_retries:
                    logger.error(f"Failed to extract from board {board_id} after {max_retries} attempts")
                    break
                
                # Exponential backoff
                wait_time = 2 ** retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
        
        return items
    
    def _filter_items_by_update_time(self, items: List[Dict], since: datetime) -> List[Dict]:
        """Filter items that were updated since the given time"""
        # Monday.com doesn't provide updated_at in the API response by default
        # For now, we'll return all items and let the upsert handle duplicates
        # In production, you might want to implement more sophisticated filtering
        return items
    
    async def _sync_updated_items(self, updated_items: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Sync the updated items to Supabase"""
        stats = {
            'processed': 0,
            'updated': 0,
            'errors': 0
        }
        
        try:
            # Process and resolve mirrors for updated items
            if updated_items['projects'] or updated_items['subitems'] or updated_items['hidden']:
                processed_data = self._process_and_resolve_mirrors(
                    updated_items['projects'],
                    updated_items['subitems'], 
                    updated_items['hidden']
                )
                
                # Transform and sync each type (hidden → subitems → projects so rollups can be computed)
                hidden_data = []
                subitems_data = []
                projects_data = []

                if processed_data['hidden']:
                    hidden_data = self._transform_for_hidden_table(processed_data['hidden'])
                    stats['updated'] += await self._batch_upsert_hidden_items(hidden_data)
                    stats['processed'] += len(hidden_data)

                if processed_data['subitems']:
                    subitems_data = self._transform_for_subitems_table(processed_data['subitems'])
                    # Compute rollups from whatever subitems we have in this delta
                    rollup_map = self._rollup_new_enquiry_from_subitems(subitems_data)
                    # NEW: compute gestation fallback map
                    gmap = self._compute_gestation_fallback_from_subitems(subitems_data)
                    stats['updated'] += await self._batch_upsert_subitems(subitems_data)
                    stats['processed'] += len(subitems_data)

                # Compute rollups from whatever subitems we have in this delta
                rollup_map = self._rollup_new_enquiry_from_subitems(subitems_data)

                if processed_data['projects']:
                    projects_data = self._transform_for_projects_table(processed_data['projects'])
                    self._apply_project_new_enquiry_rollup(projects_data, rollup_map)
                    # NEW: apply gestation fallback
                    self._apply_project_gestation_fallback(projects_data, gmap)
                    stats['updated'] += await self._batch_upsert_projects(projects_data)
                    stats['processed'] += len(projects_data)
                elif rollup_map or gmap:
                    # Patch existing projects directly when only subitems changed (update-only mode)
                    patch = []
                    if rollup_map:
                        patch.extend(
                            {'monday_id': pid, 'new_enquiry_value': val, 'last_synced_at': datetime.now().isoformat()}
                            for pid, val in rollup_map.items() if val is not None
                        )
                    if gmap:
                        patch.extend(
                            {'monday_id': pid, 'gestation_period': val, 'last_synced_at': datetime.now().isoformat()}
                            for pid, val in gmap.items() if val is not None and val > 0
                        )
                    if patch:
                        existing_ids = self.supabase_client.get_existing_project_ids()
                        patch_in_db = [p for p in patch if str(p.get('monday_id') or '') in existing_ids]

                        updated = 0
                        for row in patch_in_db:
                            try:
                                payload = {
                                    k: v for k, v in row.items()
                                    if k in ('new_enquiry_value', 'gestation_period', 'last_synced_at')
                                }
                                self.supabase_client.client.table('projects')\
                                    .update(payload)\
                                    .eq('monday_id', row['monday_id'])\
                                    .execute()
                                updated += 1
                            except Exception as e:
                                logger.error(f"Update-only patch failed for {row.get('monday_id')}: {e}")

                        stats['updated'] += updated
            
        except Exception as e:
            logger.error(f"Error syncing updated items: {e}")
            stats['errors'] += 1
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
        
        # Intelligent-sync style helpers
        from collections import deque
        last_seen_ring = deque(maxlen=10)
        chunks_completed = 0
        total_processed = 0
        start_time = datetime.now()

        # Sync parameters (aligned with intelligent flow)
        BATCH_DELAY = 1.5
        THROTTLE_BATCH = 1000
        board_batch_size = self._get_board_batch_size(board_id)
        
        while True:
            try:
                cursor_preview = 'start' if not cursor else f"{cursor[:8]}...{cursor[-8:]}"
                logger.info(f"Fetching page (cursor: {cursor_preview})")
                # Use items_page only for the first page; then switch to next_items_page
                if cursor:
                    if board_id == SUBITEM_BOARD_ID:
                        batch = self.monday_client.get_subitems_page(
                            board_id, column_ids,
                            limit=board_batch_size,
                            cursor=cursor
                        )
                    else:
                        batch = self.monday_client.get_next_items_page(
                            cursor=cursor,
                            column_ids=column_ids,
                            limit=board_batch_size
                        )
                else:
                    if board_id == SUBITEM_BOARD_ID:
                        batch = self.monday_client.get_subitems_page(
                            board_id, column_ids,
                            limit=board_batch_size,
                            cursor=None
                        )
                    else:
                        batch = self.monday_client.get_items_page(
                            board_id, column_ids, 
                            limit=board_batch_size, cursor=None
                        )
                if batch['items']:
                    items.extend(batch['items'])
                    logger.debug(f"Extracted {len(items)}/{total_count} items from {board_id}")
                    # Track last-seen id to enable fast-forward after reseed
                    try:
                        last_id = str(batch['items'][-1].get('id')) if batch['items'] else None
                        if last_id:
                            last_seen_ring.append(last_id)
                    except Exception:
                        pass
                
                cursor = batch.get('next_cursor')
                if not cursor:
                    break

                # Persist cursor for this board
                self.supabase_client.save_cursor_checkpoint_for_board(board_id, cursor)

                # Stats + periodic progress log
                chunks_completed += 1
                total_processed += len(batch['items'] or [])
                if chunks_completed % 5 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds() or 1
                    rate = total_processed / elapsed
                    logger.info(f"[{board_id}] pages={chunks_completed} items={total_processed} rate={rate:.1f}/s cursor={cursor_preview}")

                # Throttle between pages and apply batch pause
                await asyncio.sleep(0.5)
                if total_processed > 0 and (total_processed % THROTTLE_BATCH == 0):
                    logger.info(f"[{board_id}] batch throttle pause {BATCH_DELAY}s after {total_processed} items")
                    await asyncio.sleep(BATCH_DELAY)
                
            except Exception as e:
                if "CursorExpiredError" in str(e):
                    # Save last-known cursor and reseed a fresh chain
                    try:
                        self.supabase_client.save_cursor_checkpoint_for_board(board_id, cursor)
                    except Exception:
                        pass
                    logger.warning(f"Cursor expired for board {board_id} - reseeding and continuing with next_items_page")

                    # Reseed via items_page(limit=1) → get a fresh next_cursor
                    seed = self.monday_client.get_items_page(
                        board_id,
                        column_ids,
                        limit=1,
                        cursor=None
                    )
                    cursor = seed.get('next_cursor')  # continue with next_items_page chain

                    # Optional: fast-forward near previous position using last_seen_ring (IDs-only scan)
                    try:
                        if last_seen_ring:
                            ff_cursor = None
                            hops = 0
                            max_hops = 500
                            while hops < max_hops:
                                page_ids = (self.monday_client.get_next_item_ids_page(ff_cursor, board_batch_size)
                                            if ff_cursor else
                                            self.monday_client.get_item_ids_page(board_id, board_batch_size, None))
                                ids_on_page = {str(it.get('id')) for it in (page_ids.get('items') or [])}
                                if any(k in ids_on_page for k in list(last_seen_ring)):
                                    cursor = page_ids.get('next_cursor')  # resume after matched page
                                    break
                                ff_cursor = page_ids.get('next_cursor')
                                if not ff_cursor:
                                    break
                                hops += 1
                                await asyncio.sleep(0.2)
                    except Exception:
                        pass

                    await asyncio.sleep(1)
                    continue

                # Robust transient handling (aligned with intelligent sync)
                msg = str(e).lower()
                if "504" in msg or "timeout" in msg:
                    logger.warning("API timeout - waiting 30s before retry...")
                    await asyncio.sleep(30)
                    continue
                if "rate limit" in msg or "429" in msg:
                    # Use server-provided retry_in_seconds if present
                    retry_sec = 60
                    try:
                        import re
                        m = re.search(r"retry_in_seconds['\"]?:\s*(\d+)", str(e))
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
                            # Bound per spec: invalid/huge → 0
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
                            # most frequent
                            project_name = max(set(names), key=names.count)
                    except Exception:
                        pass

                # Dedup for mirror fallbacks
                account_val = item.get('account') or item.get('account_mirror') or ''
                product_val = item.get('product_type') or item.get('product_mirror') or ''
                account_val = self._dedup_csv_labels(account_val)
                product_val = self._dedup_csv_labels(product_val)

                # Primary project build with corrected gestation preference order
                project = {
                    'monday_id': item.get('monday_id') or item.get('id'),
                    'item_name': item.get('name', ''),
                    'project_name': project_name or '',

                    # Prefer resolver-computed values first, then mirrors (deduped)
                    'account': account_val,
                    'product_type': product_val,
                    'new_enquiry_value': (
                        self._parse_numeric_value(item.get('new_enquiry_value'))  # resolver path
                        or self._parse_numeric_value(item.get('new_enq_value_mirror'))  # parent mirror
                        or fallback_nev  # hidden-items fallback
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

                    # Calculate gestation period: prefer calc or item value, else fallback
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
    
    def _calculate_gestation_period(self, item: Dict) -> Optional[int]:
        """
        Calculate gestation period from dates
        Formula: DAYS(First Date Invoiced, First Date Designed)
        """
        try:
            fdi = item.get('first_date_invoiced')
            fdd = item.get('first_date_designed')
            
            # If no design date, we cannot compute
            if not fdd:
                return None
            # If invoice date missing but design present → Monday formula returns 0
            if not fdi:
                return 0
            
            # Parse dates
            if isinstance(fdi, str):
                fdi_date = datetime.strptime(fdi, '%Y-%m-%d')
            else:
                return 0  # treat missing/invalid invoice as 0
            
            if isinstance(fdd, str):
                fdd_date = datetime.strptime(fdd, '%Y-%m-%d')
            else:
                return None
            
            # Calculate days difference
            days = (fdi_date - fdd_date).days
            
            # Apply the formula logic: IF(days > 500000, 0, days)
            if days > 500000:
                return 0
            
            return max(0, days)  # Ensure non-negative integer
            
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
        
        for idx, item in enumerate(subitems, 1):
            try:
                # Normalize values but retain raw for parent reference
                normalized = self._normalize_monday_item(item)

                # Determine name early for hidden fallback usage
                nm = normalized.get('name') or normalized.get('item_name') or item.get('name')

                parent_id = normalized.get('parent_monday_id') or (item.get('parent_item') or {}).get('id')
                if not parent_id:
                    logger.warning(f"Skipping subitem without parent reference: {normalized.get('monday_id') or normalized.get('id') or item.get('id')}")
                    skipped_ids.append(normalized.get('monday_id') or normalized.get('id') or item.get('id'))
                    skipped_count += 1
                    continue

                # Reason fallback (mirror may be broken → use hidden items)
                reason = (normalized.get('reason_for_change') or '').strip()
                if not reason and nm and nm in self._hidden_lookup_by_name:
                    r = self._hidden_lookup_by_name[nm].get('reason_for_change')
                    reason = (r or '').strip() if r is not None else ''

                # Fallback enrichment for quote_amount when mirror is broken
                quote_val = self._parse_numeric_value(normalized.get('quote_amount'))
                if quote_val is None:
                    if nm and nm in self._hidden_lookup_by_name:
                        quote_val = self._hidden_lookup_by_name[nm].get('quote_amount')
                        if quote_val is not None:
                            logger.info(f"Enriched {nm} quote_amount: {quote_val}")

                # Robust account / product fallbacks from raw column_values if normalization missed
                account_fallback = self.mirror_resolver._extract_column_value(item, 'mirror_12__1') or ''
                product_fallback = self.mirror_resolver._extract_column_value(item, 'mirror875__1') or ''

                new_enq_val = self._parse_numeric_value(normalized.get('new_enquiry_value'))
                if new_enq_val is None:
                    # Try the formula cell directly first
                    direct_nev = self.mirror_resolver._extract_numeric_value(item, 'formula_mkqa31kh')
                    if direct_nev is not None:
                        new_enq_val = direct_nev
                    else:
                        # Derive from reason + quote (using hidden-fallback reason)
                        if reason == 'New Enquiry':
                            new_enq_val = quote_val if quote_val is not None else 0.0
                        else:
                            new_enq_val = 0.0

                # Prefer real board relation for hidden link; fall back to name
                hidden_id = normalized.get('hidden_item_id')
                if not hidden_id:
                    if nm and nm in self._hidden_lookup_by_name:
                        candidate = self._hidden_lookup_by_name[nm].get('monday_id')
                        if candidate:
                            hidden_id = candidate

                # If reason still empty and we have hidden_id, try lookup by id
                if (not reason) and hidden_id:
                    hid = str(hidden_id)
                    if hid and hid in self._hidden_lookup_by_id:
                        r = self._hidden_lookup_by_id[hid].get('reason_for_change')
                        reason = (r or '').strip() if r is not None else reason

                subitem = {
                    'monday_id': normalized.get('monday_id') or normalized.get('id') or item.get('id'),
                    'parent_monday_id': parent_id,
                    'item_name': normalized.get('name', '') or item.get('name', ''),

                    'account': (normalized.get('account') or account_fallback),
                    'product_type': (normalized.get('product_type') or product_fallback),
                    'new_enquiry_value': new_enq_val,
                    'reason_for_change': reason,

                    'hidden_item_id': hidden_id,

                    'designer': normalized.get('designer', ''),
                    'surveyor': normalized.get('surveyor', ''),
                    'area': self._parse_numeric_value(normalized.get('area')),
                    'fall': normalized.get('fall', ''),
                    'deck_type': normalized.get('deck_type', ''),
                    'layering': normalized.get('layering', ''),

                    'time_taken': self._parse_numeric_value(normalized.get('time_taken')),
                    'min_thickness': self._parse_numeric_value(normalized.get('min_thickness')),
                    'max_thickness': self._parse_numeric_value(normalized.get('max_thickness')),
                    'u_value': self._parse_numeric_value(normalized.get('u_value')),
                    'm2_rate': self._parse_numeric_value(normalized.get('m2_rate')),
                    'material_value': self._parse_numeric_value(normalized.get('material_value')),
                    'transport_cost': self._parse_numeric_value(normalized.get('transport_cost')),
                    'order_status': normalized.get('order_status', ''),
                    'date_order_received': self._parse_date_value(normalized.get('date_order_received')),
                    'customer_po': normalized.get('customer_po', ''),
                    'supplier1': normalized.get('supplier1', ''),
                    'supplier2': normalized.get('supplier2', ''),
                    'requested_delivery_date': self._parse_date_value(normalized.get('requested_delivery_date')),
                    'final_delivery_date': self._parse_date_value(normalized.get('final_delivery_date')),
                    'delivery_address': normalized.get('delivery_address', ''),

                    'date_received': self._parse_date_value(normalized.get('date_received')),
                    'date_design_completed': self._parse_date_value(
                        normalized.get('date_design_completed_mirror') or normalized.get('date_design_completed')
                    ),
                    'invoice_number': normalized.get('invoice_number', ''),
                    'invoice_date': self._parse_date_value(normalized.get('invoice_date')),

                    # Fallback from mirror
                    'quote_amount': quote_val,

                    'amount_invoiced': self._parse_numeric_value(normalized.get('amount_invoiced')),
                    
                    'last_synced_at': datetime.now().isoformat()
                }
                
                transformed.append(subitem)
                logger.info(f"Successfully transformed subitem {subitem.get('monday_id') or item.get('id')}")
                
            except Exception as e:
                logger.error(f"Error transforming subitem {item.get('monday_id') or item.get('id')}: {e}")
                exception_count += 1
                continue
        
        if skipped_ids:
            logger.info(f"Skipped subitems (no parent): {skipped_ids[:20]}{'...' if len(skipped_ids)>20 else ''}")
        logger.info(f"Transformation complete: {len(transformed)} transformed, {skipped_count} skipped, {exception_count} exceptions")
        return transformed
    
    def _transform_for_hidden_table(self, hidden_items: List[Dict]) -> List[Dict]:
        """Enhanced transform function for hidden items"""
        transformed = []
        
        for item in hidden_items:
            try:
                normalized_item = self._normalize_monday_item(item)
                
                hidden = {
                    'monday_id': normalized_item.get('id'),
                    'item_name': normalized_item.get('name', ''),
                    'status': normalized_item.get('status', ''),
                    'project_attachments': self._parse_json_field(normalized_item.get('project_attachments')),
                    'prefix': normalized_item.get('prefix', ''),
                    'revision': normalized_item.get('revision', ''),
                    'project_name': normalized_item.get('project_name', ''),
                    'reason_for_change': normalized_item.get('reason_for_change', ''),
                    'urgency': normalized_item.get('urgency', ''),
                    'approx_bonding': normalized_item.get('approx_bonding', ''),
                    'volume_m3': self._parse_numeric_value(normalized_item.get('volume_m3')),
                    'wastage_percent': self._parse_numeric_value(normalized_item.get('wastage_percent')),
                    'min_thickness': self._parse_numeric_value(normalized_item.get('min_thickness')),
                    'max_thickness': self._parse_numeric_value(normalized_item.get('max_thickness')),
                    'time_taken': self._parse_numeric_value(normalized_item.get('time_taken')),
                    'date_received': self._parse_date_value(normalized_item.get('date_received')),
                    'date_design_completed': self._parse_date_value(normalized_item.get('date_design_completed')),
                    'invoice_date': self._parse_date_value(normalized_item.get('invoice_date')),
                    'date_quoted': self._parse_date_value(normalized_item.get('date_quoted')),
                    'date_project_won': self._parse_date_value(normalized_item.get('date_project_won')),
                    'date_project_closed': self._parse_date_value(normalized_item.get('date_project_closed')),
                    'quote_amount': self._parse_numeric_value(normalized_item.get('quote_amount', 0)),
                    'material_value': self._parse_numeric_value(normalized_item.get('material_value')),
                    'transport_cost': self._parse_numeric_value(normalized_item.get('transport_cost')),
                    'target_gpm': self._parse_numeric_value(normalized_item.get('target_gpm')),
                    'tp_margin': self._parse_numeric_value(normalized_item.get('tp_margin')),
                    'commission': self._parse_numeric_value(normalized_item.get('commission')),
                    'distributor_margin': self._parse_numeric_value(normalized_item.get('distributor_margin')),
                    'account_id': normalized_item.get('account_id', ''),
                    'contact_id': normalized_item.get('contact_id', ''),
                    'last_synced_at': datetime.now().isoformat()
                }

                
                # Update fallback lookups for later subitem enrichment
                try:
                    if hidden.get('monday_id') is not None:
                        self._hidden_lookup_by_id[str(hidden['monday_id'])] = hidden
                    if hidden.get('item_name'):
                        self._hidden_lookup_by_name[hidden['item_name']] = hidden
                except Exception as e:
                    logger.warning(f"Failed to update hidden lookup: {e}")
                                  
                transformed.append({k: v for k, v in hidden.items() if v is not None})
                
            except Exception as e:
                logger.warning(f"Failed to transform hidden item {item.get('id')}: {e}")
        
        return transformed
    
    def _normalize_monday_item(self, item: Dict) -> Dict:
        """Normalize Monday item using existing logic from data_processor"""
        # Use the existing label normalizer to process column values
        normalized = {}
        
        # Copy basic fields
        normalized['id'] = item.get('id')
        normalized['name'] = item.get('name', '')

        # Preserve parent reference if present (needed for mirror aggregation)
        if isinstance(item.get('parent_item'), dict):
            normalized['parent_item'] = item.get('parent_item')
        
        # Process column values using the label normalizer
        for column_value in item.get('column_values', []):
            column_id = column_value.get('id')
            if column_id:
                normalized_value = self.label_normalizer.normalize_column_value(
                    column_value, column_id
                )
                if normalized_value is not None:
                    # Map column IDs to field names
                    field_name = self._map_column_to_field(column_id)
                    if field_name:
                        normalized[field_name] = normalized_value
        
        return normalized
    
    # def _map_column_to_field(self, column_id: str) -> Optional[str]:
    #     """Map Monday column IDs to database field names using config mappings"""
    #     # Create reverse mapping from config
    #     all_config_mappings = {
    #         **PARENT_COLUMNS,
    #         **SUBITEM_COLUMNS, 
    #         **HIDDEN_ITEMS_COLUMNS
    #     }
        
    #     # Reverse the mapping (column_id → field_name)
    #     reverse_mapping = {v: k for k, v in all_config_mappings.items()}
        
    #     return reverse_mapping.get(column_id)

    def _map_column_to_field(self, column_id: str) -> Optional[str]:
        # Build reverse map by column_id → field_name across all boards (avoid field-name collisions)
        reverse_mapping = {}
        for field, col in PARENT_COLUMNS.items():
            reverse_mapping[col] = field
        for field, col in SUBITEM_COLUMNS.items():
            reverse_mapping[col] = field
        for field, col in HIDDEN_ITEMS_COLUMNS.items():
            reverse_mapping[col] = field
        return reverse_mapping.get(column_id)
    
    def _normalize_pipeline_stage(self, stage_value: Any) -> str:
        """Normalize pipeline stage using existing mappings"""
        if isinstance(stage_value, str) and stage_value.isdigit():
            return PIPELINE_STAGE_LABELS.get(stage_value, stage_value)
        return str(stage_value) if stage_value else ''
    
    def _normalize_type(self, type_value: Any) -> str:
        """Normalize type using existing mappings"""
        if isinstance(type_value, str) and type_value.isdigit():
            return TYPE_LABELS.get(type_value, type_value)
        return str(type_value) if type_value else ''
    
    def _normalize_category(self, category_value: Any) -> str:
        """Normalize category using existing mappings"""
        if isinstance(category_value, str) and category_value.isdigit():
            return CATEGORY_LABELS.get(category_value, category_value)
        return str(category_value) if category_value else ''
    
    def _parse_numeric_value(self, value: Any) -> Optional[float]:
        """Parse numeric values with proper error handling"""
        if value is None or value == '':
            return None
        
        try:
            if isinstance(value, str):
                # Remove currency symbols and parse
                clean_value = value.replace('£', '').replace(',', '').strip()
                return float(clean_value) if clean_value else None
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_gestation_period(self, value: Any) -> Optional[int]:
        """Parse gestation period with validation (0..500000, align with Monday formula)."""
        if value is None:
            return None
        
        try:
            gestation = int(float(value))
            if 0 <= gestation <= 500000:
                return gestation
            else:
                logger.warning(f"Gestation period out of range: {gestation}")
                return None
        except (ValueError, TypeError):
            return None
    
    def _parse_probability(self, value: Any) -> Optional[int]:
        """Parse probability percentage with validation"""
        if value is None:
            return None
        
        try:
            prob = int(float(value))
            # Validate range (0-100%)
            if 0 <= prob <= 100:
                return prob
            else:
                logger.warning(f"Probability out of range: {prob}")
                return None
        except (ValueError, TypeError):
            return None
    
    def _parse_date_value(self, value: Any) -> Optional[str]:
        """Parse date values with multiple format support"""
        if not value:
            return None
        
        try:
            if isinstance(value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        return datetime.strptime(value, fmt).date().isoformat()
                    except ValueError:
                        continue
            return str(value)  # Return as-is if no format matches
        except Exception:
            return None
    
    def _parse_json_field(self, value: Any) -> Optional[Dict]:
        """Parse JSON fields safely"""
        if not value:
            return None
        
        try:
            if isinstance(value, str):
                return json.loads(value)
            elif isinstance(value, dict):
                return value
            else:
                return None
        except (json.JSONDecodeError, TypeError):
            return None

    def _dedup_csv_labels(self, v: Any) -> str:
        """Remove repeated labels from comma-separated mirror display strings."""
        if not v:
            return ''
        parts = [p.strip() for p in str(v).split(',') if p and p.strip()]
        seen = set()
        uniq = []
        for p in parts:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return ', '.join(uniq)

    def _rollup_new_enquiry_from_subitems(self, subitems_data: List[Dict]) -> Dict[str, float]:
        rollup: Dict[str, float] = {}
        for s in subitems_data or []:
            pid = s.get('parent_monday_id')
            if not pid:
                continue
            val = self._parse_numeric_value(s.get('new_enquiry_value'))
            if val is None:
                if (s.get('reason_for_change') or '').strip() == 'New Enquiry':
                    val = self._parse_numeric_value(s.get('quote_amount')) or 0.0
                else:
                    val = 0.0
            rollup[pid] = round(rollup.get(pid, 0.0) + float(val), 2)
        return rollup

    def _apply_project_new_enquiry_rollup(self, projects_data: List[Dict], rollup_map: Dict[str, float]) -> None:
        if not projects_data or not rollup_map:
            return
        for p in projects_data:
            pid = str(p.get('monday_id') or p.get('id') or '')
            if not pid:
                continue
            current = self._parse_numeric_value(p.get('new_enquiry_value'))
            rolled = rollup_map.get(pid)
            if rolled is not None and (current is None or float(current) <= 0):
                p['new_enquiry_value'] = rolled

    def _compute_gestation_fallback_from_subitems(self, subitems_data: List[Dict]) -> Dict[str, int]:
        """
        Build gestation fallback per parent from hidden_items, using subitem → hidden linkage.
        Uses earliest design date vs earliest invoice date and bounds result (negatives / huge → 0).
        """
        from datetime import datetime
        earliest_design: Dict[str, datetime] = {}
        earliest_invoice: Dict[str, datetime] = {}

        def _get_hidden_for_sub(s: Dict) -> Optional[Dict]:
            hid = s.get('hidden_item_id')
            if hid and str(hid) in self._hidden_lookup_by_id:
                return self._hidden_lookup_by_id[str(hid)]
            nm = s.get('item_name') or s.get('name')
            if nm and nm in self._hidden_lookup_by_name:
                return self._hidden_lookup_by_name[nm]
            return None

        for s in subitems_data or []:
            pid = s.get('parent_monday_id')
            if not pid:
                continue

            hidden = _get_hidden_for_sub(s)

            # Prefer hidden_items dates; fallback to subitem dates if needed
            d = (hidden or {}).get('date_design_completed') or s.get('date_design_completed')
            i = (hidden or {}).get('invoice_date') or s.get('invoice_date')

            try:
                if d:
                    d_dt = datetime.strptime(d, '%Y-%m-%d')
                    if pid not in earliest_design or d_dt < earliest_design[pid]:
                        earliest_design[pid] = d_dt
                if i:
                    i_dt = datetime.strptime(i, '%Y-%m-%d')
                    if pid not in earliest_invoice or i_dt < earliest_invoice[pid]:
                        earliest_invoice[pid] = i_dt
            except Exception:
                continue

        out: Dict[str, int] = {}
        for pid, d_dt in earliest_design.items():
            i_dt = earliest_invoice.get(pid)
            if not i_dt:
                # Per spec: design exists but no invoice → 0
                out[pid] = 0
                continue
            days = (i_dt - d_dt).days
            out[pid] = 0 if (days < 0 or days > 500000) else int(days)
        return out

    def _apply_project_gestation_fallback(self, projects_data: List[Dict], gmap: Dict[str, int]) -> None:
        """
        Apply gestation fallback only where current value is missing/zero and we have >0 fallback.
        """
        if not projects_data or not gmap:
            return
        for p in projects_data:
            pid = str(p.get('monday_id') or p.get('id') or '')
            if not pid:
                continue
            current = self._parse_gestation_period(p.get('gestation_period'))
            rolled = gmap.get(pid)
            if rolled is not None and rolled > 0 and (current is None or int(current) <= 0):
                p['gestation_period'] = rolled
            # if rolled is not None and (current is None):
            #     p['gestation_period'] = rolled
    
    async def _batch_upsert_projects(self, projects_data: List[Dict]) -> int:
        """Batch upsert projects with error handling"""
        total_updated = 0
        
        for i in range(0, len(projects_data), SYNC_BATCH_SIZE):
            batch = projects_data[i:i + SYNC_BATCH_SIZE]
            try:
                result = self.supabase_client.upsert_projects(batch)
                if result['success']:
                    inserts = result.get('count', 0)
                    updates = result.get('updates', 0)
                    affected = result.get('total_affected', 0)
                    total_updated += updates  # or track both inserts and updates separately
                    logger.info(f"[upsert projects] batch={i//SYNC_BATCH_SIZE+1} size={len(batch)} inserts={inserts} updates={updates} affected={affected}")
                else:
                    logger.error(f"Failed to upsert projects batch: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error upserting projects batch: {e}")
        
        return total_updated
    
    async def _batch_upsert_subitems(self, subitems_data: List[Dict]) -> int:
        """Batch upsert subitems with error handling"""
        total_updated = 0
        
        for i in range(0, len(subitems_data), SYNC_BATCH_SIZE):
            batch = subitems_data[i:i + SYNC_BATCH_SIZE]
            try:
                result = self.supabase_client.upsert_subitems(batch)
                if result['success']:
                    total_updated += result['count']
                    logger.info(f"[upsert subitems] batch={i//SYNC_BATCH_SIZE+1} size={len(batch)} updated={result['count']} total={total_updated}")
                else:
                    logger.error(f"Failed to upsert subitems batch: {result.get('error')}")
            except Exception as e:
                logger.error(f"Error upserting subitems batch: {e}")
        
        return total_updated
    
    async def _batch_upsert_hidden_items(self, hidden_data: List[Dict]) -> int:
        """Batch upsert hidden items with error handling"""
        total_updated = 0
        
        for i in range(0, len(hidden_data), SYNC_BATCH_SIZE):
            batch = hidden_data[i:i + SYNC_BATCH_SIZE]
            try:
                result = self.supabase_client.upsert_hidden_items(batch)
                if result['success']:
                    total_updated += result['count']
                    logger.info(f"[upsert hidden] batch={i//SYNC_BATCH_SIZE+1} size={len(batch)} updated={result['count']} total={total_updated}")
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
        """Process and resolve mirror columns with enhanced error handling"""
        try:
            # Use existing mirror resolver logic
            processed_projects = self.mirror_resolver.resolve_mirrors_with_hidden_items(
                parent_items, subitems, hidden_items
            )
            
            return {
                'projects': processed_projects,
                'subitems': subitems,
                'hidden': hidden_items
            }
        except Exception as e:
            logger.error(f"Error resolving mirrors: {e}")
            # Return original data if mirror resolution fails
            return {
                'projects': parent_items,
                'subitems': subitems,
                'hidden': hidden_items
            }
    
    async def _extract_items_chunk(
        self, 
        board_id: str, 
        column_ids: List[str],
        limit: int,
        offset: int = 0
    ) -> List[Dict]:
        """Extract a specific chunk of items from a board starting at exact offset."""
        try:
            if offset < 0:
                logger.error(f"Invalid offset: {offset}")
                return []
            
            # Add timeout to prevent hanging
            items = await asyncio.wait_for(
                self._fetch_with_offset(board_id, column_ids, limit, offset),
                timeout=30.0  # 30 second timeout
            )
            return items
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching chunk at offset {offset}")
            return []
    
    async def _fetch_with_offset(
        self, 
        board_id: str, 
        column_ids: List[str],
        limit: int,
        offset: int
    ) -> List[Dict]:
        """Helper to fetch items with a specific offset using cursor-based pagination."""
        items: List[Dict] = []
        cursor: Optional[str] = None
        fetched = 0
        max_retries = 5
        retry_count = 0
        
        while fetched < limit and retry_count < max_retries:
            try:
                batch_limit = min(self._get_board_batch_size(board_id), limit - fetched)
                batch = self.monday_client.get_items_page(
                    board_id, column_ids, limit=batch_limit, cursor=cursor
                )
                
                if not batch['items']:
                    break
                
                items.extend(batch['items'])
                fetched += len(batch['items'])
                
                cursor = batch.get('next_cursor')
                if not cursor:
                    break
                
                await asyncio.sleep(0.5)  # Rate limiting
                retry_count = 0  # Reset retry count on success
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Error fetching batch from {board_id} (attempt {retry_count}/{max_retries}): {e}")
                
                if retry_count >= max_retries:
                    logger.error(f"Failed to fetch from board {board_id} after {max_retries} attempts")
                    break
                
                # Exponential backoff
                wait_time = 2 ** retry_count
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
        
        return items
    
    async def _process_projects_chunk(self, projects: List[Dict]) -> List[Dict]:
        """Process a chunk of projects data"""
        try:
            # For projects-only chunk, we can't resolve mirrors without subitems
            # So we'll process basic project data and resolve mirrors later
            processed = []
            
            for item in projects:
                try:
                    processed_item = self._normalize_monday_item(item)
                    # Add default values for mirror fields
                    processed_item.update({
                        'account': None,
                        'product_type': None,
                        'new_enquiry_value': 0
                    })
                    processed.append(processed_item)
                except Exception as e:
                    logger.warning(f"Failed to process project {item.get('id')}: {e}")
                    continue
            
            return self._transform_for_projects_table(processed)
            
        except Exception as e:
            logger.error(f"Error processing projects chunk: {e}")
            return []
    
    async def sync_single_item(
        self, 
        board_id: str, 
        item_id: str, 
        item_data: Dict
    ) -> Dict[str, Any]:
        """Sync a single item (used by webhook processing)"""
        try:
            # Transform based on board type
            if board_id == PARENT_BOARD_ID:
                transformed = self._transform_for_projects_table([item_data])
                if transformed:
                    result = self.supabase_client.upsert_projects(transformed)
                    return {"success": True, "updated": result['count'] if result['success'] else 0}
            
            elif board_id == SUBITEM_BOARD_ID:
                transformed = self._transform_for_subitems_table([item_data])
                if transformed:
                    result = self.supabase_client.upsert_subitems(transformed)
                    return {"success": True, "updated": result['count'] if result['success'] else 0}
            
            elif board_id == HIDDEN_ITEMS_BOARD_ID:
                transformed = self._transform_for_hidden_table([item_data])
                if transformed:
                    result = self.supabase_client.upsert_hidden_items(transformed)
                    return {"success": True, "updated": result['count'] if result['success'] else 0}
            
            return {"success": False, "error": f"Unknown board ID: {board_id}"}
            
        except Exception as e:
            logger.error(f"Error syncing single item {item_id}: {e}")
            return {"success": False, "error": str(e)}

    async def _extract_all_items_with_dedup(
        self, 
        board_id: str, 
        column_ids: List[str],
        existing_ids: set,
        max_items: int = None
    ) -> List[Dict]:
        """Extract items using set-based deduplication"""
        new_items = []
        cursor = self.supabase_client.get_last_cursor_checkpoint(board_id) or None
        total_fetched = 0
        consecutive_duplicates = 0
        
        logger.info(f"Extracting new items from board {board_id}")
        
        while len(new_items) < (max_items or float('inf')):
            try:
                cursor_preview = 'start' if not cursor else f"{cursor[:8]}...{cursor[-8:]}"
                logger.info(f"Fetching page (cursor: {cursor_preview})")
                # Use items_page only for the first page; then switch to next_items_page
                if cursor:
                    batch = self.monday_client.get_next_items_page(
                        cursor=cursor,
                        column_ids=column_ids,
                        limit=self._get_board_batch_size(board_id),
                    )
                else:
                    batch = self.monday_client.get_items_page(
                        board_id, column_ids, 
                        limit=self._get_board_batch_size(board_id),
                        cursor=None
                    )
                
                if not batch['items']:
                    logger.info("No more items to fetch")
                    break
                
                # Filter using set membership (O(1) per item)
                added_this_batch = 0
                for item in batch['items']:
                    item_id = str(item.get('id', ''))
                    if item_id and item_id not in existing_ids:
                        new_items.append(item)
                        added_this_batch += 1
                        if len(new_items) >= (max_items or float('inf')):
                            break
                
                total_fetched += len(batch['items'])
                logger.info(f"Fetched batch: {len(batch['items'])} items, {added_this_batch} are new (total new: {len(new_items)})")
                
                # Early-stop if too many duplicate-only batches
                if added_this_batch == 0:
                    consecutive_duplicates += 1
                    if consecutive_duplicates >= self.max_duplicate_chunks:
                        logger.info(f"No new items in {self.max_duplicate_chunks} consecutive chunks - stopping early")
                        break
                else:
                    consecutive_duplicates = 0
                
                cursor = batch.get('next_cursor')
                if not cursor:
                    break

                # Persist cursor for this board
                self.supabase_client.save_cursor_checkpoint_for_board(board_id, cursor)
                
                # Rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                if "CursorExpiredError" in str(e):
                    # Save last-good cursor and reseed
                    try:
                        self.supabase_client.save_cursor_checkpoint_for_board(board_id, cursor)
                    except Exception:
                        pass
                    logger.warning(f"Cursor expired for board {board_id} during dedup extraction - resetting cursor and continuing")
                    cursor = None
                    await asyncio.sleep(1)
                    continue
                logger.error(f"Error extracting batch: {e}")
                break
        
        logger.info(f"Extraction complete: {total_fetched} total fetched, {len(new_items)} are new")
        return new_items

    def _get_board_batch_size(self, board_id: str) -> int:
        """Get appropriate batch size based on board complexity"""
        board_batch_sizes = {
            PARENT_BOARD_ID: 100,      # Main board - complex queries
            SUBITEM_BOARD_ID: 100,     # Reduce to avoid rate limits
            HIDDEN_ITEMS_BOARD_ID: 100 # Reduce to avoid rate limits
        }
        return board_batch_sizes.get(board_id, 100)