# scripts/intelligent_initial_sync.py

import asyncio
import logging
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
from collections import deque

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.core.monday_client import MondayClient
from src.database.sync_service import DataSyncService
from src.config import PARENT_BOARD_ID, SUBITEM_BOARD_ID, HIDDEN_ITEMS_BOARD_ID, PARENT_COLUMNS, SUBITEM_COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntelligentSyncManager:
    """Manages intelligent chunked synchronization with resume capability"""
    
    def __init__(self):
        self.supabase = SupabaseClient()
        self.monday = MondayClient()
        self.sync_service = DataSyncService()
        
        # Persist pagination cursor across sessions
        self.cursor: Optional[str] = None
        
        # Sync parameters
        self.CHUNK_SIZE = 100  # avoid rate limit by using 100
        self.BATCH_SIZE = 1000  # Items per batch before pause
        self.SESSION_SIZE = 15000  # Max items per session
        self.RATE_LIMIT_DELAY = 0.5
        self.BATCH_DELAY = 1.5  # Seconds between batches
        self.MAX_DUPLICATE_CHUNKS = 30  # set a reasonable threshold
        self.SCAN_RATE_DELAY = 0.2  # add
        
    async def get_sync_status(self) -> Dict:
        """Get current synchronization status"""
        status = {}
        
        # Get Monday.com counts
        logger.info("Getting board information from Monday...")
        parent_info = self.monday.get_board_info(PARENT_BOARD_ID)
        subitem_info = self.monday.get_board_info(SUBITEM_BOARD_ID)
        hidden_info = self.monday.get_board_info(HIDDEN_ITEMS_BOARD_ID)
        
        status['monday'] = {
            'projects': parent_info['items_count'],
            'subitems': subitem_info['items_count'],
            'hidden_items': hidden_info['items_count']
        }
        
        # Get Supabase counts
        logger.info("Getting current counts from Supabase...")
        projects_count = self.supabase.client.table('projects').select('*', count='exact', head=True).execute().count
        subitems_count = self.supabase.client.table('subitems').select('*', count='exact', head=True).execute().count
        hidden_count = self.supabase.client.table('hidden_items').select('*', count='exact', head=True).execute().count
        
        status['supabase'] = {
            'projects': projects_count,
            'subitems': subitems_count,
            'hidden_items': hidden_count
        }
        
        # Calculate remaining
        status['remaining'] = {
            'projects': status['monday']['projects'] - status['supabase']['projects'],
            'subitems': status['monday']['subitems'] - status['supabase']['subitems'],
            'hidden_items': status['monday']['hidden_items'] - status['supabase']['hidden_items']
        }
        
        return status

    async def _fast_forward_to_known(self, known_ids: List[str], max_hops: int = 500) -> Optional[str]:
        if not known_ids:
            return None
        ff_cursor = None
        hops = 0
        while hops < max_hops:
            page = (self.monday.get_next_item_ids_page(ff_cursor, self.CHUNK_SIZE)
                    if ff_cursor else
                    self.monday.get_item_ids_page(PARENT_BOARD_ID, self.CHUNK_SIZE, None))
            items = page.get('items') or []
            if not items:
                return None
            ids_on_page = {str(it.get('id')) for it in items}
            if any(k in ids_on_page for k in known_ids):
                return page.get('next_cursor')  # resume AFTER the matched page
            ff_cursor = page.get('next_cursor')
            if not ff_cursor:
                return None
            hops += 1
        return None

    async def _scan_ahead_for_new(self, existing_ids: Set[str], max_hops: int = 2000) -> Optional[Dict]:
        cursor = None
        hops = 0
        while hops < max_hops:
            try:
                page = (self.monday.get_next_item_ids_page(cursor, self.CHUNK_SIZE)
                        if cursor else
                        self.monday.get_item_ids_page(PARENT_BOARD_ID, self.CHUNK_SIZE, None))
                items = page.get('items') or []
                if not items:
                    return None
                # Found any truly new item?
                if any(str(it.get('id')) not in existing_ids for it in items):
                    return page  # return this page to process immediately
                cursor = page.get('next_cursor')
                if not cursor:
                    return None
                # Persist cursor while scanning
                try:
                    self.supabase.save_cursor_checkpoint(
                        self._current_sync_log_id,
                        cursor,
                        {'scan_hops': hops}
                    )
                except Exception:
                    pass
                hops += 1
                await asyncio.sleep(self.SCAN_RATE_DELAY)
            except Exception as e:
                msg = str(e).lower()
                if "cursorexpirederror" in msg:
                    # reseed a fresh chain and continue scanning
                    seed = self.monday.get_item_ids_page(
                        board_id=PARENT_BOARD_ID,
                        limit=1,
                        cursor=None
                    )
                    cursor = seed.get('next_cursor')
                    await asyncio.sleep(1)
                    continue
                if "internal server error" in msg or "internal server" in msg:
                    await asyncio.sleep(5)
                    continue
                if "rate limit" in msg:
                    await asyncio.sleep(60)
                    continue
                if "504" in msg or "timeout" in msg:
                    await asyncio.sleep(30)
                    continue
                # unknown error: stop scanning but don't crash the session
                return None
        return None
    
    async def sync_projects_intelligently(self, max_items: Optional[int] = None) -> Dict:
        """Sync projects with intelligent chunking and resume"""
        
        # Get existing project IDs to avoid duplicates
        existing_ids = self.supabase.get_existing_project_ids()
        logger.info(f"Found {len(existing_ids)} existing projects in Supabase")
        
        # Create sync log
        sync_log_id = self.supabase.log_sync_operation(
            'full', PARENT_BOARD_ID, 'Intelligent Projects Sync'
        )
        
        # Resume from last saved cursor if available and not already set
        if not self.cursor:
            # Prefer last_good_cursor if present (falls back to last_cursor)
            saved_cursor = self.supabase.get_last_good_cursor(PARENT_BOARD_ID, 'Intelligent Projects Sync')
            if not saved_cursor:
                saved_cursor = self.supabase.get_last_cursor_checkpoint(PARENT_BOARD_ID, 'Intelligent Projects Sync')
            if saved_cursor:
                logger.info("Resuming from saved cursor checkpoint")
                self.cursor = saved_cursor

        stats = {
            'total_processed': 0,
            'new_items': 0,
            'updated_items': 0,
            'chunks_completed': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        try:
            # Initialize state before the while loop
            last_seen_ring = deque(maxlen=10)
            preloaded_result: Optional[Dict] = None
            self._current_sync_log_id = sync_log_id  # for scanner persistence
            
            last_seen_id: Optional[str] = None
            items_this_session = 0
            max_for_session = max_items or self.SESSION_SIZE
            
            while items_this_session < max_for_session:
                # Check if we need a batch pause
                if stats['total_processed'] > 0 and stats['total_processed'] % self.BATCH_SIZE == 0:
                    logger.info(f"Batch pause after {stats['total_processed']} items...")
                    await asyncio.sleep(self.BATCH_DELAY)
                
                # Get next chunk (persisted cursor across sessions)
                cursor_preview = 'start' if not self.cursor else f"{self.cursor[:8]}...{self.cursor[-8:]}"
                logger.info(f"Fetching chunk (cursor: {cursor_preview})")
                
                try:
                    if preloaded_result is not None:
                        page = preloaded_result
                        preloaded_result = None
                    else:
                        if self.cursor:
                            # Subsequent batches: next_items_page (IDs only)
                            page = self.monday.get_next_item_ids_page(
                                self.cursor,
                                self.CHUNK_SIZE
                            )
                        else:
                            # Initial batch: items_page (IDs only)
                            page = self.monday.get_item_ids_page(
                                board_id=PARENT_BOARD_ID,
                                limit=self.CHUNK_SIZE,
                                cursor=None
                            )
                except Exception as e:
                    msg = str(e).lower()
                    if "504" in msg or "timeout" in msg:
                        logger.warning("API timeout - waiting 30 seconds before retry...")
                        await asyncio.sleep(30)
                        continue
                    if "rate limit" in msg:
                        logger.warning("Rate limit hit - waiting 60 seconds...")
                        await asyncio.sleep(60)
                        continue
                    if "internal server error" in msg:
                        logger.warning("Internal error - retrying in 5 seconds...")
                        await asyncio.sleep(5)
                        continue
                    if "cursorexpirederror" in msg:
                        logger.warning("Cursor expired - reseeding and continuing with next_items_page")
                        try:
                            self.supabase.save_cursor_checkpoint(sync_log_id, self.cursor, {
                                'processed_so_far': stats['total_processed']
                            })
                        except Exception:
                            pass

                        # get a fresh cursor via items_page(limit=1)
                        seed = self.monday.get_item_ids_page(
                            board_id=PARENT_BOARD_ID,
                            limit=1,
                            cursor=None
                        )
                        self.cursor = seed.get('next_cursor')  # use this with next_items_page

                        # optional: fast-forward near previous position using last_seen_ring
                        if last_seen_ring:
                            try:
                                ff = await self._fast_forward_to_known(list(last_seen_ring), max_hops=500)
                                if ff:
                                    self.cursor = ff
                            except Exception:
                                pass

                        await asyncio.sleep(1)
                        continue
                    raise

                if not page['items']:
                    logger.info("No more items to sync")
                    break

                # IDs on this page
                ids_in_page = [str(it.get('id')) for it in page['items'] if it.get('id') is not None]

                # Filter out existing items
                new_ids = [mid for mid in ids_in_page if mid not in existing_ids]

                # Track consecutive duplicate-only chunks
                if not new_ids:
                    consecutive_duplicates = stats.get('consecutive_duplicates', 0) + 1
                    stats['consecutive_duplicates'] = consecutive_duplicates
                    if consecutive_duplicates >= self.MAX_DUPLICATE_CHUNKS:
                        logger.info("Duplicate streak - scanning ahead for unsynced items")
                        scanned_page = await self._scan_ahead_for_new(existing_ids, max_hops=2000)
                        if scanned_page:
                            preloaded_result = scanned_page
                            self.cursor = scanned_page.get('next_cursor')
                            stats['consecutive_duplicates'] = 0
                            await asyncio.sleep(self.SCAN_RATE_DELAY)
                            continue
                        logger.info("Scan-ahead found no new pages; ending session")
                        break
                else:
                    stats['consecutive_duplicates'] = 0

                # Fetch and normalize details for only the new IDs
                detailed_items = []
                normalized_subitems = []  # safety default

                if new_ids:
                    raw_items = self.monday.get_items_by_ids(new_ids, list(PARENT_COLUMNS.values()))
                    detailed_items = [self.sync_service._normalize_monday_item(it) for it in raw_items]

                    # Fetch related subitems and normalize for mirror resolution
                    raw_subitems = self.monday.get_subitems_for_parents(new_ids, list(SUBITEM_COLUMNS.values()))
                    normalized_subitems = [self.sync_service._normalize_monday_item(si) for si in raw_subitems]

                if detailed_items:
                    # Resolve mirrors using existing, tested resolver, then transform
                    processed = self.sync_service._process_and_resolve_mirrors(detailed_items, normalized_subitems, [])
                    transformed = self.sync_service._transform_for_projects_table(processed['projects'])
                    if transformed:
                        upload_result = self.supabase.upsert_projects(transformed)
                        if upload_result['success']:
                            stats['new_items'] += upload_result.get('count', 0)
                            stats['updated_items'] += upload_result.get('updates', 0)

                            # Add to existing IDs
                            for mid in new_ids:
                                existing_ids.add(mid)

                            logger.info(f"✅ Uploaded {len(detailed_items)} new items")
                        else:
                            stats['errors'] += 1
                            logger.error(f"Upload failed: {upload_result.get('error')}")
                else:
                    logger.info(f"All {len(ids_in_page)} items already exist")

                stats['total_processed'] += len(ids_in_page)
                stats['chunks_completed'] += 1
                items_this_session += len(ids_in_page)

                # Update cursor
                self.cursor = page.get('next_cursor')
                if not self.cursor:
                    logger.info("Reached end of data")
                    break

                # Track last_seen ids ring
                if ids_in_page:
                    last_seen_ring.append(ids_in_page[-1])

                # Persist both cursor and ring
                self.supabase.save_cursor_checkpoint(
                    sync_log_id,
                    self.cursor,
                    {'last_seen_ring': list(last_seen_ring), 'processed_so_far': stats['total_processed']}
                )

                # Rate limiting
                await asyncio.sleep(self.RATE_LIMIT_DELAY)

                # Progress report
                if stats['chunks_completed'] % 5 == 0:
                    elapsed = (datetime.now() - stats['start_time']).total_seconds()
                    rate = stats['total_processed'] / elapsed if elapsed > 0 else 0
                    logger.info(f"Progress: {stats['total_processed']} processed, "
                                f"{stats['new_items']} new, {rate:.1f} items/sec")

                first_id = ids_in_page[0] if ids_in_page else '-'
                last_id = ids_in_page[-1] if ids_in_page else '-'
                dup_chunks = stats.get('consecutive_duplicates', 0)
                logger.info(
                    f"Batch summary: total={len(ids_in_page)}, new={len(new_ids)}, "
                    f"dupes={len(ids_in_page) - len(new_ids)}, ids={first_id}..{last_id}, "
                    f"cursor={cursor_preview}, dup_chunks={dup_chunks}, total_processed={stats['total_processed']}"
                )
            # Final stats
            stats['success'] = True
            stats['duration'] = (datetime.now() - stats['start_time']).total_seconds()
            
            # Update sync log with JSON-serializable metadata
            stats_for_log = dict(stats)
            if isinstance(stats_for_log.get('start_time'), datetime):
                stats_for_log['start_time'] = stats_for_log['start_time'].isoformat()
            # Persist last cursor on completion
            stats_for_log['last_cursor'] = self.cursor
            self.supabase.update_sync_log(
                sync_log_id, 'completed', stats_for_log
            )
            return stats
            
        except Exception as e:
            stats['success'] = False
            stats['error'] = str(e)
            
            # Update sync log with JSON-serializable metadata
            stats_for_log = dict(stats)
            if isinstance(stats_for_log.get('start_time'), datetime):
                stats_for_log['start_time'] = stats_for_log['start_time'].isoformat()
            self.supabase.update_sync_log(
                sync_log_id, 'failed', stats_for_log, str(e)
            )
            
            logger.error(f"Sync failed: {e}")
            return stats
    
    async def run_full_sync(self):
        """Run a complete intelligent sync"""
        
        print("=" * 70)
        print("INTELLIGENT FULL SYNC")
        print("=" * 70)
        
        # Get initial status
        status = await self.get_sync_status()
        
        print("\n📊 Current Status:")
        print(f"  Monday.com:")
        print(f"    Projects: {status['monday']['projects']:,}")
        print(f"    Subitems: {status['monday']['subitems']:,}")
        print(f"    Hidden Items: {status['monday']['hidden_items']:,}")
        print(f"\n  Supabase:")
        print(f"    Projects: {status['supabase']['projects']:,}")
        print(f"    Subitems: {status['supabase']['subitems']:,}")
        print(f"    Hidden Items: {status['supabase']['hidden_items']:,}")
        print(f"\n  Remaining to sync:")
        print(f"    Projects: {status['remaining']['projects']:,}")
        print(f"    Subitems: {status['remaining']['subitems']:,}")
        print(f"    Hidden Items: {status['remaining']['hidden_items']:,}")
        # Phase 1: Sync Projects First (they're the parent)
        # If hidden or subitems aren't seeded, use tested full sync first
        if status['remaining']['hidden_items'] > 0 or status['remaining']['subitems'] > 0:
            print("\n📌 Seeding dependencies with tested full sync (hidden → subitems → projects)")
            result = await self.sync_service.perform_full_sync()
            if result.get("success"):
                print("  ✅ Dependency seed complete. Skipping intelligent session this run.")
            else:
                print(f"  ❌ Full sync failed: {result.get('error')}")
            return
        if status['remaining']['projects'] > 0:
            print(f"\n📌 Phase 1: Syncing Projects")
            print(f"  Will sync in sessions of {self.SESSION_SIZE} items")
            
            session_num = 1
            while status['remaining']['projects'] > 0:
                print(f"\n  Session {session_num}:")
                
                result = await self.sync_projects_intelligently(self.SESSION_SIZE)
                
                if result['success']:
                    print(f"    ✅ Processed: {result['total_processed']}")
                    print(f"    ✅ New items: {result['new_items']}")
                    print(f"    ✅ Duration: {result['duration']:.1f}s")
                    
                    # Update status
                    status = await self.get_sync_status()
                    
                    if status['remaining']['projects'] > 0:
                        print(f"    📊 Remaining: {status['remaining']['projects']:,}")
                        print(f"    ⏸️  Pausing 30 seconds before next session...")
                        await asyncio.sleep(30)
                else:
                    print(f"    ❌ Session failed: {result.get('error')}")
                    break
                
                session_num += 1
        
        print("\n✅ Sync completed!")
        
        # Final status
        final_status = await self.get_sync_status()
        print("\n📊 Final Status:")
        print(f"  Projects synced: {final_status['supabase']['projects']:,} / {final_status['monday']['projects']:,}")
        
        if final_status['remaining']['projects'] == 0:
            print("  ✅ All projects synced successfully!")
        else:
            print(f"  ⚠️  {final_status['remaining']['projects']:,} projects still need syncing")

async def main():
    """Main entry point"""
    manager = IntelligentSyncManager()
    await manager.run_full_sync()

if __name__ == "__main__":
    asyncio.run(main())
