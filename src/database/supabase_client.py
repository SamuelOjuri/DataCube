import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Supabase client for database operations"""
    
    def __init__(self):
        """Initialize Supabase client"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_KEY")  # Use service key for admin operations
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL and key must be set in environment variables")
        
        self.client: Client = create_client(self.url, self.key)
        logger.info("Supabase client initialized")
    
    def upsert_projects(self, projects: List[Dict]) -> Dict:
        """Upsert multiple projects"""
        try:
            # Get count before upsert
            count_before = self.client.table('projects')\
                .select('*', count='exact', head=True)\
                .execute().count
            
            # Perform upsert
            result = self.client.table('projects').upsert(
                projects,
                on_conflict='monday_id'
            ).execute()
            
            # Get count after upsert
            count_after = self.client.table('projects')\
                .select('*', count='exact', head=True)\
                .execute().count
            
            # Calculate actual new inserts
            new_inserts = count_after - count_before
            
            return {
                "success": True, 
                "count": new_inserts,  # Only count NEW records
                "total_affected": len(result.data),  # Total records touched
                "updates": len(result.data) - new_inserts  # Existing records updated
            }
        except Exception as e:
            logger.error(f"Failed to upsert projects: {e}")
            return {"success": False, "error": str(e)}
    
    def upsert_subitems(self, subitems: List[Dict]) -> Dict:
        """Upsert multiple subitems"""
        try:
            result = self.client.table('subitems').upsert(
                subitems,
                on_conflict='monday_id'
            ).execute()
            return {"success": True, "count": len(result.data)}
        except Exception as e:
            logger.error(f"Failed to upsert subitems: {e}")
            return {"success": False, "error": str(e)}
    
    def upsert_hidden_items(self, hidden_items: List[Dict]) -> Dict:
        """Upsert multiple hidden items"""
        try:
            result = self.client.table('hidden_items').upsert(
                hidden_items,
                on_conflict='monday_id'
            ).execute()
            return {"success": True, "count": len(result.data)}
        except Exception as e:
            logger.error(f"Failed to upsert hidden items: {e}")
            return {"success": False, "error": str(e)}
    
    def get_last_sync_time(self, board_id: str) -> Optional[datetime]:
        """Get last successful sync time for a board"""
        try:
            result = self.client.table('sync_log')\
                .select('completed_at')\
                .eq('board_id', board_id)\
                .eq('status', 'completed')\
                .order('completed_at', desc=True)\
                .limit(1)\
                .execute()
            
            if result.data:
                return datetime.fromisoformat(result.data[0]['completed_at'])
            return None
        except Exception as e:
            logger.error(f"Failed to get last sync time: {e}")
            return None
    
    def log_sync_operation(
        self, 
        sync_type: str, 
        board_id: str, 
        board_name: str = None
    ) -> str:
        """Create a sync log entry"""
        try:
            allowed = {'full', 'delta', 'webhook', 'reset'}
            stored_type = sync_type if sync_type in allowed else 'delta'
            payload = {
                'sync_type': stored_type,
                'board_id': board_id,
                'board_name': board_name,
                'started_at': datetime.now().isoformat()
            }
            # Preserve the original flow name if we had to remap it
            if stored_type != sync_type:
                payload['metadata'] = {'flow': sync_type}
            result = self.client.table('sync_log').insert(payload).execute()
            return result.data[0]['id']
        except Exception as e:
            logger.error(f"Failed to create sync log: {e}")
            return None
    
    def update_sync_log(
        self, 
        log_id: str, 
        status: str, 
        stats: Dict = None, 
        error: str = None
    ):
        """Update sync log with results"""
        try:
            update_data = {
                'status': status,
                'completed_at': datetime.now().isoformat()
            }
            if stats:
                # Merge metadata instead of replacing blindly
                existing = self.client.table('sync_log')\
                    .select('metadata')\
                    .eq('id', log_id)\
                    .execute()
                current_meta = (existing.data[0].get('metadata') if existing.data else {}) or {}
                merged = {**current_meta, **stats}
                update_data.update({'metadata': merged})
            if error:
                update_data['error_message'] = error
            self.client.table('sync_log')\
                .update(update_data)\
                .eq('id', log_id)\
                .execute()
        except Exception as e:
            logger.error(f"Failed to update sync log: {e}")

    def _merge_log_metadata(self, log_id: str, patch: Dict):
        """Safely merge a patch into sync_log.metadata for a specific log_id."""
        try:
            res = self.client.table('sync_log')\
                .select('metadata')\
                .eq('id', log_id)\
                .limit(1)\
                .execute()
            current = (res.data[0].get('metadata') if res.data else {}) or {}
            current.update(patch or {})
            self.client.table('sync_log')\
                .update({'metadata': current})\
                .eq('id', log_id)\
                .execute()
        except Exception as e:
            logger.error(f"Failed to merge metadata for log {log_id}: {e}")

    def save_cursor_checkpoint(self, log_id: str, cursor: Optional[str], ring_max: int = 10):
        """Record last_good_cursor and maintain a small ring of recent cursors on this log."""
        try:
            res = self.client.table('sync_log')\
                .select('metadata')\
                .eq('id', log_id)\
                .limit(1)\
                .execute()
            meta = (res.data[0].get('metadata') if res.data else {}) or {}
            ring = meta.get('cursor_ring') or []
            if cursor and (len(ring) == 0 or ring[-1] != cursor):
                ring.append(cursor)
                if len(ring) > ring_max:
                    ring = ring[-ring_max:]
            meta.update({
                'last_cursor': cursor,
                'last_good_cursor': cursor,
                'last_good_at': datetime.now().isoformat(),
                'cursor_ring': ring
            })
            self.client.table('sync_log')\
                .update({'metadata': meta})\
                .eq('id', log_id)\
                .execute()
        except Exception as e:
            logger.error(f"Failed to save cursor checkpoint for log {log_id}: {e}")

    def get_last_good_cursor(self, board_id: str, board_name: Optional[str] = None) -> Optional[str]:
        """Return last_good_cursor from the most recent log for a board/flow."""
        try:
            q = self.client.table('sync_log')\
                .select('metadata, started_at')\
                .eq('board_id', board_id)\
                .order('started_at', desc=True)\
                .limit(1)
            if board_name:
                q = q.eq('board_name', board_name)
            res = q.execute()
            if res.data and res.data[0].get('metadata'):
                return res.data[0]['metadata'].get('last_good_cursor') or res.data[0]['metadata'].get('last_cursor')
            return None
        except Exception as e:
            logger.error(f"Failed to read last_good_cursor: {e}")
            return None

    def get_cursor_ring_for_log(self, log_id: str) -> List[str]:
        """Return recent cursor ring for a specific log."""
        try:
            res = self.client.table('sync_log')\
                .select('metadata')\
                .eq('id', log_id)\
                .limit(1)\
                .execute()
            meta = (res.data[0].get('metadata') if res.data else {}) or {}
            return meta.get('cursor_ring') or []
        except Exception as e:
            logger.error(f"Failed to read cursor_ring for log {log_id}: {e}")
            return []

    def get_last_cursor_checkpoint(self, board_id: str, board_name: Optional[str] = None) -> Optional[str]:
        """Return the last saved pagination cursor for a board."""
        try:
            q = self.client.table('sync_log')\
                .select('metadata, started_at, completed_at, status, board_name')\
                .eq('board_id', board_id)\
                .order('started_at', desc=True)\
                .limit(1)
            if board_name:
                q = q.eq('board_name', board_name)
            result = q.execute()
            if result.data and result.data[0].get('metadata'):
                return result.data[0]['metadata'].get('last_cursor')
            return None
        except Exception as e:
            logger.error(f"Failed to get last cursor checkpoint: {e}")
            return None

    def save_cursor_checkpoint_for_board(self, board_id: str, cursor: Optional[str], board_name: Optional[str] = None):
        """Persist the latest pagination cursor into the most recent running sync_log for a board."""
        try:
            q = self.client.table('sync_log')\
                .select('id, started_at, status, board_name')\
                .eq('board_id', board_id)\
                .order('started_at', desc=True)\
                .limit(1)
            if board_name:
                q = q.eq('board_name', board_name)
            res = q.execute()
            if res.data:
                log_id = res.data[0]['id']
                self.client.table('sync_log')\
                    .update({'metadata': {'last_cursor': cursor}})\
                    .eq('id', log_id)\
                    .execute()
        except Exception as e:
            logger.error(f"Failed to save cursor checkpoint for board {board_id}: {e}")
    
    def get_projects_for_analysis(
        self, 
        filters: Dict = None, 
        limit: int = 100
    ) -> List[Dict]:
        """Get projects for analysis with optional filters"""
        try:
            query = self.client.table('projects').select('*')
            
            if filters:
                for key, value in filters.items():
                    if value is not None:
                        query = query.eq(key, value)
            
            result = query.limit(limit).execute()
            return result.data
        except Exception as e:
            logger.error(f"Failed to get projects: {e}")
            return []
    
    def store_analysis_result(
        self, 
        project_id: str, 
        analysis_result: Dict
    ):
        """Store LLM analysis results"""
        try:
            self.client.table('analysis_results').upsert(
                {'project_id': project_id, **analysis_result, 'analysis_timestamp': datetime.now().isoformat()},
                on_conflict='project_id'
            ).execute()
        except Exception as e:
            logger.error(f"Failed to store analysis result: {e}")

    
    def get_latest_analysis_result(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the most recent analysis_results row for a project_id.

        Returns numeric fields normalised as ints/floats and keeps the JSON/text
        blobs (reasoning, confidence_notes, etc.) as-is.
        """
        if not project_id:
            return None

        try:
            response = (
                self.client.table("analysis_results")
                .select("*")
                .eq("project_id", str(project_id))
                .order("analysis_timestamp", desc=True)  # newest first
                .limit(1)
                .execute()
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load analysis result for %s: %s", project_id, exc)
            return None

        row = (response.data or [None])[0]
        if not row:
            return None

        def _to_int(value: Any) -> Optional[int]:
            try:
                return int(value)
            except (TypeError, ValueError):
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return None

        def _to_float(value: Any) -> Optional[float]:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        result = {
            "project_id": row.get("project_id"),
            "analysis_timestamp": row.get("analysis_timestamp"),
            "expected_gestation_days": _to_int(row.get("expected_gestation_days")),
            "gestation_confidence": _to_float(row.get("gestation_confidence")),
            "expected_conversion_rate": _to_float(row.get("expected_conversion_rate")),
            "conversion_confidence": _to_float(row.get("conversion_confidence")),
            "rating_score": _to_int(row.get("rating_score")),
            "reasoning": row.get("reasoning"),
            "adjustments": row.get("adjustments"),
            "confidence_notes": row.get("confidence_notes"),
            "special_factors": row.get("special_factors"),
            "llm_model": row.get("llm_model"),
            "analysis_version": row.get("analysis_version"),
            "processing_time_ms": _to_int(row.get("processing_time_ms")),
        }
        return result


    def get_existing_project_ids(self) -> set:
        """Return set of all existing monday_id in projects table (paged)."""
        page_size = 1000
        start = 0
        ids = set()
        try:
            while True:
                result = self.client.table('projects')\
                    .select('monday_id')\
                    .range(start, start + page_size - 1)\
                    .execute()
                
                rows = result.data or []
                for row in rows:
                    mid = row.get('monday_id')
                    if mid is not None:
                        ids.add(str(mid))
                
                if len(rows) < page_size:
                    break
                start += page_size
            return ids
        except Exception as e:
            logger.error(f"Failed to fetch existing project IDs: {e}")
            return ids

    def get_last_chunk_checkpoint(self, board_id: str) -> Optional[int]:
        """Return last processed offset from the most recent completed chunked sync."""
        try:
            result = self.client.table('sync_log')\
                .select('metadata, completed_at, board_name, sync_type, status')\
                .eq('board_id', board_id)\
                .eq('board_name', 'Chunked Projects Sync')\
                .eq('status', 'completed')\
                .order('completed_at', desc=True)\
                .limit(1)\
                .execute()
            if result.data and result.data[0].get('metadata'):
                return result.data[0]['metadata'].get('last_offset')
            return None
        except Exception as e:
            logger.error(f"Failed to get last chunk checkpoint: {e}")
            return None

    def get_last_cursor_checkpoint(self, board_id: str, board_name: Optional[str] = None) -> Optional[str]:
        """Return last saved pagination cursor from the most recent sync log for a board."""
        try:
            q = self.client.table('sync_log')\
                .select('metadata, started_at, completed_at, status, board_name')\
                .eq('board_id', board_id)\
                .order('started_at', desc=True)\
                .limit(1)
            if board_name:
                q = q.eq('board_name', board_name)
            result = q.execute()
            if result.data and result.data[0].get('metadata'):
                return result.data[0]['metadata'].get('last_cursor')
            return None
        except Exception as e:
            logger.error(f"Failed to get last cursor checkpoint: {e}")
            return None

    def save_cursor_checkpoint(self, log_id: str, cursor: Optional[str], extra: Dict = None):
        """Persist the latest pagination cursor (and optional progress) into sync_log.metadata."""
        try:
            metadata = {'last_cursor': cursor}
            if extra:
                metadata.update(extra)
            self.client.table('sync_log')\
                .update({'metadata': metadata})\
                .eq('id', log_id)\
                .execute()
        except Exception as e:
            logger.error(f"Failed to save cursor checkpoint: {e}")