import logging
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from datetime import datetime, timedelta

# Use relative imports for consistency
try:
    from ..database.supabase_client import SupabaseClient
    from .data_processor import HierarchicalSegmentation
except ImportError:
    # Fallback for standalone execution
    from src.database.supabase_client import SupabaseClient
    from src.core.data_processor import HierarchicalSegmentation

logger = logging.getLogger(__name__)


class SupabaseDataExtractor:
    """Data extraction from Supabase instead of Monday API"""
    
    def __init__(self):
        self.supabase = SupabaseClient()
        self.segmentation = HierarchicalSegmentation()
    
    async def get_projects_dataframe(
        self, 
        filters: Dict = None,
        use_cache: bool = True,
        days_back: int = None  # Add this parameter
    ) -> pd.DataFrame:
        """Get projects data from Supabase as DataFrame"""
        
        # Query Supabase
        query = self.supabase.client.table('projects').select('*')
        
        if filters:
            for key, value in filters.items():
                if value is not None:
                    query = query.eq(key, value)
        
        # Only apply date filter if specified
        if days_back:
            cutoff_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            query = query.gte('date_created', cutoff_date)
        
        result = query.execute()
        
        # Convert to DataFrame
        df = pd.DataFrame(result.data)
        
        # Ensure new_enquiry_value column exists (it should be in the schema)
        if 'new_enquiry_value' not in df.columns:
            logger.warning("new_enquiry_value column not found, adding with default values")
            df['new_enquiry_value'] = 0.0
        else:
            # Fill NaN values with 0
            df['new_enquiry_value'] = df['new_enquiry_value'].fillna(0.0)
        
        # Add value bands
        df = self.segmentation.create_value_bands(df)
        
        logger.info(f"Retrieved {len(df)} projects from Supabase")
        return df
    
    async def get_enhanced_project_context(
        self, 
        project_id: str
    ) -> Dict[str, Any]:
        """Get enhanced context for a specific project"""
        
        # Get project details with related data
        project = self.supabase.client.table('projects')\
            .select('*, subitems(*)')\
            .eq('monday_id', project_id)\
            .single()\
            .execute()
        
        if not project.data:
            raise ValueError(f"Project {project_id} not found")
        
        # Get account performance
        account_stats = self.supabase.client.rpc(
            'get_account_performance',
            {'account_name': project.data['account']}
        ).execute()
        
        # Get similar projects
        similar = self.supabase.client.table('projects')\
            .select('*')\
            .eq('category', project.data['category'])\
            .eq('type', project.data['type'])\
            .neq('monday_id', project_id)\
            .limit(100)\
            .execute()
        
        return {
            'project': project.data,
            'account_stats': account_stats.data,
            'similar_projects': similar.data
        }