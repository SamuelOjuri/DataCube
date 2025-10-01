import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.sync_service import DataSyncService
from src.database.supabase_client import SupabaseClient
from src.core.data_extractor_supabase import SupabaseDataExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_full_pipeline():
    """Test the complete Supabase pipeline"""
    
    print("=" * 50)
    print("TESTING SUPABASE INTEGRATION")
    print("=" * 50)
    
    # Step 1: Test connection
    print("\n1. Testing Supabase connection...")
    supabase = SupabaseClient()
    result = supabase.client.table('projects').select('count').execute()
    print(f"   ✓ Connected. Current projects: {result.data}")
    
    # Step 2: Test sync service
    print("\n2. Testing sync service...")
    sync = DataSyncService()
    
    # Test delta sync (small test)
    delta_result = await sync.perform_delta_sync(hours_back=24)
    print(f"   ✓ Delta sync result: {delta_result}")
    
    # Step 3: Test data extraction
    print("\n3. Testing data extraction from Supabase...")
    extractor = SupabaseDataExtractor()
    df = await extractor.get_projects_dataframe()
    print(f"   ✓ Retrieved {len(df)} projects")
    print(f"   Columns: {df.columns.tolist()}")
    
    # Step 4: Test analysis pipeline (only if we have data)
    if len(df) > 0:
        print("\n4. Testing analysis with Supabase data...")
        sample = df.iloc[0]
        try:
            context = await extractor.get_enhanced_project_context(
                sample['monday_id']
            )
            print(f"   ✓ Enhanced context retrieved")
            print(f"   Account stats: {context['account_stats']}")
        except Exception as e:
            print(f"   ⚠ Enhanced context failed: {e}")
    else:
        print("\n4. Skipping analysis test - no projects in database")
        print("   💡 Try running a full sync first: python scripts/initial_sync.py")
    
    print("\n✅ All tests completed!")


if __name__ == "__main__":
    asyncio.run(test_full_pipeline())