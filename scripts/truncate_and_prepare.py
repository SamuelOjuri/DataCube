# scripts/truncate_and_prepare.py

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient

async def truncate_all_data():
    """Truncate all data from Supabase tables"""
    client = SupabaseClient()
    
    print("=" * 60)
    print("TRUNCATE AND PREPARE SUPABASE")
    print("=" * 60)
    
    # Get current counts
    projects_count = client.client.table('projects').select('*', count='exact', head=True).execute().count
    subitems_count = client.client.table('subitems').select('*', count='exact', head=True).execute().count
    hidden_count = client.client.table('hidden_items').select('*', count='exact', head=True).execute().count
    
    print(f"\nCurrent data:")
    print(f"  Projects: {projects_count}")
    print(f"  Subitems: {subitems_count}")
    print(f"  Hidden Items: {hidden_count}")
    
    # Confirm truncation
    confirm = input("\n⚠️  This will DELETE ALL DATA. Continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Aborted.")
        return
    
    try:
        # Truncate tables in order (respecting foreign keys)
        print("\n1. Truncating analysis_results...")
        client.client.table('analysis_results').delete().gte('id', '00000000-0000-0000-0000-000000000000').execute()
        
        print("2. Truncating subitems...")
        client.client.table('subitems').delete().gte('id', '00000000-0000-0000-0000-000000000000').execute()
        
        print("3. Truncating projects...")
        client.client.table('projects').delete().gte('id', '00000000-0000-0000-0000-000000000000').execute()
        
        print("4. Truncating hidden_items...")
        client.client.table('hidden_items').delete().gte('id', '00000000-0000-0000-0000-000000000000').execute()
        
        print("5. Truncating webhook_events...")
        client.client.table('webhook_events').delete().gte('id', '00000000-0000-0000-0000-000000000000').execute()
        
        print("6. Resetting sync_log...")
        # Keep sync log but mark as reset
        client.client.table('sync_log').insert({
            'sync_type': 'reset',
            'board_id': 'all',
            'board_name': 'Full Reset',
            'status': 'completed',
            'started_at': datetime.now().isoformat(),
            'completed_at': datetime.now().isoformat(),
            'metadata': {'action': 'truncate_all', 'timestamp': datetime.now().isoformat()}
        }).execute()
        
        print("\n✅ All tables truncated successfully!")
        
        # Verify
        projects_count = client.client.table('projects').select('*', count='exact', head=True).execute().count
        subitems_count = client.client.table('subitems').select('*', count='exact', head=True).execute().count
        hidden_count = client.client.table('hidden_items').select('*', count='exact', head=True).execute().count
        
        print(f"\nVerification:")
        print(f"  Projects: {projects_count} (should be 0)")
        print(f"  Subitems: {subitems_count} (should be 0)")
        print(f"  Hidden Items: {hidden_count} (should be 0)")
        
    except Exception as e:
        print(f"❌ Error during truncation: {e}")
        return

if __name__ == "__main__":
    asyncio.run(truncate_all_data())
