# scripts/monitor_sync_progress.py

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.core.monday_client import MondayClient

async def monitor_progress():
    """Monitor synchronization progress"""
    
    supabase = SupabaseClient()
    monday = MondayClient()
    
    print("=" * 70)
    print("SYNC PROGRESS MONITOR")
    print("=" * 70)
    
    # Get Monday counts
    print("\n📊 Source (Monday.com):")
    parent_info = monday.get_board_info('1825117125')
    subitem_info = monday.get_board_info('1825117144')
    hidden_info = monday.get_board_info('1825138260')
    
    print(f"  Projects: {parent_info['items_count']:,}")
    print(f"  Subitems: {subitem_info['items_count']:,}")
    print(f"  Hidden Items: {hidden_info['items_count']:,}")
    total_monday = parent_info['items_count'] + subitem_info['items_count'] + hidden_info['items_count']
    print(f"  TOTAL: {total_monday:,}")
    
    # Get Supabase counts
    print("\n📊 Destination (Supabase):")
    projects = supabase.client.table('projects').select('*', count='exact', head=True).execute().count
    subitems = supabase.client.table('subitems').select('*', count='exact', head=True).execute().count
    hidden = supabase.client.table('hidden_items').select('*', count='exact', head=True).execute().count
    
    def pct(n, d): return (n / d * 100.0) if d else 0.0

    proj_pct = pct(projects, parent_info['items_count'])
    sub_pct = pct(subitems, subitem_info['items_count'])
    hid_pct  = pct(hidden, hidden_info['items_count'])

    print(f"  Projects: {projects:,} ({proj_pct:.1f}%)")
    print(f"  Subitems: {subitems:,} ({sub_pct:.1f}%)")
    print(f"  Hidden Items: {hidden:,} ({hid_pct:.1f}%)")
    total_supabase = projects + subitems + hidden
    print(f"  TOTAL: {total_supabase:,} ({total_supabase/total_monday*100:.1f}%)")
    
    # Get recent sync logs
    print("\n📊 Recent Sync Operations:")
    recent_syncs = supabase.client.table('sync_log')\
        .select('*')\
        .gte('started_at', (datetime.now() - timedelta(hours=24)).isoformat())\
        .order('started_at', desc=True)\
        .limit(10)\
        .execute()
    
    if recent_syncs.data:
        for sync in recent_syncs.data[:5]:
            status_icon = "✅" if sync['status'] == 'completed' else "❌" if sync['status'] == 'failed' else "⏳"
            print(f"  {status_icon} {sync['sync_type']:8} | {sync['board_name']:30} | "
                  f"Items: {sync.get('items_processed', 0):5} | "
                  f"{sync['status']:10} | {sync['started_at'][:19]}")
    else:
        print("  No recent syncs found")
    
    # Calculate what's missing
    print("\n📊 Remaining to Sync:")
    print(f"  Projects: {parent_info['items_count'] - projects:,}")
    print(f"  Subitems: {subitem_info['items_count'] - subitems:,}")
    print(f"  Hidden Items: {hidden_info['items_count'] - hidden:,}")
    print(f"  TOTAL: {total_monday - total_supabase:,}")
    
    # Estimate time
    if projects > 0:
        # Get average sync rate from recent syncs
        completed_syncs = [s for s in (recent_syncs.data or []) if s['status'] == 'completed' and s.get('items_processed')]
        if completed_syncs:
            total_items = sum(s['items_processed'] for s in completed_syncs)
            total_time = sum(
                (datetime.fromisoformat(s['completed_at']) - datetime.fromisoformat(s['started_at'])).total_seconds()
                for s in completed_syncs if s.get('completed_at')
            )
            if total_time > 0:
                rate = total_items / total_time
                remaining_time = (total_monday - total_supabase) / rate
                print(f"\n⏱️  Estimated time to complete: {remaining_time/3600:.1f} hours at {rate:.1f} items/sec")

if __name__ == "__main__":
    asyncio.run(monitor_progress())
