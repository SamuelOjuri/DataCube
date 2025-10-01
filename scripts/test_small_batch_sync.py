# scripts/test_small_batch_sync.py

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.core.monday_client import MondayClient
from src.database.sync_service import DataSyncService
from src.core.enhanced_extractor import EnhancedMondayExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_comprehensive_sync():
    """Test syncing a small batch starting from 10 parent projects and fetching related subitems + hidden items"""
    
    print("=" * 70)
    print("COMPREHENSIVE 3-BOARD SYNC TEST (Parents -> Subitems -> Hidden Items)")
    print("=" * 70)
    
    # Test parameters
    TEST_PARENT_COUNT = 10         # number of parent projects to seed the test with
        
    try:
        # 1. Test Monday connection
        print("\n1. Testing Monday connection...")
        monday = MondayClient()
        if not monday.test_connection():
            print("❌ Monday connection failed")
            return
        print("✅ Monday connected")
        
        # 2. Test Supabase connection and get baseline counts
        print("\n2. Testing Supabase connection...")
        supabase = SupabaseClient()
        
        # Get current counts for all tables
        projects_before = supabase.client.table('projects').select('*', count='exact', head=True).execute().count
        subitems_before = supabase.client.table('subitems').select('*', count='exact', head=True).execute().count
        hidden_before = supabase.client.table('hidden_items').select('*', count='exact', head=True).execute().count
        
        print(f"✅ Supabase connected")
        print(f"   Current counts - Projects: {projects_before}, Subitems: {subitems_before}, Hidden: {hidden_before}")
        
        # Import board configs
        from src.config import (
            PARENT_BOARD_ID, PARENT_COLUMNS,
            SUBITEM_BOARD_ID, SUBITEM_COLUMNS,
            HIDDEN_ITEMS_BOARD_ID, HIDDEN_ITEMS_COLUMNS
        )
        
        sync_service = DataSyncService()
        
        # Initialize enhanced extractor
        enhanced_extractor = EnhancedMondayExtractor(monday)
        
        # 3. STEP 1: Extract 10 Parent Projects
        print(f"\n3. STEP 1: Extracting Parent Projects - Board ID: {PARENT_BOARD_ID}")
        projects_raw = enhanced_extractor.extract_all_board_items(
            board_id=PARENT_BOARD_ID,
            column_mapping=PARENT_COLUMNS,
            batch_size=50,
            max_items=TEST_PARENT_COUNT
        )
        if not projects_raw:
            print("❌ No projects retrieved from Monday")
            return
        print(f"✅ Retrieved {len(projects_raw)} parent projects")
        
        # Show sample
        sample_p = projects_raw[0]
        print("   Sample parent project:")
        print(f"     monday_id: {sample_p.get('monday_id')}")
        print(f"     project_name: {sample_p.get('project_name')}")
        print(f"     pipeline_stage: {sample_p.get('pipeline_stage')}")
        
        parent_ids = {p.get('monday_id') for p in projects_raw if p.get('monday_id')}
        print(f"   Parent monday_ids: {list(parent_ids)}")
        
        # 4. STEP 2: Extract Related Subitems (targeted by parent IDs)
        print(f"\n4. STEP 2: Extracting Related Subitems - Board ID: {SUBITEM_BOARD_ID}")
        raw_subitems = monday.get_subitems_for_parents(
            parent_ids=list(parent_ids),
            column_ids=list(SUBITEM_COLUMNS.values())
        )
        # Use the same extractor logic to parse values + parent reference
        subitems_extracted = [
            enhanced_extractor.column_extractor.extract_item_data(si, SUBITEM_COLUMNS)
            for si in raw_subitems
        ]
        print(f"✅ Found {len(subitems_extracted)} related subitems for the selected parents")
        
        if subitems_extracted[:1]:
            s = subitems_extracted[0]
            print("   Sample related subitem:")
            print(f"     monday_id: {s.get('monday_id')}")
            print(f"     name: {s.get('name')}")
            print(f"     parent_monday_id: {s.get('parent_monday_id')}")
            print(f"     account: {s.get('account')}, product_type: {s.get('product_type')}, quote_amount: {s.get('quote_amount')}")
        
        # 5. STEP 3: Extract Related Hidden Items - Board ID: 1825138260
        print(f"\n5. STEP 3: Extracting Related Hidden Items - Board ID: {HIDDEN_ITEMS_BOARD_ID}")
        
        # Linked hidden item IDs from subitems via connect_boards
        hidden_ids = {s.get('hidden_item_id') for s in subitems_extracted if s.get('hidden_item_id')}
        print(f"   Linked hidden_item_ids found: {list(hidden_ids)[:10]}{'...' if len(hidden_ids) > 10 else ''}")

        hidden_items_raw = []
        if hidden_ids:
            # Fetch hidden items by IDs directly
            hidden_items_raw = monday.get_items_by_ids(
                item_ids=list(hidden_ids),
                column_ids=list(HIDDEN_ITEMS_COLUMNS.values())
            )
            print(f"✅ Retrieved {len(hidden_items_raw)} hidden items via linked IDs")
        else:
            # Match hidden items by exact name using items_page_by_column_values (no scanning)
            subitem_names = sorted({s.get('name') for s in subitems_extracted if s.get('name')})
            print(f"   Looking up {len(subitem_names)} hidden items by name...")

            hidden_items_raw = []
            column_ids = list(HIDDEN_ITEMS_COLUMNS.values())
            query = """
            query GetByName($board_id: ID!, $name: String!, $column_ids: [String!], $limit: Int) {
              items_page_by_column_values(
                board_id: $board_id
                columns: [{ column_id: "name", column_values: [$name] }]
                limit: $limit
              ) {
                cursor
                items {
                  id
                  name
                  column_values(ids: $column_ids) {
                    id
                    text
                    value
                    type
                    ... on FormulaValue { display_value }
                    ... on MirrorValue { display_value }
                  }
                }
              }
            }
            """

            for name in subitem_names:
                vars = {
                    "board_id": HIDDEN_ITEMS_BOARD_ID,
                    "name": name,
                    "column_ids": column_ids,
                    "limit": 25
                }
                res = monday.execute_query(query, vars)
                page = (res.get("data", {}) or {}).get("items_page_by_column_values") or {}
                items = page.get("items") or []
                hidden_items_raw.extend(items)

            # De-duplicate by id
            seen = set()
            hidden_items_raw = [h for h in hidden_items_raw if not (h.get('id') in seen or seen.add(h.get('id')))]

            print(f"✅ Found {len(hidden_items_raw)} related hidden items by name")

                # Transform and upload hidden items FIRST
        if hidden_items_raw:
            # # Process through enhanced extractor first
            # hidden_items_extracted = [
            #     enhanced_extractor.column_extractor.extract_item_data(hi, HIDDEN_ITEMS_COLUMNS)
            #     for hi in hidden_items_raw
            # ]
            
            # # Filter out items with null monday_id BEFORE transformation
            # valid_hidden_items = [hi for hi in hidden_items_extracted if hi.get('monday_id')]
            # print(f"   Filtered {len(hidden_items_extracted)} → {len(valid_hidden_items)} valid hidden items")
            
            # if valid_hidden_items:
                # Transform through sync_service to populate cache
                # hidden_transformed = sync_service._transform_for_hidden_table(valid_hidden_items)
                hidden_transformed = sync_service._transform_for_hidden_table(hidden_items_raw)
                hidden_upload = supabase.upsert_hidden_items(hidden_transformed)
                print(f"✅ Hidden items upload: {hidden_upload}")
        else:
                print("⚠️ No valid hidden items to upload")

        # 6. Transform and Upload Projects and Subitems
        print(f"\n6. Transforming and Uploading Data...")
        
        # Transform and upload projects first
        if projects_raw:
            projects_transformed = sync_service._transform_for_projects_table(projects_raw)
            projects_upload = supabase.upsert_projects(projects_transformed)
            print(f"✅ Projects upload: {projects_upload}")
        
        # Transform and upload subitems (should now have populated cache for enrichment)
        subitems_transformed = sync_service._transform_for_subitems_table(subitems_extracted)
        if subitems_transformed:
            subitems_upload = supabase.upsert_subitems(subitems_transformed)
            print(f"✅ Subitems upload: {subitems_upload}")
        else:
            print("⚠️ No related subitems to upload")
        
        
        # 7. Final verification - count all tables
        print(f"\n7. Final Verification...")
        
        projects_after = supabase.client.table('projects').select('*', count='exact', head=True).execute().count
        subitems_after = supabase.client.table('subitems').select('*', count='exact', head=True).execute().count
        hidden_after = supabase.client.table('hidden_items').select('*', count='exact', head=True).execute().count
        
        print(f"✅ Final counts:")
        print(f"   Projects: {projects_after} (added {projects_after - projects_before})")
        print(f"   Subitems: {subitems_after} (added {subitems_after - subitems_before})")
        print(f"   Hidden Items: {hidden_after} (added {hidden_after - hidden_before})")
        
        # 8. Column Data Verification...
        print(f"\n8. Column Data Verification...")
        
        if projects_after > 0:
            sample_project = supabase.client.table('projects').select('*').limit(1).execute()
            if sample_project.data:
                project = sample_project.data[0]
                filled_columns = sum(1 for key, value in project.items() if value is not None and value != '')
                total_columns = len(project.keys())
                print(f"✅ Projects table: {filled_columns}/{total_columns} columns have data")
        
        if subitems_after > 0:
            sample_subitem = supabase.client.table('subitems').select('*').limit(1).execute()
            if sample_subitem.data:
                subitem = sample_subitem.data[0]
                filled_columns = sum(1 for key, value in subitem.items() if value is not None and value != '')
                total_columns = len(subitem.keys())
                print(f"✅ Subitems table: {filled_columns}/{total_columns} columns have data")
        
        if hidden_after > 0:
            sample_hidden = supabase.client.table('hidden_items').select('*').limit(1).execute()
            if sample_hidden.data:
                hidden = sample_hidden.data[0]
                filled_columns = sum(1 for key, value in hidden.items() if value is not None and value != '')
                total_columns = len(hidden.keys())
                print(f"✅ Hidden items table: {filled_columns}/{total_columns} columns have data")
        
        print(f"\n🎉 COMPREHENSIVE TEST COMPLETED SUCCESSFULLY!")
        print(f"Parents → Subitems → Hidden items flow validated.")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_comprehensive_sync())