"""
Enhanced data extractor that properly handles mirror columns and display values
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class EnhancedColumnExtractor:
    """Enhanced extraction logic for Monday.com column values"""
    
    def extract_column_value(self, column_value: Dict) -> Any:
        """
        Extract the actual value from a Monday.com column value object
        Properly handles mirror columns with display_value
        """
        column_type = column_value.get('type')
        column_id = column_value.get('id')
        
        # Debug logging for formula columns
        if column_type == 'formula' and column_id == 'formula63__1':
            print(f"DEBUG extract_column_value: Processing formula63__1")
            print(f"  display_value: {column_value.get('display_value')}")
            print(f"  text: {column_value.get('text')}")
            print(f"  value: {column_value.get('value')}")
        
        # For mirror and formula columns, prioritize display_value
        if column_type in ['mirror', 'formula']:
            display_value = column_value.get('display_value')
            if display_value and str(display_value).strip() not in ['', 'null']:
                # Handle multiple values (comma-separated)
                if ',' in str(display_value):
                    # Take the first value for now
                    result = str(display_value).split(',')[0].strip()
                else:
                    result = str(display_value).strip()
                
                if column_id == 'formula63__1':
                    print(f"DEBUG extract_column_value: Returning display_value: {result}")
                return result
                
        # For status columns, extract the label
        if column_type == 'status':
            text = column_value.get('text')
            if text:
                return text
            # Try to parse from value
            value = column_value.get('value')
            if value and isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, dict) and 'label' in parsed:
                        return parsed['label']
                except:
                    pass
        
        # For dropdown columns, get the text
        if column_type == 'dropdown':
            text = column_value.get('text')
            if text:
                return text
            # Try to get from value
            value = column_value.get('value')
            if value and isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, dict) and 'labels' in parsed:
                        return ', '.join(parsed['labels'])
                except:
                    pass
        
        # For date columns
        if column_type == 'date':
            text = column_value.get('text')
            if text:
                return text
            value = column_value.get('value')
            if value and isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, dict) and 'date' in parsed:
                        return parsed['date']
                except:
                    pass
        
        # For numeric columns
        if column_type in ['numbers', 'numeric']:
            text = column_value.get('text')
            if text:
                try:
                    return float(text.replace(',', ''))
                except:
                    return text
        
        # Board relation (connect boards) → extract linked item IDs
        if column_type in ['board_relation', 'board-relation', 'connect_boards']:
            value = column_value.get('value')
            if value and isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    # Possible shapes: {'linkedPulseIds': [{'linkedPulseId': '123'}]} or {'linkedItemIds':[123]}
                    if isinstance(parsed, dict):
                        ids = []
                        if 'linkedPulseIds' in parsed and isinstance(parsed['linkedPulseIds'], list):
                            ids = [str(x.get('linkedPulseId')) for x in parsed['linkedPulseIds'] if x.get('linkedPulseId')]
                        elif 'linkedItemIds' in parsed and isinstance(parsed['linkedItemIds'], list):
                            ids = [str(x) for x in parsed['linkedItemIds']]
                        # Return first id for single-valued field; adjust if you want a list
                        if ids:
                            return ids[0]
                except:
                    pass
            # Fall through to default if parsing fails

        # Default: try text, then value
        text = column_value.get('text')
        if text and str(text).strip():
            return text
        
        value = column_value.get('value')
        if value:
            if isinstance(value, str) and value.startswith('{'):
                try:
                    parsed = json.loads(value)
                    # Extract meaningful data from parsed JSON
                    if isinstance(parsed, dict):
                        return parsed.get('text') or parsed.get('label') or str(value)
                except:
                    pass
            return value
        
        return None
    
    def extract_item_data(self, item: Dict, column_mapping: Dict) -> Dict:
        """
        Extract all column values from an item using the column mapping
        """
        extracted = {
            'monday_id': item.get('id'),
            'name': item.get('name', '')
        }
        
        column_values = item.get('column_values', [])
        
        # Create a lookup dict for column values by ID
        column_lookup = {cv['id']: cv for cv in column_values}
        
        # Extract values based on mapping
        for field_name, column_id in column_mapping.items():
            if field_name in ['monday_id', 'name']:
                continue  # Already handled
            
            if column_id in column_lookup:
                value = self.extract_column_value(column_lookup[column_id])
                extracted[field_name] = value
                
                # Debug logging for quote_amount specifically
                if field_name == 'quote_amount':
                    col_data = column_lookup[column_id]
                    print(f"DEBUG extract_item_data: {field_name} = {value}")
                    print(f"  Raw column: {col_data}")
                    print(f"  display_value: {col_data.get('display_value')}")
                    print(f"  text: {col_data.get('text')}")
                    print(f"  value: {col_data.get('value')}")
                    print(f"  type: {col_data.get('type')}")
            else:
                extracted[field_name] = None
                if field_name == 'quote_amount':
                    print(f"DEBUG extract_item_data: {field_name} = None (column {column_id} not found)")
        
        # Handle parent_item for subitems
        if 'parent_item' in item and item['parent_item']:
            parent_item = item['parent_item']
            if isinstance(parent_item, dict):
                extracted['parent_monday_id'] = parent_item.get('id')
        
        return extracted

class EnhancedMondayExtractor:
    """Enhanced Monday.com data extractor with proper mirror resolution"""
    
    def __init__(self, monday_client):
        self.monday_client = monday_client
        self.column_extractor = EnhancedColumnExtractor()
        
    def extract_items_with_values(
        self, 
        board_id: str, 
        column_ids: List[str],
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> Dict:
        """
        Extract items with proper value extraction including mirror display values
        """
        # Build the GraphQL query with proper value extraction
        column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)
        
        query = f"""
        query GetItems($board_id: ID!, $limit: Int!, $cursor: String) {{
            boards(ids: [$board_id]) {{
                items_page(limit: $limit, cursor: $cursor) {{
                    cursor
                    items {{
                        id
                        name
                        parent_item {{
                            id
                        }}
                        column_values(ids: [{column_ids_str}]) {{
                            id
                            text
                            value
                            type
                            ... on FormulaValue {{
                                display_value
                            }}
                            ... on MirrorValue {{
                                display_value
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """
        
        variables = {
            "board_id": board_id,
            "limit": limit,
            "cursor": cursor
        }
        
        result = self.monday_client.execute_query(query, variables)
        
        if result.get("data", {}).get("boards"):
            items_page = result["data"]["boards"][0].get("items_page", {})
            return {
                "items": items_page.get("items", []),
                "next_cursor": items_page.get("cursor")
            }
        
        return {"items": [], "next_cursor": None}
    
    def extract_all_board_items(
        self,
        board_id: str,
        column_mapping: Dict,
        batch_size: int = 100,
        max_items: Optional[int] = None  # Add this parameter
    ) -> List[Dict]:
        """
        Extract items from a board with proper value extraction
        """
        all_items = []
        cursor = None
        column_ids = list(column_mapping.values())
        
        while True:
            # Calculate how many items to fetch in this batch
            if max_items:
                remaining = max_items - len(all_items)
                if remaining <= 0:
                    break
                current_limit = min(batch_size, remaining)
            else:
                current_limit = batch_size
                
            result = self.extract_items_with_values(
                board_id=board_id,
                column_ids=column_ids,
                limit=current_limit,
                cursor=cursor
            )
            
            items = result.get("items", [])
            if not items:
                break
            
            # Extract data from each item
            for item in items:
                extracted = self.column_extractor.extract_item_data(item, column_mapping)
                all_items.append(extracted)
                
                # Stop if we've reached max_items
                if max_items and len(all_items) >= max_items:
                    break
            
            cursor = result.get("next_cursor")
            if not cursor or (max_items and len(all_items) >= max_items):
                break
        
        return all_items
