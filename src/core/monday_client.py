"""
Monday.com API client for interacting with boards and items.
"""

import json
import time
import logging
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Change absolute import to relative import with fallback
try:
    from ..config import (
        MONDAY_API_KEY,
        MONDAY_API_URL,
        BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES
    )
except ImportError:
    # Fallback for when running as standalone script
    from config import (
        MONDAY_API_KEY,
        MONDAY_API_URL,
        BATCH_SIZE,
        RATE_LIMIT_DELAY,
        MAX_RETRIES
    )

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MondayClient:
    """Client for interacting with Monday.com API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Monday.com client.
        
        Args:
            api_key: Monday.com API key. If not provided, uses environment variable.
        """
        self.api_key = api_key or MONDAY_API_KEY
        if not self.api_key:
            raise ValueError("Monday.com API key is required. Set MONDAY_API_KEY in environment.")
        
        self.api_url = MONDAY_API_URL
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
            "API-Version": "2025-07"  # Use current stable version
        }
        
        # Set up session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
    
    def execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """
        Execute a GraphQL query against Monday.com API.
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            API response as dictionary
            
        Raises:
            requests.RequestException: If API request fails
        """
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        try:
            response = self.session.post(
                self.api_url,
                json=payload,
                headers=self.headers,
                timeout=300  # Increase from 2 minutes for large boards
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Check for GraphQL errors
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                raise Exception(f"GraphQL errors: {data['errors']}")
                
            return data
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_board_columns(self, board_id: str) -> List[Dict]:
        """
        Get all columns for a board.
        
        Args:
            board_id: The board ID
            
        Returns:
            List of column dictionaries
        """
        query = """
        query GetBoardColumns($board_id: ID!) {
            boards(ids: [$board_id]) {
                columns {
                    id
                    title
                    type
                    settings_str
                }
            }
        }
        """
        
        result = self.execute_query(query, {"board_id": board_id})
        
        if result["data"]["boards"]:
            return result["data"]["boards"][0]["columns"]
        return []
    
    def get_board_info(self, board_id: str) -> Dict:
        """
        Get basic information about a board.
        
        Args:
            board_id: The board ID
            
        Returns:
            Board information dictionary
        """
        query = """
        query GetBoardInfo($board_id: ID!) {
            boards(ids: [$board_id]) {
                id
                name
                items_count
                description
            }
        }
        """
        
        result = self.execute_query(query, variables={"board_id": board_id})
        
        if result["data"]["boards"]:
            return result["data"]["boards"][0]
        return {}
    
    def test_connection(self) -> bool:
        """
        Test the API connection by making a simple query.
        
        Returns:
            True if connection is successful, False otherwise
        """
        query = """
        query {
            me {
                id
                name
            }
        }
        """
        
        try:
            result = self.execute_query(query)
            if result.get("data", {}).get("me"):
                logger.info(f"Successfully connected as: {result['data']['me']['name']}")
                return True
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_items_page(
        self, 
        board_id: str, 
        column_ids: List[str],
        limit: int = BATCH_SIZE,
        cursor: Optional[str] = None
    ) -> Dict:
        """
        Get a page of items from a board with specified columns.
        
        Args:
            board_id: The board ID
            column_ids: List of column IDs to fetch
            limit: Number of items to fetch (default: BATCH_SIZE)
            cursor: Pagination cursor
            
        Returns:
            Dictionary with items and next cursor
        """
        column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)
        
        query = f"""
        query GetItems($board_id: ID!, $limit: Int!, $cursor: String) {{
            boards(ids: [$board_id]) {{
                items_page(limit: $limit, cursor: $cursor) {{
                    cursor
                    items {{
                        id
                        name
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
        
        result = self.execute_query(query, variables)
        
        if result["data"]["boards"]:
            items_page = result["data"]["boards"][0]["items_page"]
            return {
                "items": items_page["items"],
                "next_cursor": items_page.get("cursor")
            }
        
        return {"items": [], "next_cursor": None}
    
    def get_subitems_page(
        self, 
        board_id: str, 
        column_ids: List[str],
        limit: int = BATCH_SIZE,
        cursor: Optional[str] = None
    ) -> Dict:
        """
        Get a page of subitems from a board with parent relationship.
        
        Args:
            board_id: The board ID
            column_ids: List of column IDs to fetch
            limit: Number of items to fetch (default: BATCH_SIZE)
            cursor: Pagination cursor
            
        Returns:
            Dictionary with subitems and next cursor
        """
        column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)
        
        query = f"""
        query GetSubitems($board_id: ID!, $limit: Int!, $cursor: String) {{
            boards(ids: [$board_id]) {{
                items_page(limit: $limit, cursor: $cursor) {{
                    cursor
                    items {{
                        id
                        name
                        parent_item {{ id }}
                        column_values(ids: [{column_ids_str}]) {{
                            id
                            text
                            value
                            type
                            ... on FormulaValue {{ display_value }}
                            ... on MirrorValue {{ display_value }}
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
        
        result = self.execute_query(query, variables)
        
        if result["data"]["boards"]:
            items_page = result["data"]["boards"][0]["items_page"]
            return {
                "items": items_page["items"],
                "next_cursor": items_page.get("cursor")
            }
        
        return {"items": [], "next_cursor": None}

    def get_subitems_for_parents(
        self,
        parent_ids: List[str],
        column_ids: List[str]
    ) -> List[Dict]:
        """
        Fetch subitems for a set of parent item IDs, including parent reference and specified columns.
        """
        if not parent_ids:
            return []

        parent_ids_str = ', '.join(parent_ids)
        column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)

        query = f"""
        query GetSubitemsForParents {{
            items(ids: [{parent_ids_str}]) {{
                id
                subitems {{
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
        """

        result = self.execute_query(query)
        items = result.get("data", {}).get("items", []) or []
        subitems = []
        for item in items:
            for s in item.get("subitems", []) or []:
                subitems.append(s)
        return subitems

    def get_items_by_ids(
        self,
        item_ids: List[str],
        column_ids: List[str],
        per_call_limit: int = 100,
        include_parent: bool = False
    ) -> List[Dict]:
        """
        Fetch items by IDs with up to per_call_limit per call.
        Optionally include parent_item { id } (needed for subitems transform).
        """
        ids = [str(i) for i in item_ids if i]
        if not ids:
            return []
        column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)
        parent_fragment = "parent_item { id }" if include_parent else ""

        all_items: List[Dict] = []
        for i in range(0, len(ids), per_call_limit):
            batch = ids[i:i+per_call_limit]
            query = f"""
            query GetItemsByIds($ids: [ID!], $limit: Int!) {{
                items(ids: $ids, limit: $limit) {{
                    id
                    name
                    {parent_fragment}
                    column_values(ids: [{column_ids_str}]) {{
                        id
                        text
                        value
                        type
                        ... on FormulaValue {{ display_value }}
                        ... on MirrorValue {{ display_value }}
                    }}
                }}
            }}
            """
            result = self.execute_query(query, {"ids": batch, "limit": min(per_call_limit, len(batch))})
            all_items.extend(result.get("data", {}).get("items", []) or [])
        return all_items

    # def get_items_by_ids(
    # self,
    # item_ids: List[str],
    # column_ids: List[str],
    # per_call_limit: int = 100
    # ) -> List[Dict]:
    #     """
    #     Fetch items by IDs with up to 100 per call (default Monday limit when specified).
    #     Falls back to chunking if more than 100 IDs are requested.
    #     """
    #     ids = [str(i) for i in item_ids if i]
    #     if not ids:
    #         return []
    #     column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)

    #     all_items: List[Dict] = []
    #     for i in range(0, len(ids), per_call_limit):
    #         batch = ids[i:i+per_call_limit]
    #         query = f"""
    #         query GetItemsByIds($ids: [ID!], $limit: Int!) {{
    #         items(ids: $ids, limit: $limit) {{
    #             id
    #             name
    #             column_values(ids: [{column_ids_str}]) {{
    #             id
    #             text
    #             value
    #             type
    #             ... on FormulaValue {{ display_value }}
    #             ... on MirrorValue {{ display_value }}
    #             }}
    #         }}
    #         }}
    #         """
    #         result = self.execute_query(query, {"ids": batch, "limit": min(per_call_limit, len(batch))})
    #         all_items.extend(result.get("data", {}).get("items", []) or [])
    #     return all_items

    # def get_items_by_ids(
    #     self,
    #     item_ids: List[str],
    #     column_ids: List[str]
    # ) -> List[Dict]:
    #     """
    #     Fetch items by their IDs, returning name and requested column_values.
    #     """
    #     ids = [str(i) for i in item_ids if i]
    #     if not ids:
    #         return []
    #     item_ids_str = ', '.join(ids)
    #     column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)

    #     query = f"""
    #     query GetItemsByIds {{
    #         items(ids: [{item_ids_str}]) {{
    #             id
    #             name
    #             column_values(ids: [{column_ids_str}]) {{
    #                 id
    #                 text
    #                 value
    #                 type
    #                 ... on FormulaValue {{
    #                     display_value
    #                 }}
    #                 ... on MirrorValue {{
    #                     display_value
    #                 }}
    #             }}
    #         }}
    #     }}
    #     """

    #     result = self.execute_query(query)
    #     return result.get("data", {}).get("items", []) or []


    def get_next_items_page(
        self, 
        cursor: str,
        column_ids: List[str],
        limit: int = BATCH_SIZE
    ) -> Dict:
        """
        Get next page of items using next_items_page query (more efficient).
        """
        column_ids_str = ', '.join(f'"{col_id}"' for col_id in column_ids)
        query = f"""
        query GetNextItems($cursor: String!, $limit: Int!) {{
            next_items_page(cursor: $cursor, limit: $limit) {{
                cursor
                items {{
                    id
                    name
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
        """
        
        variables = { "cursor": cursor, "limit": limit }
        result = self.execute_query(query, variables)
        if result.get("data", {}).get("next_items_page"):
            page_data = result["data"]["next_items_page"]
            return { "items": page_data.get("items", []), "next_cursor": page_data.get("cursor") }
        return {"items": [], "next_cursor": None}

    def get_item_ids_page(
        self,
        board_id: str,
        limit: int = BATCH_SIZE,
        cursor: Optional[str] = None
    ) -> Dict:
        """
        IDs-only pagination using items_page.
        """
        query = """
        query GetItemIds($board_id: ID!, $limit: Int!, $cursor: String) {
        boards(ids: [$board_id]) {
            items_page(limit: $limit, cursor: $cursor) {
            cursor
            items { id }
            }
        }
        }
        """
        variables = {"board_id": board_id, "limit": limit, "cursor": cursor}
        result = self.execute_query(query, variables)
        if result.get("data", {}).get("boards"):
            page = result["data"]["boards"][0].get("items_page") or {}
            return {"items": page.get("items", []) or [], "next_cursor": page.get("cursor")}
        return {"items": [], "next_cursor": None}

    def get_next_item_ids_page(
        self,
        cursor: str,
        limit: int = BATCH_SIZE
    ) -> Dict:
        """
        IDs-only pagination using next_items_page.
        """
        query = """
        query GetNextItemIds($cursor: String!, $limit: Int!) {
        next_items_page(cursor: $cursor, limit: $limit) {
            cursor
            items { id }
        }
        }
        """
        variables = {"cursor": cursor, "limit": limit}
        result = self.execute_query(query, variables)
        data = result.get("data", {}).get("next_items_page") or {}
        return {"items": data.get("items", []) or [], "next_cursor": data.get("cursor")}

    def get_items_page_since_date(
        self,
        board_id: str,
        column_ids: List[str],
        cutoff_date: str,
        limit: int = BATCH_SIZE,
        cursor: Optional[str] = None,
    ) -> Dict:
        if cursor:
            return self.get_next_items_page(cursor, column_ids, limit)

        column_ids_str = ", ".join(f'"{col}"' for col in column_ids)
        compare_values = f'["EXACT", "{cutoff_date}"]'

        query = f"""
        query GetItemsSinceDate($board: [ID!], $limit: Int!) {{
          boards(ids: $board) {{
            items_page(
              limit: $limit
              query_params: {{
                rules: [
                  {{
                    column_id: "date9__1"
                    compare_value: {compare_values}
                    operator: greater_than_or_equals
                  }}
                ]
              }}
            ) {{
              cursor
              items {{
                id
                name
                column_values(ids: [{column_ids_str}]) {{
                  id
                  text
                  value
                  ... on FormulaValue {{ display_value }}
                  ... on MirrorValue {{ display_value }}
                }}
              }}
            }}
          }}
        }}
        """

        variables = {"board": [board_id], "limit": limit}
        result = self.execute_query(query, variables)

        boards = result.get("data", {}).get("boards") or []
        if not boards:
            return {"items": [], "next_cursor": None}

        page = boards[0].get("items_page") or {}
        return {
            "items": page.get("items") or [],
            "next_cursor": page.get("cursor"),
        }


    
