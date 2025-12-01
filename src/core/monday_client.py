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

    def get_hidden_items_by_prefix(
        self,
        board_id: str,
        prefixes: List[str],
        column_ids: List[str],
        limit: int = 100,
    ) -> List[Dict]:
        """
        Query hidden items board by name prefix using items_page with contains_text.
        
        Args:
            board_id: Hidden items board ID
            prefixes: List of prefixes to search for (e.g., ["17701", "17702"])
            column_ids: Column IDs to fetch in the response
            limit: Max items per prefix query
            
        Returns:
            List of matching items with column values
        """
        all_items = []
        column_ids_str = json.dumps(column_ids)  # ["col1", "col2", ...]
        
        for prefix in prefixes:
            query = f"""
            query GetHiddenByPrefix(
                $board_id: ID!
                $columnIds: [String!]
                $compare_value: CompareValue!
                $limit: Int!
            ) {{
                boards(ids: [$board_id]) {{
                    items_page(
                        limit: $limit
                        query_params: {{
                            rules: [
                                {{
                                    column_id: "name"
                                    operator: contains_text
                                    compare_value: $compare_value
                                }}
                            ]
                        }}
                    ) {{
                        cursor
                        items {{
                            id
                            name
                            column_values(ids: $columnIds) {{
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
                "columnIds": column_ids,
                "compare_value": [prefix],
                "limit": limit,
            }
            
            try:
                result = self.execute_query(query, variables)
                boards = result.get("data", {}).get("boards", [])
                if boards:
                    items = boards[0].get("items_page", {}).get("items", [])
                    all_items.extend(items)
                time.sleep(RATE_LIMIT_DELAY)
            except Exception as e:
                logger.warning(f"Failed to query hidden items for prefix {prefix}: {e}")
                continue
        
        return all_items

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

    def update_item_columns(
        self,
        board_id: str,
        item_id: str,
        column_values: Dict[str, Any],
        throttle: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Change multiple column values on a Monday item with retry/backoff handling.
        """
        if not column_values:
            logger.debug("No column values supplied for item %s; skipping update.", item_id)
            return None

        mutation = """
        mutation ChangeMultipleColumnValues(
            $board_id: ID!,
            $item_id: ID!,
            $column_values: JSON!
        ) {
            change_multiple_column_values(
                board_id: $board_id,
                item_id: $item_id,
                column_values: $column_values
            ) {
                id
            }
        }
        """
        variables = {
            "board_id": str(board_id),
            "item_id": str(item_id),
            "column_values": json.dumps(column_values),
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug("Updating Monday item %s (attempt %d)", item_id, attempt)
                result = self.execute_query(mutation, variables)
                if throttle and RATE_LIMIT_DELAY:
                    time.sleep(RATE_LIMIT_DELAY)
                return result.get("data", {}).get("change_multiple_column_values")
            except Exception as exc:
                logger.warning(
                    "Failed to update Monday item %s on attempt %d/%d: %s",
                    item_id,
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
                if attempt == MAX_RETRIES:
                    logger.error("Giving up on Monday item %s after %d attempts.", item_id, attempt)
                    raise
                time.sleep(RATE_LIMIT_DELAY * attempt)

        return None

    def create_item_update(
        self,
        item_id: str,
        body: str,
        throttle: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Create an update (activity log post) on a Monday item.
        """
        if not body:
            logger.debug("Empty update body for item %s; skipping create_update.", item_id)
            return None

        mutation = """
        mutation CreateUpdate($item_id: ID!, $body: String!) {
            create_update(item_id: $item_id, body: $body) {
                id
                text_body
            }
        }
        """
        variables = {"item_id": str(item_id), "body": body}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug("Posting update to Monday item %s (attempt %d)", item_id, attempt)
                result = self.execute_query(mutation, variables)
                if throttle and RATE_LIMIT_DELAY:
                    time.sleep(RATE_LIMIT_DELAY)
                return result.get("data", {}).get("create_update")
            except Exception as exc:
                logger.warning(
                    "Failed to create update for item %s on attempt %d/%d: %s",
                    item_id,
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
                if attempt == MAX_RETRIES:
                    logger.error("Giving up on Monday update for item %s after %d attempts.", item_id, attempt)
                    raise
                time.sleep(RATE_LIMIT_DELAY * attempt)

        return None

    def edit_item_update(
        self,
        update_id: str,
        body: str,
        throttle: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Edit an existing Monday item update (activity log entry).
        """
        if not update_id or not body:
            logger.debug("Missing update_id/body for edit_update; skipping.")
            return None

        mutation = """
        mutation EditUpdate($update_id: ID!, $body: String!) {
            edit_update(id: $update_id, body: $body) {
                id
                item_id
                creator_id
            }
        }
        """
        variables = {"update_id": str(update_id), "body": body}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug("Editing Monday update %s (attempt %d)", update_id, attempt)
                result = self.execute_query(mutation, variables)
                if throttle and RATE_LIMIT_DELAY:
                    time.sleep(RATE_LIMIT_DELAY)
                return result.get("data", {}).get("edit_update")
            except Exception as exc:
                logger.warning(
                    "Failed to edit update %s on attempt %d/%d: %s",
                    update_id,
                    attempt,
                    MAX_RETRIES,
                    exc,
                )
                if attempt == MAX_RETRIES:
                    logger.error("Giving up on Monday edit_update %s after %d attempts.", update_id, attempt)
                    raise
                time.sleep(RATE_LIMIT_DELAY * attempt)

        return None

    def get_item_updates(
        self,
        item_id: str,
        limit: int = 10,
        throttle: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent updates for an item (used to detect prior Datacube posts).
        """
        query = """
        query GetItemUpdates($item_id: [ID!], $limit: Int!) {
            items(ids: $item_id) {
                updates(limit: $limit) {
                    id
                    body
                    text_body
                    created_at
                    creator {
                        id
                        name
                    }
                }
            }
        }
        """
        variables = {"item_id": [str(item_id)], "limit": limit}
        result = self.execute_query(query, variables)
        if throttle and RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY)

        items = result.get("data", {}).get("items") or []
        if not items:
            return []
        return items[0].get("updates") or []


    