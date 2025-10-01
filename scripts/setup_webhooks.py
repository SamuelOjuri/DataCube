import requests
import json
from typing import Dict
import os
from dotenv import load_dotenv

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
WEBHOOK_URL = os.getenv("MONDAY_WEBHOOK_URL")
BOARDS_TO_WATCH = ["1825117125", "1825117144", "1825138260"]


def create_webhook(board_id: str, event: str) -> Dict:
    """Create a webhook for a board and event type"""
    
    query = """
    mutation create_webhook($board_id: ID!, $url: String!, $event: WebhookEventType!) {
        create_webhook(
            board_id: $board_id,
            url: $url,
            event: $event
        ) {
            id
            board_id
        }
    }
    """
    
    variables = {
        "board_id": board_id,
        "url": WEBHOOK_URL,
        "event": event
    }
    
    response = requests.post(
        "https://api.monday.com/v2",
        json={"query": query, "variables": variables},
        headers={
            "Authorization": MONDAY_API_KEY,
            "Content-Type": "application/json"
        }
    )
    
    return response.json()


def setup_all_webhooks():
    """Setup webhooks for all boards and events"""
    
    events = [
        "create_item",
        "change_column_value", 
        "delete_item"
    ]
    
    for board_id in BOARDS_TO_WATCH:
        for event in events:
            result = create_webhook(board_id, event)
            print(f"Created webhook for board {board_id}, event {event}: {result}")


if __name__ == "__main__":
    setup_all_webhooks()