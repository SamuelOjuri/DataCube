import argparse
import os
from typing import Dict, List

import requests
from dotenv import load_dotenv


load_dotenv()

MONDAY_TOKEN = os.getenv("MONDAY_API_KEY") or os.getenv("MONDAY_OAUTH_TOKEN")
TOKEN_MODE = os.getenv("MONDAY_TOKEN_MODE", "api_key").lower()
WEBHOOK_URL = os.getenv("MONDAY_WEBHOOK_URL")
DEFAULT_BOARDS = ["1825117125", "1825117144", "1825138260"]


def _headers() -> Dict[str, str]:
    if not MONDAY_TOKEN:
        raise RuntimeError("Monday token not provided. Set MONDAY_API_KEY or MONDAY_OAUTH_TOKEN")
    auth_header = MONDAY_TOKEN if TOKEN_MODE != "oauth" else f"Bearer {MONDAY_TOKEN}"
    return {
        "Authorization": auth_header,
        "Content-Type": "application/json",
    }


def _request(payload: Dict) -> Dict:
    response = requests.post(
        "https://api.monday.com/v2",
        json=payload,
        headers=_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_existing_webhooks(board_id: str) -> List[Dict]:
    query = """
    query getWebhooks($board_id: ID!) {
        webhooks(board_id: $board_id) {
            id
            event
            url
        }
    }
    """
    data = _request({"query": query, "variables": {"board_id": board_id}})
    return data.get("data", {}).get("webhooks", []) or []


def create_webhook(board_id: str, event: str) -> Dict:
    if not WEBHOOK_URL:
        raise RuntimeError("MONDAY_WEBHOOK_URL must be set")

    existing = [w for w in get_existing_webhooks(board_id) if w.get("event") == event and w.get("url") == WEBHOOK_URL]
    if existing:
        return {"status": "exists", "webhook": existing[0]}

    query = """
    mutation createWebhook($board_id: ID!, $url: String!, $event: WebhookEventType!) {
        create_webhook(
            board_id: $board_id,
            url: $url,
            event: $event
        ) {
            id
            board_id
            event
        }
    }
    """

    variables = {
        "board_id": board_id,
        "url": WEBHOOK_URL,
        "event": event,
    }

    return _request({"query": query, "variables": variables})


def setup_all_webhooks(boards: List[str], events: List[str]) -> None:
    for board_id in boards:
        for event in events:
            try:
                result = create_webhook(board_id, event)
                print(f"Board {board_id} | {event}: {result}")
            except Exception as exc:  # noqa: BLE001
                print(f"Failed to register webhook for board {board_id} ({event}): {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision Monday webhooks for DataCube")
    parser.add_argument("--boards", nargs="*", default=DEFAULT_BOARDS, help="Board IDs to register")
    parser.add_argument(
        "--events",
        nargs="*",
        default=["create_item", "change_column_value", "delete_item"],
        help="Webhook event types",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_all_webhooks(args.boards, args.events)