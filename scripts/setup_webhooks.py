import argparse

import os
import json
from pathlib import Path
from uuid import uuid4
from typing import Dict, List
import requests
from dotenv import load_dotenv

load_dotenv()

MONDAY_TOKEN = os.getenv("MONDAY_API_KEY") or os.getenv("MONDAY_OAUTH_TOKEN")
TOKEN_MODE = os.getenv("MONDAY_TOKEN_MODE", "api_key").lower()
WEBHOOK_URL = os.getenv("MONDAY_WEBHOOK_URL")
DEFAULT_BOARDS = ["1825117125", "1825138260"]
UNSUPPORTED_BOARDS = {
    "1825117144": "Monday does not allow webhooks to be created directly on the subitems board; subscribe via the parent board instead."
}
EVENT_ALIASES = {
    "delete_item": "item_deleted",
    "delete_pulse": "item_deleted",
    "delete_subitem": "subitem_deleted",
    "restore_item": "item_restored",
}
DEFAULT_EVENTS = ['create_item', 'change_column_value', 'item_deleted', 'item_restored', 'subitem_deleted']
REGISTRY_PATH = Path('outputs/monday_webhooks/registry.json')


def _headers() -> Dict[str, str]:
    if not MONDAY_TOKEN:
        raise RuntimeError("Monday token not provided. Set MONDAY_API_KEY or MONDAY_OAUTH_TOKEN")
    auth_header = MONDAY_TOKEN if TOKEN_MODE != "oauth" else f"Bearer {MONDAY_TOKEN}"
    return {
        "Authorization": auth_header,
        "Content-Type": "application/json",
    }


def _request(payload: Dict, *, verbose: bool = False) -> Dict:
    response = requests.post(
        "https://api.monday.com/v2",
        json=payload,
        headers=_headers(),
        timeout=30,
    )
    if verbose and not response.ok:
        print("Monday error payload:", response.text)
    response.raise_for_status()
    body = response.json()
    if body.get('errors'):
        raise RuntimeError(f"Monday GraphQL rejected webhook request: {body['errors']}")
    if not isinstance(body.get('data'), dict):
        raise RuntimeError('Monday returned no webhook data')
    return body


def normalize_event(event: str) -> str:
    normalized = EVENT_ALIASES.get(event, event)
    if normalized != event:
        print(f"Normalizing event '{event}' → '{normalized}'")
    return normalized


def get_existing_webhooks(board_id: str) -> List[Dict]:
    query = """
    query getWebhooks($board_id: ID!) {
        webhooks(board_id: $board_id) {
            id
            event
            board_id
        }
    }
    """
    data = _request({"query": query, "variables": {"board_id": board_id}})
    return data.get("data", {}).get("webhooks", []) or []


def create_webhook(board_id: str, event: str, *, verbose: bool = False) -> Dict:
    if not WEBHOOK_URL:
        raise RuntimeError("MONDAY_WEBHOOK_URL must be set")

    normalized_event = normalize_event(event)
    # The documented Webhook fields do not include its callback URL. Remember
    # the IDs we provisioned for each destination; never mistake another app's
    # same-event webhook for ours or query an undocumented `url` field.
    registry = json.loads(REGISTRY_PATH.read_text(encoding='utf-8')) if REGISTRY_PATH.exists() else []
    if not isinstance(registry, list) or any(not isinstance(r, dict) for r in registry):
        raise ValueError('Invalid webhook registry; preserve it and inspect before provisioning')
    recorded = {str(r['id']) for r in registry if r.get('board_id') == board_id
                and r.get('event') == normalized_event and r.get('url') == WEBHOOK_URL}
    existing = [w for w in get_existing_webhooks(board_id)
                if w.get('event') == normalized_event and str(w.get('id')) in recorded]
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
        "event": normalized_event,
    }

    result = _request({"query": query, "variables": variables}, verbose=verbose)
    created = result.get('data', {}).get('create_webhook')
    if not created or not created.get('id'):
        raise RuntimeError('Monday did not return a created webhook ID')
    registry.append({'id': str(created['id']), 'board_id': board_id,
                     'event': normalized_event, 'url': WEBHOOK_URL})
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = REGISTRY_PATH.with_suffix(f'.{uuid4().hex}.tmp')
    temporary.write_text(json.dumps(registry, indent=2), encoding='utf-8')
    temporary.replace(REGISTRY_PATH)
    return result


def setup_all_webhooks(boards: List[str], events: List[str], *, verbose: bool = False) -> None:
    failures = []
    for board_id in boards:
        if board_id in UNSUPPORTED_BOARDS:
            print(f"Skipping board {board_id}: {UNSUPPORTED_BOARDS[board_id]}")
            continue
        for event in events:
            if normalize_event(event) == 'subitem_deleted' and board_id != DEFAULT_BOARDS[0]:
                continue  # Configured subitems belong to the project parent board.
            try:
                result = create_webhook(board_id, event, verbose=verbose)
                print(f"Board {board_id} | {event}: {result}")
            except Exception as exc:  # noqa: BLE001
                print(f"Failed to register webhook for board {board_id} ({event}): {exc}")
                failures.append((board_id, event))
    if failures:
        raise RuntimeError(f'{len(failures)} webhook registrations failed; inspect the errors above')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision Monday webhooks for DataCube")
    parser.add_argument("--boards", nargs="*", default=DEFAULT_BOARDS, help="Board IDs to register")
    parser.add_argument('--registry-file', type=Path, default=REGISTRY_PATH,
                        help='Preserve callback-to-subscription IDs here for safe repeat runs')
    parser.add_argument(
        "--events",
        nargs="*",
        default=DEFAULT_EVENTS,
        help="Webhook event types (aliases like 'delete_item' are accepted)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print Monday API error payloads when requests fail",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    REGISTRY_PATH = args.registry_file
    setup_all_webhooks(args.boards, args.events, verbose=args.verbose)
