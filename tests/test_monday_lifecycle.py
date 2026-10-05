"""Offline lifecycle routing, durable ingress and subscription regressions."""
import asyncio
import importlib
import sys
from unittest.mock import Mock, patch

from fastapi import BackgroundTasks, HTTPException
from starlette.requests import Request
import json
import pytest
import requests

from src.services import monday_lifecycle as life
from scripts import setup_webhooks


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('No external HTTP in unit tests'))


def payload(event_type='delete_pulse', board=None, item='201', **extra):
    return {'event': {'type': event_type, 'boardId': board or life.SUBITEM_BOARD_ID,
                      'pulseId': item, 'triggerUuid': 'event-1', **extra}}


@pytest.mark.parametrize('event_type', sorted(life.DELETE_EVENTS | life.RESTORE_EVENTS))
def test_all_deletion_restoration_names_route_exactly(event_type):
    event = life.event_from_payload(payload(event_type))
    assert event['item_id'] == '201' and event['board_id'] == life.SUBITEM_BOARD_ID
    assert event['kind'] == ('delete' if event_type in life.DELETE_EVENTS else 'restore')


def test_archive_and_business_status_do_not_delete():
    assert life.event_from_payload(payload('item_archived')) is None
    assert life.event_from_payload(payload('change_column_value', value={'label': 'Archived'})) is None


@pytest.mark.parametrize('data', [
    payload(board='unknown'), payload(item='bad-id'),
    payload(itemId='202'), payload('subitem_deleted', board=life.PARENT_BOARD_ID),
    payload(parentItemId='101', parentItemBoardId='unknown'),
])
def test_conflicting_identity_fails_closed(data):
    with pytest.raises(ValueError):
        life.event_from_payload(data)


def test_persistence_does_not_reset_duplicate_job_status():
    client = Mock()
    event = life.event_from_payload(payload())
    assert life.persist_event(client, event) == event['event_key']
    client.table.assert_called_once_with('monday_lifecycle_events')
    client.table.return_value.upsert.assert_called_once_with(event, on_conflict='event_key', ignore_duplicates=True)
    assert not {'status','attempts','lease_token'} & set(event)


@pytest.fixture
def server(monkeypatch):
    name = 'src.webhooks.webhook_server'
    existed = name in sys.modules
    with patch('src.database.supabase_client.SupabaseClient'), patch('src.database.sync_service.DataSyncService'):
        module = importlib.import_module(name)
    monkeypatch.setattr(module, 'supabase_client', Mock())
    monkeypatch.setattr(module, 'verify_webhook_signature', Mock(return_value=True))
    monkeypatch.setattr(module.rate_limiter, 'is_allowed', Mock(return_value=True))
    monkeypatch.setattr(module, 'is_duplicate_event', Mock(side_effect=AssertionError('Must use durable dedupe')))
    monkeypatch.setenv('MONDAY_LIFECYCLE_ENABLED', 'true')
    yield module
    if not existed:
        sys.modules.pop(name, None)
        package = sys.modules.get('src.webhooks')
        if getattr(package, 'webhook_server', None) is module:
            delattr(package, 'webhook_server')


def request(data):
    async def receive():
        return {'type': 'http.request', 'body': json.dumps(data).encode(), 'more_body': False}
    return Request({'type': 'http', 'method': 'POST', 'path': '/webhooks/monday',
                    'headers': [], 'client': ('127.0.0.1', 1)}, receive)


def test_acknowledgement_requires_successful_durable_insert(server, monkeypatch):
    persist = Mock(return_value='key')
    monkeypatch.setattr(life, 'persist_event', persist)
    tasks = BackgroundTasks()
    response = asyncio.run(server.handle_monday_webhook(request(payload()), tasks))
    assert response.status_code == 202 and not tasks.tasks
    assert json.loads(response.body)['durable'] is True
    assert persist.call_count == 1


def test_failed_persistence_returns_retryable_http_error(server, monkeypatch):
    monkeypatch.setattr(life, 'persist_event', Mock(side_effect=RuntimeError('database unavailable')))
    with pytest.raises(HTTPException) as error:
        asyncio.run(server.handle_monday_webhook(request(payload()), BackgroundTasks()))
    assert error.value.status_code == 503


def test_disabled_feature_never_falls_back_to_blind_delete(server, monkeypatch):
    monkeypatch.setenv('MONDAY_LIFECYCLE_ENABLED', 'false')
    persist = Mock()
    monkeypatch.setattr(life, 'persist_event', persist)
    with pytest.raises(HTTPException) as error:
        asyncio.run(server.handle_monday_webhook(request(payload()), BackgroundTasks()))
    assert error.value.status_code == 503
    persist.assert_not_called()


def test_invalid_signature_cannot_enqueue(server, monkeypatch):
    monkeypatch.setattr(server, 'verify_webhook_signature', Mock(return_value=False))
    persist = Mock()
    monkeypatch.setattr(life, 'persist_event', persist)
    with pytest.raises(HTTPException) as error:
        asyncio.run(server.handle_monday_webhook(request(payload()), BackgroundTasks()))
    assert error.value.status_code == 401
    persist.assert_not_called()


def test_default_setup_adds_subitem_delete_only_on_parent(monkeypatch):
    create = Mock(return_value={})
    monkeypatch.setattr(setup_webhooks, 'create_webhook', create)
    setup_webhooks.setup_all_webhooks(setup_webhooks.DEFAULT_BOARDS, setup_webhooks.DEFAULT_EVENTS)
    pairs = {(call.args[0], call.args[1]) for call in create.call_args_list}
    assert (life.PARENT_BOARD_ID, 'subitem_deleted') in pairs
    assert (life.HIDDEN_ITEMS_BOARD_ID, 'subitem_deleted') not in pairs
    for board in setup_webhooks.DEFAULT_BOARDS:
        assert (board, 'item_deleted') in pairs and (board, 'item_restored') in pairs


def test_setup_graphql_errors_and_registration_failures_propagate(monkeypatch):
    response = Mock()
    response.json.return_value = {'errors': [{'message': 'denied'}], 'data': {}}
    monkeypatch.setattr(setup_webhooks.requests, 'post', Mock(return_value=response))
    monkeypatch.setattr(setup_webhooks, '_headers', lambda: {})
    with pytest.raises(RuntimeError, match='GraphQL'):
        setup_webhooks._request({})
    monkeypatch.setattr(setup_webhooks, 'create_webhook', Mock(side_effect=RuntimeError('denied')))
    with pytest.raises(RuntimeError, match='registrations failed'):
        setup_webhooks.setup_all_webhooks([life.PARENT_BOARD_ID], ['item_deleted'])


def test_setup_uses_documented_fields_and_preserves_callback_identity(monkeypatch, tmp_path):
    monkeypatch.setattr(setup_webhooks, 'REGISTRY_PATH', tmp_path/'registry.json')
    monkeypatch.setattr(setup_webhooks, 'WEBHOOK_URL', 'https://example.test/webhooks/monday')
    existing = [{'id': 'old', 'event': 'item_deleted', 'board_id': life.PARENT_BOARD_ID}]
    calls = []
    def request(body, **kwargs):
        calls.append(body)
        if body['query'].lstrip().startswith('query'):
            assert 'url' not in body['query']
            return {'data': {'webhooks': existing}}
        created = {'id': 'new', 'event': 'item_deleted', 'board_id': life.PARENT_BOARD_ID}
        existing.append(created)
        return {'data': {'create_webhook': created}}
    monkeypatch.setattr(setup_webhooks, '_request', request)
    setup_webhooks.create_webhook(life.PARENT_BOARD_ID, 'item_deleted')
    assert len(calls) == 2  # Another app's old same-event hook was not trusted.
    result = setup_webhooks.create_webhook(life.PARENT_BOARD_ID, 'item_deleted')
    assert result['status'] == 'exists' and result['webhook']['id'] == 'new'
    assert len(calls) == 3  # Repeated setup only reads; no second creation.
