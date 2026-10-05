"""Offline evidence checks for explicit activity-log deletion recovery."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import Mock

import pytest
import requests

from scripts import monday_lifecycle as cli
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_activity as activity


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session, 'request',
                        lambda *a, **k: pytest.fail('No live HTTP in activity recovery tests'))


def event(log_id='deletion-1', event_type='delete_pulse', *, seconds=0, **data):
    when = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=2) + timedelta(seconds=seconds)
    payload = dict(pulse_id=201, board_id=int(life.SUBITEM_BOARD_ID), parent_item_id=101,
                   parent_board_id=int(life.PARENT_BOARD_ID), pulse_name='Same name')
    payload.update(data)
    return dict(id=log_id, event=event_type, entity='pulse', data=json.dumps(payload),
                user_id='1', created_at=str(int(when.timestamp()) * 10_000_000))


def request():
    return dict(mode=activity.MODE, item_id='201', parent_id='101', board_id=life.SUBITEM_BOARD_ID,
                log_id='deletion-1', **{'from': (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()})


def parent():
    return dict(id='101', name='Parent', state='active', board={'id': life.PARENT_BOARD_ID},
                parent_item=None, updated_at='2026-09-01T00:00:00Z', column_values=[], subitems=[
                    dict(id='202', state='active', board={'id': life.SUBITEM_BOARD_ID}, parent_item={'id': '101'})])


class Monday:
    def __init__(self):
        self.rows = {'101': parent()}
        self.histories = {life.SUBITEM_BOARD_ID: [event()], life.PARENT_BOARD_ID: []}
        self.calls = []

    def execute_query(self, query, variables):
        self.calls.append((query, deepcopy(variables)))
        if query == activity.ACTIVITY_QUERY:
            offset = (variables['page'] - 1) * variables['limit']
            rows = self.histories[variables['board']][offset:offset + variables['limit']]
            return {'data': {'boards': [{'id': variables['board'], 'state': 'active',
                                        'activity_logs': deepcopy(rows)}]}}
        assert query.lstrip().startswith('query CompareMonday(')
        return {'data': {'items': [deepcopy(self.rows[i]) for i in variables['ids'] if i in self.rows]}}


def test_exact_deletion_captured_without_fabricating_current_state():
    monday = Monday()
    proof = activity.capture(monday, request())
    assert proof['deletion_event']['event'] == 'delete_pulse'
    assert proof['request']['deletion_sha256'] == activity.fingerprint(proof['deletion_event'])
    assert set(proof['after_items']) == {'101'}
    assert len([c for c in monday.calls if c[0] == activity.ACTIVITY_QUERY]) == 2
    assert proof['before_items'] == proof['after_items']


@pytest.mark.parametrize('change', [
    {'pulse_id': 202}, {'board_id': 123}, {'parent_item_id': 999},
    {'parent_board_id': 123}, {'item_id': 202},
])
def test_wrong_item_board_parent_or_conflicting_id_is_rejected(change):
    monday = Monday()
    monday.histories[life.SUBITEM_BOARD_ID] = [event(**change)]
    with pytest.raises(life.ReviewRequired, match='does not match|ambiguous'):
        activity.capture(monday, request())


@pytest.mark.parametrize('event_type', ['archive_pulse', 'restore_pulse', 'move_pulse', 'unknown_event'])
def test_only_explicit_deletion_event_is_accepted(event_type):
    monday = Monday()
    monday.histories[life.SUBITEM_BOARD_ID] = [event(event_type=event_type)]
    with pytest.raises(life.ReviewRequired, match='does not match'):
        activity.capture(monday, request())


@pytest.mark.parametrize('event_type', ['restore_pulse', 'move_pulse', 'update_column_value', 'new_unknown_event'])
@pytest.mark.parametrize('board', [life.SUBITEM_BOARD_ID, life.PARENT_BOARD_ID])
def test_any_later_target_activity_in_either_board_requires_review(event_type, board):
    monday = Monday()
    monday.histories[board].insert(0, event('later', event_type, seconds=1))
    with pytest.raises(life.ReviewRequired, match='Later or simultaneous'):
        activity.capture(monday, request())


def test_tied_timestamp_and_nested_move_reference_require_review():
    monday = Monday()
    selected = monday.histories[life.SUBITEM_BOARD_ID][0]
    tied = event('tie', 'move_items', pulse_id=101, value={'item_ids': [201]})
    tied['created_at'] = selected['created_at']
    monday.histories[life.PARENT_BOARD_ID] = [tied]
    with pytest.raises(life.ReviewRequired, match='Later or simultaneous'):
        activity.capture(monday, request())


@pytest.mark.parametrize('event_type', ['restore_pulse', 'move_pulse', 'new_unknown_event'])
def test_parent_lifecycle_changes_after_deletion_require_review(event_type):
    monday = Monday()
    monday.histories[life.PARENT_BOARD_ID] = [event('parent-change', event_type, seconds=1, pulse_id=101)]
    with pytest.raises(life.ReviewRequired, match='activity on the parent'):
        activity.capture(monday, request())


def test_ordinary_later_parent_edits_do_not_block_the_deleted_subitem():
    monday = Monday()
    monday.histories[life.PARENT_BOARD_ID] = [event('parent-change', 'update_column_value', seconds=1, pulse_id=101)]
    assert activity.capture(monday, request())['deletion_event']['id'] == 'deletion-1'


def removal_event():
    return event('parent-removal', 'update_column_value', seconds=1, pulse_id=101,
                 board_id=int(life.PARENT_BOARD_ID), column_id='subitems__1', column_type='subtasks',
                 previous_value={'linkedPulseIds': [{'linkedPulseId': 201}, {'linkedPulseId': 202}]},
                 value={'linkedPulseIds': [{'linkedPulseId': 202}]})


def test_parent_membership_removal_confirms_deletion_and_remains_in_audit_history():
    monday = Monday()
    removed = removal_event()
    monday.histories[life.PARENT_BOARD_ID] = [removed]
    proof = activity.capture(monday, request())
    assert proof['histories'][life.PARENT_BOARD_ID] == [removed]


@pytest.mark.parametrize('fault', ['readded', 'added_other', 'wrong_column', 'wrong_board', 'wrong_parent',
                                  'malformed', 'duplicate', 'undo', 'other_reference', 'later_restore'])
def test_only_unambiguous_parent_membership_removal_is_allowed(fault):
    monday = Monday()
    row = removal_event()
    data = json.loads(row['data'])
    if fault == 'readded':
        data['value']['linkedPulseIds'].append({'linkedPulseId': 201})
    elif fault == 'added_other':
        data['value']['linkedPulseIds'].append({'linkedPulseId': 203})
    elif fault == 'wrong_column':
        data['column_id'] = 'connect_boards'
    elif fault == 'wrong_board':
        data['board_id'] = 123
    elif fault == 'wrong_parent':
        data['pulse_id'] = 999
    elif fault == 'malformed':
        data['value'] = None
    elif fault == 'duplicate':
        data['previous_value']['linkedPulseIds'].append({'linkedPulseId': 201})
    elif fault == 'undo':
        data['is_undo_action'] = True
    elif fault == 'other_reference':
        data['other_item'] = 201
    row['data'] = json.dumps(data)
    monday.histories[life.PARENT_BOARD_ID] = [row]
    if fault == 'later_restore':
        monday.histories[life.SUBITEM_BOARD_ID].insert(0, event('restore', 'restore_pulse', seconds=2))
    with pytest.raises(life.ReviewRequired):
        activity.capture(monday, request())


def test_unidentifiable_later_activity_cannot_be_silently_ignored():
    monday = Monday()
    unknown = event('later', 'restore_pulse', seconds=1)
    unknown['data'] = '{}'
    monday.histories[life.SUBITEM_BOARD_ID].insert(0, unknown)
    with pytest.raises(life.ReviewRequired, match='ambiguous or unexpected item identity'):
        activity.capture(monday, request())


@pytest.mark.parametrize('state', ['active', 'archived', 'deleted'])
def test_current_item_response_disables_activity_exception(state):
    monday = Monday()
    monday.rows['201'] = {**parent(), 'id': '201', 'state': state,
                          'board': {'id': life.SUBITEM_BOARD_ID}, 'parent_item': {'id': '999'}}
    with pytest.raises(life.ReviewRequired, match='current Monday record'):
        activity.capture(monday, request())


@pytest.mark.parametrize('fault', ['missing_parent', 'wrong_parent_board', 'parent_archived', 'target_member'])
def test_unverifiable_parent_membership_stops_recovery(fault):
    monday = Monday()
    if fault == 'missing_parent':
        monday.rows.clear()
    elif fault == 'wrong_parent_board':
        monday.rows['101']['board']['id'] = '123'
    elif fault == 'parent_archived':
        monday.rows['101']['state'] = 'archived'
    else:
        monday.rows['101']['subitems'][0]['id'] = '201'
    with pytest.raises(life.ReviewRequired):
        activity.capture(monday, request())


def test_membership_drift_between_checks_stops_recovery():
    monday = Monday()
    execute = monday.execute_query
    def change_after_read(query, variables):
        result = execute(query, variables)
        if query == activity.ACTIVITY_QUERY:
            monday.rows['101']['subitems'] = []
        return result
    monday.execute_query = change_after_read
    with pytest.raises(life.ReviewRequired, match='membership changed'):
        activity.capture(monday, request())


@pytest.mark.parametrize('fault', ['errors', 'missing_board', 'archived_board', 'null_logs',
                                  'bad_json', 'bad_timestamp', 'duplicate', 'missing_deletion'])
def test_unreadable_or_incomplete_history_cannot_authorize_deletion(fault):
    monday = Monday()
    execute = monday.execute_query
    def broken(query, variables):
        result = execute(query, variables)
        if query != activity.ACTIVITY_QUERY:
            return result
        board = result['data']['boards'][0]
        if fault == 'errors':
            result['errors'] = [{'message': 'partial permission failure'}]
        elif fault == 'missing_board':
            result['data']['boards'] = []
        elif fault == 'archived_board':
            board['state'] = 'archived'
        elif fault == 'null_logs':
            board['activity_logs'] = None
        elif board['activity_logs']:
            row = board['activity_logs'][0]
            if fault == 'bad_json':
                row['data'] = 'not json'
            elif fault == 'bad_timestamp':
                row['created_at'] = 'yesterday'
            elif fault == 'duplicate':
                board['activity_logs'].append(deepcopy(row))
            elif fault == 'missing_deletion':
                board['activity_logs'] = []
        return result
    monday.execute_query = broken
    with pytest.raises(life.ReviewRequired):
        activity.capture(monday, request())


def test_history_exhausts_pages_and_rejects_limit_or_reordered_pages(monkeypatch):
    monkeypatch.setattr(activity, 'PAGE_SIZE', 1)
    monday = Monday()
    monday.histories[life.SUBITEM_BOARD_ID].append(event('created', 'create_pulse', seconds=-1))
    activity.capture(monday, request())
    pages = [v['page'] for q, v in monday.calls if q == activity.ACTIVITY_QUERY and v['board'] == life.SUBITEM_BOARD_ID]
    assert pages == [1, 2, 3]
    monkeypatch.setattr(activity, 'MAX_PAGES', 2)
    with pytest.raises(life.ReviewRequired, match='page limit'):
        activity.capture(monday, request())
    monday.histories[life.SUBITEM_BOARD_ID].reverse()
    with pytest.raises(life.ReviewRequired, match='pagination order'):
        activity.capture(monday, request())


def test_reviewed_event_cannot_be_replaced_by_a_changed_event():
    monday = Monday()
    reviewed = activity.capture(monday, request())['request']
    monday.histories[life.SUBITEM_BOARD_ID][0]['user_id'] = 'different-user'
    with pytest.raises(life.ReviewRequired, match='differs from the reviewed event'):
        activity.capture(monday, reviewed)


def test_cli_activity_options_are_explicit_and_single_subitem_only():
    from types import SimpleNamespace
    args = SimpleNamespace(review_csv=None, activity_log_id='log', activity_log_from='2026-09-01T00:00:00Z', parent_id='101')
    assert cli.activity_request(args, [(life.SUBITEM_BOARD_ID, '201')])['item_id'] == '201'
    for targets in [[(life.PARENT_BOARD_ID, '101')], [(life.SUBITEM_BOARD_ID, '201'), (life.SUBITEM_BOARD_ID, '202')]]:
        with pytest.raises(ValueError):
            cli.activity_request(args, targets)
    args.activity_log_id = None
    with pytest.raises(ValueError):
        cli.activity_request(args, [(life.SUBITEM_BOARD_ID, '201')])


def test_activity_transport_only_adds_the_exact_reviewed_read_query():
    client = activity.LifecycleMondayClient(api_key='unit-test-only')
    client._execute_read = Mock(return_value={'data': {}})
    try:
        client.execute_query(activity.ACTIVITY_QUERY, {})
        client._execute_read.assert_called_once()
        for query in ['mutation { delete_item(item_id: 201) { id } }', 'query Other { boards { id } }',
                      activity.ACTIVITY_QUERY.replace('activity_logs', 'other_field')]:
            with pytest.raises(ValueError, match='only accepts'):
                client.execute_query(query, {})
        assert client._execute_read.call_count == 1
    finally:
        client.session.close()
