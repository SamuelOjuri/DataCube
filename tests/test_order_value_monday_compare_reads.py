"""Monday service failures, bounded retry and read-only Stage diagnostics."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from scripts import order_value_monday_compare as compare
from test_order_value_monday_compare import fixture_data, no_live


SERVER_ERRORS = [{'message': 'Internal Server Error', 'extensions': {'code': 'INTERNAL_SERVER_ERROR'}},
                 {'message': 'Internal server error', 'extensions': {
                     'code': 'DOWNSTREAM_SERVICE_ERROR', 'status_code': 500,
                     'error_code': 'INTERNAL_SERVER_ERROR'}}]


@pytest.fixture(autouse=True)
def no_wait(monkeypatch):
    delays = []
    monkeypatch.setattr(compare.time, 'sleep', delays.append)
    return delays


class FakeResponse:
    def __init__(self, body, status=200, headers=None):
        self.body = body
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
        return deepcopy(self.body)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def transport(monkeypatch, responses):
    client = compare.ComparisonMondayClient(api_key='offline-test-key')
    calls = []
    def post(*args, **kwargs):
        calls.append(kwargs['json']['variables']['ids'])
        assert kwargs['timeout'] == (10, 45)
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response
    monkeypatch.setattr(client.session, 'post', post)
    return client, calls


def test_exact_reported_http200_errors_retry_and_discard_partial_data(monkeypatch, no_wait):
    evidence, *_ = fixture_data()
    parent = evidence['projects']['101']
    partial = {**deepcopy(parent), 'name': 'MUST NOT USE ERROR DATA'}
    client, calls = transport(monkeypatch, [
        FakeResponse({'errors': SERVER_ERRORS, 'data': {'items': [partial]}}),
        FakeResponse({'data': {'items': [parent]}})])
    result = compare.fetch_items(client, ['101'], ['column'], parents=True)
    assert result['101']['name'] == parent['name']
    assert calls == [['101'], ['101']]
    assert no_wait == [2]


@pytest.mark.parametrize('status', [500, 502, 503, 504])
def test_non_json_http_server_failure_retries(monkeypatch, no_wait, status):
    client, calls = transport(monkeypatch, [FakeResponse(ValueError('HTML gateway error'), status),
                                           FakeResponse({'data': {'items': []}})])
    assert compare.fetch_items(client, ['101'], ['column']) == {}
    assert calls == [['101'], ['101']] and no_wait == [2]


def test_rate_limit_honors_greater_header_or_graphql_hint(monkeypatch, no_wait):
    body = {'errors': [{'extensions': {'code': 'IP_RATE_LIMIT_EXCEEDED', 'retry_in_seconds': 5}}]}
    client, calls = transport(monkeypatch, [FakeResponse(body, headers={'Retry-After': '7'}),
                                           FakeResponse({'data': {'items': []}})])
    compare.fetch_items(client, ['101'], ['column'])
    assert no_wait == [7] and len(calls) == 2


def test_retry_after_http_date_and_nested_error_data():
    future = datetime.now(timezone.utc) + timedelta(seconds=20)
    delay = compare.retry_delay({'errors': [{'extensions': {'error_data': {'retry_in_seconds': 2}}}]},
                                {'Retry-After': format_datetime(future)})
    assert 18 <= delay <= 20


def test_long_cooldown_stops_without_early_retry_or_split(monkeypatch, no_wait):
    body = {'errors': [{'extensions': {'code': 'IP_RATE_LIMIT_EXCEEDED', 'retry_in_seconds': 120}}]}
    client, calls = transport(monkeypatch, [FakeResponse(body)])
    with pytest.raises(ValueError, match='longer cooldown.*120'):
        compare.fetch_items(client, ['101', '102'], ['column'])
    assert len(calls) == 1 and no_wait == []


def test_exhausted_throttling_never_splits(monkeypatch, no_wait):
    responses = [FakeResponse({}, 429, {'Retry-After': '3'}) for _ in range(3)]
    client, calls = transport(monkeypatch, responses)
    with pytest.raises(ValueError, match='after 3 attempts'):
        compare.fetch_items(client, ['101', '102'], ['column'])
    assert calls == [['101', '102']] * 3
    assert no_wait == [3, 5]


@pytest.mark.parametrize('status,errors', [
    (401, []), (403, []), (400, []),
    (200, [{'extensions': {'code': 'GRAPHQL_VALIDATION_FAILED'}}]),
    (200, SERVER_ERRORS + [{'extensions': {'code': 'USER_ACCESS_DENIED'}}]),
])
def test_permanent_or_mixed_failure_never_retries(monkeypatch, no_wait, status, errors):
    client, calls = transport(monkeypatch, [FakeResponse({'errors': errors}, status)])
    with pytest.raises(ValueError, match='no partial data accepted'):
        compare.fetch_items(client, ['101', '102'], ['column'])
    assert len(calls) == 1 and not no_wait


def test_persistent_server_error_splits_only_failing_batch(no_wait):
    calls = []
    class SizeSensitiveMonday:
        def execute_query(self, query, variables):
            batch = variables['ids']
            calls.append(batch)
            if len(batch) > 1:
                return {'errors': SERVER_ERRORS, 'data': {'items': [{'id': 'DO_NOT_KEEP'}]}}
            # Absence is a valid complete response, unlike an error response.
            return {'data': {'items': []}}
    assert compare.fetch_items(SizeSensitiveMonday(), ['101', '102'], ['column']) == {}
    assert calls == [['101', '102']] * 3 + [['101'], ['102']]
    assert no_wait == [2, 5]


def test_failed_singleton_stops_capture_and_reports_request_id(no_wait):
    calls = []
    class BrokenMonday:
        def execute_query(self, query, variables):
            calls.append(variables['ids'])
            return {'errors': SERVER_ERRORS, 'extensions': {'request_id': 'monday-request-123'}}
    with pytest.raises(ValueError, match=r"IDs \['101'\].*request_id=monday-request-123"):
        compare.fetch_items(BrokenMonday(), ['101', '102'], ['column'])
    # Does not proceed to remaining projects, fabricate absence or hide the error.
    assert calls == [['101', '102']] * 3 + [['101']] * 3
    assert no_wait == [2, 5, 2, 5]


def test_split_rejects_a_response_for_the_wrong_half():
    class WrongMonday:
        def execute_query(self, query, variables):
            if len(variables['ids']) > 1:
                return {'errors': SERVER_ERRORS}
            return {'data': {'items': [{'id': '102'}]}}
    with pytest.raises(ValueError, match='Unexpected Monday IDs'):
        compare.fetch_items(WrongMonday(), ['101', '102'], ['column'])


def test_split_request_budget_is_bounded(monkeypatch):
    monkeypatch.setattr(compare, 'MAX_BATCH_REQUESTS', 4)
    calls = []
    class BrokenMonday:
        def execute_query(self, query, variables):
            calls.append(variables['ids'])
            return {'errors': SERVER_ERRORS}
    with pytest.raises(ValueError, match='budget exhausted'):
        compare.fetch_items(BrokenMonday(), ['101', '102'], ['column'])
    assert len(calls) == 4


@pytest.mark.parametrize('exception', [requests.Timeout('timeout'), requests.ConnectionError('offline')])
def test_transport_failure_retries(monkeypatch, no_wait, exception):
    client, calls = transport(monkeypatch, [exception, FakeResponse({'data': {'items': []}})])
    compare.fetch_items(client, ['101'], ['column'])
    assert len(calls) == 2 and no_wait == [2]


def test_certificate_failure_is_not_retried(monkeypatch, no_wait):
    client, calls = transport(monkeypatch, [requests.exceptions.SSLError('certificate')])
    with pytest.raises(requests.exceptions.SSLError):
        compare.fetch_items(client, ['101'], ['column'])
    assert len(calls) == 1 and not no_wait


def test_comparison_transport_rejects_mutations_before_http(monkeypatch):
    client, calls = transport(monkeypatch, [])
    with pytest.raises(ValueError, match='only accepts'):
        client.execute_query('mutation { create_item { id } }')
    assert not calls


def test_only_flat_mirror_references_and_small_batches_are_queried():
    calls = []
    class EmptyMonday:
        def execute_query(self, query, variables):
            assert 'mirrored_value' not in query
            calls.append((query.count('mirrored_items'), len(variables['ids'])))
            return {'data': {'items': []}}
    monday = EmptyMonday()
    compare.fetch_items(monday, ['101'], ['x'], parents=True)
    compare.fetch_items(monday, ['201'], ['x'])
    compare.fetch_items(monday, ['301'], ['x'], mirror_depth=0)
    compare.fetch_items(monday, [str(i) for i in range(11)], ['x'])
    assert calls == [(1, 1), (1, 1), (0, 1), (1, 10), (1, 1)]


def test_stage_diagnostic_does_not_suggest_possible_commits():
    message = compare.failure_message('stage', Exception('unsafe connection secret'))
    assert 'read-only and made no Supabase changes' in message
    assert 'committed' not in message
    assert 'unsafe connection secret' not in message
    assert 'Earlier scopes may have committed' in compare.failure_message('apply', Exception())
    assert 'Verification is read-only' in compare.failure_message('verify', Exception())
