"""Offline checks for the bulk archive backfill's Monday transport and batch sizes."""
import json

import pytest

from scripts import monday_archive_bulk_backfill as bulk
from scripts import order_value_monday_compare as compare


class FakeResponse:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.posts = list(responses), []

    def post(self, url, json=None, headers=None, timeout=None):
        self.posts.append(json)
        return self.responses.pop(0)

    def close(self):
        pass


def ok(items=(), limits='"minuteRate";r=7000, "complexityMinute";r=9000000;t=40'):
    return FakeResponse({'data': {'items': list(items)}}, headers={'RateLimit': limits})


def failure(code, retry_in=None, status=200):
    extensions = {'code': code}
    if retry_in is not None:
        extensions['retry_in_seconds'] = retry_in
    return FakeResponse({'errors': [{'message': code, 'extensions': extensions}]}, status=status)


@pytest.fixture
def client():
    def make(responses, max_calls=10):
        sleeps, now = [], [0.0]

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds
        c = bulk.PacedMondayClient(max_calls, api_key='test-token', sleep=sleep, clock=lambda: now[0])
        c.session = FakeSession(responses)
        return c, sleeps
    return make


QUERY = 'query CompareMonday($ids: [ID!]!, $columns: [String!]!) { items(ids: $ids) { id } }'


def test_reads_record_rate_limits(client):
    c, sleeps = client([ok()])
    assert c.execute_query(QUERY, {'ids': ['1'], 'columns': []}) == {'data': {'items': []}}
    assert c.calls == 1 and c.min_complexity == 9000000 and not sleeps


@pytest.mark.parametrize('code', ['COMPLEXITY_BUDGET_EXHAUSTED', 'RATE_LIMIT_EXCEEDED', 'maxConcurrencyExceeded'])
def test_long_cooldowns_are_waited_out_not_fatal(client, code):
    # The old runner stopped the whole campaign for any cooldown over 30 seconds.
    c, sleeps = client([failure(code, retry_in=45), ok()])
    assert c.execute_query(QUERY, {'ids': ['1'], 'columns': []})['data'] == {'items': []}
    assert c.calls == 2 and c.throttled == 1 and 45 in sleeps


def test_http_429_without_a_hint_backs_off(client):
    c, sleeps = client([FakeResponse({}, status=429), ok()])
    c.execute_query(QUERY, {'ids': ['1'], 'columns': []})
    assert sleeps and min(s for s in sleeps if s >= 5) == 5


def test_daily_limit_stops_the_run_without_retrying(client):
    c, sleeps = client([failure('DAILY_LIMIT_EXCEEDED', status=429), ok()])
    with pytest.raises(bulk.DailyLimitReached):
        c.execute_query(QUERY, {'ids': ['1'], 'columns': []})
    assert c.calls == 1 and not [s for s in sleeps if s > bulk.MIN_INTERVAL]


def test_call_budget_stops_before_the_next_call(client):
    c, _ = client([ok(), ok()], max_calls=1)
    c.execute_query(QUERY, {'ids': ['1'], 'columns': []})
    with pytest.raises(bulk.CallBudgetReached):
        c.execute_query(QUERY, {'ids': ['2'], 'columns': []})
    assert len(c.session.posts) == 1


def test_low_complexity_budget_pauses_before_the_next_call(client):
    c, sleeps = client([ok(limits='"complexityMinute";r=1000;t=30'), ok()])
    c.execute_query(QUERY, {'ids': ['1'], 'columns': []})
    c.execute_query(QUERY, {'ids': ['2'], 'columns': []})
    assert 30 in sleeps


@pytest.mark.parametrize('response', [failure('FORBIDDEN', status=403), failure('RATE_LIMIT_EXCEEDED', retry_in=3600)])
def test_permanent_errors_and_excessive_cooldowns_are_not_retried(client, response):
    c, sleeps = client([response, ok()])
    with pytest.raises(compare.MondayReadError):
        c.execute_query(QUERY, {'ids': ['1'], 'columns': []})
    assert c.calls == 1 and not [s for s in sleeps if s > bulk.MIN_INTERVAL]


class Recorder:
    def __init__(self):
        self.calls = []

    def execute_query(self, query, variables):
        self.calls.append(list(variables['ids']))
        return {'data': {'items': [
            {'id': i, 'name': i, 'state': 'active', 'updated_at': 'x', 'board': {'id': '1'},
             'parent_item': None, 'column_values': []} for i in variables['ids']]}}


def test_fetch_items_keeps_ten_ids_by_default_and_accepts_up_to_one_hundred():
    ids = [str(n) for n in range(1000, 1250)]
    default, large = Recorder(), Recorder()
    assert len(compare.fetch_items(default, ids, [])) == 250
    assert len(compare.fetch_items(large, ids, [], batch_size=100)) == 250
    assert [len(c) for c in default.calls] == [10] * 25
    assert [len(c) for c in large.calls] == [100, 100, 50]
    with pytest.raises(ValueError, match='1-100'):
        compare.fetch_items(Recorder(), ids, [], batch_size=101)
