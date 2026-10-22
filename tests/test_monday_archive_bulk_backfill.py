"""Offline checks for the bulk backfill's transport, batches and source matching."""
from copy import deepcopy
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


def formula_source(amount, field='project_value'):
    return {'projects': {'101': {'updated_at': 'unchanged', 'column_values': [{
        'id': compare.PARENT_COLUMNS[field], '__typename': 'FormulaValue',
        'display_value': amount}]}}}


FORMULA_CONTRACT = {'projects': {'project_value': {'type': 'numeric', 'scale': 2}}}


@pytest.mark.parametrize('first,second', [
    ('241502.0100000001', '241502.01'),
    ('877140.0599999999', '877140.06'),
    ('12.345', '12.35'),  # Same half-up rounding as persisted numeric columns.
])
def test_parent_formula_float_noise_matches_without_changing_raw_evidence(first, second):
    left, right = formula_source(first), formula_source(second)
    before = deepcopy(left)
    assert bulk.source_digest(left, FORMULA_CONTRACT) == bulk.source_digest(right, FORMULA_CONTRACT)
    assert left == before and right == formula_source(second)


@pytest.mark.parametrize('first,second', [
    ('241502.01', '241502.02'), ('12.3449', '12.345'),
    ('NaN', '0'), ('Infinity', '0'), ('#ERROR!', '0'), ('', '0'),
    (None, '0'), ('1,000.00', '1000.00'),
])
def test_parent_formula_real_changes_and_unreadable_values_remain_different(first, second):
    assert (bulk.source_digest(formula_source(first), FORMULA_CONTRACT)
            != bulk.source_digest(formula_source(second), FORMULA_CONTRACT))


@pytest.mark.parametrize('column', [
    {}, {'type': 'numeric', 'scale': None}, {'type': 'text', 'scale': 2},
    {'type': 'numeric', 'scale': 2, 'generated': 'ALWAYS'},
])
def test_formula_comparison_requires_a_stored_numeric_column_with_known_scale(column):
    contract = {'projects': {'project_value': column}}
    assert (bulk.source_digest(formula_source('12.0000000001'), contract)
            != bulk.source_digest(formula_source('12'), contract))


def test_parent_formula_comparison_uses_the_declared_scale():
    contract = {'projects': {'project_value': {'type': 'numeric', 'scale': 4}}}
    assert (bulk.source_digest(formula_source('12.0001'), contract)
            != bulk.source_digest(formula_source('12.0002'), contract))


@pytest.mark.parametrize('change', ['updated_at', 'unmapped_formula', 'mirror', 'subitems', 'hidden_items'])
def test_other_evidence_and_contributing_formula_values_stay_exact(change):
    left = formula_source('12')
    if change in ('subitems', 'hidden_items'):
        left[change] = {'201': {'column_values': [{
            'id': 'formula', '__typename': 'FormulaValue', 'display_value': '12.0000000001'}]}}
    right = deepcopy(left)
    if change == 'updated_at':
        right['projects']['101']['updated_at'] = 'changed'
    elif change == 'unmapped_formula':
        left['projects']['101']['column_values'][0]['id'] = 'unmapped'
        right['projects']['101']['column_values'][0].update(id='unmapped', display_value='12.0000000001')
    elif change == 'mirror':
        left['projects']['101']['column_values'][0]['__typename'] = 'MirrorValue'
        right['projects']['101']['column_values'][0].update(__typename='MirrorValue', display_value='12.0000000001')
    else:
        right[change]['201']['column_values'][0]['display_value'] = '12'
    assert bulk.source_digest(left, FORMULA_CONTRACT) != bulk.source_digest(right, FORMULA_CONTRACT)
