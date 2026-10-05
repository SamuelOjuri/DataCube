"""Diagnostic probes cannot traverse boards or connect to Supabase."""
import json
from types import SimpleNamespace

import pytest

from scripts import diagnose_order_value_monday as diagnostic
from scripts import order_value_monday_compare as compare
from test_order_value_monday_compare import no_live
from test_order_value_monday_compare_reads import FakeResponse, SERVER_ERRORS


def client(responses, calls):
    def post(url, **kwargs):
        payload = kwargs['json']
        calls.append(payload)
        assert payload['variables']['ids'] == ['1770216092']
        assert 'mutation' not in payload['query'] and 'items_page' not in payload['query']
        return responses.pop(0)
    return SimpleNamespace(session=SimpleNamespace(post=post), api_url='https://api.monday.com/v2',
                           headers={'Authorization': 'TEST_SECRET_NEVER_SAVED', 'API-Version': '2025-07'})


def test_diagnostics_preserve_failure_and_request_without_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(compare.psycopg, 'connect', lambda *a, **k: pytest.fail('No database access permitted'))
    calls = []
    monday = client([FakeResponse({'data': {'items': [{'id': '1770216092'}]}}),
                     FakeResponse({'errors': SERVER_ERRORS, 'extensions': {'request_id': 'abc-123'}})], calls)
    out = tmp_path / 'diagnostic'
    results = diagnostic.diagnose(monday, '1770216092', out, ['metadata', 'combined_typed'])
    assert [r['success'] for r in results] == [True, False]
    assert results[1]['request_id'] == 'abc-123'
    assert len(calls) == 2
    for path in out.glob('*.json'):
        assert 'TEST_SECRET_NEVER_SAVED' not in path.read_text()
    saved = json.loads((out / 'combined_typed.json').read_text())
    assert saved['response']['errors'] == SERVER_ERRORS
    assert saved['query'] == calls[1]['query']


def test_diagnostics_stop_at_rate_limit_without_retry(tmp_path):
    calls = []
    monday = client([FakeResponse({'errors': [{'extensions': {'code': 'IP_RATE_LIMIT_EXCEEDED',
                                                               'retry_in_seconds': 120}}]})], calls)
    results = diagnostic.diagnose(monday, '1770216092', tmp_path / 'diagnostic')
    assert len(calls) == len(results) == 1


def test_diagnostics_do_not_overwrite_evidence(tmp_path):
    with pytest.raises(ValueError, match='new diagnostic'):
        diagnostic.diagnose(None, '1770216092', tmp_path)


def test_every_probe_is_bounded_and_has_unique_name():
    probes = diagnostic.probes()
    assert len(probes) == 15 and len({p[0] for p in probes}) == 15
    for _, selection, columns in probes:
        query = diagnostic.make_query(selection, columns)
        assert query.startswith('query CompareMonday(')
        assert 'items(ids: $ids' in query and 'exclude_nonactive: false' in query
        assert 'boards(' not in query and 'mutation' not in query
        assert ('$columns:' in query) == bool(columns)
