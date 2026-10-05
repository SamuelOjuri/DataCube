"""The batch selects only reviewed IDs and cannot perform broad queue work."""
from copy import deepcopy
from unittest.mock import Mock

import pytest

from scripts import monday_review_cleanup as cleanup
from src.services import monday_lifecycle as life
from test_order_value_monday_compare_flat import flat_data
from test_order_value_monday_compare import no_live


def test_exact_reviewed_selection_excludes_all_seven_other_records():
    rows = cleanup.targets()
    ids = {r['item_id'] for r in rows}
    assert len(ids) == 31 and len({r['parent_id'] for r in rows}) == 27
    assert '3182178903' in ids
    assert not ids & {'2916255740','2902522882','2858434576','2882589812','2892998514','2727533090','2857539042'}
    for row in rows:
        request = cleanup.recovery_request(row)
        assert request['parent_id'] == row['parent_id'] and request['log_id'] == row['log_id']
        assert cleanup.activity.utc_date(request['from']) < cleanup.activity.utc_date(row['deleted_at_utc'])


def test_enquiry_capture_reads_current_formula_even_without_a_parent_mirror():
    source, *_ = flat_data()
    calls = []
    class Monday:
        def execute_query(self, query, variables):
            calls.append(variables)
            index = {i: r for t in ('projects', 'subitems') for i, r in source[t].items()}
            rows = []
            for item_id in variables['ids']:
                row = deepcopy(index[item_id])
                row['column_values'] = [c for c in row['column_values'] if c['id'] in variables['columns']]
                rows.append(row)
            return {'data': {'items': rows}}
    evidence = cleanup.capture_enquiry(Monday(), '101')
    assert cleanup.compare.project_new_enquiry_total(evidence, evidence['projects']['101']) == 90
    assert calls[0]['columns'] == [cleanup.compare.PARENT_COLUMNS['pipeline_stage']]
    assert calls[1]['columns'] == [cleanup.compare.SUBITEM_COLUMNS['new_enquiry_value']]


def test_stored_parent_category_must_agree_with_live_monday_for_targeted_refresh():
    source, *_ = flat_data()
    parent = source['projects']['101']
    assert cleanup.compare.require_stored_enquiry_category(parent, {'status_category': 'Open'}) == 'Open'
    for stored in ({}, {'status_category': 'Won'}, {'status_category': 'OPEN'}):
        with pytest.raises(ValueError, match='status_category differs'):
            cleanup.compare.require_stored_enquiry_category(parent, stored)


@pytest.mark.parametrize('prefixes', [[], ['%'], ['recovery:bad_%:'], ['not-recovery:x:']])
def test_scoped_claim_rejects_wildcard_or_unbounded_selections(prefixes):
    connection = Mock()
    with pytest.raises(ValueError):
        life.claim(connection, event_prefixes=prefixes)
    connection.execute.assert_not_called()


def test_idle_scoped_worker_does_not_schedule_or_process_unrelated_work(monkeypatch):
    connection, monday = Mock(), Mock()
    claim = Mock(return_value=None)
    schedule = Mock(side_effect=AssertionError('No unrelated lifecycle scheduling'))
    monkeypatch.setattr(life, 'claim', claim)
    monkeypatch.setattr(life, 'schedule_rechecks', schedule)
    assert life.run_once(connection, monday, event_prefixes=['recovery:example:']) is False
    claim.assert_called_once_with(connection, event_prefixes=['recovery:example:'])
    schedule.assert_not_called()
