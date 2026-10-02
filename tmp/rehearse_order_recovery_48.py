"""Offline rehearsal against supplied snapshots. Never connects to either service."""
from contextlib import contextmanager
from copy import deepcopy
import csv
import json
import logging
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from scripts import backfill_order_values as backfill
from scripts import order_value_scopes as scopes
from scripts import order_value_scopes_refresh as refresh
from scripts import order_value_scopes_targeted as targeted
from scripts import reconcile_order_values as reconcile

logging.getLogger('src.database.sync_service').setLevel(logging.WARNING)

DATA = Path('outputs/order_value_backfill/attached_scope_assessment_20261002')
PREVIOUS = Path('outputs/order_value_backfill/targeted_orders_20261002_092604')
SELECTED = refresh.project_ids_from_file(Path('docs/order-value-recovery-48-projects.txt'))
previous, previous_plan = targeted.load_run(PREVIOUS)
parents, children, hidden = {}, {}, {}
database = {t: {} for t in scopes.TABLES}
for path in DATA.glob('*_monday_response.json'):
    source = json.loads(path.read_text())['data']
    parents.update(backfill.indexed(source['projects'], 'id'))
    children.update(backfill.indexed(source['knownSubitems'], 'id'))
    hidden.update(backfill.indexed(source['knownHiddenSources'], 'id'))
for path in DATA.glob('*_database_report.json'):
    report = json.loads(path.read_text())
    for table, label in [('projects', 'projects'), ('subitems', 'subitems'), ('hidden_items', 'hidden_sources')]:
        for row in report['current_' + label]:
            database[table][row['monday_id']] = {k: row[k] for k in backfill.BASELINE_COLUMNS[table]}
            # PostgreSQL's JSON report loses numeric scale. The real database
            # reader returns Decimal values serialized as strings with that scale.
            for field, column in previous_plan['contract'][table].items():
                if field in database[table][row['monday_id']] and column['type'] == 'numeric':
                    database[table][row['monday_id']][field] = backfill.money(row[field])
baseline = {t: scopes.stable_rows(list(rows.values())) for t, rows in database.items()}
index = scopes.index_baseline(baseline)


def metadata(item):
    parent = item.get('parent_item')
    return {'id': item['id'], 'state': item['state'], 'board': {'id': item['board']['id']},
            'parent_item': None if parent is None else {'id': parent['id'], 'state': parent['state'],
                                                      'board': {'id': parent['board']['id']}}}


class SnapshotMonday:
    def __init__(self):
        self.calls = []

    def execute_query(self, query, variables):
        self.calls.append(deepcopy(variables))
        assert 'items_page' not in query and 'mutation' not in query
        rows = []
        for item_id in variables['ids']:
            if 'InventoryDetails' in query:
                parent = parents[item_id]
                rows.append({**metadata(parent), 'subitems': [metadata(child) for child in parent['subitems']]})
            else:
                item = children[item_id] if item_id in children else hidden[item_id]
                rows.append({**metadata(item), 'name': item['name'], 'column_values': [
                    {k: v for k, v in col.items() if k != 'column' and k != 'linked_items'}
                    for col in item['column_values'] if col['id'] in variables['columns']]})
        return {'data': {'items': deepcopy(rows)}}


class Connection:
    @contextmanager
    def transaction(self):
        yield

    def execute(self, query):
        assert 'READ ONLY' in query


def forbidden(*a, **kw):
    raise AssertionError('Live I/O is forbidden in this rehearsal')


monday = SnapshotMonday()
output = Path('tmp') / ('order-recovery-rehearsal-' + uuid4().hex)
with patch.object(backfill, 'target_fingerprint', return_value=previous['target']), \
        patch.object(scopes, 'schema_safety', return_value={'offline_rehearsal': True}), \
        patch.object(reconcile, 'read_contract', return_value=previous_plan['contract']), \
        patch.object(scopes, 'read_boundary', side_effect=lambda c, b: scopes.select_boundary(index, b)), \
        patch.object(refresh.psycopg, 'connect', side_effect=forbidden), \
        patch('requests.sessions.Session.request', side_effect=forbidden), \
        patch('socket.socket.connect', side_effect=forbidden):
    manifest = refresh.stage_run(Connection(), monday, PREVIOUS, output, SELECTED)
    _, staged = targeted.load_run(output)
# Deliberately make this simulated run ineligible for apply, even if assertions fail.
(output / 'manifest.json').rename(output / 'SIMULATION-manifest.json')
assert manifest['projects'] == 48 and manifest['scopes'] == 2 and manifest['changes'] == 502
assert manifest['deferred_scopes'] == 0
with (DATA / 'proposed_order_differences.csv').open(newline='') as stream:
    expected = {(r['table'], r['monday_id'], r['field'], r['before'], r['after']) for r in csv.DictReader(stream)}
with (output / 'changes.csv').open(newline='') as stream:
    actual = {(r['table'], r['monday_id'], r['field'], r['before'], r['after']) for r in csv.DictReader(stream)}
assert actual == expected
assert set(SELECTED) == {pid for r in staged['scopes'] for pid in r['scope']['projects']}
print(json.dumps({'offline_rehearsal': True, 'projects': manifest['projects'], 'scopes': manifest['scopes'],
                  'changes': manifest['changes'], 'exact_match_to_assessed_differences': actual == expected,
                  'simulated_exact_id_queries': len(monday.calls), 'directory': str(output)}, indent=2))
