"""Read-only Phase 2 parity against the sealed Phase 1 TEST dataset.

Expands checked-in view SELECTs as CTEs, mapped only to frozen sources. It does
not install migrations, update answers, use production, or import ETL startup.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

import psycopg

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'services/bi_analyst'))
sys.path.insert(0, str(ROOT / 'services/bi_analyst/tests/evals'))
from bi_analyst.semantic import load_catalogue
from dataset import REFERENCE_VERSIONS, equivalent, names, reference_version, restore_result_types, verify_snapshot
from manage import connect
from phase1 import typed_rows

MIGRATION = ROOT / 'src/database/migrations/20261007_001_analytics_contracts.sql'
FROZEN_SOURCES = {
    'reportable_projects': 'reportable_projects', 'subitems': 'subitems', 'hidden_items': 'hidden_items',
    'analysis_results': 'analysis_results', 'vw_actual_enquiry_monthly_v1': 'baseline_enquiry',
    'vw_actual_bookings_monthly_v1': 'baseline_bookings', 'vw_actual_revenue_monthly_v1': 'baseline_revenue',
}


def statements(path):
    parts = re.split(r'^-- name: ([a-z_]+)\s*$', path.read_text(encoding='utf-8'), flags=re.M)
    return dict(zip(parts[1::2], (q.strip().removesuffix(';') for q in parts[2::2])))


def view_definitions(contract_version='1.0.0'):
    if contract_version not in REFERENCE_VERSIONS:
        raise ValueError('Unsupported analytical contract version')
    migrations = [MIGRATION]
    if contract_version == '1.1.0':
        migrations.append(ROOT/'src/database/migrations/20261008_004_analyst_reportable_population.sql')
    definitions = {}
    for migration in migrations:
        definitions.update(re.findall(
            r'CREATE OR REPLACE VIEW analytics\.([a-z_0-9]+) WITH \([^\n]+\) AS\n(.*?);',
            migration.read_text(encoding='utf-8'), flags=re.S))
    if definitions.keys() != {r.id for r in load_catalogue().relations if not r.optional}:
        raise ValueError('Migration/catalogue relation mismatch')
    return definitions


def frozen_ctes(dataset, contract_version='1.0.0'):
    names(dataset)  # Strict versioned identifier allowlist before interpolation.
    definitions = view_definitions(contract_version)
    ctes = []
    for name, query in definitions.items():
        def source(match):
            original = match.group(1)
            if original not in FROZEN_SOURCES:
                raise ValueError('Unexpected source in Phase 2 migration')
            return f'{dataset}.{FROZEN_SOURCES[original]}'
        query = re.sub(r'public\.([a-z_0-9]+)', source, query)
        query = re.sub(r'analytics\.([a-z_0-9]+)', r'phase2_\1', query)
        query = query.replace('CURRENT_DATE', f'(SELECT as_of_date FROM {dataset}.context)')
        ctes.append(f'phase2_{name} AS ({query})')
    return ',\n'.join(ctes)


def expanded_query(dataset, query, contract_version='1.0.0'):
    query = re.sub(r'analytics\.([a-z_0-9]+)', r'phase2_\1', query)
    # Nest the acceptance query to preserve its own WITH and ORDER BY clauses.
    return f'WITH {frozen_ctes(dataset, contract_version)} SELECT * FROM ({query}) phase2_result'


def compare_results(actual, expected):
    types_match = actual['columns'] == expected['columns']
    return {'matches': types_match and equivalent(typed_rows(actual), typed_rows(expected)),
            'types_match': types_match, 'actual_rows': len(actual['rows']), 'expected_rows': len(expected['rows'])}


def verify(connection, dataset, identity):
    schema, key, reader = names(dataset)
    queries = statements(Path(__file__).with_name('parity.sql'))
    # Read sealed answers as the evaluator, then execute candidates as reader.
    # Never expose numerical cells in the output report.
    artifacts, _ = verify_snapshot(connection, identity, dataset)
    contract_version = reference_version(artifacts['manifest'])
    expected = artifacts['expected']
    connection.execute(f'SET LOCAL ROLE {reader}')
    connection.execute(f'SET LOCAL search_path TO {schema}, pg_catalog')
    results = {}
    for name, query in queries.items():
        cursor = connection.execute(expanded_query(schema, query, contract_version))
        actual = {'columns': [{'name': d.name, 'type_oid': d.type_code} for d in cursor.description],
                  'rows': cursor.fetchall()}
        results[name] = compare_results(actual, restore_result_types(expected[name]))
    return results, contract_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', default='bi_eval_20261007_v1')
    args = parser.parse_args()
    try:
        connection, identity = connect()  # Existing TEST-only target isolation.
        with connection:
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            connection.execute("SET LOCAL statement_timeout='60s'")
            connection.execute("SET LOCAL lock_timeout='3s'")
            connection.execute("SET LOCAL timezone='Europe/London'")
            results, contract_version = verify(connection, args.dataset, identity)
        print(json.dumps({'dataset': args.dataset, 'reference_contract_version': contract_version,
                          'checks': results, 'business_certification': 'pending'}, indent=2))
        return 0 if all(r['matches'] for r in results.values()) else 2
    except psycopg.Error as exc:
        print(f'Frozen parity failed (SQLSTATE {exc.sqlstate or "connection_error"}); details suppressed')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
