"""Exploration context: column labels and meanings reach the model, within the prompt budget."""
from datetime import date
import json
import os

import psycopg
import pytest

from bi_analyst.exploration.context import COLUMN_NOTES, TYPES, approved_relations, build_context
from bi_analyst.metrics.compiler import Compiler

CATALOGUE = Compiler().catalogue
RELATIONS = approved_relations(CATALOGUE)
LABEL_CHECK_ENV_FILE = os.environ.get('BI_ANALYST_LABEL_CHECK_ENV_FILE')


def context():
    return build_context(CATALOGUE, RELATIONS, today=date(2026, 10, 12), timezone='Europe/London')


def test_column_notes_name_approved_catalogue_columns():
    relations = {r.id: r for r in CATALOGUE.relations}
    for relation, notes in COLUMN_NOTES.items():
        assert relation in RELATIONS
        assert set(notes) <= relations[relation].columns.keys(), relation


def test_context_carries_labels_meanings_and_definition_semantics():
    ctx = context()
    views = {v['name'].removeprefix('analyst_query.'): v for v in ctx['views']}
    projects = views['projects_v1']['columns']
    assert projects['type'] == {'type': 'text', 'meaning': projects['type']['meaning'], 'values': TYPES}
    assert 'Refurbishment' not in projects['category']['values']
    assert {'Lost', 'Open Enquiry'} <= set(projects['pipeline_stage']['values'])
    assert 'not yet invoiced' in projects['gestation_period']['meaning']
    assert 'first invoice is dated before the first design' in projects['gestation_period']['meaning']
    assert projects['monday_id'] == {'type': 'text'}
    assert views['conversion_cohorts_v1']['columns']['cohort_years']['values'] == [2, 5]
    assert {d['column'] for d in ctx['dimensions']} == {'category', 'type'}
    gestation = next(d for d in ctx['definitions'] if d['id'] == 'gestation_five_year')
    assert any('first design completion to first invoice' in line for line in gestation['meaning'])
    assert gestation['periods'] == ['five_year_cohort'] and gestation['dimensions'] == ['category', 'type']
    assert 'NULL when no known values.' in gestation['rules']


def test_context_leaves_prompt_budget_for_follow_ups_and_repairs():
    # GeminiProvider rejects prompts above 64,000 bytes, including previous SQL and repair feedback.
    assert len(json.dumps(context(), ensure_ascii=True).encode()) < 40000


def test_listed_labels_match_live_source_values():
    if not LABEL_CHECK_ENV_FILE:
        pytest.skip('Set BI_ANALYST_LABEL_CHECK_ENV_FILE to compare listed labels with the live reader DSN')
    from dotenv import dotenv_values
    dsn = dotenv_values(LABEL_CHECK_ENV_FILE).get('BI_ANALYST_READ_DSN')
    if not dsn:
        pytest.skip('No BI_ANALYST_READ_DSN configured')
    drift = {}
    try:
        conn = psycopg.connect(dsn)
    except psycopg.Error as error:
        # Tracebacks would print the DSN and its password.
        pytest.fail(f'Live label check could not connect ({type(error).__name__}); relative sslrootcert paths '
                    'resolve from the working directory', pytrace=False)
    with conn:
        conn.read_only = True
        for relation, notes in COLUMN_NOTES.items():
            for column, note in notes.items():
                if 'values' not in note:
                    continue
                # Identifiers come from the module constants above, never from input.
                rows = conn.execute(f'SELECT DISTINCT "{column}" FROM analyst_query."{relation}" '
                                    f'WHERE "{column}" IS NOT NULL').fetchall()
                stored = {value for (value,) in rows}
                if column == 'reason_for_change':
                    stored = {label for value in stored for label in value.split(', ') if label}
                if stored != set(note['values']):
                    drift[f'{relation}.{column}'] = {'unlisted': sorted(map(str, stored - set(note['values']))),
                                                     'absent': sorted(map(str, set(note['values']) - stored))}
    assert not drift, drift
