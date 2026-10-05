"""Archive metadata migration on disposable loopback databases; no live services."""
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import pytest

from src.services import monday_lifecycle as life
from test_order_value_scopes_postgres import database
from test_monday_lifecycle_postgres import db, no_http, Monday, item, job


MIGRATION = (Path(__file__).resolve().parents[1] / 'src' / 'database' / 'schema'
             / 'monday_lifecycle_archive_state.sql').read_text(encoding='utf-8')
STATE_COLUMNS = ['monday_state', 'state_verified_at', 'state_event_key', 'state_evidence']
OBSERVATION = ['archived', '2026-10-05T12:00:00+00:00', 'test-observation',
               {'item_id': '201', 'api_state': 'archived'}]


def snapshot(connection):
    result = {}
    for table in ('projects', 'subitems', 'hidden_items', 'monday_item_lifecycle',
                  'monday_lifecycle_events', 'monday_lifecycle_audit'):
        rows = connection.execute(sql.SQL(
            'SELECT to_jsonb(t) AS row FROM public.{} t ORDER BY to_jsonb(t)::text'
        ).format(sql.Identifier(table))).fetchall()
        result[table] = [
            {key: value for key, value in row['row'].items()
             if table != 'monday_item_lifecycle' or key not in STATE_COLUMNS}
            for row in rows
        ]
    return result


@pytest.fixture
def migrated(db):
    connection, dsn = db
    connection.execute(MIGRATION)
    connection.execute("SELECT public.monday_item_is_blocked('subitems','201')")
    return connection, dsn


def write_observation(connection, values):
    state, verified, event_key, evidence = values
    connection.execute(
        "UPDATE monday_item_lifecycle SET monday_state=%s,state_verified_at=%s,"
        "state_event_key=%s,state_evidence=%s WHERE table_name='subitems' AND monday_id='201'",
        (state, verified, event_key, Jsonb(evidence) if evidence is not None else None),
    )


def test_migration_is_repeatable_and_preserves_data_labels_guards_and_access(db):
    c, _ = db
    c.execute("UPDATE subitems SET order_status='Archived' WHERE monday_id='201'")
    deletion = job(c, item_id='299')
    life.process_job(c, Monday({'299': item('299')}), deletion)
    c.execute('CREATE VIEW reportable_projects AS SELECT * FROM projects')
    before = snapshot(c)
    definitions = c.execute(
        "SELECT oid,pg_get_functiondef(oid) AS definition FROM pg_proc "
        "WHERE proname IN ('guard_monday_deleted_item','guard_monday_cleanup_scope','monday_item_is_blocked')"
        " ORDER BY oid").fetchall()
    access = c.execute(
        "SELECT relname,relrowsecurity,relacl FROM pg_class "
        "WHERE oid IN ('monday_item_lifecycle'::regclass,'monday_lifecycle_events'::regclass,"
        "'monday_lifecycle_audit'::regclass) ORDER BY relname").fetchall()
    view = c.execute("SELECT pg_get_viewdef('reportable_projects'::regclass) AS definition").fetchone()

    c.execute(MIGRATION)
    c.execute(MIGRATION)

    assert snapshot(c) == before
    assert c.execute(
        "SELECT oid,pg_get_functiondef(oid) AS definition FROM pg_proc "
        "WHERE proname IN ('guard_monday_deleted_item','guard_monday_cleanup_scope','monday_item_is_blocked')"
        " ORDER BY oid").fetchall() == definitions
    assert c.execute(
        "SELECT relname,relrowsecurity,relacl FROM pg_class "
        "WHERE oid IN ('monday_item_lifecycle'::regclass,'monday_lifecycle_events'::regclass,"
        "'monday_lifecycle_audit'::regclass) ORDER BY relname").fetchall() == access
    assert c.execute("SELECT pg_get_viewdef('reportable_projects'::regclass) AS definition").fetchone() == view
    rows = c.execute("SELECT monday_state,state_verified_at,state_event_key,state_evidence "
                     "FROM monday_item_lifecycle").fetchall()
    assert rows and all(all(value is None for value in row.values()) for row in rows)
    assert c.execute("SELECT convalidated FROM pg_constraint WHERE conrelid='monday_item_lifecycle'::regclass "
                     "AND conname='monday_item_lifecycle_state_observation_check'").fetchone()['convalidated']
    index = c.execute("SELECT indisvalid,pg_get_expr(indpred,indrelid) AS predicate FROM pg_index "
                      "WHERE indexrelid='monday_item_lifecycle_archived_rechecks'::regclass").fetchone()
    assert index['indisvalid'] and "'archived'::text" in index['predicate']


@pytest.mark.parametrize('state', ['active', 'archived', 'deleted'])
def test_verified_state_is_stored_without_changing_business_rows_or_legacy_blocking(migrated, state):
    c, _ = migrated
    before = snapshot(c)
    values = [state, *OBSERVATION[1:3], {'item_id': '201', 'api_state': state}]
    write_observation(c, values)
    observed = c.execute("SELECT * FROM monday_item_lifecycle WHERE table_name='subitems' AND monday_id='201'").fetchone()
    assert observed['monday_state'] == state and observed['blocked'] is False
    c.execute("SELECT public.monday_item_is_blocked('subitems','201')")
    assert c.execute("SELECT * FROM monday_item_lifecycle WHERE table_name='subitems' AND monday_id='201'").fetchone() == observed
    c.execute(MIGRATION)
    assert c.execute("SELECT * FROM monday_item_lifecycle WHERE table_name='subitems' AND monday_id='201'").fetchone() == observed
    assert snapshot(c) == before


@pytest.mark.parametrize('mask', range(1, 15))
def test_partial_observations_are_rejected_including_sql_null_edge_cases(migrated, mask):
    c, _ = migrated
    values = [value if mask & (1 << index) else None for index, value in enumerate(OBSERVATION)]
    with pytest.raises(psycopg.errors.CheckViolation, match='state_observation_check'):
        write_observation(c, values)
    assert c.execute("SELECT monday_state FROM monday_item_lifecycle "
                     "WHERE table_name='subitems' AND monday_id='201'").fetchone()['monday_state'] is None


@pytest.mark.parametrize('state', ['archive', 'Archive', 'Archived', 'not_returned', 'unknown', ''])
def test_only_exact_api_states_are_allowed(migrated, state):
    c, _ = migrated
    with pytest.raises(psycopg.errors.CheckViolation):
        write_observation(c, [state, *OBSERVATION[1:]])


@pytest.mark.parametrize('evidence', [{}, [], 'archived', 1, True])
def test_evidence_must_be_a_nonempty_object(migrated, evidence):
    c, _ = migrated
    with pytest.raises(psycopg.errors.CheckViolation):
        write_observation(c, [*OBSERVATION[:3], evidence])


def test_json_null_is_not_valid_evidence(migrated):
    c, _ = migrated
    with pytest.raises(psycopg.errors.CheckViolation):
        c.execute("UPDATE monday_item_lifecycle SET monday_state='archived',"
                  "state_verified_at=now(),state_event_key='test',state_evidence='null'::jsonb")


@pytest.mark.parametrize('event_key', ['', '   '])
def test_blank_event_key_is_rejected(migrated, event_key):
    c, _ = migrated
    with pytest.raises(psycopg.errors.CheckViolation):
        write_observation(c, [*OBSERVATION[:2], event_key, OBSERVATION[3]])


@pytest.mark.parametrize('verified', ['infinity', '-infinity'])
def test_verification_requires_a_finite_timestamp(migrated, verified):
    c, _ = migrated
    with pytest.raises(psycopg.errors.CheckViolation):
        write_observation(c, [OBSERVATION[0], verified, *OBSERVATION[2:]])


@pytest.mark.parametrize('definition', ['integer', "text DEFAULT 'active'", 'text NOT NULL'])
def test_conflicting_preexisting_columns_abort_atomically(db, definition):
    c, _ = db
    c.execute(f'ALTER TABLE monday_item_lifecycle ADD COLUMN monday_state {definition}')
    with pytest.raises(psycopg.errors.RaiseException, match='Unexpected archive metadata'):
        c.execute(MIGRATION)
    c.rollback()
    names = {row['column_name'] for row in c.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='monday_item_lifecycle'").fetchall()}
    assert 'monday_state' in names and not (set(STATE_COLUMNS[1:]) & names)


def test_disabled_rls_stops_migration_before_schema_changes(db):
    c, _ = db
    c.execute('ALTER TABLE monday_item_lifecycle DISABLE ROW LEVEL SECURITY')
    with pytest.raises(psycopg.errors.RaiseException, match='RLS enabled'):
        c.execute(MIGRATION)
    c.rollback()
    assert not c.execute("SELECT 1 FROM information_schema.columns WHERE table_schema='public' "
                         "AND table_name='monday_item_lifecycle' AND column_name='monday_state'").fetchall()


def test_existing_deletion_worker_and_stale_write_guard_still_work(migrated):
    c, _ = migrated
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id,item_name) VALUES ('202','101','Same name')")
    deletion = job(c)
    life.process_job(c, Monday({'201': item('201')}), deletion)
    c.execute("INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('201','101'),('203','101')")
    assert not life.read_rows(c, 'subitems', 'monday_id', ['201'])
    assert len(life.read_rows(c, 'subitems', 'monday_id', ['202', '203'])) == 2
    marker = c.execute("SELECT blocked,monday_state FROM monday_item_lifecycle "
                       "WHERE table_name='subitems' AND monday_id='201'").fetchone()
    assert marker == {'blocked': True, 'monday_state': None}
    assert c.execute("SELECT count(*) AS n FROM monday_lifecycle_audit WHERE action='delete'").fetchone()['n'] == 1


def test_schema_preparation_does_not_enable_archive_processing(migrated):
    c, _ = migrated
    deletion = job(c)
    before = snapshot(c)
    with pytest.raises(life.ReviewRequired, match='archive is not a deletion'):
        life.process_job(c, Monday({'201': item('201', state='archived')}), deletion)
    assert snapshot(c) == before
