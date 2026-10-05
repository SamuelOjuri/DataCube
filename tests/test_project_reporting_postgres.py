"""Run only against an explicitly supplied isolated loopback PostgreSQL server."""
import json

import pytest
import requests
from psycopg.rows import dict_row

from scripts import project_reporting as migration
from scripts import project_placeholders as placeholders
from test_project_reporting import empty_source
from test_order_value_scopes_postgres import database


@pytest.fixture(autouse=True)
def no_http(monkeypatch):
    monkeypatch.setattr(requests.sessions.Session,'request',lambda *a,**k: pytest.fail('No live HTTP'))


@pytest.fixture
def db(database):
    c, dsn, _, _ = database
    c.row_factory = dict_row
    c.execute('ALTER TABLE projects ADD COLUMN project_name text, ADD COLUMN account text, '
        'ADD COLUMN pipeline_stage text, ADD COLUMN date_created date, ADD COLUMN product_key text, '
        'ADD COLUMN first_date_designed date, ADD COLUMN probability_percent integer')
    c.execute("INSERT INTO projects(monday_id,item_name,pipeline_stage,date_created) "
              "VALUES ('600','New project','Open Enquiry','2026-09-01'),('601','New project','Open Enquiry','2026-09-01')")
    c.execute("UPDATE projects SET project_name='FREE' WHERE monday_id='601'")
    c.execute('CREATE TABLE analysis_results(project_id text REFERENCES projects(monday_id) ON DELETE CASCADE, result jsonb)')
    c.execute("INSERT INTO analysis_results VALUES ('600','{}')")
    c.execute(migration.CORE.read_text())
    return c


def decide(c, item='600', classification='redundant_placeholder'):
    c.execute("INSERT INTO project_reporting_classifications(monday_id,classification,reason,reviewed_by) "
              "VALUES (%s,%s,'Reviewed exact ID','test reviewer') ON CONFLICT(monday_id) DO UPDATE "
              "SET classification=excluded.classification",(item,classification))


def ids(c, relation='reportable_projects'):
    # Only fixed test relation names are accepted here.
    assert relation in {'reportable_projects','projects'}
    return {r['monday_id'] for r in c.execute(f'SELECT monday_id FROM {relation}')}


def test_only_reviewed_empty_records_excluded_and_history_retained(db):
    c = db
    before = ids(c,'projects')
    assert '600' in ids(c)
    decide(c)
    assert '600' not in ids(c) and '601' in ids(c)
    assert ids(c,'projects')==before
    assert c.execute('SELECT count(*) AS n FROM analysis_results').fetchone()['n']==1
    assert c.execute('SELECT count(*) AS n FROM project_reporting_audit').fetchone()['n']==1
    decide(c,classification='released')
    assert '600' in ids(c)
    assert c.execute('SELECT count(*) AS n FROM project_reporting_audit').fetchone()['n']==2


@pytest.mark.parametrize('change',[
    "UPDATE projects SET item_name='12345' WHERE monday_id='600'",
    "UPDATE projects SET project_name='Actual project' WHERE monday_id='600'",
    "UPDATE projects SET account='Customer' WHERE monday_id='600'",
    "UPDATE projects SET total_order_value=1 WHERE monday_id='600'",
    "UPDATE projects SET total_amount_invoiced=-1 WHERE monday_id='600'",
    "UPDATE projects SET probability_percent=10 WHERE monday_id='600'",
    "UPDATE projects SET first_date_designed=CURRENT_DATE WHERE monday_id='600'",
    "UPDATE projects SET pipeline_stage='Lost' WHERE monday_id='600'",
    "INSERT INTO subitems(monday_id,parent_monday_id) VALUES ('700','600')",
])
def test_meaningful_data_immediately_returns_project_to_reporting_and_review(db,change):
    decide(db)
    assert '600' not in ids(db)
    db.execute(change)
    assert '600' in ids(db)
    assert db.execute("SELECT review_status FROM project_reporting_review WHERE monday_id='600'").fetchone()['review_status']=='needs_review_changed_record'


def test_free_exception_cannot_be_excluded_even_by_a_bad_decision(db):
    decide(db,'601')
    assert '601' in ids(db)


def test_sync_updates_keep_decision_and_audit_survives_lifecycle_deletion(db):
    decide(db)
    db.execute("UPDATE projects SET new_enquiry_value=0,product_key='unknown' WHERE monday_id='600'")
    assert '600' not in ids(db)
    db.execute("DELETE FROM projects WHERE monday_id='600'")
    assert db.execute("SELECT count(*) AS n FROM project_reporting_audit WHERE monday_id='600'").fetchone()['n']==1
    assert db.execute("SELECT classification FROM project_reporting_classifications WHERE monday_id='600'").fetchone()['classification']=='redundant_placeholder'


def test_migration_preserves_dependents_indexes_grants_and_aggregates(db):
    c = db
    c.execute('CREATE MATERIALIZED VIEW conversion_metrics AS SELECT count(*) AS n FROM projects')
    c.execute('CREATE UNIQUE INDEX conversion_metrics_unique ON conversion_metrics(n)')
    c.execute('CREATE VIEW summary AS SELECT n FROM conversion_metrics')
    c.execute('GRANT SELECT ON conversion_metrics TO PUBLIC')
    c.execute("COMMENT ON MATERIALIZED VIEW conversion_metrics IS 'Existing metric'")
    c.execute('CREATE VIEW data_freshness AS SELECT count(*) AS n FROM projects')
    c.execute('CREATE VIEW enquiry_counts AS SELECT count(*) AS n FROM projects')
    c.execute('CREATE TABLE snapshots(n bigint)')
    c.execute('INSERT INTO snapshots SELECT count(*) FROM projects')
    initial = c.execute('SELECT count(*) AS n FROM projects').fetchone()['n']
    decide(c)
    before = migration.capture(c)
    plan = migration.plan(before)
    with c.transaction():
        c.execute(migration.render_migration(c,before,plan))
    assert c.execute('SELECT n FROM summary').fetchone()['n']==initial-1
    assert c.execute('SELECT n FROM enquiry_counts').fetchone()['n']==initial-1
    assert c.execute('SELECT n FROM data_freshness').fetchone()['n']==initial
    assert c.execute('SELECT n FROM snapshots').fetchone()['n']==initial
    after = migration.capture(c)
    assert [g for g in before['grants'] if g['name']=='conversion_metrics']==[g for g in after['grants'] if g['name']=='conversion_metrics']
    assert [i for i in before['indexes'] if i['tablename']=='conversion_metrics']==[i for i in after['indexes'] if i['tablename']=='conversion_metrics']
    assert not migration.plan(after)['changed']
    # Reapplying the generated operation is safe and retains the exact decision.
    c.execute(migration.render_migration(c,after,migration.plan(after)))
    assert '600' not in ids(c)


def stage_empty(c):
    source = {'600':empty_source() | {'id':'600'}}
    stored = placeholders.read_projects(c,['600'])
    selection = dict(project_ids=['600'],reason='User reviewed exact ID')
    return json.loads(json.dumps(dict(selection=selection,stored=stored,source=source,
        decisions=placeholders.decisions(selection,stored,source)),default=str))


def test_classification_round_trips_saved_evidence_and_is_idempotent(db):
    staged = stage_empty(db)
    placeholders.classify(db,staged,staged['source'],'Reviewer')
    assert '600' not in ids(db)
    placeholders.classify(db,staged,staged['source'],'Reviewer')
    assert db.execute('SELECT count(*) AS n FROM project_reporting_audit').fetchone()['n']==1
    assert db.execute('SELECT count(*) AS n FROM analysis_results').fetchone()['n']==1


def test_stored_drift_aborts_classification_without_partial_decisions(db):
    staged = stage_empty(db)
    db.execute("UPDATE projects SET project_name='Now a real project' WHERE monday_id='600'")
    with pytest.raises(ValueError,match='changed since review'):
        placeholders.classify(db,staged,staged['source'],'Reviewer')
    assert not db.execute('SELECT * FROM project_reporting_classifications').fetchall()
    assert '600' in ids(db)
