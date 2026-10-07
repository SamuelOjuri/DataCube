"""Real SQL on an explicitly selected loopback server, using disposable databases."""
import os
from pathlib import Path
from uuid import uuid4
from decimal import Decimal

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
import pytest

from bi_analyst.semantic import load_catalogue
from bi_analyst.semantic.check import check_database
from verify_frozen import compare_results, statements

ROOT = Path(__file__).resolve().parents[4]
MIGRATION = ROOT/'src/database/migrations/20261007_001_analytics_contracts.sql'
pytestmark = pytest.mark.postgres


@pytest.fixture(scope='module')
def database():
    dsn = os.environ.get('BI_ANALYST_TEST_DSN')
    if not dsn:
        pytest.skip('Set BI_ANALYST_TEST_DSN to an isolated loopback postgres admin database')
    info = conninfo_to_dict(dsn)
    if (info.get('host') not in {'127.0.0.1','::1','localhost'} or info.get('dbname') != 'postgres'
            or info.get('hostaddr',info['host']) not in {'127.0.0.1','::1','localhost'}
            or info.get('service')):
        pytest.fail('Only an explicit loopback postgres connection is allowed')
    name = 'bi_semantic_test_'+uuid4().hex
    with psycopg.connect(dsn,autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        try:
            with psycopg.connect(make_conninfo(dsn,dbname=name),autocommit=True,row_factory=dict_row) as conn:
                conn.execute(Path(__file__).with_name('source_fixture.sql').read_text(encoding='utf-8'))
                # Use the real current reporting-view definitions, not a test copy.
                import re
                source = (ROOT/'src/database/schema/schema.sql').read_text(encoding='utf-8')
                for view in ['vw_actual_enquiry_monthly_v1','vw_actual_bookings_monthly_v1','vw_actual_revenue_monthly_v1']:
                    definition = re.search(r'CREATE OR REPLACE VIEW '+view+r' AS\n.*?;',source,re.S).group()
                    conn.execute(definition)
                with conn.transaction():
                    conn.execute(MIGRATION.read_text(encoding='utf-8'))
                yield conn
        finally:
            # Random database name created above, never supplied externally.
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


def result(conn,query):
    cur=conn.execute(query)
    return {'columns':[{'name':d.name,'type_oid':d.type_code} for d in cur.description],'rows':cur.fetchall()}


def test_real_migration_matches_catalogue_and_reapply_preserves_sources(database):
    assert check_database(database,load_catalogue()) == []
    before=database.execute('SELECT * FROM projects ORDER BY monday_id').fetchall()
    with database.transaction():
        database.execute(MIGRATION.read_text(encoding='utf-8'))
    assert before==database.execute('SELECT * FROM projects ORDER BY monday_id').fetchall()
    assert check_database(database,load_catalogue()) == []


@pytest.mark.parametrize('name',list(statements(Path(__file__).with_name('parity.sql'))))
def test_all_metric_variants_match_independent_phase1_sql(database,name):
    actual=statements(Path(__file__).with_name('parity.sql'))[name]
    expected=statements(ROOT/'services/bi_analyst/tests/evals/reference.sql')[name]
    assert compare_results(result(database,actual),result(database,expected))['matches']


def test_exclusions_and_parent_child_join_cannot_amplify_money(database):
    rows=database.execute('SELECT monday_id FROM analytics.projects_v1').fetchall()
    assert {'monday_id':'excluded'} not in rows
    sums=database.execute('''SELECT sum(p.total_order_value) AS amount FROM analytics.projects_v1 p
      LEFT JOIN analytics.child_totals_v1 c ON c.parent_monday_id=p.monday_id''').fetchone()
    assert sums['amount']==Decimal('350')
    assert database.execute('SELECT count(*) AS n FROM analytics.children_v1').fetchone()['n']==6


def test_blank_zero_signed_invoices_and_repeated_sources(database):
    rows={r['parent_monday_id']:r for r in database.execute('SELECT * FROM analytics.child_totals_v1')}
    assert rows['E1']['child_invoice_amount']==Decimal('80')
    assert rows['E2']['child_invoice_amount']==0
    assert rows['E3']['child_invoice_amount'] is None
    assert rows['E1']['child_count']==2  # Same hidden source contributes twice to mirror membership.
    assert rows['E1']['child_formula_enquiry_amount']==100
    coverage=database.execute('SELECT * FROM analytics.coverage_v1').fetchone()
    assert coverage['repeated_source_ids']==1 and coverage['incomplete_order_inputs']==1
    assert coverage['children_without_source']==1 and coverage['unresolved_source_links']==1


def test_latest_analysis_timestamp_ties_and_nulls_are_deterministic(database):
    row=database.execute("SELECT * FROM analytics.latest_analysis_v1 WHERE project_id='E1'").fetchone()
    assert str(row['analysis_id'])=='00000000-0000-0000-0000-000000000002'
    assert row['expected_conversion_rate']==Decimal('.8')


def test_current_month_excluded_and_null_stage_remains_conversion_eligible(database):
    ids=database.execute('SELECT monday_id FROM analytics.invoice_reporting_facts_v1').fetchall()
    assert ids==[{'monday_id':'C1'}]
    row=database.execute("SELECT * FROM analytics.conversion_cohorts_v1 WHERE monday_id='E4' AND cohort_years=5").fetchone()
    assert (row['eligible_count'],row['win_count'],row['closed_count'])==(1,0,0)
    assert database.execute("SELECT count(*) AS n FROM analytics.conversion_cohorts_v1 WHERE monday_id='future'").fetchone()['n']==2


def test_default_public_cannot_access_analytics(database):
    # aclexplode identifies PUBLIC by grantee=0, independent of current superuser.
    row=database.execute("""SELECT count(*) AS n FROM pg_namespace n,
      LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
      WHERE n.nspname='analytics' AND a.grantee=0""").fetchone()
    assert row['n']==0


def test_schema_check_detects_actual_column_drift(database):
    with database.transaction(force_rollback=True):
        database.execute('ALTER VIEW analytics.hidden_values_v1 RENAME COLUMN order_inputs_complete TO missing')
        assert any('hidden_values_v1' in message for message in check_database(database,load_catalogue()))
