"""Gateway display labels preserve project grain, acceptance and access controls."""
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo
import pytest

from bi_analyst.metrics.compiler import Compiler

pytestmark = pytest.mark.postgres
ROOT = Path(__file__).resolve().parents[4]
MIGRATION = ROOT / 'src/database/migrations/20261024_011_analyst_project_labels.sql'


def test_label_migration_preserves_population_measures_owner_and_privileges(database):
    admin, dsn = database
    before = admin.execute("""SELECT oid,relowner,relacl,reloptions FROM pg_class
        WHERE relnamespace IN ('public'::regnamespace,'analytics'::regnamespace,'analyst_query'::regnamespace)
        ORDER BY oid""").fetchall()
    source = admin.execute('SELECT * FROM public.projects ORDER BY monday_id').fetchall()
    with admin.transaction():
        admin.execute(MIGRATION.read_text(encoding='utf-8'))
    assert admin.execute("""SELECT oid,relowner,relacl,reloptions FROM pg_class
        WHERE relnamespace IN ('public'::regnamespace,'analytics'::regnamespace,'analyst_query'::regnamespace)
        ORDER BY oid""").fetchall() == before
    assert admin.execute('SELECT * FROM public.projects ORDER BY monday_id').fetchall() == source
    assert admin.execute((ROOT / 'services/bi_analyst/bi_analyst/permissions.sql').read_text(encoding='utf-8')).fetchall() == []
    assert Compiler().catalogue.owner_accepted
    with psycopg.connect(make_conninfo(dsn, user='bi_analyst_reader')) as reader:
        assert reader.execute("""SELECT to_jsonb(g)-'item_name' FROM analyst_query.projects_v1 g ORDER BY monday_id""").fetchall() == reader.execute(
            'SELECT to_jsonb(p) FROM analytics.projects_v1 p ORDER BY monday_id').fetchall()
        assert reader.execute("SELECT monday_id FROM analyst_query.projects_v1 WHERE monday_id='excluded'").fetchall() == []
        assert reader.execute("SELECT pg_typeof(item_name)::text FROM analyst_query.projects_v1 LIMIT 1").fetchone() == ('text',)
