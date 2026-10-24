"""Read-only investigation of the conversation supplied in the screenshot."""
import json
from pathlib import Path
import sys
import time

from dotenv import dotenv_values
import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
ENV = dotenv_values(ROOT / 'services/bi_analyst/.env')
CONVERSATION = '1cf77c56-1e05-4013-87ce-ce6ee4a28fd9'


def show(name, value):
    print(json.dumps({'probe': name, 'data': value}, default=str), flush=True)


def connect(key):
    return psycopg.connect(ENV[key], connect_timeout=10, row_factory=dict_row,
                          application_name='gestation_timeout_readonly',
                          options='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=1000')


def run():
    with connect('SUPABASE_DB_URL') as conn:
        jobs = conn.execute('''SELECT r.id,r.question,r.status,r.created_at,j.error_code,
            j.plan,j.model_calls,j.tool_calls,j.versions
            FROM analyst_state.runs r JOIN analyst_state.workflow_jobs j ON j.run_id=r.id
            WHERE r.conversation_id=%s ORDER BY r.created_at''', (CONVERSATION,)).fetchall()
        show('jobs', jobs)
        for job in jobs:
            show('events', conn.execute('''SELECT sequence,kind,payload,created_at
                FROM analyst_state.workflow_events WHERE run_id=%s ORDER BY sequence''', (job['id'],)).fetchall())
            show('checkpoint', conn.execute('''SELECT checkpoint,metadata
                FROM analyst_state.checkpoints WHERE thread_id=%s ORDER BY checkpoint_id DESC LIMIT 1''', (str(job['id']),)).fetchall())
            for blob in conn.execute('''SELECT type,blob FROM analyst_state.checkpoint_blobs
                WHERE thread_id=%s AND channel='plan' ORDER BY version DESC LIMIT 1''', (str(job['id']),)).fetchall():
                if blob['type'] == 'msgpack':
                    import ormsgpack
                    show('saved_plan', ormsgpack.unpackb(bytes(blob['blob'])))
                else:
                    show('plan_encoding', blob['type'])


def timings():
    sys.path.insert(0, str(ROOT / 'services/bi_analyst'))
    from datetime import datetime, timezone
    from bi_analyst.metrics.compiler import Compiler
    from bi_analyst.metrics.contracts import MetricRequest
    request = MetricRequest(metric_id='gestation_median_five_year', metric_version='1.0.0',
                            population='reportable', period='five_year_cohort', dimensions=['type'])
    plan = Compiler().compile(request, now=datetime(2026, 10, 24, 16, 3, tzinfo=timezone.utc),
                              business_timezone='Europe/London')
    if len(sys.argv) > 2 and sys.argv[2] == 'flow':
        with connect('BI_ANALYST_READ_DSN') as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            conn.execute("SET LOCAL statement_timeout='5s'")
            conn.execute("SET LOCAL search_path='pg_catalog'")
            conn.execute("SET LOCAL TIME ZONE 'Europe/London'")
            started = time.perf_counter()
            step = 'total'
            try:
                conn.execute(plan.total.sql, plan.total.params).fetchone()
                show('flow_total', round(time.perf_counter()-started, 3))
                step = 'coverage'
                started = time.perf_counter()
                conn.execute('SELECT * FROM analyst_query.coverage_v1').fetchone()
                show('flow_coverage', round(time.perf_counter()-started, 3))
                step = 'cursor'
                started = time.perf_counter()
                with conn.cursor(name='metric_rows') as cursor:
                    cursor.execute(plan.rows.sql, plan.rows.params)
                    rows = []
                    while row := cursor.fetchone():
                        rows.append(row)
                show('flow_cursor', {'seconds': round(time.perf_counter()-started, 3), 'rows': rows})
            except psycopg.Error as error:
                show('flow_failed', {'step': step, 'seconds': round(time.perf_counter()-started, 3),
                                    'error': type(error).__name__, 'message': error.diag.message_primary,
                                    'context': error.diag.context})
                conn.rollback()
        return
    for name, query, params in [('total', plan.total.sql, plan.total.params),
                                ('coverage', 'SELECT * FROM analyst_query.coverage_v1', ()),
                                ('grouped_median', plan.rows.sql, plan.rows.params)]:
        with connect('BI_ANALYST_READ_DSN') as conn:
            conn.execute("SET LOCAL statement_timeout='5s'")
            conn.execute("SET LOCAL search_path='pg_catalog'")
            conn.execute("SET LOCAL TIME ZONE 'Europe/London'")
            started = time.perf_counter()
            try:
                rows = conn.execute(query, params).fetchall()
                show(name, {'seconds': round(time.perf_counter()-started, 3), 'rows': rows})
            except psycopg.Error as error:
                show(name, {'seconds': round(time.perf_counter()-started, 3), 'error': type(error).__name__,
                            'message': error.diag.message_primary, 'context': error.diag.context})
                conn.rollback()


if __name__ == '__main__':
    try:
        timings() if len(sys.argv) > 1 and sys.argv[1] == 'timings' else run()
    except Exception as error:
        show('error', {'type': type(error).__name__, 'sqlstate': getattr(error, 'sqlstate', None)})
        sys.exit(1)
