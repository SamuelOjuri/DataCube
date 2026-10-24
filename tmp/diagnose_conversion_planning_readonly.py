"""Inspect only the reported conversion run, or replay planning without DB writes."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from uuid import uuid4

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'services/bi_analyst'))
QUESTION = ('Compare five-year closed-only conversion between New Build and '
            'Refurbishment projects, showing the supporting counts.')


def show(name, data):
    print(json.dumps({'probe': name, 'data': data}, default=str), flush=True)


def state(env):
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(env['SUPABASE_DB_URL'], connect_timeout=10, row_factory=dict_row,
                         application_name='conversion_planning_readonly',
                         options='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=1000') as conn:
        jobs = conn.execute('''SELECT r.id,r.status,r.created_at,j.error_code,j.plan,j.clarification,
            j.model_calls,j.tool_calls,j.versions FROM analyst_state.runs r
            JOIN analyst_state.workflow_jobs j ON j.run_id=r.id
            WHERE r.question=%s ORDER BY r.created_at DESC LIMIT 3''', (QUESTION,)).fetchall()
        show('reported_jobs', jobs)
        for job in jobs:
            show('events', conn.execute('''SELECT sequence,kind,payload,created_at
                FROM analyst_state.workflow_events WHERE run_id=%s AND kind<>'answer'
                ORDER BY sequence''', (job['id'],)).fetchall())


def query(env):
    import time
    import psycopg
    from psycopg.rows import dict_row
    from bi_analyst.metrics.compiler import Compiler
    from bi_analyst.metrics.contracts import MetricRequest

    request = MetricRequest(metric_id='conversion_closed_five_year', metric_version='1.0.0',
        population='reportable', period='five_year_cohort', dimensions=['type'],
        filters=[{'dimension':'type', 'values':['New Build', 'Refurbishment']}])
    compiled = Compiler().compile(request, now=datetime(2026, 10, 24, tzinfo=timezone.utc),
                                  business_timezone='Europe/London')
    with psycopg.connect(env['BI_ANALYST_READ_DSN'], connect_timeout=10, row_factory=dict_row,
                         application_name='conversion_query_readonly',
                         options='-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000') as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        started = time.monotonic()
        total = conn.execute(compiled.total.sql, compiled.total.params).fetchone()
        rows = conn.execute(compiled.rows.sql, compiled.rows.params).fetchall()
        show('query', {'total':total, 'rows':rows, 'seconds':round(time.monotonic()-started, 3)})


async def plan(env):
    import httpx
    from pydantic import SecretStr
    from bi_analyst.metrics.compiler import Compiler
    from bi_analyst.workflow.graph import ConversationGraph, GraphState
    from bi_analyst.workflow.provider import GeminiProvider, PROMPT_VERSION, ProviderFailure
    from bi_analyst.settings import DEFAULT_MODEL_TIMEOUT_SECONDS
    sys.path.insert(0, str(ROOT / 'services/bi_analyst/tests/evals'))
    from conversations import EvaluationJobs, RecordedProvider

    compiler = Compiler()
    jobs = EvaluationJobs()
    key = env.get('BI_ANALYST_GEMINI_API_KEY') or env.get('GEMINI_API_KEY')
    if not key:
        raise SystemExit('No planning credential configured')
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as client:
        provider = RecordedProvider(GeminiProvider(SimpleNamespace(gemini_api_key=SecretStr(key),
            model_timeout_seconds=DEFAULT_MODEL_TIMEOUT_SECONDS), client))
        service = SimpleNamespace(jobs=jobs, provider=provider,
            metrics=SimpleNamespace(compiler=compiler, require_access=compiler.metric),
            settings=SimpleNamespace(metric_max_rows=1000, business_timezone='Europe/London', exploratory_enabled=True))
        workflow = ConversationGraph(service, None, uuid4(), uuid4(), None)
        try:
            result = await workflow.make_plan(GraphState(question=QUESTION))
        except ProviderFailure as error:
            show('provider_error', error.code)
            raise SystemExit(1) from None
        show('planning', {'prompt': PROMPT_VERSION, 'decisions': provider.decisions, 'result': result})
        if result.get('plan'):
            from bi_analyst.metrics.contracts import MetricRequest
            request = MetricRequest.model_validate(result['plan'])
            compiled = compiler.compile(request, now=datetime(2026, 10, 24, tzinfo=timezone.utc),
                                        business_timezone='Europe/London')
            show('compiled', {'rows': compiled.rows.sql, 'params': compiled.rows.params})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['state', 'plan', 'query'])
    args = parser.parse_args()
    env = dotenv_values(ROOT / 'services/bi_analyst/.env')
    try:
        if args.mode == 'state':
            state(env)
        elif args.mode == 'query':
            query(env)
        else:
            asyncio.run(plan(env))
    except Exception as error:
        show('error_type', type(error).__name__)
        raise SystemExit(1) from None
