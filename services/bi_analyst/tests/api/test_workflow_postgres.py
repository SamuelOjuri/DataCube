"""Real LangGraph/Postgres/RLS and API scheduling, with deterministic provider fixtures."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import time
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
import pytest

from bi_analyst.api import create_app
from bi_analyst.identity import Identity, current_identity
from bi_analyst.metrics.compiler import Compiler
from bi_analyst.workflow.provider import ProviderFailure

pytestmark = pytest.mark.postgres
ROOT = Path(__file__).resolve().parents[4]


class ScriptedProvider:
    def __init__(self, metric='order_parent_value', *, patch=None, clarify=False, delay=0,
                 malicious=False, retry=False, unsupported=False):
        self.metric, self.patch, self.clarify = metric, patch or {}, clarify
        self.delay, self.malicious, self.retry, self.unsupported = delay, malicious, retry, unsupported
        self.calls = []

    async def generate(self, stage, payload, schema):
        self.calls.append((stage,payload))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.retry:
            self.retry = False
            raise ProviderFailure(retryable=True)
        metric = Compiler().metrics[self.metric]
        if stage == 'interpret':
            data = {'families':[] if self.unsupported else [metric.family],'supported':not self.unsupported,'asks_for_cause':True}
        elif stage == 'plan':
            if self.clarify and not payload['replies']:
                data = {'action':'clarify','reason':'metric','candidate_metrics':[metric.id]}
            else:
                data = {'action':'plan','patch':self.patch if payload['previous_plan'] else
                        {'metric_id':metric.id,'period':metric.periods[0],**self.patch}}
        else:
            data = {'claim_ids':['invented'] if self.malicious else ['total'], 'chart':{'kind':'table'}}
        return schema.model_validate(data), {'promptTokenCount':11,'candidatesTokenCount':7}


@pytest.fixture(scope='module')
def workflow_database(database):
    admin,dsn = database
    before = admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall()
    for migration in ('004_analyst_reportable_population','005_analyst_auth','006_analyst_workflow'):
        with admin.transaction():
            admin.execute((ROOT/f'src/database/migrations/20261008_{migration}.sql').read_text(encoding='utf-8'))
    assert admin.execute("SELECT oid,relacl,relowner FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid").fetchall() == before
    return admin,dsn


@pytest.fixture
def setup(workflow_database,settings):
    admin,dsn = workflow_database
    owner = uuid4()
    admin.execute('INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)',(owner,))
    config = settings.model_copy(update={'workflow_enabled':True,'requests_per_minute':600,
        'metric_evaluation_enabled':True,'workflow_timeout_seconds':10})

    @contextmanager
    def start(provider=None, subject=owner, config_override=None):
        app = create_app(config_override or config)
        async def identity():
            return Identity(subject)
        app.dependency_overrides[current_identity] = identity
        with TestClient(app) as client:
            app.state.workflow.provider = provider or ScriptedProvider()
            client.app_instance = app
            yield client
    return admin,dsn,owner,config,start


def conversation(client):
    response = client.post('/v1/conversations',json={'title':'Workflow test'})
    assert response.status_code == 201,response.text
    return response.json()['id']


def submit(client,thread=None,**updates):
    body = {'question':'Show stored parent Order Value','idempotency_key':str(uuid4()),**updates}
    response = client.post(f'/v1/conversations/{thread or conversation(client)}/messages',json=body)
    assert response.status_code == 202,response.text
    return response.json()['run_id']


def settle(client,run_id):
    deadline = time.monotonic()+12
    while time.monotonic()<deadline:
        response = client.get(f'/v1/runs/{run_id}/workflow')
        assert response.status_code == 200,response.text
        row = response.json()
        if row['status'] not in {'registered','running'}:
            return row
        time.sleep(.025)
    pytest.fail('Workflow failed to settle')


@pytest.mark.parametrize('metric,expected',[
    ('new_enquiry_value','350.00'),('order_parent_value','350.00'),('order_hidden_complete_subtotal','107.50'),
    ('invoice_project_value','95.00'),('invoice_hidden_value','80.00'),('invoice_stored_child_value','95.00'),
    ('enquiry_monthly_actual','300.00'),('bookings_monthly_actual','150.00'),('invoice_monthly_actual','100.00'),
    ('conversion_five_year','0.200'),('conversion_two_year','0.200'),('conversion_closed_five_year','0.500'),
    ('conversion_closed_two_year','0.500'),('gestation_five_year','20.000000'),('gestation_two_year','20.000000'),
    ('gestation_median_five_year','20.000000')])
def test_golden_graph_answers(setup,metric,expected):
    admin,_,_,_,start = setup
    provider = ScriptedProvider(metric)
    with start(provider) as client:
        run_id = submit(client)
        row = settle(client,run_id)
        assert row['status'] == 'completed',row
        assert row['answer']['claims'][0]['value'] == expected
        assert row['model_calls'] == 3 and row['tool_calls'] == 1
        assert any('freshness is unknown' in text for text in row['answer']['notices'])
        events = client.get(f'/v1/runs/{run_id}/events')
        assert events.status_code == 200
        assert 'event: answer' in events.text and 'event: terminal' in events.text
        assert 'systemInstruction' not in events.text and 'previous_plan' not in events.text
        assert admin.execute('SELECT count(*) AS n FROM analyst_state.checkpoints WHERE thread_id=%s',(run_id,)).fetchone()['n'] > 4
        # Stored datasets are never embedded in checkpoint channels.
        channels = admin.execute('SELECT DISTINCT channel FROM analyst_state.checkpoint_blobs WHERE thread_id=%s',(run_id,)).fetchall()
        assert not {'rows','dataset','answer'} & {r['channel'] for r in channels}


def test_clarification_survives_restart_and_resume_is_idempotent(setup):
    _,_,_,_,start = setup
    with start(ScriptedProvider(clarify=True)) as client:
        thread = conversation(client)
        run_id = submit(client,thread)
        row = settle(client,run_id)
        assert row['status'] == 'awaiting_clarification',row
        reply = {'clarification_id':row['clarification']['id'],'idempotency_key':str(uuid4()),'answer':'Use parent value'}
    provider = ScriptedProvider(clarify=True)
    with start(provider) as client:
        response = client.post(f'/v1/runs/{run_id}/resume',json=reply)
        assert response.status_code == 200,response.text
        assert settle(client,run_id)['status'] == 'completed'
        assert client.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 200
        assert client.post(f'/v1/runs/{run_id}/resume',json={**reply,'answer':'Different'}).status_code == 409
        assert [stage for stage,_ in provider.calls] == ['plan','presentation']


def test_followup_patch_preserves_filters_and_exposes_changes(setup):
    _,_,_,_,start = setup
    with start(ScriptedProvider(patch={'filters':[{'dimension':'category','values':['A']}]})) as client:
        thread = conversation(client)
        first = submit(client,thread)
        assert settle(client,first)['status'] == 'completed'
        client.app_instance.state.workflow.provider = ScriptedProvider(patch={'dimensions':['type']})
        second = submit(client,thread,follow_up_to=first,question='Break that down by type')
        row = settle(client,second)
        assert row['status'] == 'completed',row
        assert row['answer']['scope_changes'] == {'dimensions':{'before':[],'after':['type']}}
        stored = client.get('/v1/results/'+row['answer']['result']['result_id']).json()
        assert stored['provenance']['request']['filters'][0]['values'] == ['A']


def test_entities_interrupt_before_query_and_accept_exact_reply(setup):
    admin,_,_,_,start = setup
    admin.execute("INSERT INTO public.projects(monday_id,category,total_order_value) VALUES ('wf_roof1','Roofing',3),('wf_roof2','Roofing Supplies',7)")
    try:
        with start(ScriptedProvider(patch={'filters':[{'dimension':'category','values':['roof']}]})) as client:
            run_id = submit(client)
            row = settle(client,run_id)
            assert row['status'] == 'awaiting_clarification',row
            assert row['clarification']['options'] == ['Roofing','Roofing Supplies']
            assert admin.execute('SELECT count(*) AS n FROM analyst_state.results WHERE run_id=%s',(run_id,)).fetchone()['n'] == 0
            reply = {'clarification_id':row['clarification']['id'],'idempotency_key':str(uuid4()),'answer':'Roofing'}
            assert client.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 200
            row = settle(client,run_id)
            assert row['status'] == 'completed',row
            assert row['answer']['claims'][0]['value'] == '3.00'
    finally:
        admin.execute("DELETE FROM public.projects WHERE monday_id IN ('wf_roof1','wf_roof2')")


def test_duplicate_and_competing_submissions_across_instances(setup):
    _,_,_,_,start = setup
    with start(ScriptedProvider(delay=.15)) as first, start(ScriptedProvider(delay=.15)) as second:
        thread = conversation(first)
        payload = {'question':'Parent order total','idempotency_key':str(uuid4())}
        def send(client):
            return client.post(f'/v1/conversations/{thread}/messages',json=payload)
        with ThreadPoolExecutor(2) as workers:
            responses = list(workers.map(send,[first,second]))
        assert all(r.status_code == 202 for r in responses),[r.text for r in responses]
        run_id = responses[0].json()['run_id']
        assert responses[1].json()['run_id'] == run_id
        assert second.post(f'/v1/conversations/{thread}/messages',json={**payload,'question':'changed'}).status_code == 409
        assert second.post(f'/v1/conversations/{thread}/messages',json={**payload,'idempotency_key':str(uuid4())}).status_code == 409
        assert settle(first,run_id)['status'] == 'completed'


def test_cancel_on_another_instance_and_terminal_result_denied(setup):
    _,_,_,_,start = setup
    with start(ScriptedProvider(delay=2)) as first, start() as second:
        run_id = submit(first)
        assert second.post(f'/v1/runs/{run_id}/cancel').status_code == 200
        assert settle(first,run_id)['status'] == 'cancelled'
        time.sleep(.25)
        assert not first.app_instance.state.workflow.tasks


def test_owner_permission_and_checkpoint_isolation(setup):
    admin,dsn,owner,_,start = setup
    with start(ScriptedProvider(clarify=True)) as client:
        run_id = submit(client)
        row = settle(client,run_id)
        other = uuid4()
        admin.execute('INSERT INTO analyst_state.principals(subject,enabled,company_wide) VALUES (%s,true,true)',(other,))
        reply = {'clarification_id':row['clarification']['id'],'idempotency_key':str(uuid4()),'answer':'parent'}
        with start(subject=other) as stranger:
            for path in (f'/v1/runs/{run_id}/workflow',f'/v1/runs/{run_id}/events'):
                assert stranger.get(path).status_code == 404
            assert stranger.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 404
        with psycopg.connect(make_conninfo(dsn,user='bi_analyst_state'),row_factory=dict_row) as state:
            state.execute("SELECT set_config('bi_analyst.subject',%s,true),set_config('bi_analyst.workflow_run_id',%s,true)",(str(other),run_id))
            assert state.execute('SELECT * FROM analyst_state.checkpoints').fetchall() == []
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with state.transaction():
                    state.execute("INSERT INTO analyst_state.checkpoints(thread_id,checkpoint_id,checkpoint) VALUES (%s,'forged','{}')",(run_id,))
        admin.execute('UPDATE analyst_state.principals SET permissions_version=2 WHERE subject=%s',(owner,))
        assert client.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 403
        assert client.get(f'/v1/runs/{run_id}/workflow').status_code == 403


def test_invalid_evidence_retries_and_runtime_budget(setup):
    _,_,_,config,start = setup
    with start(ScriptedProvider(malicious=True)) as client:
        row = settle(client,submit(client))
        assert row['status'] == 'failed' and row['error_code'] == 'invalid_evidence',row
        assert row['answer'] is None
    with start(ScriptedProvider(retry=True)) as client:
        row = settle(client,submit(client))
        assert row['status'] == 'completed' and row['model_calls'] == 4,row
    with start(ScriptedProvider(delay=2),config_override=config.model_copy(update={'workflow_timeout_seconds':.5})) as client:
        row = settle(client,submit(client))
        assert row['status'] in {'failed','interrupted'} and row['answer'] is None,row


def test_shutdown_and_abandoned_execution_require_new_submission(setup):
    admin,_,_,_,start = setup
    with start(ScriptedProvider(delay=3)) as client:
        run_id = submit(client)
    with start() as client:
        assert settle(client,run_id)['status'] == 'interrupted'
        reply = {'clarification_id':str(uuid4()),'idempotency_key':str(uuid4()),'answer':'resume'}
        assert client.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 409
        # Simulate a hard process loss without graceful shutdown and an expired lease.
        admin.execute("UPDATE analyst_state.workflow_jobs SET status='running',deadline=now()-interval '1 second' WHERE run_id=%s",(run_id,))
        admin.execute("UPDATE analyst_state.runs SET status='running' WHERE id=%s",(run_id,))
        assert settle(client,run_id)['status'] == 'interrupted'


def test_permission_change_during_model_wait_cannot_publish(setup):
    admin,_,owner,_,start = setup
    with start(ScriptedProvider(delay=1)) as client:
        thread = conversation(client)
        run_id = submit(client,thread)
        admin.execute('UPDATE analyst_state.principals SET permissions_version=permissions_version+1 WHERE subject=%s',(owner,))
        time.sleep(.35)
        assert client.get(f'/v1/runs/{run_id}/workflow').status_code == 403
        assert admin.execute('SELECT count(*) AS n FROM analyst_state.workflow_events WHERE run_id=%s AND kind=\'answer\'',(run_id,)).fetchone()['n'] == 0
        # A new authenticated permissions version can start a new run on this thread.
        client.app_instance.state.workflow.provider = ScriptedProvider()
        new_run = submit(client,thread)
        assert settle(client,new_run)['status'] == 'completed'


def test_model_retry_budget_is_persisted_and_bounded(setup):
    _,_,_,config,start = setup
    class AlwaysTransient(ScriptedProvider):
        async def generate(self,*args):
            raise ProviderFailure(retryable=True)
    with start(AlwaysTransient()) as client:
        row = settle(client,submit(client))
        assert row['status'] == 'failed' and row['model_calls'] == 2
    provider = ScriptedProvider(clarify=True)
    with start(provider,config_override=config.model_copy(update={'workflow_max_model_calls':3})) as client:
        run_id = submit(client)
        row = settle(client,run_id)
        reply = {'clarification_id':row['clarification']['id'],'idempotency_key':str(uuid4()),'answer':'parent'}
        assert client.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 200
        row = settle(client,run_id)
        assert row['status'] == 'failed' and row['error_code'] == 'run_budget_exceeded' and row['model_calls'] == 3


def test_null_results_and_limited_rows_preserve_evidence(setup):
    _,_,_,_,start = setup
    with start(ScriptedProvider(patch={'filters':[{'dimension':'category','operator':'is_null'}]})) as client:
        row = settle(client,submit(client))
        assert row['status'] == 'completed',row
        assert row['answer']['claims'][0]['value'] is None
        assert 'unknown' in row['answer']['text']
    with start(ScriptedProvider(patch={'dimensions':['category'],'limit':1})) as client:
        row = settle(client,submit(client))
        assert row['answer']['claims'][0]['value'] == '350.00'
        assert any('Displayed rows are limited' in n for n in row['answer']['notices'])


def test_wrong_clarification_and_version_change_are_rejected(setup):
    _,_,_,_,start = setup
    with start(ScriptedProvider(clarify=True)) as client:
        run_id = submit(client)
        row = settle(client,run_id)
        reply = {'clarification_id':str(uuid4()),'idempotency_key':str(uuid4()),'answer':'parent'}
        assert client.post(f'/v1/runs/{run_id}/resume',json=reply).status_code == 409
        reply['clarification_id'] = row['clarification']['id']
        client.app_instance.state.workflow.versions = {**client.app_instance.state.workflow.versions,'prompt':'changed'}
        assert client.post(f'/v1/runs/{run_id}/resume',json=reply).json()['detail'] == 'workflow_version_changed'
