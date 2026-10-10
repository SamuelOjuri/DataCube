import asyncio
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from bi_analyst.metrics.compiler import Compiler
from bi_analyst.operations.maintenance import validate_dsn
from bi_analyst.operations.qualify import load, summarize, origin
from bi_analyst.operations.release import CHECKS, fingerprint, validate
from bi_analyst.operations.telemetry import Telemetry
from bi_analyst.settings import Settings
from test_settings import config


def test_telemetry_labels_cost_and_no_implicit_zero_cost():
    telemetry = Telemetry()
    telemetry.observe('model','failed',2)
    telemetry.usage({'promptTokenCount':1000,'candidatesTokenCount':100,'thoughtsTokenCount':200,'secret':9000})
    pool = SimpleNamespace(get_stats=lambda:{'pool_size':2,'pool_available':1})
    settings = SimpleNamespace(model_input_usd_per_million=1,model_output_usd_per_million=2)
    db = SimpleNamespace(read=pool,state=pool,settings=settings)
    text = telemetry.render(db,SimpleNamespace(active=0),SimpleNamespace(tasks={}))
    assert 'bi_analyst_model_estimated_usd_total 0.0016' in text
    assert 'secret' not in text
    assert 'duration_seconds_bucket{stage="model",le="2"} 1' in text
    settings.model_input_usd_per_million = None
    assert 'bi_analyst_model_estimated_usd_total' not in telemetry.render(db,SimpleNamespace(active=0),SimpleNamespace(tasks={}))
    with pytest.raises(ValueError,match='Unbounded'):
        telemetry.observe('user question','failed',1)


def test_retention_dsn_rejects_privileged_remote_or_indirect_connections():
    validate_dsn('host=127.0.0.1 dbname=postgres user=bi_analyst_maintenance',local=True)
    validate_dsn('host=db.example dbname=postgres user=bi_analyst_maintenance sslmode=verify-full')
    for dsn in ('host=db.example dbname=postgres user=postgres sslmode=verify-full',
                'host=db.example dbname=postgres user=bi_analyst_maintenance sslmode=require',
                'host=127.0.0.1 hostaddr=remote dbname=postgres user=bi_analyst_maintenance'):
        with pytest.raises(ValueError):
            validate_dsn(dsn)


@pytest.fixture
def release_case(tmp_path):
    (tmp_path/'netlify.toml').write_text('synthetic config')
    for filename in ('services/bi_analyst/pyproject.toml','services/bi_analyst/requirements.lock',
                     'services/bi_analyst/bi_analyst/semantic/catalogue.json','web/package-lock.json'):
        path = tmp_path/filename
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text('synthetic candidate input')
    artifact = tmp_path/'result.txt'
    artifact.write_text('reviewed synthetic evidence')
    evidence = {'release_sha256':fingerprint(tmp_path)['sha256'],'environment':'staging',
        'recorded_at':datetime.now(timezone.utc).isoformat(),'reviewer':'fixture reviewer',
        'deployment_reference':'fixture deployment','dataset_reference':'synthetic dataset',
        'enabled_metrics':[m.id for m in Compiler().catalogue.metrics],
        'checks':{c:{'passed':True,'artifact':'result.txt','sha256':sha256(artifact.read_bytes()).hexdigest()} for c in CHECKS},
        'load':{'query_p95_seconds':1,'answer_p95_seconds':10,'failure_rate':0,'usd_per_answer':0.01,'concurrency':2,'samples':50}}
    policy = {'max_evidence_age_days':7,'targets':{'approved':True,'approval_reference':'fixture approval',
        'max_query_p95_seconds':5,'max_answer_p95_seconds':45,'max_failure_rate':0.02,'max_usd_per_answer':0.05,
        'min_concurrency':2,'min_samples':50}}
    return tmp_path,evidence,policy


def test_release_gate_binds_source_and_evidence(release_case):
    root,evidence,policy = release_case
    assert validate(root,evidence,policy)['qualified']
    (root/'result.txt').write_text('changed')
    assert not validate(root,evidence,policy)['qualified']
    (root/'netlify.toml').write_text('new build policy')
    assert 'release_fingerprint_mismatch' in validate(root,evidence,policy)['errors']


@pytest.mark.parametrize('change', ['pending','targets','nan','old','missing_metric','path_escape','zero_samples'])
def test_release_gate_rejects_incomplete_or_unsafe_evidence(release_case,change):
    root,evidence,policy = release_case
    if change == 'pending': evidence['checks']['hosted_auth']['passed'] = False
    if change == 'targets': policy['targets']['approved'] = False
    if change == 'nan': evidence['load']['usd_per_answer'] = float('nan')
    if change == 'old': evidence['recorded_at'] = '2020-01-01T00:00:00+00:00'
    if change == 'missing_metric': evidence['enabled_metrics'].pop()
    if change == 'path_escape': evidence['checks']['permissions']['artifact'] = '../outside.txt'
    if change == 'zero_samples': evidence['load']['samples'] = 0
    assert not validate(root,evidence,policy)['qualified']


def test_load_summary_does_not_hide_failure_in_success_latency():
    assert summarize([1,2,3],{'success':3,'429':1},2) == {
        'samples':4,'concurrency':2,'successful':3,'p95_seconds':3,'failure_rate':0.25,
        'outcomes':{'429':1,'success':3}}
    assert summarize([],{'timeout':1},1)['p95_seconds'] is None


def test_load_refuses_production_before_sending_a_session():
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(200,json={'environment':'production'})
    async def run():
        async with httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler)) as client:
            with pytest.raises(ValueError,match='staging'):
                await load(client,['private-token'],samples=2,concurrency=2,mode='query')
    asyncio.run(run())
    assert len(requests) == 1 and 'Authorization' not in requests[0].headers
    for value in ('http://prod.example','https://user:pass@example','https://example/path','https://example?token=bad'):
        with pytest.raises(ValueError): origin(value)


@pytest.mark.parametrize('mode,deadline', [('query', 120), ('answer', 660)])
def test_staging_load_is_bounded_and_output_excludes_results_and_sessions(monkeypatch, mode, deadline):
    active, peak = 0, 0
    deadlines = []
    timeout = asyncio.timeout
    def record_timeout(seconds):
        deadlines.append(seconds)
        return timeout(seconds)
    monkeypatch.setattr(asyncio, 'timeout', record_timeout)
    async def handler(request):
        nonlocal active,peak
        if request.url.path == '/health/ready':
            return httpx.Response(200,json={'environment':'staging','metric_execution':'owner_accepted',
                'identity':'monday','pilot_only':True,'schema_version':7,'workflow':'enabled',
                'api_version':'0.7.0','catalogue_sha256':'synthetic'})
        assert request.headers['Authorization'] == 'Bearer secret-load-token'
        if request.url.path.endswith(('/metric', '/messages')):
            active += 1
            peak = max(peak,active)
            await asyncio.sleep(0.01)
            active -= 1
            body = json.loads(request.content)
            if request.url.path.endswith('/messages'):
                return httpx.Response(202,json={'run_id':'synthetic','status':'completed'})
            return httpx.Response(201,json={'rows':[['private-result']], 'provenance':{'metric_id':body['metric_id']}})
        return httpx.Response(201,json={'id':'synthetic'})
    async def run():
        async with httpx.AsyncClient(base_url='https://example.test',transport=httpx.MockTransport(handler)) as client:
            return await load(client,['secret-load-token'],samples=4,concurrency=2,mode=mode)
    result = asyncio.run(run())
    assert result['successful'] == 4 and result['failure_rate'] == 0 and peak == 2
    assert deadlines == [deadline] * 4
    assert 'secret-load-token' not in json.dumps(result) and 'private-result' not in json.dumps(result)


def test_release_blueprints_and_ci_are_isolated():
    import yaml
    root = Path(__file__).resolve().parents[4]
    for file,environment in (('render.yaml','production'),('render-staging.yaml','staging')):
        doc = yaml.safe_load((root/'services/bi_analyst'/file).read_text())
        service, = doc['services']
        values = {v['key']:v.get('value') for v in service['envVars']}
        assert service['type'] == 'web' and service['autoDeployTrigger'] == 'off'
        assert values['BI_ANALYST_ENVIRONMENT'] == environment
        assert values['BI_ANALYST_ANALYST_ENABLED'] == 'false'
        assert values['BI_ANALYST_PILOT_ONLY'] == 'true'
        assert values['BI_ANALYST_MODEL_TIMEOUT_SECONDS'] == 90
        assert values['BI_ANALYST_WORKFLOW_TIMEOUT_SECONDS'] == 600
        assert '--workers 1' in service['startCommand']
        assert 'BI_ANALYST_MAINTENANCE_DSN' not in values
    dashboard = json.loads((root/'services/bi_analyst/deploy/dashboard.json').read_text())
    assert len(dashboard['panels']) >= 8


def test_rolling_budget_and_invalid_operational_configuration():
    assert config(deployment_overlap=2,connection_budget=8).connection_budget == 8
    for update in ({'deployment_overlap':2},{'disabled_metrics':['invented_metric']},
                   {'telemetry_token':'short'},{'model_input_usd_per_million':1},
                   {'pilot_subjects':['untrusted-subject']}):
        with pytest.raises(ValueError):
            config(**update)
