"""Exercise the real database protocol with latency, not mocked query results."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
import json
import socket
import threading
import time
from uuid import uuid4

from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic import SecretStr
import pytest
import httpx
import uvicorn

from bi_analyst.api import create_app
from bi_analyst.identity import Identity, current_identity
from test_workflow_postgres import workflow_database, setup, ScriptedProvider, conversation

pytestmark = pytest.mark.postgres


@contextmanager
def running_api(config, owner, provider):
    """Use real HTTP timeouts and ASGI disconnects, as the frontend does."""
    app = create_app(config)
    async def identity():
        return Identity(owner)
    app.dependency_overrides[current_identity] = identity
    original_lifespan = app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(app):
        async with original_lifespan(app):
            app.state.workflow.provider = provider
            yield
    app.router.lifespan_context = lifespan
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='error', access_log=False,
                                          timeout_graceful_shutdown=5))
    thread = threading.Thread(target=server.run, kwargs={'sockets':[listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic()+30
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(.01)
        assert server.started, 'Local API failed to start'
        with httpx.Client(base_url=f'http://127.0.0.1:{port}', timeout=20, trust_env=False) as client:
            yield client
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        listener.close()
        assert not thread.is_alive(), 'Local API failed to shut down'


@contextmanager
def delayed_database(dsn, delay):
    """Loopback-only proxy: add one delay to each database response batch."""
    target = conninfo_to_dict(dsn)
    assert target['host'] == '127.0.0.1'
    loop = asyncio.new_event_loop()
    connections = set()

    async def connect(reader, writer):
        task = asyncio.current_task()
        connections.add(task)
        upstream = None
        try:
            source, upstream = await asyncio.open_connection(target['host'], int(target['port']))

            async def copy(source, destination, latency=0):
                while data := await source.read(65536):
                    if latency:
                        await asyncio.sleep(latency)
                    destination.write(data)
                    await destination.drain()
                destination.close()

            await asyncio.gather(copy(reader, upstream), copy(source, writer, delay))
        finally:
            writer.close()
            if upstream:
                upstream.close()
            connections.discard(task)

    server = loop.run_until_complete(asyncio.start_server(connect, '127.0.0.1', 0))
    port = server.sockets[0].getsockname()[1]
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield lambda value: make_conninfo(value, host='127.0.0.1', port=port)
    finally:
        async def close():
            server.close()
            await server.wait_closed()
            tasks = list(connections)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        asyncio.run_coroutine_threadsafe(close(), loop).result(timeout=10)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
        loop.close()


@pytest.mark.parametrize('database_delay,model_delays,budget', [
    pytest.param(.1, {}, 60, id='database-only'),
    pytest.param(.125, {'interpret': 5.272, 'plan': 11.138, 'presentation': 17.930}, 90,
                 id='production-model-timings'),
])
def test_answer_survives_database_latency_and_health_checks(setup, database_delay, model_delays, budget):
    _, dsn, owner, config, _ = setup

    class TimedProvider(ScriptedProvider):
        async def generate(self, stage, payload, schema):
            await asyncio.sleep(model_delays.get(stage, 0))
            return await super().generate(stage, payload, schema)

    provider = TimedProvider('invoice_monthly_actual', patch={'period': 'last_month'})
    with delayed_database(dsn, database_delay) as through_proxy:
        config = config.model_copy(update={
            'read_dsn': SecretStr(through_proxy(config.read_dsn.get_secret_value())),
            'state_dsn': SecretStr(through_proxy(config.state_dsn.get_secret_value())),
            'workflow_timeout_seconds': budget,
        })
        with running_api(config, owner, provider) as client, ThreadPoolExecutor(max_workers=1) as streams:
            thread = conversation(client)
            started = time.monotonic()
            response = client.post(f'/v1/conversations/{thread}/messages', json={
                'question': 'Show revenue for the last completed month.',
                'idempotency_key': str(uuid4()),
            })
            assert response.status_code == 202, response.text
            run_id = response.json()['run_id']

            def collect_events():
                lines = []
                with httpx.Client(base_url=client.base_url, timeout=20, trust_env=False) as subscriber:
                    with subscriber.stream('GET', f'/v1/runs/{run_id}/events') as stream:
                        assert stream.status_code == 200
                        for line in stream.iter_lines():
                            assert time.monotonic()-started < budget+30, 'Live event stream failed to finish'
                            lines.append(line)
                return '\n'.join(lines)

            live_events = streams.submit(collect_events)
            health_times = []
            while time.monotonic()-started < budget+30:
                checked = time.monotonic()
                health = client.get('/health/ready')
                health_times.append(time.monotonic()-checked)
                assert health.status_code == 200, health.text
                status = client.get(f'/v1/runs/{run_id}/workflow')
                assert status.status_code == 200, status.text
                saved = status.json()
                if saved['status'] not in {'registered','running'}:
                    break
            elapsed = time.monotonic()-started
            print({'answer_seconds': round(elapsed, 2),
                   'max_readiness_seconds': round(max(health_times), 2),
                   'status': saved['status']})
            assert saved['status'] == 'completed', saved
            assert saved['answer']['claims'][0]['value'] == '100.00'
            assert saved['model_calls'] == 3 and saved['tool_calls'] == 1
            assert elapsed < budget, 'Answer exceeded the end-to-end latency budget'
            streamed = live_events.result(timeout=20)
            assert 'event: answer' in streamed and 'event: terminal' in streamed
            blocks = streamed.split('\n\n')
            answers = [json.loads(block.split('\ndata: ', 1)[1]) for block in blocks
                       if '\nevent: answer\n' in block]
            assert answers == [saved['answer']]
            stages = [json.loads(block.split('\ndata: ', 1)[1])['stage'] for block in blocks
                      if '\nevent: progress\n' in block]
            assert stages == ['registered', 'authorizing', 'interpreting', 'retrieving_definitions',
                              'planning', 'resolving_entities', 'querying', 'validating_evidence',
                              'selecting_presentation', 'grounding_answer']
            events = client.get(f'/v1/runs/{run_id}/events')
            assert 'event: answer' in events.text
            reopened = client.get(f'/v1/runs/{run_id}/workflow')
            assert reopened.json()['answer'] == saved['answer']
            assert max(health_times) < 5, 'Readiness exceeded the hosting health-check deadline'
