"""Exercise the real database protocol with latency, not mocked query results."""
import asyncio
from contextlib import asynccontextmanager, contextmanager
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
def running_api(config, owner):
    """Use real HTTP timeouts and ASGI disconnects, as the frontend does."""
    app = create_app(config)
    async def identity():
        return Identity(owner)
    app.dependency_overrides[current_identity] = identity
    original_lifespan = app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(app):
        async with original_lifespan(app):
            app.state.workflow.provider = ScriptedProvider('invoice_monthly_actual')
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


def test_answer_survives_database_latency_and_health_checks(setup):
    _, dsn, owner, config, _ = setup
    with delayed_database(dsn, .1) as through_proxy:
        config = config.model_copy(update={
            'read_dsn': SecretStr(through_proxy(config.read_dsn.get_secret_value())),
            'state_dsn': SecretStr(through_proxy(config.state_dsn.get_secret_value())),
            'workflow_timeout_seconds': 60,
        })
        with running_api(config, owner) as client:
            thread = conversation(client)
            started = time.monotonic()
            response = client.post(f'/v1/conversations/{thread}/messages', json={
                'question': 'Show revenue for the last completed month.',
                'idempotency_key': str(uuid4()),
            })
            assert response.status_code == 202, response.text
            run_id = response.json()['run_id']
            health_times = []
            while time.monotonic()-started < 90:
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
            events = client.get(f'/v1/runs/{run_id}/events')
            assert 'event: answer' in events.text
            assert max(health_times) < 5, 'Readiness exceeded the hosting health-check deadline'
