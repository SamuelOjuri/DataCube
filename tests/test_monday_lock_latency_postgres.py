"""Locked writers on real PostgreSQL behind a network delay, as in production.

The service is far enough from the database that each round trip costs tens of
milliseconds. Writers that made a round trip per row while holding the three-table
lock exceeded its ten-second transaction limit and made other writers wait it out.
"""
from copy import deepcopy
import json
import queue
import socket
import threading
import time

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
import pytest

from scripts import order_value_monday_compare as compare
from src.services import monday_archive as archive
from src.services import monday_lifecycle as life
from src.services import monday_lifecycle_refresh as refresh
from test_monday_archive_postgres import archive_db, linked_subitem
from test_monday_lifecycle_postgres import db, Monday, item, job, full_source, no_http
from test_order_value_scopes_postgres import database


class LatencyProxy:
    """Loopback TCP proxy delaying client-to-server data; counts the client's round trips."""

    def __init__(self, target, delay):
        self.target, self.delay, self.round_trips = target, delay, 0
        self.listener = socket.create_server(('127.0.0.1', 0))
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            server = socket.create_connection(self.target)
            replied = [True]
            threading.Thread(target=self.pump, args=(client, server, True, replied), daemon=True).start()
            threading.Thread(target=self.pump, args=(server, client, False, replied), daemon=True).start()

    def pump(self, source, sink, outbound, replied):
        pending = queue.Queue()

        def deliver():
            while (entry := pending.get()) is not None:
                time.sleep(max(0.0, entry[0] - time.monotonic()))
                try:
                    sink.sendall(entry[1])
                except OSError:
                    return
            try:
                sink.shutdown(socket.SHUT_WR)
            except OSError:
                pass
        threading.Thread(target=deliver, daemon=True).start()
        try:
            while data := source.recv(65536):
                if outbound and replied[0]:
                    # Only data sent after a reply waits again; pipelined data does not.
                    self.round_trips += 1
                replied[0] = not outbound
                pending.put((time.monotonic() + (self.delay if outbound else 0), data))
        except OSError:
            pass
        pending.put(None)


@pytest.fixture
def slow(archive_db):
    c, dsn = archive_db
    info = conninfo_to_dict(dsn)
    proxy = LatencyProxy((info['host'], int(info.get('port') or 5432)), 0.05)
    yield c, make_conninfo(dsn, host='127.0.0.1', port=proxy.port, sslmode='disable', gssencmode='disable'), proxy
    proxy.listener.close()


def large_source(count):
    """full_source() with `count` subitems, each linked to its own hidden source."""
    source = full_source()
    child, hidden = json.dumps(source['subitems']['201']), json.dumps(source['hidden_items']['301'])
    for n in range(1, count):
        cid, hid = str(2000 + n), str(3000 + n)
        source['subitems'][cid] = json.loads(child.replace('"201"', f'"{cid}"').replace('"301"', f'"{hid}"'))
        source['hidden_items'][hid] = json.loads(hidden.replace('"301"', f'"{hid}"'))
        source['projects']['101']['subitems'].append({'id': cid, 'parent_item': {'id': '101'}})
    return source


def test_large_project_refresh_fits_the_locked_transaction_limit(slow, monkeypatch):
    # Sawyers Close Block C1 (33 subitems) timed out on every periodic recheck.
    c, slow_dsn, proxy = slow
    source = large_source(33)
    monkeypatch.setattr(refresh, 'fetch_project', lambda *a: deepcopy(source))
    j = job(c, life.PARENT_BOARD_ID, '101', 'refresh')
    with psycopg.connect(slow_dsn, autocommit=True, row_factory=dict_row) as worker:
        start = proxy.round_trips
        refresh.refresh_project(worker, Monday({'101': item('101', 'projects', 'active')}), j)
        round_trips = proxy.round_trips - start
    assert len(life.read_rows(c, 'subitems', 'parent_monday_id', ['101'])) == 33
    assert c.execute("SELECT count(*) AS n FROM monday_item_lifecycle WHERE monday_state='active'").fetchone()['n'] == 67
    assert c.execute('SELECT status FROM monday_lifecycle_events WHERE event_key=%s',
                     (j['event_key'],)).fetchone()['status'] in {'processed', 'review'}
    # Hundreds of sequential round trips before batching; a fixed number now.
    assert round_trips < 60, round_trips


def test_sync_gate_round_trips_do_not_grow_with_rows(slow, monkeypatch):
    c, slow_dsn, proxy = slow
    monkeypatch.setenv('SUPABASE_DB_URL', slow_dsn)

    def gate(first, count):
        links = {str(2000 + n): str(3000 + n) for n in range(first, first + count)}
        monday = Monday({**{i: linked_subitem(i, h) for i, h in links.items()},
                         **{h: item(h, 'hidden_items', 'active') for h in links.values()},
                         '101': item('101', 'projects', 'active')})
        monkeypatch.setattr(compare, 'ComparisonMondayClient', lambda: monday)
        rows = [{'monday_id': i, 'parent_monday_id': '101', 'hidden_item_id': h} for i, h in links.items()]
        start = proxy.round_trips
        assert archive.prepare_sync_rows('subitems', rows) == rows
        return proxy.round_trips - start

    small, large = gate(0, 2), gate(2, 40)
    assert large <= small + 3, (small, large)
    assert c.execute("SELECT count(*) AS n FROM monday_item_lifecycle WHERE table_name='subitems' "
                     "AND monday_state='active'").fetchone()['n'] == 42
