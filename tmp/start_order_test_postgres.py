"""Start only the existing workspace PostgreSQL fixture on loopback port 55439."""
from pathlib import Path
import subprocess
import time

import psycopg

root = Path(__file__).resolve().parents[1]
data = (root / 'outputs/order_scope_postgres/data').resolve()
binary = (root / 'outputs/order_scope_postgres/pgsql/bin/postgres.exe').resolve()
assert data.is_relative_to(root) and binary.is_relative_to(root)
dsn = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres connect_timeout=2'
try:
    with psycopg.connect(dsn) as connection:
        print('Local PostgreSQL already ready on 127.0.0.1:55439')
        raise SystemExit(0)
except psycopg.OperationalError:
    pass
with (root / 'outputs/order_scope_postgres/rehydration-test-server.log').open('ab') as log:
    process = subprocess.Popen([str(binary), '-D', str(data), '-h', '127.0.0.1', '-p', '55439',
                                '-c', 'fsync=off'], stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                               creationflags=subprocess.CREATE_NO_WINDOW)
for attempt in range(20):
    if process.poll() is not None:
        raise SystemExit('Local PostgreSQL exited; inspect rehydration-test-server.log')
    try:
        with psycopg.connect(dsn) as connection:
            print('Local PostgreSQL ready on 127.0.0.1:55439')
            break
    except psycopg.OperationalError:
        time.sleep(0.25)
else:
    raise SystemExit('Local PostgreSQL did not become ready')
