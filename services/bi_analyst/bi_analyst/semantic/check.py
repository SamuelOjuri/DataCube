"""Offline catalogue validation and optional read-only installed-schema checks."""
import argparse
import os

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from .catalogue import Catalogue, load_catalogue


def check_database(connection, catalogue: Catalogue) -> list[str]:
    """Check exact public surface, types, comments, security mode and declared keys.

    Caller owns the transaction. This routine neither grants nor mutates anything.
    Successful checks describe structure, not source/business certification.
    """
    problems = []
    for relation in catalogue.relations:
        name = f"analytics.{relation.id}"
        rows = connection.execute("""
            SELECT a.attname, t.typname, col_description(c.oid,a.attnum) AS comment,
                   c.reloptions, obj_description(c.oid,'pg_class') AS relation_comment
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
            JOIN pg_type t ON t.oid=a.atttypid
            WHERE n.nspname='analytics' AND c.relname=%s AND c.relkind='v'
            ORDER BY a.attnum
        """, (relation.id,)).fetchall()
        if not rows and relation.optional:
            continue
        actual = {row['attname']: row['typname'] for row in rows}
        if actual != relation.columns:
            problems.append(f"{name}: missing relation or column/type drift")
            continue
        if any(not row['comment'] or not row['relation_comment'] for row in rows):
            problems.append(f"{name}: missing documentation")
        if not {'security_invoker=true', 'security_barrier=true'} <= set(rows[0]['reloptions'] or []):
            problems.append(f"{name}: unexpected view security mode")
        keys = sql.SQL(',').join(map(sql.Identifier, relation.key))
        nulls = sql.SQL(' OR ').join(sql.SQL('{} IS NULL').format(sql.Identifier(k)) for k in relation.key)
        duplicate = connection.execute(sql.SQL("""
            SELECT EXISTS(SELECT FROM analytics.{} GROUP BY {} HAVING count(*)>1)
                OR EXISTS(SELECT FROM analytics.{} WHERE {}) AS invalid
        """).format(sql.Identifier(relation.id), keys, sql.Identifier(relation.id), nulls)).fetchone()
        if duplicate['invalid']:
            problems.append(f"{name}: declared key is NULL or not unique")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', action='store_true', help='Read BI_ANALYST_CHECK_DSN; no fallback credentials')
    args = parser.parse_args(argv)
    catalogue = load_catalogue()
    problems = []
    if args.database:
        dsn = os.environ.get('BI_ANALYST_CHECK_DSN')
        if not dsn:
            parser.error('BI_ANALYST_CHECK_DSN is required for --database')
        try:
            with psycopg.connect(dsn, connect_timeout=10, row_factory=dict_row) as connection:
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                connection.execute("SET LOCAL statement_timeout='30s'")
                connection.execute("SET LOCAL lock_timeout='3s'")
                problems = check_database(connection, catalogue)
        except psycopg.Error as exc:
            print(f'Database check failed (SQLSTATE {exc.sqlstate or "connection_error"}); details suppressed')
            return 1
    for problem in problems:
        print(problem)
    print(f'Catalogue {catalogue.version}: {len(catalogue.metrics)} candidate metrics, '
          f'{len(catalogue.relations)} relations; certification pending.')
    return int(bool(problems))


if __name__ == '__main__':
    raise SystemExit(main())
