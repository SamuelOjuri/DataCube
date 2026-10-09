"""Offline bounded retention and redacted pilot counts. Never runs inside the API.

Default is a read-only preview. --apply commits one batch. Dedicated role only;
no repository .env loading, no source tables, no CRM calls, no raw data output.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from importlib.resources import files

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

TABLES = ('conversations', 'runs', 'results', 'result_feedback', 'workflow_jobs', 'workflow_events',
          'checkpoints', 'checkpoint_blobs', 'checkpoint_writes', 'audit_events', 'sessions',
          'oauth_attempts', 'rate_limits', 'auth_rate_limit')


def validate_dsn(dsn, *, local=False):
    info = conninfo_to_dict(dsn)
    role = 'bi_analyst_maintenance'
    pooler = info.get('host', '').endswith('.pooler.supabase.com')
    user = info.get('user', '')
    if not (user == role or pooler and user.startswith(role + '.') and len(user) > len(role)+1):
        raise ValueError('Use only the dedicated maintenance role')
    if any(k in info for k in ('service', 'options', 'hostaddr')) or ',' in info.get('host', ''):
        raise ValueError('Connection indirection is forbidden')
    if not info.get('host') or not info.get('dbname'):
        raise ValueError('Explicit database and host required')
    if local and info['host'] in {'127.0.0.1', 'localhost', '::1'}:
        return
    if local or info.get('sslmode') != 'verify-full' or info.get('port', '5432') != '5432':
        raise ValueError('Remote maintenance requires verified TLS on port 5432')


def verify_role(conn):
    role = conn.execute("""SELECT current_user AS name,rolsuper,rolbypassrls,rolcreaterole,rolcreatedb,rolreplication
        FROM pg_roles WHERE rolname=current_user""").fetchone()
    if role['name'] != 'bi_analyst_maintenance' or any(role[k] for k in ('rolsuper','rolbypassrls','rolcreaterole','rolcreatedb','rolreplication')):
        raise ValueError('Unsafe maintenance role')
    if conn.execute("SELECT 1 FROM pg_auth_members WHERE member=current_user::regrole").fetchone():
        raise ValueError('Maintenance role must not inherit other roles')
    # Effective rights, including accidental PUBLIC privileges, cannot reach sources.
    unsafe = conn.execute("""SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname NOT LIKE 'pg_%%' AND n.nspname <> 'information_schema'
          AND c.relkind IN ('r','p','v','m','f') AND (
          has_table_privilege(c.oid,'INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER')
          OR has_any_column_privilege(c.oid,'INSERT,UPDATE,REFERENCES')
          OR (has_table_privilege(c.oid,'SELECT,DELETE') AND NOT
              (n.nspname='analyst_state' AND c.relname=ANY(%s))
              AND NOT (c.relkind='v' AND c.relname IN ('pg_stat_statements','pg_stat_statements_info')
                AND EXISTS (SELECT FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid
                  WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e'
                    AND d.refclassid='pg_extension'::regclass AND e.extname='pg_stat_statements')))) LIMIT 1""",
        (list(TABLES) + ['schema_version'],)).fetchone()
    if unsafe:
        raise ValueError('Unexpected maintenance privileges')
    if conn.execute("""SELECT 1 WHERE has_database_privilege(current_database(),'CREATE')
        OR EXISTS(SELECT FROM pg_namespace WHERE nspname NOT LIKE 'pg_temp_%' AND has_schema_privilege(oid,'CREATE'))
        OR EXISTS(SELECT FROM pg_class WHERE CASE WHEN relkind='S'
          THEN has_sequence_privilege(oid,'USAGE,UPDATE') ELSE false END)""").fetchone():
        raise ValueError('Unexpected maintenance create or sequence privileges')
    functions = conn.execute("""SELECT p.oid::regprocedure::text AS signature,p.prosrc,p.prosecdef,
        p.provolatile,p.prokind,p.proconfig,p.prosqlbody,lang.lanname
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language lang ON lang.oid=p.prolang
        WHERE has_function_privilege(p.oid,'EXECUTE')
          AND NOT(n.nspname IN ('pg_catalog','information_schema') AND p.oid<16384 AND NOT p.prosecdef)
          AND NOT(NOT p.prosecdef AND EXISTS(
            SELECT FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            WHERE a.grantee=0 AND a.privilege_type='EXECUTE'))""").fetchall()
    # Reuse the exact reviewed PUBLIC helper body from the runtime permission audit.
    reviewed = files('bi_analyst').joinpath('permissions_presentation.sql').read_text(encoding='utf-8').split('$excluded$')[1]
    for function in functions:
        if not (function['signature']=='public.excluded_project_ids()' and function['prosecdef']
            and function['provolatile']=='s' and function['prokind']=='f' and function['lanname']=='sql'
            and function['proconfig']==['search_path=""'] and function['prosqlbody'] is None
            and function['prosrc'].replace('\r\n','\n')==reviewed.replace('\r\n','\n')):
            raise ValueError('Unexpected maintenance function privilege')
    if conn.execute('SELECT version FROM analyst_state.schema_version').fetchone() != {'version':7}:
        raise ValueError('Maintenance requires schema 7')


def retain(conn, *, conversation_days=30, audit_days=90, batch=100, apply=False, now=None):
    if not 1 <= conversation_days <= 3650 or not conversation_days <= audit_days <= 3650 or not 1 <= batch <= 1000:
        raise ValueError('Invalid retention bounds')
    now = now or datetime.now(timezone.utc)
    cutoff, audit_cutoff = now-timedelta(days=conversation_days), now-timedelta(days=audit_days)
    with conn.transaction():
        if not apply:
            conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL statement_timeout='30s'")
        conn.execute("SET LOCAL lock_timeout='1s'")
        verify_role(conn)
        if apply:
            # Block new runs/resumes and result writes while choosing/deleting a batch.
            # The role has DELETE but no UPDATE, so row FOR UPDATE is not available.
            conn.execute(sql.SQL('LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE').format(
                sql.SQL(',').join(sql.Identifier('analyst_state', t) for t in TABLES)))
        ids = [r['id'] for r in conn.execute("""SELECT c.id FROM analyst_state.conversations c
            WHERE c.created_at < %s
              AND NOT EXISTS (SELECT FROM analyst_state.runs r WHERE r.conversation_id=c.id AND r.created_at >= %s)
              AND NOT EXISTS (SELECT FROM analyst_state.workflow_jobs j WHERE j.conversation_id=c.id AND
                   (j.segment_started_at >= %s OR j.status='running' AND j.deadline >= %s))
              AND NOT EXISTS (SELECT FROM analyst_state.results x JOIN analyst_state.runs r ON r.id=x.run_id
                   WHERE r.conversation_id=c.id AND x.created_at >= %s)
              AND NOT EXISTS (SELECT FROM analyst_state.workflow_jobs j JOIN analyst_state.runs r ON r.id=j.follow_up_to
                   WHERE r.conversation_id=c.id AND j.conversation_id<>c.id)
            ORDER BY c.created_at,c.id LIMIT %s""", (cutoff,cutoff,cutoff,now,cutoff,batch)).fetchall()]
        runs = [r['id'] for r in conn.execute('SELECT id FROM analyst_state.runs WHERE conversation_id=ANY(%s)',(ids,))]
        counts = {'conversations': len(ids), 'runs': len(runs)}
        # Dependency order; checkpoints use run UUID strings and have no SQL FK.
        for table in ('checkpoint_writes','checkpoint_blobs','checkpoints'):
            counts[table] = change(conn, table, sql.SQL('thread_id=ANY(%s)'), ([str(r) for r in runs],), apply)
        counts['result_feedback'] = change(conn, 'result_feedback', sql.SQL(
            'result_id IN (SELECT id FROM analyst_state.results WHERE run_id=ANY(%s))'), (runs,), apply)
        for table in ('results','workflow_events','workflow_jobs'):
            counts[table] = change(conn, table, sql.SQL('run_id=ANY(%s)'), (runs,), apply)
        if apply:
            change(conn, 'runs', sql.SQL('id=ANY(%s)'), (runs,), True)
            change(conn, 'conversations', sql.SQL('id=ANY(%s)'), (ids,), True)
        # Auth and audit batches are bounded independently of conversation size.
        for table, condition, params in (
            ('sessions', 'expires_at < %s', (now,)),
            ('oauth_attempts', 'expires_at < %s', (now,)),
            ('audit_events', 'created_at < %s', (audit_cutoff,)),
            ('rate_limits', 'window_start < %s', (now-timedelta(days=1),)),
            ('auth_rate_limit', 'window_start < %s', (now-timedelta(days=1),)),
        ):
            predicate = sql.SQL('ctid IN (SELECT ctid FROM analyst_state.{} WHERE {} LIMIT %s)').format(
                sql.Identifier(table), sql.SQL(condition))
            counts[table] = change(conn, table, predicate, (*params,batch), apply)
        return {'event':'analyst_retention','applied':apply,'observed_at':now.isoformat(),
                'conversation_days':conversation_days,'audit_days':audit_days,'counts':counts,
                'batch_full':any(counts[name] >= batch for name in
                    ('conversations','sessions','oauth_attempts','audit_events','rate_limits','auth_rate_limit'))}


def change(conn, table, predicate, params, apply):
    statement = sql.SQL('DELETE FROM analyst_state.{} WHERE {}' if apply else
                        'SELECT count(*) AS n FROM analyst_state.{} WHERE {}').format(sql.Identifier(table),predicate)
    cursor = conn.execute(statement, params)
    return cursor.rowcount if apply else cursor.fetchone()['n']


def pilot_report(conn):
    with conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        verify_role(conn)
        return {'event':'analyst_pilot','observed_at':datetime.now(timezone.utc).isoformat(),
            'runs':conn.execute("""SELECT status,count(*) AS count FROM analyst_state.workflow_jobs
                GROUP BY status ORDER BY status""").fetchall(),
            'feedback':conn.execute("SELECT rating,count(*) AS count FROM analyst_state.result_feedback GROUP BY rating ORDER BY rating").fetchall(),
            'overdue_runs':conn.execute("SELECT count(*) AS n FROM analyst_state.workflow_jobs WHERE status='running' AND deadline<now()").fetchone()['n']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--report', action='store_true')
    parser.add_argument('--local-test', action='store_true')
    parser.add_argument('--conversation-days', type=int, default=30)
    parser.add_argument('--audit-days', type=int, default=90)
    parser.add_argument('--batch', type=int, default=100)
    args = parser.parse_args()
    try:
        dsn = os.environ['BI_ANALYST_MAINTENANCE_DSN']
        validate_dsn(dsn, local=args.local_test)
        with psycopg.connect(dsn, autocommit=True, row_factory=dict_row, connect_timeout=5) as conn:
            result = pilot_report(conn) if args.report else retain(conn, conversation_days=args.conversation_days,
                audit_days=args.audit_days,batch=args.batch,apply=args.apply)
        print(json.dumps(result))
    except Exception:
        print(json.dumps({'event':'analyst_retention','status':'failed'}))
        raise SystemExit(2) from None


if __name__ == '__main__':
    main()
