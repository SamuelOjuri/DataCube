"""Stage/apply a reporting-only migration while preserving deployed SQL definitions.

No Monday mutation, business-row deletion, lifecycle marker or historical
snapshot rewrite is performed. Review files and SQL are retained per run.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re

from dotenv import load_dotenv
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

CORE = Path('src/database/schema/project_reporting.sql')
OPERATIONAL_VIEWS = {'data_freshness'}
REPORTING_FUNCTIONS = {'get_account_performance', 'invoice_smoothing_training_rows',
                       'create_pipeline_smoothing_forecast_snapshot'}
SOURCE = re.compile(r'\b(?:FROM|JOIN)\s+(?:public\.)?projects\b', re.I)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def capture(connection):
    views = connection.execute("""SELECT c.relname AS name,c.relkind AS kind,
        pg_get_viewdef(c.oid,true) AS definition,pg_get_userbyid(c.relowner) AS owner,
        c.reloptions AS options,obj_description(c.oid,'pg_class') AS comment,
        t.spcname AS tablespace
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_tablespace t ON t.oid=c.reltablespace
        WHERE n.nspname='public' AND c.relkind IN ('v','m') ORDER BY c.relname""").fetchall()
    functions = connection.execute("""SELECT p.proname AS name,
        pg_get_function_identity_arguments(p.oid) AS arguments,pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='public' AND p.prokind='f' AND p.proname=ANY(%s)
        ORDER BY p.proname,arguments""", (sorted(REPORTING_FUNCTIONS),)).fetchall()
    edges = connection.execute("""SELECT DISTINCT child.relname AS child,parent.relname AS parent
        FROM pg_rewrite r JOIN pg_class child ON child.oid=r.ev_class
        JOIN pg_depend d ON d.objid=r.oid AND d.classid='pg_rewrite'::regclass
        JOIN pg_class parent ON parent.oid=d.refobjid AND d.refclassid='pg_class'::regclass
        WHERE child.relnamespace='public'::regnamespace AND parent.relnamespace='public'::regnamespace
          AND child.oid<>parent.oid AND child.relkind IN ('v','m') AND parent.relkind IN ('v','m')
        ORDER BY child.relname,parent.relname""").fetchall()
    indexes = connection.execute("SELECT tablename,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY indexname").fetchall()
    grants = connection.execute("""SELECT c.relname AS name,CASE WHEN a.grantee=0 THEN 'PUBLIC'
        ELSE pg_get_userbyid(a.grantee) END AS grantee,a.privilege_type,a.is_grantable
        FROM pg_class c CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
        WHERE c.relnamespace='public'::regnamespace AND c.relkind IN ('v','m')
        ORDER BY c.relname,a.grantee,a.privilege_type""").fetchall()
    return dict(views=views,functions=functions,edges=edges,indexes=indexes,grants=grants)


def filtered_definition(name, definition):
    if name == 'project_analytics':
        if 'excluded_project_ids' in definition:
            return definition
        # Retain the base-table primary-key GROUP BY functional dependency.
        if len(re.findall(r'\bGROUP BY p\.id', definition)) != 1 or re.search(r'\bWHERE\b', definition):
            raise ValueError('Unexpected project_analytics definition; review before migration')
        return definition.replace('GROUP BY p.id',
            'WHERE p.monday_id NOT IN (SELECT monday_id FROM public.excluded_project_ids())\n  GROUP BY p.id')
    # Deparsed identifiers are unquoted here. Preserve all SQL string literals.
    return re.sub(r"('(?:''|[^'])*')|\b(?:public\.)?projects\b",
                  lambda m: m.group(1) or 'public.reportable_projects', definition)


def ordered(names, edges):
    pending, result = set(names), []
    while pending:
        ready = sorted(n for n in pending if not any(e['child']==n and e['parent'] in pending for e in edges))
        if not ready:
            raise ValueError('Cyclic reporting dependencies')
        result.extend(ready)
        pending.difference_update(ready)
    return result


def plan(catalog):
    views = {v['name']: v for v in catalog['views']}
    changed = {n for n,v in views.items() if n not in OPERATIONAL_VIEWS
               and n not in {'reportable_projects','project_reporting_review'}
               and SOURCE.search(v['definition'])
               and filtered_definition(n,v['definition']) != v['definition']}
    rebuild = {n for n in changed if views[n]['kind']=='m'}
    while True:
        expanded = rebuild | {e['child'] for e in catalog['edges'] if e['parent'] in rebuild}
        if expanded == rebuild:
            break
        rebuild = expanded
    return dict(changed=sorted(changed), rebuild=ordered(rebuild,catalog['edges']),
                create=ordered(changed | rebuild,catalog['edges']))


def render_migration(connection, catalog, migration_plan):
    statements = [CORE.read_text(encoding='utf-8')]
    views = {v['name']:v for v in catalog['views']}
    def emit(statement):
        statements.append(statement.as_string(connection)+';')
    for name in reversed(migration_plan['rebuild']):
        kind = 'MATERIALIZED VIEW' if views[name]['kind']=='m' else 'VIEW'
        emit(sql.SQL('DROP {} public.{}').format(sql.SQL(kind),sql.Identifier(name)))
    for name in migration_plan['create']:
        view = views[name]
        definition = (filtered_definition(name,view['definition']) if name in migration_plan['changed']
                      else view['definition']).rstrip(';')
        kind = 'MATERIALIZED VIEW' if view['kind']=='m' else 'OR REPLACE VIEW'
        options = (' WITH ('+', '.join(view['options'])+')') if view['options'] else ''
        tablespace = sql.SQL(' TABLESPACE {}').format(sql.Identifier(view['tablespace'])) if view['tablespace'] else sql.SQL('')
        emit(sql.SQL('CREATE {} public.{}{}{} AS {}').format(sql.SQL(kind),sql.Identifier(name),
            sql.SQL(options),tablespace,sql.SQL(definition)))
        if name not in migration_plan['rebuild']:
            continue
        for index in catalog['indexes']:
            if index['tablename']==name:
                statements.append(index['indexdef']+';')
        # Clear creator default grants before restoring the original ACL exactly.
        emit(sql.SQL("DO $acl$ DECLARE grantee_name text; BEGIN FOR grantee_name IN "
            "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END "
            "FROM pg_class c CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
            "WHERE c.oid={}::regclass LOOP EXECUTE format('REVOKE ALL ON {} FROM %s', "
            "CASE WHEN grantee_name='PUBLIC' THEN 'PUBLIC' ELSE quote_ident(grantee_name) END); END LOOP; END $acl$").format(
                sql.Literal('public.'+name),sql.SQL('public.'+sql.Identifier(name).as_string(connection))))
        for grant in catalog['grants']:
            if grant['name']==name:
                role = sql.SQL('PUBLIC') if grant['grantee']=='PUBLIC' else sql.Identifier(grant['grantee'])
                emit(sql.SQL('GRANT {} ON public.{} TO {}{}').format(sql.SQL(grant['privilege_type']),
                    sql.Identifier(name),role,sql.SQL(' WITH GRANT OPTION' if grant['is_grantable'] else '')))
        if view['comment'] is not None:
            emit(sql.SQL('COMMENT ON {} public.{} IS {}').format(
                sql.SQL('MATERIALIZED VIEW' if view['kind']=='m' else 'VIEW'),sql.Identifier(name),sql.Literal(view['comment'])))
        emit(sql.SQL('ALTER {} public.{} OWNER TO {}').format(
            sql.SQL('MATERIALIZED VIEW' if view['kind']=='m' else 'VIEW'),sql.Identifier(name),sql.Identifier(view['owner'])))
    for function in catalog['functions']:
        if SOURCE.search(function['definition']):
            statements.append(filtered_definition(function['name'],function['definition']).rstrip(';')+';')
    # Refresh current aggregates, including dependencies through SQL functions.
    # Snapshot tables deliberately retain their historical point-in-time values.
    for name in ordered({n for n,v in views.items() if v['kind']=='m'},catalog['edges']):
        emit(sql.SQL('REFRESH MATERIALIZED VIEW public.{}').format(sql.Identifier(name)))
    statements.append("NOTIFY pgrst, 'reload schema';")
    return ('\n\n'.join(statements)+'\n').replace('\r\n', '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['stage','apply','verify'])
    parser.add_argument('--run-dir',type=Path,required=True)
    args = parser.parse_args()
    load_dotenv()
    with psycopg.connect(os.environ['SUPABASE_DB_URL'],autocommit=True,row_factory=dict_row,connect_timeout=10) as connection:
        if args.command=='stage':
            args.run_dir.mkdir(parents=True,exist_ok=False)
            with connection.transaction():
                connection.execute('SET TRANSACTION READ ONLY')
                catalog = capture(connection)
                migration_plan = plan(catalog)
                statement = render_migration(connection,catalog,migration_plan)
            manifest = dict(catalog_hash=digest(catalog),sql_hash=hashlib.sha256(statement.encode()).hexdigest(),plan=migration_plan)
            (args.run_dir/'before.json').write_text(json.dumps(catalog,indent=2),encoding='utf-8')
            (args.run_dir/'migration.sql').write_bytes(statement.encode('utf-8'))
            (args.run_dir/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
            print(json.dumps(manifest,indent=2))
        elif args.command=='apply':
            manifest = json.loads((args.run_dir/'manifest.json').read_text())
            statement = (args.run_dir/'migration.sql').read_bytes().decode('utf-8')
            if hashlib.sha256(statement.encode()).hexdigest()!=manifest['sql_hash']:
                raise ValueError('Staged SQL changed; stage again')
            with connection.transaction():
                connection.execute("SET LOCAL lock_timeout='2s'")
                connection.execute("SET LOCAL statement_timeout='120s'")
                connection.execute("SET LOCAL transaction_timeout='180s'")
                if digest(capture(connection))!=manifest['catalog_hash']:
                    raise ValueError('Database definitions changed; stage again')
                connection.execute(statement)
                if plan(capture(connection))['changed']:
                    raise ValueError('Unfiltered reporting definitions remain')
            (args.run_dir/'applied.json').write_text(json.dumps({'status':'applied','sql_hash':manifest['sql_hash']}),encoding='utf-8')
            print('Reporting migration committed; business rows and historical snapshots retained.')
        else:
            residual = plan(capture(connection))['changed']
            rows = connection.execute('SELECT review_status,count(*) AS count FROM public.project_reporting_review GROUP BY review_status').fetchall()
            counts = connection.execute('SELECT (SELECT count(*) FROM projects) AS retained_projects, '
                '(SELECT count(*) FROM reportable_projects) AS reportable_projects, '
                '(SELECT count(*) FROM excluded_project_ids()) AS excluded_projects').fetchone()
            result = dict(unfiltered_reporting_objects=residual,classifications=rows,counts=counts)
            (args.run_dir/'verification.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
            print(json.dumps(result,indent=2))
            if residual:
                raise SystemExit(1)


if __name__=='__main__':
    main()
