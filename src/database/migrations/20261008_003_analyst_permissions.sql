-- Phase 3 bootstrap. Run once, in one transaction, as the platform migration
-- administrator. No passwords, source rewrites, auth provisioning or certification.
-- Option A: preserve existing PUBLIC/ETL/Power BI grants, including USAGE/TEMP.
-- Approved dependencies are directly readable. The final effective-privilege
-- audit rejects writes/CREATE outside each role's approved scope and unreviewed
-- elevated/private functions. Existing PUBLIC invoker helpers remain callable.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE ROLE bi_analyst_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
CREATE ROLE bi_analyst_state NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
CREATE ROLE bi_analyst_view_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
-- Offline owner of analyst schemas/state only; no operational or role administration.
CREATE ROLE bi_analyst_migrator NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT bi_analyst_view_owner, bi_analyst_migrator TO CURRENT_USER;

CREATE SCHEMA analyst_query AUTHORIZATION bi_analyst_migrator;
CREATE SCHEMA analyst_state AUTHORIZATION bi_analyst_migrator;
REVOKE ALL ON SCHEMA analyst_query, analyst_state FROM PUBLIC;
DO $browser_roles$
DECLARE browser_role text;
BEGIN
 FOR browser_role IN SELECT rolname FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role') LOOP
   EXECUTE format('REVOKE ALL ON SCHEMA analyst_query, analyst_state FROM %I',browser_role);
 END LOOP;
END $browser_roles$;
GRANT USAGE ON SCHEMA analyst_query TO bi_analyst_reader, bi_analyst_view_owner;
GRANT CREATE ON SCHEMA analyst_query TO bi_analyst_view_owner;
GRANT USAGE ON SCHEMA analyst_state TO bi_analyst_state;
GRANT USAGE ON SCHEMA analytics, public TO bi_analyst_view_owner, bi_analyst_reader;

-- Nested invoker views check the original caller even beneath owner-executed
-- gateways. Option A intentionally permits direct SELECT on these dependencies.
-- No shared PUBLIC privileges or existing consumer grants are changed.
GRANT SELECT ON public.projects, public.reportable_projects, public.subitems,
 public.hidden_items, public.analysis_results, public.vw_actual_enquiry_monthly_v1,
 public.vw_actual_bookings_monthly_v1 TO bi_analyst_view_owner, bi_analyst_reader;
CREATE POLICY analyst_gateway_read ON public.projects FOR SELECT TO bi_analyst_view_owner, bi_analyst_reader USING (true);
CREATE POLICY analyst_gateway_read ON public.subitems FOR SELECT TO bi_analyst_view_owner, bi_analyst_reader USING (true);
CREATE POLICY analyst_gateway_read ON public.hidden_items FOR SELECT TO bi_analyst_view_owner, bi_analyst_reader USING (true);
CREATE POLICY analyst_gateway_read ON public.analysis_results FOR SELECT TO bi_analyst_view_owner, bi_analyst_reader USING (true);
DO $source_function$
BEGIN
 IF to_regprocedure('public.excluded_project_ids()') IS NOT NULL THEN
   GRANT EXECUTE ON FUNCTION public.excluded_project_ids() TO bi_analyst_view_owner, bi_analyst_reader;
 END IF;
END $source_function$;

-- Explicit allowlist, no default/future grants. Optional archive and legacy revenue
-- diagnostics are excluded until their dependencies receive separate review.
DO $gateways$
DECLARE relation_name text;
BEGIN
 FOREACH relation_name IN ARRAY ARRAY['projects_v1','children_v1','child_totals_v1',
   'hidden_values_v1','latest_analysis_v1','enquiry_monthly_v1','bookings_monthly_v1',
   'invoice_reporting_facts_v1','conversion_cohorts_v1','coverage_v1'] LOOP
   EXECUTE format('GRANT SELECT ON analytics.%I TO bi_analyst_view_owner, bi_analyst_reader',relation_name);
   EXECUTE format('CREATE VIEW analyst_query.%I WITH (security_barrier=true, security_invoker=false) AS SELECT * FROM analytics.%I',relation_name,relation_name);
   EXECUTE format('ALTER VIEW analyst_query.%I OWNER TO bi_analyst_view_owner',relation_name);
   EXECUTE format('REVOKE ALL ON analyst_query.%I FROM PUBLIC',relation_name);
   EXECUTE format('GRANT SELECT ON analyst_query.%I TO bi_analyst_reader',relation_name);
   EXECUTE format('COMMENT ON VIEW analyst_query.%I IS %L',relation_name,
     'Company-wide candidate gateway. Source certification remains pending; not a metric enablement gate.');
 END LOOP;
END $gateways$;
REVOKE CREATE ON SCHEMA analyst_query FROM bi_analyst_view_owner;

CREATE TABLE analyst_state.schema_version (version integer PRIMARY KEY CHECK (version = 3));
INSERT INTO analyst_state.schema_version VALUES (3);
CREATE TABLE analyst_state.principals (
 subject uuid PRIMARY KEY,
 enabled boolean NOT NULL DEFAULT false,
 company_wide boolean NOT NULL DEFAULT false,
 permissions_version integer NOT NULL DEFAULT 1 CHECK (permissions_version > 0)
);
COMMENT ON TABLE analyst_state.principals IS 'Administrator-controlled grants. Increment permissions_version on every access change; never derived from client claims.';
CREATE TABLE analyst_state.conversations (
 id uuid PRIMARY KEY,
 owner_id uuid NOT NULL REFERENCES analyst_state.principals(subject),
 title text NOT NULL CHECK (length(btrim(title)) BETWEEN 1 AND 160),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (id, owner_id)
);
CREATE INDEX ON analyst_state.conversations(owner_id, created_at DESC);
CREATE TABLE analyst_state.runs (
 id uuid PRIMARY KEY,
 conversation_id uuid NOT NULL,
 owner_id uuid NOT NULL,
 question text NOT NULL CHECK (length(btrim(question)) BETWEEN 1 AND 8000),
 status text NOT NULL DEFAULT 'registered' CHECK (status IN ('registered','cancelled','completed','failed')),
 permissions_version integer NOT NULL CHECK (permissions_version > 0),
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (id, owner_id),
 FOREIGN KEY (conversation_id, owner_id) REFERENCES analyst_state.conversations(id, owner_id)
);
CREATE INDEX ON analyst_state.runs(owner_id, conversation_id, created_at DESC);
CREATE TABLE analyst_state.results (
 id uuid PRIMARY KEY,
 run_id uuid NOT NULL,
 owner_id uuid NOT NULL,
 permissions_version integer NOT NULL CHECK (permissions_version > 0),
 columns jsonb NOT NULL CHECK (jsonb_typeof(columns)='array' AND jsonb_array_length(columns) BETWEEN 1 AND 100),
 rows jsonb NOT NULL CHECK (jsonb_typeof(rows)='array' AND jsonb_array_length(rows)<=10000),
 provenance jsonb NOT NULL CHECK (jsonb_typeof(provenance)='object'),
 created_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY (run_id, owner_id) REFERENCES analyst_state.runs(id, owner_id),
 CHECK (octet_length(columns::text)+octet_length(rows::text)+octet_length(provenance::text)<=1048576)
);
CREATE INDEX ON analyst_state.results(owner_id, run_id);
CREATE TABLE analyst_state.rate_limits (
 owner_id uuid PRIMARY KEY REFERENCES analyst_state.principals(subject),
 window_start timestamptz NOT NULL,
 requests integer NOT NULL CHECK (requests > 0)
);
CREATE TABLE analyst_state.audit_events (
 id uuid PRIMARY KEY,
 request_id uuid NOT NULL,
 owner_id uuid NOT NULL,
 route text NOT NULL CHECK (length(route)<=160),
 method text NOT NULL CHECK (length(method)<=16),
 status integer NOT NULL CHECK (status BETWEEN 100 AND 599),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON analyst_state.audit_events(created_at);

DO $state$
DECLARE table_name text;
BEGIN
 FOREACH table_name IN ARRAY ARRAY['schema_version','principals','conversations','runs','results','rate_limits','audit_events'] LOOP
   EXECUTE format('ALTER TABLE analyst_state.%I OWNER TO bi_analyst_migrator',table_name);
   EXECUTE format('REVOKE ALL ON analyst_state.%I FROM PUBLIC',table_name);
 END LOOP;
 FOREACH table_name IN ARRAY ARRAY['principals','conversations','runs','results','rate_limits','audit_events'] LOOP
   EXECUTE format('ALTER TABLE analyst_state.%I ENABLE ROW LEVEL SECURITY',table_name);
   EXECUTE format('ALTER TABLE analyst_state.%I FORCE ROW LEVEL SECURITY',table_name);
 END LOOP;
 FOREACH table_name IN ARRAY ARRAY['conversations','runs','results','rate_limits'] LOOP
   EXECUTE format($policy$CREATE POLICY owner_scope ON analyst_state.%I TO bi_analyst_state
     USING (owner_id = nullif(current_setting('bi_analyst.subject',true),'')::uuid
       AND EXISTS (SELECT FROM analyst_state.principals p WHERE p.subject=owner_id AND p.enabled AND p.company_wide))
     WITH CHECK (owner_id = nullif(current_setting('bi_analyst.subject',true),'')::uuid
       AND EXISTS (SELECT FROM analyst_state.principals p WHERE p.subject=owner_id AND p.enabled AND p.company_wide))$policy$, table_name);
 END LOOP;
END $state$;
CREATE POLICY principal_self ON analyst_state.principals FOR SELECT TO bi_analyst_state
 USING (subject = nullif(current_setting('bi_analyst.subject',true),'')::uuid);
CREATE POLICY audit_append ON analyst_state.audit_events FOR INSERT TO bi_analyst_state
 WITH CHECK (owner_id = nullif(current_setting('bi_analyst.subject',true),'')::uuid);
-- Access records are provisioned offline by the scoped non-login owner.
-- This role can maintain its own structures/policies, never operational data.
CREATE POLICY principal_admin ON analyst_state.principals TO bi_analyst_migrator USING (true) WITH CHECK (true);
GRANT SELECT ON analyst_state.schema_version, analyst_state.principals TO bi_analyst_state;
GRANT SELECT, INSERT ON analyst_state.conversations, analyst_state.runs, analyst_state.results TO bi_analyst_state;
GRANT UPDATE (status) ON analyst_state.runs TO bi_analyst_state;
GRANT SELECT, INSERT, UPDATE ON analyst_state.rate_limits TO bi_analyst_state;
GRANT INSERT ON analyst_state.audit_events TO bi_analyst_state;

ALTER ROLE bi_analyst_reader SET default_transaction_read_only = on;
ALTER ROLE bi_analyst_reader SET statement_timeout = '5s';
ALTER ROLE bi_analyst_reader SET lock_timeout = '1s';
ALTER ROLE bi_analyst_reader SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE bi_analyst_reader SET search_path = pg_catalog;
ALTER ROLE bi_analyst_state SET statement_timeout = '5s';
ALTER ROLE bi_analyst_state SET lock_timeout = '1s';
ALTER ROLE bi_analyst_state SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE bi_analyst_state SET search_path = pg_catalog;
-- LOGIN/passwords/CONNECT privileges are provisioned out of band. No credentials
-- for migrator, view_owner or the existing administrator belong in the Render service.

-- Audit runs in the same transaction; any violation rolls back the bootstrap.
DO $effective_permissions$
DECLARE failures text;
BEGIN
 SELECT string_agg(role_name||': '||issue||' ['||object_name||']', E'\n') INTO failures FROM (
-- BEGIN SHARED AUDIT
-- Option A service-role audit. PUBLIC invoker helpers, USAGE and TEMP are retained.
-- This exact query is embedded in migration 003 and used by API startup.
WITH targets AS (
 SELECT r.* FROM pg_catalog.pg_roles r
 WHERE rolname IN ('bi_analyst_reader','bi_analyst_state','bi_analyst_view_owner','bi_analyst_migrator')
), gateway_names(name) AS (
 VALUES ('projects_v1'),('children_v1'),('child_totals_v1'),('hidden_values_v1'),
 ('latest_analysis_v1'),('enquiry_monthly_v1'),('bookings_monthly_v1'),
 ('invoice_reporting_facts_v1'),('conversion_cohorts_v1'),('coverage_v1')
), sources(name) AS (
 VALUES ('projects'),('reportable_projects'),('subitems'),('hidden_items'),
 ('analysis_results'),('vw_actual_enquiry_monthly_v1'),('vw_actual_bookings_monthly_v1')
), state_names(name) AS (
 VALUES ('schema_version'),('principals'),('conversations'),('runs'),('results'),('rate_limits'),('audit_events')
), approved_relations(role_name,schema_name,relation_name) AS (
 SELECT r.rolname,s.schema_name,g.name FROM targets r CROSS JOIN gateway_names g
 CROSS JOIN (VALUES ('analytics'),('analyst_query')) s(schema_name)
 WHERE r.rolname IN ('bi_analyst_reader','bi_analyst_view_owner')
 UNION ALL SELECT r.rolname,'public',s.name FROM targets r CROSS JOIN sources s
 WHERE r.rolname IN ('bi_analyst_reader','bi_analyst_view_owner')
 UNION ALL SELECT 'bi_analyst_state','analyst_state',name FROM state_names WHERE name<>'audit_events'
 UNION ALL SELECT 'bi_analyst_migrator','analyst_state',name FROM state_names
 UNION ALL
 SELECT r.rolname,n.nspname,c.relname FROM targets r CROSS JOIN pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_class'::regclass AND d.objid=c.oid AND d.deptype='e'
 JOIN pg_catalog.pg_extension e ON d.refclassid='pg_catalog.pg_extension'::regclass AND e.oid=d.refobjid
 WHERE e.extname='pg_stat_statements' AND c.relkind='v'
 AND c.relname IN ('pg_stat_statements','pg_stat_statements_info')
 AND EXISTS (SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
             WHERE a.grantee=0 AND a.privilege_type='SELECT')
), reviewed_functions(signature,definer,volatility,body) AS (
 -- Exact reviewed definitions, not blanket trust in STABLE/IMMUTABLE labels.
 VALUES ('public.project_placeholder_is_empty(jsonb)',false,'i', $placeholder$
    SELECT lower(btrim(COALESCE(p->>'item_name',''))) = 'new project'
       AND COALESCE(p->>'pipeline_stage','') IN ('', 'Open Enquiry')
       AND lower(btrim(COALESCE(p->>'product_key',''))) IN ('', 'unknown')
       AND NOT EXISTS (
           SELECT 1 FROM unnest(ARRAY['project_name','account','type','category',
               'zip_code','sales_representative','funding','product_type','feedback',
               'lost_to_who_or_why','expected_start_date','follow_up_date',
               'first_date_designed','last_date_designed','first_date_invoiced',
               'last_date_invoiced','date_order_received']) AS f(name)
           WHERE btrim(COALESCE(p->>f.name,'')) <> '')
       AND NOT EXISTS (
           SELECT 1 FROM unnest(ARRAY['new_enquiry_value','project_value',
               'weighted_pipeline','total_order_value','total_amount_invoiced',
               'probability_percent','gestation_period']) AS f(name)
           WHERE COALESCE(NULLIF(p->>f.name,''),'0')::numeric <> 0);
$placeholder$), ('public.excluded_project_ids()',true,'s', $excluded$
    SELECT p.monday_id
    FROM public.project_reporting_classifications c
    JOIN public.projects p ON p.monday_id = c.monday_id
    WHERE c.classification = 'redundant_placeholder'
      AND public.project_placeholder_is_empty(to_jsonb(p))
      AND NOT EXISTS (SELECT 1 FROM public.subitems s WHERE s.parent_monday_id = p.monday_id);
$excluded$)
), relations AS (
 SELECT c.*,n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast%'
), violations(role_name,issue,object_name) AS (
 SELECT name,'missing_role',name FROM (VALUES ('bi_analyst_reader'),('bi_analyst_state'),('bi_analyst_view_owner'),('bi_analyst_migrator')) wanted(name)
 WHERE NOT EXISTS(SELECT FROM targets WHERE rolname=name)
 UNION ALL
 SELECT rolname,'unsafe_role',rolname FROM targets r
 WHERE rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls
    OR (rolname IN ('bi_analyst_view_owner','bi_analyst_migrator') AND rolcanlogin)
    OR EXISTS(SELECT FROM pg_catalog.pg_auth_members WHERE member=r.oid)
 UNION ALL
 SELECT rolname,'database_create',current_database() FROM targets
 WHERE has_database_privilege(oid,current_database(),'CREATE')
 UNION ALL
 SELECT r.rolname,'schema_create',n.nspname FROM targets r CROSS JOIN pg_catalog.pg_namespace n
 WHERE n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast%'
 AND has_schema_privilege(r.oid,n.oid,'CREATE')
 AND NOT (r.rolname='bi_analyst_migrator' AND n.nspname IN ('analyst_query','analyst_state'))
 UNION ALL
 SELECT r.rolname,'schema_ownership',n.nspname FROM targets r JOIN pg_catalog.pg_namespace n ON n.nspowner=r.oid
 WHERE n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast%'
 AND NOT (r.rolname='bi_analyst_migrator' AND n.nspname IN ('analyst_query','analyst_state'))
 UNION ALL
 SELECT r.rolname,'object_ownership',c.nspname||'.'||c.relname FROM targets r JOIN relations c ON c.relowner=r.oid
 WHERE NOT (r.rolname='bi_analyst_view_owner' AND c.nspname='analyst_query' AND c.relkind='v'
            AND c.relname IN (SELECT name FROM gateway_names))
 AND NOT (r.rolname='bi_analyst_migrator' AND c.nspname='analyst_state')
 UNION ALL
 SELECT r.rolname,'function_ownership',p.oid::regprocedure::text FROM targets r JOIN pg_catalog.pg_proc p ON p.proowner=r.oid
 UNION ALL
 SELECT r.rolname,'unapproved_read',c.nspname||'.'||c.relname FROM targets r CROSS JOIN relations c
 WHERE c.relkind IN ('r','v','m','p','f') AND c.nspname NOT IN ('pg_catalog','information_schema')
 AND (has_table_privilege(r.oid,c.oid,'SELECT') OR has_any_column_privilege(r.oid,c.oid,'SELECT'))
 AND NOT EXISTS(SELECT FROM approved_relations a WHERE a.role_name=r.rolname AND a.schema_name=c.nspname AND a.relation_name=c.relname)
 UNION ALL
 SELECT r.rolname,'persistent_write',c.nspname||'.'||c.relname FROM targets r CROSS JOIN relations c
 WHERE c.relkind IN ('r','v','m','p','f')
 AND (has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
      OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')
      OR CASE WHEN current_setting('server_version_num')::integer>=170000
              THEN has_table_privilege(r.oid,c.oid,'MAINTAIN') ELSE false END)
 AND NOT (r.rolname='bi_analyst_state' AND c.nspname='analyst_state'
          AND c.relname IN ('conversations','runs','results','rate_limits','audit_events'))
 -- The non-login migrator administers only the named analyst state tables.
 AND NOT (r.rolname='bi_analyst_migrator' AND c.nspname='analyst_state'
          AND c.relname IN (SELECT name FROM state_names))
 -- pg_settings exposes session SET semantics, not persistent operational DML.
 AND NOT (c.oid='pg_catalog.pg_settings'::regclass AND c.oid<16384)
 -- The non-login owner necessarily has rights on its own approved views.
 AND NOT (r.rolname='bi_analyst_view_owner' AND c.nspname='analyst_query'
          AND c.relkind='v' AND c.relname IN (SELECT name FROM gateway_names))
 UNION ALL
 SELECT r.rolname,'sequence_write',c.nspname||'.'||c.relname FROM targets r CROSS JOIN relations c
 WHERE c.relkind='S' AND has_sequence_privilege(r.oid,c.oid,'USAGE,UPDATE')
 UNION ALL
 SELECT r.rolname,'large_object_write',l.oid::text FROM targets r CROSS JOIN pg_catalog.pg_largeobject_metadata l
 WHERE l.lomowner=r.oid OR EXISTS (
   SELECT FROM aclexplode(coalesce(l.lomacl,acldefault('L',l.lomowner))) acl
   WHERE acl.privilege_type='UPDATE' AND acl.grantee IN (0,r.oid))
 UNION ALL
 SELECT r.rolname,'parameter_privilege',p.parname FROM targets r CROSS JOIN pg_catalog.pg_parameter_acl p
 WHERE has_parameter_privilege(r.oid,p.parname,'SET,ALTER SYSTEM')
 UNION ALL
 SELECT r.rolname,'foreign_server_access',s.srvname FROM targets r CROSS JOIN pg_catalog.pg_foreign_server s
 WHERE has_server_privilege(r.oid,s.oid,'USAGE')
 UNION ALL
 SELECT r.rolname,'unreviewed_function',p.oid::regprocedure::text FROM targets r
 CROSS JOIN pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 JOIN pg_catalog.pg_language lang ON lang.oid=p.prolang
 WHERE has_function_privilege(r.oid,p.oid,'EXECUTE')
 -- Built-in PostgreSQL functions are trusted platform code, not unrestricted
 -- query tools. Phase 4 must allowlist SQL/functions within READ ONLY transactions.
 AND NOT (n.nspname IN ('pg_catalog','information_schema') AND p.oid<16384 AND NOT p.prosecdef)
 -- PUBLIC invoker functions run with the caller's ACLs, not the owner's.
 -- This is compatibility with the shared database, not arbitrary-SQL approval.
 AND NOT (NOT p.prosecdef AND EXISTS (
   SELECT FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
   WHERE a.grantee=0 AND a.privilege_type='EXECUTE'))
 AND NOT EXISTS (SELECT FROM reviewed_functions f
   WHERE p.oid=to_regprocedure(f.signature) AND p.prosecdef=f.definer
     AND p.provolatile::text=f.volatility AND lang.lanname='sql'
     AND p.prokind='f' AND p.proconfig=ARRAY['search_path=""']::text[]
     AND replace(p.prosrc,E'\r\n',E'\n')=replace(f.body,E'\r\n',E'\n') AND p.prosqlbody IS NULL)
)
SELECT role_name,issue,object_name FROM violations ORDER BY role_name,issue,object_name
-- END SHARED AUDIT
 ) violations;
 IF failures IS NOT NULL THEN
   RAISE EXCEPTION 'Unsafe analyst effective privileges:%', E'\n'||failures;
 END IF;
END $effective_permissions$;
