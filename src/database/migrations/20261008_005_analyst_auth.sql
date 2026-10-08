-- Additive Phase 3 authentication. Apply after 003 in one administrator transaction.
-- No operational grants, ETL credentials, source definitions or existing data change.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $version$
BEGIN
 IF (SELECT version FROM analyst_state.schema_version) IS DISTINCT FROM 3 THEN
   RAISE EXCEPTION 'Authentication migration requires analyst state version 3';
 END IF;
END $version$;

CREATE TABLE analyst_state.external_identities (
 provider text NOT NULL CHECK (provider='monday'),
 account_id text NOT NULL CHECK (account_id ~ '^[1-9][0-9]{0,29}$'),
 user_id text NOT NULL CHECK (user_id ~ '^[1-9][0-9]{0,29}$'),
 subject uuid NOT NULL REFERENCES analyst_state.principals(subject),
 PRIMARY KEY(provider,account_id,user_id),
 UNIQUE(provider,subject)
);
COMMENT ON TABLE analyst_state.external_identities IS
 'Offline approved provider/account/user mapping; never auto-provision from email or browser claims.';
CREATE TABLE analyst_state.oauth_attempts (
 state_hash text PRIMARY KEY CHECK (state_hash ~ '^[0-9a-f]{64}$'),
 nonce_hash text NOT NULL CHECK (nonce_hash ~ '^[0-9a-f]{64}$'),
 provider_verifier text CHECK (provider_verifier ~ '^[A-Za-z0-9_-]{43}$'),
 client_challenge text NOT NULL CHECK (client_challenge ~ '^[A-Za-z0-9_-]{43}$'),
 phase text NOT NULL DEFAULT 'pending' CHECK (phase IN ('pending','exchanging','complete','used')),
 subject uuid REFERENCES analyst_state.principals(subject),
 permissions_version integer CHECK (permissions_version>0),
 expires_at timestamptz NOT NULL,
 CHECK (phase NOT IN ('complete','used') OR (subject IS NOT NULL AND permissions_version IS NOT NULL))
);
CREATE INDEX ON analyst_state.oauth_attempts(expires_at);
CREATE TABLE analyst_state.sessions (
 token_hash text PRIMARY KEY CHECK (token_hash ~ '^[0-9a-f]{64}$'),
 owner_id uuid NOT NULL REFERENCES analyst_state.principals(subject),
 permissions_version integer NOT NULL CHECK (permissions_version>0),
 created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
 expires_at timestamptz NOT NULL,
 revoked_at timestamptz,
 CHECK (expires_at>created_at AND expires_at<=created_at+interval '15 minutes')
);
CREATE INDEX ON analyst_state.sessions(owner_id);
CREATE INDEX ON analyst_state.sessions(expires_at);
CREATE TABLE analyst_state.auth_rate_limit (
 id integer PRIMARY KEY CHECK (id=1),
 window_start timestamptz NOT NULL,
 requests integer NOT NULL CHECK (requests>0)
);

DO $tables$
DECLARE name text;
BEGIN
 FOREACH name IN ARRAY ARRAY['external_identities','oauth_attempts','sessions','auth_rate_limit'] LOOP
  EXECUTE format('ALTER TABLE analyst_state.%I OWNER TO bi_analyst_migrator',name);
  EXECUTE format('REVOKE ALL ON analyst_state.%I FROM PUBLIC',name);
  EXECUTE format('ALTER TABLE analyst_state.%I ENABLE ROW LEVEL SECURITY',name);
  EXECUTE format('ALTER TABLE analyst_state.%I FORCE ROW LEVEL SECURITY',name);
 END LOOP;
END $tables$;
CREATE POLICY identity_lookup ON analyst_state.external_identities FOR SELECT TO bi_analyst_state
 USING (provider||':'||account_id||':'||user_id=current_setting('bi_analyst.external_identity',true));
CREATE POLICY identity_admin ON analyst_state.external_identities TO bi_analyst_migrator USING(true) WITH CHECK(true);
CREATE POLICY attempt_scope ON analyst_state.oauth_attempts TO bi_analyst_state
 USING (state_hash=current_setting('bi_analyst.oauth_state',true))
 WITH CHECK (state_hash=current_setting('bi_analyst.oauth_state',true));
CREATE POLICY attempt_maintenance ON analyst_state.oauth_attempts TO bi_analyst_migrator USING(true) WITH CHECK(true);
CREATE POLICY session_read ON analyst_state.sessions FOR SELECT TO bi_analyst_state
 USING (token_hash=current_setting('bi_analyst.session_hash',true));
CREATE POLICY session_create ON analyst_state.sessions FOR INSERT TO bi_analyst_state
 WITH CHECK (token_hash=current_setting('bi_analyst.session_hash',true)
   AND owner_id=nullif(current_setting('bi_analyst.subject',true),'')::uuid
   AND EXISTS (SELECT FROM analyst_state.principals p WHERE p.subject=owner_id
       AND p.enabled AND p.company_wide AND p.permissions_version=sessions.permissions_version));
CREATE POLICY session_revoke ON analyst_state.sessions FOR UPDATE TO bi_analyst_state
 USING (token_hash=current_setting('bi_analyst.session_hash',true))
 WITH CHECK (token_hash=current_setting('bi_analyst.session_hash',true));
CREATE POLICY session_admin ON analyst_state.sessions TO bi_analyst_migrator USING(true) WITH CHECK(true);
CREATE POLICY auth_budget ON analyst_state.auth_rate_limit TO bi_analyst_state USING(id=1) WITH CHECK(id=1);
CREATE POLICY budget_admin ON analyst_state.auth_rate_limit TO bi_analyst_migrator USING(true) WITH CHECK(true);
GRANT SELECT ON analyst_state.external_identities TO bi_analyst_state;
GRANT SELECT,INSERT ON analyst_state.oauth_attempts,analyst_state.sessions,analyst_state.auth_rate_limit TO bi_analyst_state;
GRANT UPDATE(phase,provider_verifier,subject,permissions_version,expires_at) ON analyst_state.oauth_attempts TO bi_analyst_state;
GRANT UPDATE(revoked_at) ON analyst_state.sessions TO bi_analyst_state;
GRANT UPDATE(window_start,requests) ON analyst_state.auth_rate_limit TO bi_analyst_state;
ALTER TABLE analyst_state.schema_version DROP CONSTRAINT schema_version_version_check;
UPDATE analyst_state.schema_version SET version=5;
ALTER TABLE analyst_state.schema_version ADD CHECK (version=5);

-- The version-specific effective privilege audit is appended below.
-- BEGIN AUTH AUDIT
DO $audit$
DECLARE failures text;
BEGIN
 SELECT string_agg(role_name||': '||issue||' ['||object_name||']', E'\n') INTO failures FROM (
-- BEGIN SHARED AUDIT
-- Option A service-role audit. PUBLIC invoker helpers, USAGE and TEMP are retained.
-- State version 5: this exact query is embedded in migration 005 and used by API startup.
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
 VALUES ('schema_version'),('principals'),('conversations'),('runs'),('results'),('rate_limits'),('audit_events'),('external_identities'),('oauth_attempts'),('sessions'),('auth_rate_limit')
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
          AND c.relname IN ('conversations','runs','results','rate_limits','audit_events','oauth_attempts','sessions','auth_rate_limit'))
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
) findings;
 IF failures IS NOT NULL THEN RAISE EXCEPTION 'Unsafe analyst privileges: %', failures; END IF;
END $audit$;
