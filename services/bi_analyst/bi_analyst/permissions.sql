-- Option A effective-privilege audit. USAGE and TEMP are accepted.
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
 AND NOT EXISTS (SELECT FROM reviewed_functions f
   WHERE p.oid=to_regprocedure(f.signature) AND p.prosecdef=f.definer
     AND p.provolatile::text=f.volatility AND lang.lanname='sql'
     AND p.prokind='f' AND p.proconfig=ARRAY['search_path=""']::text[]
     AND p.prosrc=f.body AND p.prosqlbody IS NULL)
)
SELECT role_name,issue,object_name FROM violations ORDER BY role_name,issue,object_name
