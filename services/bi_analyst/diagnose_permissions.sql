-- Read-only catalogue report for a failed Phase 3 bootstrap. Run in TEST as the
-- platform administrator. No grants, roles, schemas, policies or data are changed,
-- and none of the application/extension functions being inspected are executed.
-- One result set; export all rows. This is evidence, not an approval allowlist.
WITH wanted_roles(name) AS (
 VALUES ('bi_analyst_reader'),('bi_analyst_state'),
        ('bi_analyst_view_owner'),('bi_analyst_migrator')
), wanted_schemas(name) AS (
 VALUES ('analyst_query'),('analyst_state')
), wanted_helpers(signature) AS (
 VALUES ('public.project_placeholder_is_empty(jsonb)'),('public.excluded_project_ids()')
), relations AS (
 SELECT c.*,n.nspname
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.relkind IN ('r','v','m','p','f')
   AND n.nspname NOT IN ('pg_catalog','information_schema')
   AND n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast%'
), functions AS (
 SELECT p.oid,
   pg_catalog.format('%I.%I(%s)',n.nspname,p.proname,pg_catalog.oidvectortypes(p.proargtypes)) AS signature,
   EXISTS (
     SELECT FROM pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
     WHERE a.grantee=0 AND a.privilege_type='EXECUTE'
   ) AS public_execute,
   pg_catalog.jsonb_build_object(
     'owner',pg_catalog.pg_get_userbyid(p.proowner),
     'kind',p.prokind,'language',l.lanname,'security_definer',p.prosecdef,
     'volatility',p.provolatile,'settings',p.proconfig,
     'source_md5',pg_catalog.md5(p.prosrc),'sql_standard_body',p.prosqlbody IS NOT NULL,
     'acl',coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))::text,
     'acl_source',CASE WHEN p.proacl IS NULL THEN 'postgres_default' ELSE 'stored_acl' END,
     'public_schema_usage',EXISTS (
       SELECT FROM pg_catalog.aclexplode(coalesce(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) a
       WHERE a.grantee=0 AND a.privilege_type='USAGE'
     ),
     'extension',e.extname,'extension_version',e.extversion
   ) AS details
 FROM pg_catalog.pg_proc p
 JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 LEFT JOIN pg_catalog.pg_depend d
   ON d.classid='pg_catalog.pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e'
 LEFT JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid
 WHERE NOT (n.nspname IN ('pg_catalog','information_schema') AND p.oid<16384 AND NOT p.prosecdef)
), report(section,object_name,details) AS (
 SELECT 'environment',pg_catalog.current_database(),
   pg_catalog.jsonb_build_object(
     'server_version',pg_catalog.current_setting('server_version'),
     'transaction_read_only',pg_catalog.current_setting('transaction_read_only'),
     'current_role',current_user)
 UNION ALL
 SELECT 'bootstrap_role',w.name,pg_catalog.jsonb_build_object(
   'present',r.oid IS NOT NULL,'login',r.rolcanlogin,
   'superuser',r.rolsuper,'bypass_rls',r.rolbypassrls)
 FROM wanted_roles w LEFT JOIN pg_catalog.pg_roles r ON r.rolname=w.name
 UNION ALL
 SELECT 'bootstrap_schema',w.name,pg_catalog.jsonb_build_object(
   'present',n.oid IS NOT NULL,'owner',pg_catalog.pg_get_userbyid(n.nspowner),'acl',n.nspacl::text)
 FROM wanted_schemas w LEFT JOIN pg_catalog.pg_namespace n ON n.nspname=w.name
 UNION ALL
 SELECT 'public_relation_grant',pg_catalog.format('%I.%I',c.nspname,c.relname),
   pg_catalog.jsonb_build_object(
     'scope','relation','privilege',a.privilege_type,'grantable',a.is_grantable,
     'grantor',pg_catalog.pg_get_userbyid(a.grantor),'owner',pg_catalog.pg_get_userbyid(c.relowner),
     'acl',c.relacl::text)
 FROM relations c
 CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
 WHERE a.grantee=0
 UNION ALL
 SELECT 'public_relation_grant',pg_catalog.format('%I.%I',c.nspname,c.relname),
   pg_catalog.jsonb_build_object(
     'scope','column','column',att.attname,'privilege',a.privilege_type,'grantable',a.is_grantable,
     'grantor',pg_catalog.pg_get_userbyid(a.grantor),'owner',pg_catalog.pg_get_userbyid(c.relowner),
     'acl',att.attacl::text)
 FROM relations c JOIN pg_catalog.pg_attribute att ON att.attrelid=c.oid
 CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
 WHERE a.grantee=0 AND att.attnum>0 AND NOT att.attisdropped
 UNION ALL
 SELECT 'public_function_execute',f.signature,f.details FROM functions f WHERE f.public_execute
 UNION ALL
 SELECT 'reporting_helper',w.signature,coalesce(f.details,'{}'::jsonb) || pg_catalog.jsonb_build_object(
   'present',f.oid IS NOT NULL,'public_execute',f.public_execute,
   'definition',pg_catalog.pg_get_functiondef(f.oid),
   'stored_source',(SELECT p.prosrc FROM pg_catalog.pg_proc p WHERE p.oid=f.oid))
 FROM wanted_helpers w LEFT JOIN functions f ON f.oid=pg_catalog.to_regprocedure(w.signature)
)
SELECT section,object_name,details FROM report ORDER BY section,object_name,details::text;
