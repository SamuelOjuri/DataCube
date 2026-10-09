-- Additive Phase 7 operations. Apply once after 007 in an administrator transaction.
-- Keep schema version 7: API 0.6.0 and 0.7.0 remain independently deployable.
-- Provision LOGIN/password separately, ONLY on the dedicated retention scheduler.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $version$
BEGIN
 IF (SELECT version FROM analyst_state.schema_version) IS DISTINCT FROM 7 THEN
   RAISE EXCEPTION 'Operations require analyst state schema version 7';
 END IF;
END $version$;
CREATE ROLE bi_analyst_maintenance NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
 NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT USAGE ON SCHEMA analyst_state TO bi_analyst_maintenance;
GRANT SELECT ON analyst_state.schema_version TO bi_analyst_maintenance;
DO $policies$
DECLARE name text;
BEGIN
 FOREACH name IN ARRAY ARRAY['conversations','runs','results','result_feedback',
   'workflow_jobs','workflow_events','checkpoints','checkpoint_blobs','checkpoint_writes',
   'audit_events','sessions','oauth_attempts','rate_limits','auth_rate_limit'] LOOP
   EXECUTE format('GRANT SELECT,DELETE ON analyst_state.%I TO bi_analyst_maintenance',name);
   EXECUTE format('CREATE POLICY retention_read ON analyst_state.%I FOR SELECT TO bi_analyst_maintenance USING(true)',name);
   EXECUTE format('CREATE POLICY retention_delete ON analyst_state.%I FOR DELETE TO bi_analyst_maintenance USING(true)',name);
 END LOOP;
END $policies$;
ALTER ROLE bi_analyst_maintenance SET search_path = pg_catalog;
ALTER ROLE bi_analyst_maintenance SET statement_timeout = '30s';
ALTER ROLE bi_analyst_maintenance SET lock_timeout = '1s';
ALTER ROLE bi_analyst_maintenance SET idle_in_transaction_session_timeout = '30s';
-- No principals/identity provisioning, source reads/writes, INSERT, UPDATE, owner
-- memberships or definer functions. Runtime roles still have no DELETE privilege.
