-- Apply once after 007 and 008 in a reviewed administrator transaction.
-- Keep schema version 7 and existing run deadlines for application rollback.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $version$
BEGIN
 IF (SELECT version FROM analyst_state.schema_version) IS DISTINCT FROM 7 THEN
   RAISE EXCEPTION 'Reasoning budgets require analyst state schema version 7';
 END IF;
END $version$;

ALTER TABLE analyst_state.workflow_jobs
 DROP CONSTRAINT workflow_jobs_remaining_seconds_check;
ALTER TABLE analyst_state.workflow_jobs
 ADD CONSTRAINT workflow_jobs_remaining_seconds_600_check
 CHECK (remaining_seconds BETWEEN 0 AND 600);
