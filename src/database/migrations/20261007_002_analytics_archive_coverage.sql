-- Optional archive coverage interface; does not enable archive reporting.
-- Exact SQL from src.services.monday_archive.coverage, with reviewed board IDs.
-- No privileged Python imports, raw lifecycle payloads, or SECURITY DEFINER.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE OR REPLACE VIEW analytics.archive_coverage_v1 WITH (security_invoker=true, security_barrier=true) AS
SELECT
        (SELECT count(*) FROM public.reportable_projects p
         LEFT JOIN public.monday_item_lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
         WHERE l.monday_state IS NULL AND NOT COALESCE(l.blocked,false)) AS unverified_projects,
        (SELECT count(*) FROM public.subitems s JOIN public.reportable_projects p ON p.monday_id=s.parent_monday_id
         JOIN public.monday_item_lifecycle pl ON pl.table_name='projects' AND pl.monday_id=p.monday_id
         LEFT JOIN public.monday_item_lifecycle l ON l.table_name='subitems' AND l.monday_id=s.monday_id
         WHERE pl.monday_state='active' AND NOT pl.blocked AND NOT COALESCE(l.blocked,false)
           AND (l.monday_state IS NULL OR (l.monday_state='active'
                AND l.state_evidence->>'parent_monday_id' IS DISTINCT FROM s.parent_monday_id))) AS unverified_subitems,
        (SELECT count(DISTINCT s.hidden_item_id) FROM public.current_subitems s
         LEFT JOIN public.monday_item_lifecycle l ON l.table_name='hidden_items' AND l.monday_id=s.hidden_item_id
         WHERE s.hidden_item_id IS NOT NULL AND l.monday_state IS NULL
           AND NOT COALESCE(l.blocked,false)) AS unverified_sources,
        (SELECT count(*) FROM public.current_projects p
         JOIN public.monday_item_lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
         WHERE l.state_evidence->>'transaction_values_verified' IS DISTINCT FROM 'true'
            OR jsonb_typeof(l.state_evidence->'item'->'subitems') IS DISTINCT FROM 'array'
            OR EXISTS (
                SELECT FROM jsonb_array_elements(CASE
                    WHEN jsonb_typeof(l.state_evidence->'item'->'subitems')='array'
                    THEN l.state_evidence->'item'->'subitems' ELSE '[]'::jsonb END) member
                WHERE COALESCE(member->>'state','active')='active' AND NOT EXISTS (
                    SELECT FROM public.current_subitems s WHERE s.monday_id=member->>'id'
                      AND s.parent_monday_id=p.monday_id))) AS unverified_current_values,
        (SELECT count(*) FROM public.monday_lifecycle_events e
         WHERE e.payload->>'archive_policy'='verified_archive_v1' AND (e.status IN ('retry','review')
           OR (e.status IN ('pending','processing')
               AND e.payload->>'cause' IS DISTINCT FROM 'periodic_lifecycle_check'))
           AND ((e.board_id='1825117125' AND EXISTS (SELECT FROM public.reportable_projects p WHERE p.monday_id=e.item_id))
             OR (e.board_id='1825117144' AND EXISTS (SELECT FROM public.subitems s JOIN public.reportable_projects p
                 ON p.monday_id=s.parent_monday_id WHERE s.monday_id=e.item_id))
             OR (e.board_id='1825138260' AND EXISTS (SELECT FROM public.subitems s JOIN public.reportable_projects p
                 ON p.monday_id=s.parent_monday_id WHERE s.hidden_item_id=e.item_id)))) AS unresolved_archive_jobs;
COMMENT ON VIEW analytics.archive_coverage_v1 IS 'Five unchanged archive.coverage counters; zero is necessary but not sufficient for verified-active reporting.';
REVOKE ALL ON analytics.archive_coverage_v1 FROM PUBLIC;
DO $revoke$
DECLARE role_name text;
BEGIN
 FOR role_name IN SELECT rolname FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role') LOOP
   EXECUTE format('REVOKE ALL ON analytics.archive_coverage_v1 FROM %I',role_name);
 END LOOP;
END $revoke$;
COMMENT ON COLUMN analytics.archive_coverage_v1.unverified_projects IS 'Blocking archive coverage count; same definition as monday_archive.coverage.';
COMMENT ON COLUMN analytics.archive_coverage_v1.unverified_subitems IS 'Blocking archive coverage count; same definition as monday_archive.coverage.';
COMMENT ON COLUMN analytics.archive_coverage_v1.unverified_sources IS 'Blocking archive coverage count; same definition as monday_archive.coverage.';
COMMENT ON COLUMN analytics.archive_coverage_v1.unverified_current_values IS 'Blocking archive coverage count; same definition as monday_archive.coverage.';
COMMENT ON COLUMN analytics.archive_coverage_v1.unresolved_archive_jobs IS 'Blocking archive coverage count; same definition as monday_archive.coverage.';
