-- Targeted compatibility fix for existing Power BI reporting views.
-- Execute as the database owner, inside a transaction.
-- Verify using the reader's direct connection; SQL Editor may not allow SET ROLE.
-- The current projects SELECT RLS policy already permits this reporting role.
-- This deliberately grants full read access to projects (including placeholders);
-- reportable_projects continues to apply reviewed placeholder exclusions.
-- No analyst roles, PUBLIC grants, policies, function bodies or view options change.
GRANT SELECT ON public.projects, public.reportable_projects TO powerbi_reader;
GRANT EXECUTE ON FUNCTION public.excluded_project_ids() TO powerbi_reader;
