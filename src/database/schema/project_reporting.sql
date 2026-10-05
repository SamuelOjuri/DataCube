-- Reporting decisions are local metadata, independent of Monday sales/lifecycle state.
-- No project, subitem, analysis, snapshot or lifecycle marker is changed here.
CREATE TABLE IF NOT EXISTS public.project_reporting_classifications (
    monday_id text PRIMARY KEY,
    classification text NOT NULL CHECK (classification IN
        ('redundant_placeholder', 'needs_review', 'released')),
    reason text NOT NULL CHECK (btrim(reason) <> ''),
    reviewed_by text NOT NULL CHECK (btrim(reviewed_by) <> ''),
    reviewed_at timestamptz NOT NULL DEFAULT now(),
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.project_reporting_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    monday_id text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT now(),
    database_actor text NOT NULL DEFAULT session_user,
    before_decision jsonb,
    after_decision jsonb
);

CREATE OR REPLACE FUNCTION public.audit_project_reporting_decision()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $$
BEGIN
    INSERT INTO public.project_reporting_audit(monday_id,before_decision,after_decision)
    VALUES (COALESCE(NEW.monday_id, OLD.monday_id),
        CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
        CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END);
    RETURN COALESCE(NEW, OLD);
END;
$$;
DROP TRIGGER IF EXISTS audit_project_reporting_decision ON public.project_reporting_classifications;
CREATE TRIGGER audit_project_reporting_decision
AFTER INSERT OR UPDATE OR DELETE ON public.project_reporting_classifications
FOR EACH ROW EXECUTE FUNCTION public.audit_project_reporting_decision();

-- Names flag candidates; only an explicit per-ID decision can exclude a record.
-- Null amounts are allowed for these reviewed empty records, never inferred as
-- verified financial zeroes. A negative amount is also meaningful data.
CREATE OR REPLACE FUNCTION public.project_placeholder_is_empty(p jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path = '' AS $$
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
$$;

-- The owner sees child membership even when a report reader cannot read subitems.
-- The function discloses only excluded IDs; raw project access retains its RLS.
CREATE OR REPLACE FUNCTION public.excluded_project_ids()
RETURNS TABLE(monday_id text) LANGUAGE sql STABLE SECURITY DEFINER SET search_path = '' AS $$
    SELECT p.monday_id
    FROM public.project_reporting_classifications c
    JOIN public.projects p ON p.monday_id = c.monday_id
    WHERE c.classification = 'redundant_placeholder'
      AND public.project_placeholder_is_empty(to_jsonb(p))
      AND NOT EXISTS (SELECT 1 FROM public.subitems s WHERE s.parent_monday_id = p.monday_id);
$$;

CREATE OR REPLACE VIEW public.reportable_projects WITH (security_invoker = true) AS
SELECT p.* FROM public.projects p
WHERE p.monday_id NOT IN (SELECT monday_id FROM public.excluded_project_ids());

CREATE OR REPLACE VIEW public.project_reporting_review WITH (security_invoker = true) AS
SELECT p.monday_id, p.item_name, p.project_name, p.date_created,
       c.classification AS recorded_classification, c.reason, c.reviewed_by, c.reviewed_at,
       p.monday_id IN (SELECT monday_id FROM public.excluded_project_ids()) AS reporting_excluded,
       CASE
           WHEN c.classification = 'redundant_placeholder'
                AND p.monday_id NOT IN (SELECT monday_id FROM public.excluded_project_ids())
               THEN 'needs_review_changed_record'
           WHEN c.classification IS NOT NULL THEN c.classification
           ELSE 'candidate_unreviewed'
       END AS review_status
FROM public.projects p
LEFT JOIN public.project_reporting_classifications c ON c.monday_id = p.monday_id
WHERE c.monday_id IS NOT NULL OR lower(btrim(p.item_name)) = 'new project';

COMMENT ON VIEW public.reportable_projects IS
    'Business reporting/analysis population. Sync and lifecycle writers must continue using projects.';
COMMENT ON VIEW public.project_reporting_review IS
    'Unreviewed default names and explicit decisions. Changed records immediately re-enter reports pending review.';

ALTER TABLE public.project_reporting_classifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.project_reporting_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.project_reporting_classifications, public.project_reporting_audit,
    public.reportable_projects, public.project_reporting_review FROM PUBLIC;
REVOKE ALL ON FUNCTION public.audit_project_reporting_decision(),
    public.excluded_project_ids(), public.project_placeholder_is_empty(jsonb) FROM PUBLIC;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN
        REVOKE ALL ON public.project_reporting_classifications, public.project_reporting_audit,
            public.reportable_projects, public.project_reporting_review FROM anon;
        REVOKE ALL ON FUNCTION public.excluded_project_ids(),
            public.project_placeholder_is_empty(jsonb) FROM anon;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN
        REVOKE ALL ON public.project_reporting_classifications, public.project_reporting_audit,
            public.project_reporting_review FROM authenticated;
        GRANT SELECT ON public.reportable_projects TO authenticated;
        GRANT EXECUTE ON FUNCTION public.excluded_project_ids() TO authenticated;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN
        GRANT SELECT, INSERT, UPDATE ON public.project_reporting_classifications TO service_role;
        GRANT SELECT ON public.project_reporting_audit, public.reportable_projects,
            public.project_reporting_review TO service_role;
        GRANT EXECUTE ON FUNCTION public.excluded_project_ids(),
            public.project_placeholder_is_empty(jsonb) TO service_role;
    END IF;
END;
$$;
NOTIFY pgrst, 'reload schema';
