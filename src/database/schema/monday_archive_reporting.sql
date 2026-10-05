-- Apply after the archive runtime migration and existing forecast migrations.
-- Copies installed formula definitions rather than maintaining a second formula.
-- Existing views, materializations, training sources and snapshots are untouched.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

DO $$
DECLARE
    spec text[];
    definition text;
    rewritten text;
    role_name text;
BEGIN
    FOREACH spec SLICE 1 IN ARRAY ARRAY[
        ['vw_pipeline_forecast_project_v1','current_pipeline_forecast_project','reportable_projects','current_projects'],
        ['vw_pipeline_smoothing_score_v1','current_pipeline_smoothing_score','vw_pipeline_forecast_project_v1','current_pipeline_forecast_project'],
        ['mv_pipeline_forecast_monthly_12m_v1','current_pipeline_forecast_monthly','vw_pipeline_forecast_project_v1','current_pipeline_forecast_project'],
        ['mv_pipeline_smoothed_revenue_monthly_12m_v1','current_pipeline_smoothed_monthly','vw_pipeline_smoothing_score_v1','current_pipeline_smoothing_score']
    ] LOOP
        IF to_regclass('public.'||spec[1]) IS NULL THEN
            RAISE EXCEPTION 'Install existing forecast relation % first',spec[1];
        END IF;
        definition := pg_get_viewdef(to_regclass('public.'||spec[1]),true);
        rewritten := regexp_replace(definition,'\m'||spec[3]||'\M',spec[4],'g');
        IF rewritten=definition THEN
            RAISE EXCEPTION '% no longer references %; review the installed formula before migrating',spec[1],spec[3];
        END IF;
        EXECUTE format('CREATE OR REPLACE VIEW public.%I WITH (security_invoker=true) AS %s',spec[2],rewritten);
        EXECUTE format('REVOKE ALL ON public.%I FROM PUBLIC',spec[2]);
        FOREACH role_name IN ARRAY ARRAY['anon','authenticated'] LOOP
            IF EXISTS (SELECT FROM pg_roles WHERE rolname=role_name) THEN
                EXECUTE format('REVOKE ALL ON public.%I FROM %I',spec[2],role_name);
            END IF;
        END LOOP;
        IF EXISTS (SELECT FROM pg_roles WHERE rolname='service_role') THEN
            EXECUTE format('GRANT SELECT ON public.%I TO service_role',spec[2]);
        END IF;
    END LOOP;
END $$;

-- New snapshot entry points cannot replace existing historical snapshots.
DO $$
DECLARE spec text[]; definition text; rewritten text; signature regprocedure;
BEGIN
    FOREACH spec SLICE 1 IN ARRAY ARRAY[
        ['create_pipeline_forecast_snapshot','create_current_pipeline_forecast_snapshot','vw_pipeline_forecast_project_v1','current_pipeline_forecast_project','pipeline_forecast_snapshot'],
        ['create_pipeline_smoothing_forecast_snapshot','create_current_pipeline_smoothing_forecast_snapshot','vw_pipeline_smoothing_score_v1','current_pipeline_smoothing_score','pipeline_smoothing_forecast_snapshot']
    ] LOOP
        signature := to_regprocedure('public.'||spec[1]||'(date)');
        IF signature IS NULL THEN RAISE EXCEPTION 'Install %(date) first',spec[1]; END IF;
        definition := pg_get_functiondef(signature);
        rewritten := regexp_replace(definition,'\m'||spec[3]||'\M',spec[4],'g');
        IF rewritten=definition OR position('target_snapshot_date' in definition)=0 THEN
            RAISE EXCEPTION 'Unexpected snapshot definition for %; review before migrating',spec[1];
        END IF;
        rewritten := regexp_replace(rewritten,'\m'||spec[1]||'\M',spec[2],'g');
        rewritten := regexp_replace(rewritten,'\mBEGIN\M',format(
            'BEGIN IF target_snapshot_date IS DISTINCT FROM CURRENT_DATE OR EXISTS
             (SELECT FROM public.%I WHERE snapshot_date=target_snapshot_date) THEN
             RAISE EXCEPTION ''Current archive-aware snapshots require today and cannot replace history'';
             END IF;',spec[5]),'i');
        EXECUTE rewritten;
        EXECUTE format('REVOKE ALL ON FUNCTION public.%I(date) FROM PUBLIC',spec[2]);
        IF EXISTS (SELECT FROM pg_roles WHERE rolname='service_role') THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION public.%I(date) TO service_role',spec[2]);
        END IF;
    END LOOP;
END $$;
NOTIFY pgrst,'reload schema';
COMMIT;
