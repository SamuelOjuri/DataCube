-- Apply after monday_lifecycle_archive_state.sql. No business rows are changed.
-- Stop old writers before recording archive states. Reporting activation is separate.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_constraint
        WHERE conrelid='public.monday_item_lifecycle'::regclass
          AND conname='monday_item_lifecycle_state_observation_check' AND convalidated) THEN
        RAISE EXCEPTION 'Apply monday_lifecycle_archive_state.sql first';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS monday_item_lifecycle_current_rechecks
    ON public.monday_item_lifecycle(recheck_after,table_name,monday_id)
    WHERE monday_state IN ('active','archived');

CREATE OR REPLACE FUNCTION public.monday_item_is_archived(p_table text, p_id text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_state text;
BEGIN
    IF p_id IS NULL THEN RETURN false; END IF;
    INSERT INTO public.monday_item_lifecycle(table_name,monday_id) VALUES (p_table,p_id)
    ON CONFLICT (table_name,monday_id) DO UPDATE SET monday_id=EXCLUDED.monday_id
    RETURNING monday_state INTO v_state;
    RETURN v_state = 'archived';
END $$;

CREATE OR REPLACE FUNCTION public.guard_monday_archived_item()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF TG_TABLE_NAME='subitems' AND TG_OP='UPDATE' THEN
        IF NEW.hidden_item_id IS NULL AND OLD.hidden_item_id IS NOT NULL
            AND current_setting('datacube.archive_worker_protocol',true)='verified_archive_v1'
            AND (to_jsonb(NEW)-ARRAY['hidden_item_id','updated_at','last_synced_at'])
                =(to_jsonb(OLD)-ARRAY['hidden_item_id','updated_at','last_synced_at'])
            AND EXISTS (SELECT FROM public.monday_item_lifecycle
                WHERE table_name='hidden_items' AND monday_id=OLD.hidden_item_id AND blocked) THEN
            RETURN NEW;
        END IF;
    END IF;
    IF public.monday_item_is_archived(TG_TABLE_NAME,NEW.monday_id) THEN
        RAISE EXCEPTION 'Archived % % requires verified lifecycle restoration',
            TG_TABLE_NAME, NEW.monday_id USING ERRCODE='55000';
    END IF;
    IF TG_TABLE_NAME='projects' AND TG_OP='UPDATE' THEN
        IF (to_jsonb(NEW)->'total_order_value',to_jsonb(NEW)->'total_amount_invoiced',to_jsonb(NEW)->'new_enquiry_value')
            IS DISTINCT FROM
            (to_jsonb(OLD)->'total_order_value',to_jsonb(OLD)->'total_amount_invoiced',to_jsonb(OLD)->'new_enquiry_value')
            AND current_setting('datacube.archive_worker_protocol',true) IS DISTINCT FROM 'verified_archive_v1'
            AND EXISTS (
                SELECT FROM public.subitems s
                WHERE s.parent_monday_id=NEW.monday_id
                  AND (public.monday_item_is_archived('subitems',s.monday_id)
                       OR public.monday_item_is_archived('hidden_items',s.hidden_item_id))
            ) THEN
            RAISE EXCEPTION 'Archive-related parent totals require a verified current-value writer'
                USING ERRCODE='55000';
        END IF;
    END IF;
    IF TG_TABLE_NAME='subitems' THEN
        IF public.monday_item_is_archived('projects',NEW.parent_monday_id)
            OR public.monday_item_is_archived('hidden_items',NEW.hidden_item_id) THEN
            RAISE EXCEPTION 'Subitem % has an archived parent or source', NEW.monday_id
                USING ERRCODE='55000';
        END IF;
    END IF;
    RETURN NEW;
END $$;

DO $$
DECLARE name text;
BEGIN
    FOREACH name IN ARRAY ARRAY['projects','subitems','hidden_items'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS guard_monday_archived_item ON public.%I',name);
        EXECUTE format('CREATE TRIGGER guard_monday_archived_item BEFORE INSERT OR UPDATE ON public.%I
            FOR EACH ROW EXECUTE FUNCTION public.guard_monday_archived_item()',name);
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION public.guard_monday_archive_worker()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.payload ? 'archive_policy' THEN
        IF NEW.payload->>'archive_policy' <> 'verified_archive_v1' THEN
            RAISE EXCEPTION 'Unsupported archive policy';
        END IF;
        IF NEW.status='processing' AND
            current_setting('datacube.archive_worker_protocol',true) IS DISTINCT FROM 'verified_archive_v1' THEN
            RAISE EXCEPTION 'Archive job requires an archive-capable worker';
        END IF;
    END IF;
    IF TG_OP='UPDATE' AND OLD.payload ? 'archive_policy' AND
        (NEW.payload->>'archive_policy' IS DISTINCT FROM OLD.payload->>'archive_policy') THEN
        RAISE EXCEPTION 'Archive policy is immutable';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_monday_archive_worker ON public.monday_lifecycle_events;
CREATE TRIGGER guard_monday_archive_worker BEFORE INSERT OR UPDATE ON public.monday_lifecycle_events
    FOR EACH ROW EXECUTE FUNCTION public.guard_monday_archive_worker();

-- Backend-only views. Do not expose the private evidence table to browser roles.
CREATE OR REPLACE VIEW public.current_projects WITH (security_invoker=true) AS
SELECT p.* FROM public.reportable_projects p
JOIN public.monday_item_lifecycle l ON l.table_name='projects' AND l.monday_id=p.monday_id
WHERE l.monday_state='active' AND NOT l.blocked;

CREATE OR REPLACE VIEW public.current_hidden_items WITH (security_invoker=true) AS
SELECT h.* FROM public.hidden_items h
JOIN public.monday_item_lifecycle l ON l.table_name='hidden_items' AND l.monday_id=h.monday_id
WHERE l.monday_state='active' AND NOT l.blocked;

CREATE OR REPLACE VIEW public.current_subitems WITH (security_invoker=true) AS
SELECT s.* FROM public.subitems s
JOIN public.current_projects p ON p.monday_id=s.parent_monday_id
JOIN public.monday_item_lifecycle l ON l.table_name='subitems' AND l.monday_id=s.monday_id
WHERE l.monday_state='active' AND NOT l.blocked
  AND l.state_evidence->>'parent_monday_id'=s.parent_monday_id;

REVOKE ALL ON public.current_projects, public.current_hidden_items, public.current_subitems FROM PUBLIC;
REVOKE ALL ON FUNCTION public.monday_item_is_archived(text,text),
    public.guard_monday_archived_item(), public.guard_monday_archive_worker() FROM PUBLIC;
DO $$
DECLARE role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['anon','authenticated'] LOOP
        IF EXISTS (SELECT FROM pg_roles WHERE rolname=role_name) THEN
            EXECUTE format('REVOKE ALL ON public.current_projects, public.current_hidden_items,
                public.current_subitems FROM %I',role_name);
        END IF;
    END LOOP;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname='service_role') THEN
        GRANT SELECT ON public.current_projects, public.current_hidden_items, public.current_subitems TO service_role;
    END IF;
END $$;
NOTIFY pgrst, 'reload schema';
COMMIT;
