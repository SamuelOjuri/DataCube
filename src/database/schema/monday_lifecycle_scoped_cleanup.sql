-- Install after monday_lifecycle.sql, before queueing a newly staged cleanup.
-- No existing jobs are requeued or rewritten. Restart workers with the new code.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE OR REPLACE FUNCTION public.guard_monday_cleanup_scope()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    root_key text;
    root_job public.monday_lifecycle_events%ROWTYPE;
    policy text;
BEGIN
    root_key := array_to_string((string_to_array(NEW.event_key, ':'))[1:4], ':');
    IF root_key <> NEW.event_key THEN
        SELECT * INTO root_job FROM public.monday_lifecycle_events WHERE event_key=root_key;
    ELSE
        root_job := NEW;
    END IF;
    policy := root_job.payload->>'cleanup_policy';
    IF TG_OP='UPDATE' AND OLD.payload ? 'cleanup_policy' AND
       (NEW.payload->>'cleanup_policy') IS DISTINCT FROM (OLD.payload->>'cleanup_policy') THEN
        RAISE EXCEPTION 'Cleanup policy cannot be removed or changed';
    END IF;
    IF policy IS NULL AND NEW.payload ? 'cleanup_policy' THEN
        RAISE EXCEPTION 'Cleanup descendant has no scoped root';
    END IF;
    IF policy IS NULL THEN RETURN NEW; END IF;
    IF policy <> 'enquiry_active_open_v1' OR
       root_key !~ '^recovery:[0-9a-f-]{36}:1825117144:[0-9]+$' OR
       root_job.kind <> 'delete' OR root_job.board_id <> '1825117144' OR
       root_job.item_id <> split_part(root_key, ':', 4) OR
       root_job.parent_id IS NULL OR root_job.parent_id !~ '^[0-9]+$' THEN
        RAISE EXCEPTION 'Invalid scoped cleanup root';
    END IF;
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.event_key,NEW.kind,NEW.board_id,NEW.item_id,NEW.parent_id)
           IS DISTINCT FROM ROW(OLD.event_key,OLD.kind,OLD.board_id,OLD.item_id,OLD.parent_id) THEN
            RAISE EXCEPTION 'Scoped cleanup identity cannot change';
        END IF;
        IF NOT (OLD.payload ? 'cleanup_policy') AND OLD.status <> 'pending' THEN
            RAISE EXCEPTION 'Only pending jobs may enter a scoped cleanup';
        END IF;
    END IF;
    IF NOT ((NEW.event_key=root_key AND NEW.kind='delete') OR
        (NEW.event_key<>root_key AND NEW.kind='refresh' AND NEW.board_id='1825117125'
            AND NEW.item_id=root_job.parent_id) OR
        (NEW.event_key<>root_key AND NEW.kind='reconcile' AND NEW.board_id=root_job.board_id
            AND NEW.item_id=root_job.item_id AND NEW.parent_id=root_job.parent_id
            AND NEW.payload->>'verification_only'='true')) THEN
        RAISE EXCEPTION 'Job exceeds scoped cleanup targets';
    END IF;
    IF NEW.payload ? 'refresh_mode' AND NEW.payload->>'refresh_mode' IS DISTINCT FROM 'new_enquiry_sum' THEN
        RAISE EXCEPTION 'Scoped cleanup requires enquiry-only refresh';
    END IF;
    NEW.payload := NEW.payload || jsonb_build_object('cleanup_policy',policy,'refresh_mode','new_enquiry_sum');
    IF NEW.status='processing' AND
       (TG_OP='INSERT' OR OLD.status IS DISTINCT FROM NEW.status OR OLD.lease_token IS DISTINCT FROM NEW.lease_token) AND
       current_setting('datacube.lifecycle_worker_protocol',true) IS DISTINCT FROM 'scoped_cleanup_v1' THEN
        RAISE EXCEPTION 'Scoped cleanup requires an updated lifecycle worker';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_monday_cleanup_scope ON public.monday_lifecycle_events;
CREATE TRIGGER guard_monday_cleanup_scope BEFORE INSERT OR UPDATE
ON public.monday_lifecycle_events FOR EACH ROW EXECUTE FUNCTION public.guard_monday_cleanup_scope();
COMMIT;
