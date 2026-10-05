-- Apply once in Supabase SQL Editor BEFORE enabling MONDAY_LIFECYCLE_ENABLED.
-- No application rows are deleted by this migration. PostgreSQL 17+.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF current_setting('server_version_num')::int < 170000 THEN
        RAISE EXCEPTION 'Monday lifecycle requires PostgreSQL 17+';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=ANY(c.conkey)
        WHERE c.contype='f' AND c.conrelid='public.subitems'::regclass
          AND c.confrelid='public.projects'::regclass AND c.confdeltype='c'
          AND a.attname='parent_monday_id' AND c.convalidated AND NOT c.condeferrable
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=ANY(c.conkey)
        WHERE c.contype='f' AND c.conrelid='public.subitems'::regclass
          AND c.confrelid='public.hidden_items'::regclass AND c.confdeltype='n'
          AND a.attname='hidden_item_id' AND c.convalidated AND NOT c.condeferrable
    ) THEN
        RAISE EXCEPTION 'Require immediate validated parent CASCADE and hidden source SET NULL foreign keys';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.monday_lifecycle_events (
    event_key text PRIMARY KEY,
    board_id text NOT NULL,
    item_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('delete','restore','refresh','reconcile')),
    parent_id text,
    payload jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','processing','retry','processed','ignored','review')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_until timestamptz,
    result jsonb,
    last_error text,
    received_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);
CREATE INDEX IF NOT EXISTS monday_lifecycle_ready
    ON public.monday_lifecycle_events(next_attempt_at, received_at)
    WHERE status IN ('pending','retry','processing');

-- An active row is also a lock record. ON CONFLICT below makes a stale writer
-- wait/recheck under READ COMMITTED, or fail serialization under REPEATABLE READ.
CREATE TABLE IF NOT EXISTS public.monday_item_lifecycle (
    table_name text NOT NULL CHECK (table_name IN ('projects','hidden_items','subitems')),
    monday_id text NOT NULL,
    blocked boolean NOT NULL DEFAULT false,
    last_event_key text,
    former_parent_id text,
    changed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, monday_id)
);
CREATE TABLE IF NOT EXISTS public.monday_lifecycle_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_key text NOT NULL,
    action text NOT NULL,
    table_name text NOT NULL,
    monday_id text NOT NULL,
    before_row jsonb,
    evidence jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS monday_lifecycle_audit_item
    ON public.monday_lifecycle_audit(table_name, monday_id, id);
ALTER TABLE public.monday_item_lifecycle ADD COLUMN IF NOT EXISTS
    recheck_after timestamptz NOT NULL DEFAULT now()+interval '1 day';
CREATE INDEX IF NOT EXISTS monday_lifecycle_rechecks
    ON public.monday_item_lifecycle(recheck_after) WHERE blocked;
CREATE INDEX IF NOT EXISTS monday_lifecycle_former_hidden_owners
    ON public.monday_lifecycle_audit((before_row->>'hidden_item_id'))
    WHERE action='unlink_deleted_hidden_source';

CREATE OR REPLACE FUNCTION public.monday_item_is_blocked(p_table text, p_id text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE v_blocked boolean;
BEGIN
    IF p_id IS NULL THEN RETURN false; END IF;
    INSERT INTO public.monday_item_lifecycle(table_name,monday_id) VALUES (p_table,p_id)
    ON CONFLICT (table_name,monday_id) DO UPDATE SET monday_id=EXCLUDED.monday_id
    RETURNING blocked INTO v_blocked;
    RETURN v_blocked;
END $$;

CREATE OR REPLACE FUNCTION public.guard_monday_deleted_item()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF TG_TABLE_NAME='subitems' THEN
        IF public.monday_item_is_blocked('projects',NEW.parent_monday_id) THEN RETURN NULL; END IF;
        IF public.monday_item_is_blocked('hidden_items',NEW.hidden_item_id) THEN RETURN NULL; END IF;
    END IF;
    IF public.monday_item_is_blocked(TG_TABLE_NAME,NEW.monday_id) THEN
        -- Skip only this deleted record, allowing other rows in a batch to sync.
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS guard_monday_deleted_item ON public.projects;
CREATE TRIGGER guard_monday_deleted_item BEFORE INSERT OR UPDATE ON public.projects
    FOR EACH ROW EXECUTE FUNCTION public.guard_monday_deleted_item();
DROP TRIGGER IF EXISTS guard_monday_deleted_item ON public.hidden_items;
CREATE TRIGGER guard_monday_deleted_item BEFORE INSERT OR UPDATE ON public.hidden_items
    FOR EACH ROW EXECUTE FUNCTION public.guard_monday_deleted_item();
DROP TRIGGER IF EXISTS guard_monday_deleted_item ON public.subitems;
CREATE TRIGGER guard_monday_deleted_item BEFORE INSERT OR UPDATE ON public.subitems
    FOR EACH ROW EXECUTE FUNCTION public.guard_monday_deleted_item();

ALTER TABLE public.monday_lifecycle_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.monday_item_lifecycle ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.monday_lifecycle_audit ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.monday_lifecycle_events, public.monday_item_lifecycle,
    public.monday_lifecycle_audit FROM PUBLIC;
REVOKE ALL ON FUNCTION public.monday_item_is_blocked(text,text),
    public.guard_monday_deleted_item() FROM PUBLIC;
DO $$
DECLARE role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['anon','authenticated'] LOOP
        IF EXISTS (SELECT FROM pg_roles WHERE rolname=role_name) THEN
            EXECUTE format('REVOKE ALL ON public.monday_lifecycle_events, public.monday_item_lifecycle, public.monday_lifecycle_audit FROM %I',role_name);
        END IF;
    END LOOP;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname='service_role') THEN
        REVOKE ALL ON public.monday_lifecycle_events, public.monday_item_lifecycle,
            public.monday_lifecycle_audit FROM service_role;
        GRANT SELECT, INSERT ON public.monday_lifecycle_events TO service_role;
        GRANT SELECT ON public.monday_item_lifecycle, public.monday_lifecycle_audit TO service_role;
    END IF;
END $$;
NOTIFY pgrst, 'reload schema';
COMMIT;
