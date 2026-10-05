-- Preparatory migration only. Apply after monday_lifecycle.sql.
-- Do not populate these fields until the archive-aware application is deployed.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    required_table text;
BEGIN
    IF current_setting('server_version_num')::int < 170000 THEN
        RAISE EXCEPTION 'Monday lifecycle archive metadata requires PostgreSQL 17+';
    END IF;
    FOREACH required_table IN ARRAY ARRAY[
        'monday_item_lifecycle', 'monday_lifecycle_events', 'monday_lifecycle_audit'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = required_table
              AND c.relkind = 'r' AND c.relrowsecurity
        ) THEN
            RAISE EXCEPTION 'Require public.% with RLS enabled; install/check monday_lifecycle.sql first',
                required_table;
        END IF;
    END LOOP;
END $$;

-- NULL means unverified. Neither an existing row nor blocked=false proves active.
ALTER TABLE public.monday_item_lifecycle
    ADD COLUMN IF NOT EXISTS monday_state text,
    ADD COLUMN IF NOT EXISTS state_verified_at timestamptz,
    ADD COLUMN IF NOT EXISTS state_event_key text,
    ADD COLUMN IF NOT EXISTS state_evidence jsonb;

DO $$
DECLARE
    column_spec record;
BEGIN
    FOR column_spec IN
        SELECT * FROM (VALUES
            ('monday_state', 'text'),
            ('state_verified_at', 'timestamp with time zone'),
            ('state_event_key', 'text'),
            ('state_evidence', 'jsonb')
        ) AS expected(column_name, type_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute a
            WHERE a.attrelid = 'public.monday_item_lifecycle'::regclass
              AND a.attname = column_spec.column_name
              AND NOT a.attisdropped AND NOT a.attnotnull
              AND a.attgenerated = '' AND a.attidentity = ''
              AND format_type(a.atttypid, a.atttypmod) = column_spec.type_name
              AND NOT EXISTS (
                  SELECT 1 FROM pg_attrdef d
                  WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
              )
        ) THEN
            RAISE EXCEPTION 'Unexpected archive metadata definition for %. Expected nullable % without a default',
                column_spec.column_name, column_spec.type_name;
        END IF;
    END LOOP;
END $$;

-- Replacing this migration-owned check is atomic and validates existing values.
ALTER TABLE public.monday_item_lifecycle
    DROP CONSTRAINT IF EXISTS monday_item_lifecycle_state_observation_check;
ALTER TABLE public.monday_item_lifecycle
    ADD CONSTRAINT monday_item_lifecycle_state_observation_check CHECK (
        (
            monday_state IS NULL
            AND state_verified_at IS NULL
            AND state_event_key IS NULL
            AND state_evidence IS NULL
        )
        OR (
            monday_state IS NOT NULL
            AND monday_state IN ('active', 'archived', 'deleted')
            AND state_verified_at IS NOT NULL
            AND isfinite(state_verified_at)
            AND state_event_key IS NOT NULL
            AND btrim(state_event_key) <> ''
            AND state_evidence IS NOT NULL
            AND jsonb_typeof(state_evidence) = 'object'
            AND state_evidence <> '{}'::jsonb
        )
    );

CREATE INDEX IF NOT EXISTS monday_item_lifecycle_archived_rechecks
    ON public.monday_item_lifecycle (recheck_after, table_name, monday_id)
    WHERE monday_state = 'archived';

COMMENT ON COLUMN public.monday_item_lifecycle.monday_state IS
    'Last verified Monday API lifecycle state; NULL is unverified. Never infer from business labels, absence, or blocked.';
COMMENT ON COLUMN public.monday_item_lifecycle.state_verified_at IS
    'Time the API lifecycle state was verified, not an inferred archive/deletion occurrence time.';
COMMENT ON COLUMN public.monday_item_lifecycle.state_event_key IS
    'Durable lifecycle event/audit correlation key for this observation; separate from the deletion guard last_event_key.';
COMMENT ON COLUMN public.monday_item_lifecycle.state_evidence IS
    'Nonempty evidence object for the verified API observation. Future application writes must validate exact IDs and retain transition history in monday_lifecycle_audit.';

NOTIFY pgrst, 'reload schema';
COMMIT;
