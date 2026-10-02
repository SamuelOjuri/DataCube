-- Install separately after review. No order data is modified by this migration.
-- Use the same privileged database role for the operator CLI and this migration.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '15s';

CREATE TABLE IF NOT EXISTS public.order_value_scope_commits (
    run_id uuid NOT NULL,
    scope_id text NOT NULL,
    plan_sha256 text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('orders', 'repair')),
    project_ids text[] NOT NULL,
    before_sha256 text NOT NULL,
    after_sha256 text NOT NULL,
    updated_rows jsonb NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, scope_id)
);

ALTER TABLE public.order_value_scope_commits ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.order_value_scope_commits FROM PUBLIC;
DO $$
DECLARE api_role text;
BEGIN
    FOREACH api_role IN ARRAY ARRAY['anon', 'authenticated', 'service_role'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = api_role) THEN
            EXECUTE format('REVOKE ALL ON public.order_value_scope_commits FROM %I', api_role);
        END IF;
    END LOOP;
END $$;
COMMENT ON TABLE public.order_value_scope_commits IS
    'Order correction commit journal. Written atomically with each scope; source verification is separate.';
COMMIT;
