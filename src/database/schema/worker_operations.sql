-- Apply BEFORE deploying worker monitoring. Additive and safe to rerun.
-- Old in-memory jobs are retained for review, never silently replayed.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS public.job_queue (
    id uuid PRIMARY KEY, job_type text NOT NULL, project_id text,
    status text NOT NULL, attempts integer DEFAULT 0, payload jsonb,
    detail text, created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now()
);
ALTER TABLE public.job_queue ADD COLUMN IF NOT EXISTS queue_name text;
ALTER TABLE public.job_queue ADD COLUMN IF NOT EXISTS dedupe_key text;
ALTER TABLE public.job_queue ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE public.job_queue ADD COLUMN IF NOT EXISTS lease_until timestamptz;
ALTER TABLE public.job_queue ADD COLUMN IF NOT EXISTS lease_token uuid;
ALTER TABLE public.job_queue ADD COLUMN IF NOT EXISTS owner_id text;
CREATE UNIQUE INDEX IF NOT EXISTS job_queue_dedupe ON public.job_queue(dedupe_key);
CREATE INDEX IF NOT EXISTS job_queue_claim ON public.job_queue(queue_name, available_at, created_at)
    WHERE status IN ('queued','retry','running');
CREATE INDEX IF NOT EXISTS job_queue_target_order ON public.job_queue(queue_name,project_id,created_at,id)
    WHERE status IN ('queued','retry','running');
CREATE INDEX IF NOT EXISTS job_queue_webhook_receipt ON public.job_queue((payload->>'webhook_log_id'))
    WHERE queue_name='webhook';

CREATE TABLE IF NOT EXISTS public.worker_heartbeats (
    instance_id text PRIMARY KEY, service text NOT NULL, revision text,
    started_at timestamptz NOT NULL, heartbeat_at timestamptz NOT NULL DEFAULT now(),
    stopped_at timestamptz, status jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_heartbeat_time ON public.worker_heartbeats(heartbeat_at);
CREATE TABLE IF NOT EXISTS public.worker_schedules (
    job_id text PRIMARY KEY, next_due_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.worker_job_runs (
    id uuid PRIMARY KEY, job_id text NOT NULL, scheduled_for timestamptz NOT NULL,
    instance_id text NOT NULL, started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz, outcome text NOT NULL, counts jsonb NOT NULL DEFAULT '{}',
    error_type text, UNIQUE(job_id, scheduled_for)
);
CREATE INDEX IF NOT EXISTS worker_runs_recent ON public.worker_job_runs(job_id, started_at DESC);
CREATE TABLE IF NOT EXISTS public.worker_alerts (
    alert_key text PRIMARY KEY, first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(), resolved_at timestamptz,
    summary jsonb NOT NULL
);

-- Backend DB connections own these tables. No new browser/API access is granted.
-- No client policies are created: queue access is for the table owner/BYPASSRLS
-- database role and the existing server-side service-role client only.
ALTER TABLE public.job_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.worker_heartbeats ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.worker_schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.worker_job_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.worker_alerts ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.worker_heartbeats, public.worker_schedules,
    public.worker_job_runs, public.worker_alerts FROM PUBLIC;
COMMIT;
