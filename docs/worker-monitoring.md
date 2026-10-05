# Worker monitoring and durable recovery

The API and webhook service now expose local worker health, persist instance
heartbeats, and use durable jobs for ordinary webhook processing and analysis /
Monday column updates. The existing Monday lifecycle queue and its deletion
evidence rules remain in place. The lifecycle CLI loop publishes the same health
information as the applications.

## Current Render service layout

The Render service names differ from the Python entry-point names:

| Render service | Start command | `MONDAY_LIFECYCLE_ENABLED` | `SCHEDULER_ENABLED` |
|---|---|---|---|
| Main API (public web service) | `uvicorn src.webhooks.webhook_server:app --host 0.0.0.0 --port 10000` | `true` | `false` (unused by this entry point) |
| Background worker | `uvicorn src.api.app:app --host 0.0.0.0 --port 10000` | `true` | `true` |

The public service receives Monday webhooks and runs the ordinary webhook,
general queue and lifecycle consumers. Keep its lifecycle flag enabled: the
current code also uses that flag to accept deletion/restoration deliveries and
returns HTTP 503 for those deliveries when it is disabled.

The background service runs all seven scheduled jobs, the general queue and the
lifecycle consumer. Database claims coordinate the consumers across services.
Setting `SCHEDULER_ENABLED=false` on this background service would stop the seven
scheduled jobs. Both services need access to the same production database.

The public service reports monitoring role `webhook`; the background service
reports role `api`. Keep the watchdog's expected roles as `api,webhook` for this
layout. There is no `lifecycle-cli` role because neither service runs the lifecycle
CLI command.

A Render background worker receives no incoming network traffic, even when its
start command launches Uvicorn. Configure `/health/live` on the public web service;
monitor the background service through its database heartbeat and the independent
watchdog. See [Render background workers](https://render.com/docs/background-workers).

## Deploy in this order

1. Apply `src/database/schema/worker_operations.sql` in the production SQL editor.
   This additive migration is safe to rerun. It does not replay or delete existing
   work, change business rows, or expose new public database policies. Do not run
   the full historical `schema.sql` against an existing database.
   The migration explicitly enables Row Level Security (RLS) on `job_queue` and
   all four monitoring tables. If Supabase shows the warning from an older copy
   of the SQL, choose **Run and enable RLS**. These are backend tables; no browser
   access policies are needed. The documented privileged PostgreSQL connection
   and server-side service-role client retain access. A custom restricted
   PostgreSQL role would need its own grants and policies.
2. Set `SUPABASE_DB_URL` on both application services and the lifecycle CLI host.
   **Use a direct PostgreSQL connection or a session-mode pooler. Transaction-mode
   pooling is unsupported:** the queue and scheduler hold session advisory locks
   across transactions and external work. Retain existing Monday, Supabase and
   analysis credentials. Retain `MONDAY_LIFECYCLE_ENABLED=true` where lifecycle
   processing is intended. `SCHEDULER_ENABLED` defaults to `true` in the main API;
   set it to `false` on API services that must not schedule work. The webhook
   service does not host the scheduler.
3. Deploy the new code to **both** services. Stop/drain old instances; do not leave
   old and new consumers running as a permanent mixed deployment. Old instances
   still enqueue into memory. Pending records they leave behind are reported as
   `legacy`, rather than silently replayed. A bounded shutdown waits for current
   work; remaining new work stays in PostgreSQL for recovery.
4. Configure the platform's HTTP restart health check as **`/health/live`** on each
   web service. For the current Render layout, this is the Main API service only;
   use database heartbeats and the watchdog for the background-worker service.
   The HTTP check detects dead worker tasks, stale polling and work exceeding its
   execution budget. Use `/health/ready` for traffic readiness and dependency
   monitoring; a database outage returns 503 there without turning a responsive
   process into a liveness failure. `/health` is an alias for readiness.
5. Set a private `WORKER_MONITOR_TOKEN` to enable `GET /health/workers` with
   `Authorization: Bearer <token>`. Without a configured, matching token the
   endpoint returns 403. It returns worker states, instance identity, current job
   identifiers, progress age and last success; no payloads or credentials.
6. Run the independent watchdog described below. A process cannot report its own
   death after it has stopped; this step is required to close that monitoring gap.

No schema migration, production deployment or notification is performed merely
by installing these source changes. Application startup never runs migrations.

## Independent watchdog and alerts

For a one-time, read-only check from an environment configured for the target DB:

```powershell
& .\report.venv\Scripts\python.exe -m scripts.health_check `
    --expected-service api --expected-service webhook
```

Exit codes: `0` healthy, `1` operational alerts, `2` check/configuration/delivery
failure. The default expected roles are `api,webhook`; override with repeated
`--expected-service` or `WORKER_EXPECTED_SERVICES`. Add `lifecycle-cli` only when
that standalone worker is intentionally deployed. Missing roles alert even if
they have never published a heartbeat.

Run this command on a **separate supervised monitoring service**, or call the
one-time check from an external scheduler and alert on nonzero exit status:

```text
python -m scripts.health_check --watch --record --notify
```

`--record` maintains active/resolved rows in `worker_alerts`. `--notify` explicitly
enables delivery to `WORKER_ALERT_WEBHOOK_URL`, an HTTPS receiver accepting JSON
`{"text":"..."}`. Configure the destination before using it. Delivery has a
five-second timeout, does not follow redirects, and excludes payloads and raw
exception text. In watch mode unchanged alerts repeat at most every 15 minutes;
restarting the watchdog resets that in-memory delivery cooldown. One-shot runs
with `--notify` can notify on every invocation. Supervise the watchdog itself and
alert externally when it stops; sharing the application's failure domain cannot
detect a total outage reliably.

The watchdog checks:

- Instance heartbeats older than 90 seconds and missing expected roles.
- Failed/degraded workers and active jobs exceeding their execution budget.
- Eligible queue work waiting over 15 minutes, expired claims, failed jobs and
  lifecycle events requiring review.
- Schedules over 15 minutes late, runs exceeding two hours, partial/failed/missed
  runs and maximum-instance skips.
- Old ordinary webhook records that were accepted before durable ingress but
  were never completed or placed on the durable queue.

Every process publishes a heartbeat approximately every 15 seconds. A worker
idle poll older than 75 seconds fails local liveness. Healthy idle workers and
explicitly disabled lifecycle/scheduler roles are valid states. The default
execution budgets are 30 minutes for general/webhook jobs, 20 minutes for lifecycle
work and two hours for scheduled work. Override a local budget with
`WORKER_<NAME>_DEADLINE_SECONDS`, for example `WORKER_GENERAL_DEADLINE_SECONDS` or
`WORKER_SCHEDULE_RECENT_REHYDRATE_DEADLINE_SECONDS` (minimum 30 seconds).
The watchdog's schedule-duration threshold is two hours; adjust it in concert
with local scheduled-job budgets before changing production deadlines.

## What survives a restart

New general/webhook jobs are stored in `job_queue` with `queue_name` and an immutable
`dedupe_key`. Claims use `FOR UPDATE SKIP LOCKED`, a lease token, a 30-minute lease
and a session advisory lock for the target. An expired lease cannot duplicate
work while its original database session still holds the lock. When the process
dies, PostgreSQL releases its locks; another worker can recover the job after
lease expiry. Jobs for one target remain ordered across retries. Failures retry
with increasing delay, up to five attempts, then remain `failed` for review.

Delivery is **at least once**, not exactly once. A network split can release a
database session while an external API call is still finishing. General queue
Monday pushes therefore assign current analysis column values and **do not create
timeline messages**. Analysis storage and ordinary webhook processing can repeat;
downstream enqueues use stable keys. Completion and the analysis-to-push follow-up
are committed atomically. The separately scheduled Monday sync retains its
existing timeline-update behavior and is never automatically replayed for the
same scheduled occurrence.

HTTP webhook acknowledgement happens only after the receipt and its queued job
commit together. Failure to persist returns 503 so the sender can retry. In-memory
deduplication no longer suppresses ordinary deliveries. Accepted delivery counts,
completed jobs and processing failures are distinct; `/metrics` process counters
reset on restart, while queue and receipt status remains durable.

All seven schedules use explicit UTC. Interval triggers share an epoch anchor
across replicas. A database lock gives each job one active owner, and a unique
`(job_id, scheduled_for)` ledger entry prevents duplicate execution of an occurrence.
Partial helper results remain partial, exceptions remain failures, and missing
maintenance functions fail visibly. Schedule expectations survive application
restarts. A crash during a schedule is reported for review rather than rerunning
possibly non-idempotent work automatically.
Later occurrences of that job also wait while its prior run remains `running`.
After confirming the original process has stopped and reviewing its effects,
mark that exact run `failed` with a finish time to allow future occurrences.

## Review and recovery

Use the watchdog report and read-only SQL to examine the affected identifiers:

```sql
SELECT id, job_type, project_id, queue_name, status, attempts, detail,
       available_at, lease_until, updated_at
FROM public.job_queue
WHERE status <> 'completed'
ORDER BY created_at;

SELECT job_id, scheduled_for, outcome, counts, error_type, started_at, finished_at
FROM public.worker_job_runs
ORDER BY scheduled_for DESC LIMIT 100;
```

For a **reviewed failed new job**, retry its existing record after resolving the
cause. This preserves its deduplication identity and avoids a duplicate enqueue:

```sql
-- Substitute the one reviewed job UUID. Never reset an actively running claim.
UPDATE public.job_queue
SET status='retry', attempts=0, available_at=now(), detail=NULL,
    lease_until=NULL, lease_token=NULL, updated_at=now()
WHERE id='REVIEWED-JOB-UUID'::uuid
  AND queue_name IN ('general','webhook') AND status='failed'
RETURNING id, status;
```

Legacy records (`queue_name IS NULL`) need individual review against current data.
Do not bulk mark them queued: the previous worker might have completed external
writes before losing status. Use a fresh, intentional rehydrate/push after review
and retain the legacy row as an audit record. Use the existing lifecycle CLI for
lifecycle review/requeue; its evidence and staging requirements still apply.

Graceful shutdown retires an instance heartbeat. If a known terminated deployment
left a stale instance, confirm the exact process has stopped before retiring its
heartbeat; never retire a current instance to silence an alert:

```sql
UPDATE public.worker_heartbeats SET stopped_at=now()
WHERE instance_id='CONFIRMED-TERMINATED-INSTANCE'
  AND heartbeat_at < now()-interval '5 minutes'
RETURNING instance_id, service, heartbeat_at;
```

## Validation

Offline tests cover health status codes and authorization, idle/disabled/busy/dead
workers, startup/shutdown wiring, durable ingress, scheduler outcomes and UTC slots.
PostgreSQL tests use randomly named disposable databases on an explicitly supplied
loopback server. They cover migration reruns, concurrent ownership, crash recovery,
stale acknowledgements, atomic follow-ups, retries, duplicate deliveries and alerts.
Do not run legacy live-service tests as deployment smoke tests.
