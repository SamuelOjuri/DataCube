<!-- e80ad9e9-8d70-48cf-afc5-eb49215f4709 6c968dd9-82ca-4053-8b0e-5c5fb8af343c -->
# Post Webhook Automation Rollout Plan

1. **Refactor Scripts into Library Tasks**  

- Extract core logic from `scripts/delta_hydrated_sync.py`, `scripts/delta_rehydrate_sync.py`, `scripts/backfill_analysis_with_llm.py`, and `scripts/sync_monday_from_supabase.py` into callable helpers (e.g., `rehydrate_delta`, `rehydrate_recent`, `backfill_llm`, `sync_project_to_monday`) inside a new module such as `src/tasks/pipeline.py`.  
- Keep thin CLI wrappers that import those helpers for ad-hoc runs, preserving current behaviour.

2. **Introduce a Job Queue Worker**  

- Add a lightweight queue adapter (e.g., Redis RQ or AsyncIO in-process) under `src/services/queue_worker.py` to accept tasks like `rehydrate_and_analyze(project_id)` and `push_project_to_monday(project_id)`.  
- Implement the worker loop so it reuses the refactored helpers and respects existing throttle/backoff rules from `DataSyncService` and `MondayUpdateService`.

3. **Enqueue Jobs from Webhooks**  

- Update `src/webhooks/webhook_server.py` so subitem/hidden-item events call the queue with the parent project ID instead of stopping at the child-table update.  
- Ensure parent-board events still run the fast path locally but enqueue downstream tasks (rehydrate if mirrors needed, Monday push) for consistency.

4. **Schedule Periodic Catch-ups**  

- Use an APScheduler instance initialised during FastAPI startup (e.g., `src/api/app.py`) to schedule hourly delta rehydrate, nightly LLM backfill, and frequent Monday sync invocations by calling the new helpers.  
- Register scheduler jobs with Supabase `sync_log` entries mirroring today’s scripts for observability.

5. **Persist Job State & Guardrails**  

- Create a lightweight `job_queue` table (id, payload, status, attempts, timestamps) via Supabase schema migration so retries and dashboards can inspect queue health.  
- In the worker, log state transitions and enforce rate limiting using existing delays (`RATE_LIMIT_DELAY`, `BATCH_DELAY`).

6. **Documentation & Ops Updates**  

- Document queue setup, scheduler configuration, and operational runbooks in `docs/automation.md`.  
- Update README to point operators to the new automated flow and remove manual script instructions.

### To-dos

- [ ] Move shared logic from manual scripts into reusable helpers under src/tasks/.
- [ ] Implement queue worker that consumes tasks and runs rehydrate, analysis, and Monday sync helpers.
- [ ] Update webhook handlers to enqueue rehydrate/analyze jobs for subitem and hidden updates.
- [ ] Configure APScheduler (or equivalent) to run periodic catch-up jobs invoking the new helpers.
- [ ] Add Supabase job state table/migrations and document automation & operations in docs.