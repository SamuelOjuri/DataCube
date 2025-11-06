## Test Plan

- **Environment Prep**  
  - Confirm `.env` includes Supabase service key, Monday API token, webhook secret, Gemini key (if LLM backfill enabled).  
  - Apply schema migrations: run `functions.sql` (ensures `update_updated_at()`), then `schema.sql`; verify `job_queue` table exists.  
  - Seed or sync baseline project/subitem data so tests have known IDs; snapshot key records for later comparison.

- **Shared Pipeline Tasks (`src/tasks/pipeline.py`)**  
  - Manual smoke: run each helper via CLI/wrapper script (or Python shell) with test project IDs; check Supabase tables (`projects`, `subitems`, `analysis_results`) for expected updates.  
  - Failure handling: simulate Supabase or Monday downtime (unset credential or use mock) to ensure helpers log/raise without corrupting state.  
  - Idempotency: rerun helpers to verify they skip or update without duplicating rows.

- **Queue Worker (`src/services/queue_worker.py`)**  
  - Start FastAPI app locally to spin up the in-process worker (`uvicorn src.api.app:app --reload`).  
  - Insert test job via `get_task_queue().enqueue_rehydrate(TEST_ID, source="manual")`; confirm `job_queue` records transition queued → running → completed and `analysis_results`/Monday update triggered.  
  - Error path: enqueue job with nonexistent project ID; expect `job_queue.status = failed` and warning logs.

- **Webhook Integration (`src/webhooks/webhook_server.py`)**  
  - Use Monday’s “Send sample webhook” or craft HTTP POSTs hitting `/webhooks/monday` with signed payloads.  
    - Parent-board column change (pipeline stage, probability, etc.): expect immediate `analysis_results` update + push job queued.  
    - Subitem/hidden item change: check for rehydrate job, then downstream analysis + Monday push.  
    - Duplicate event test: resend identical payload; confirm dedupe response and no extra jobs.  
  - Validate Supabase `webhook_events` table logging (status, retry counts) and `job_queue` entries tied to webhook IDs.

- **Scheduler (`src/api/app.py`)**  
  - With app running, inspect APScheduler logs to confirm three jobs registered.  
  - Force-run each job (`await _scheduled_delta_rehydrate()`, etc.) from an interactive shell or `/docs` endpoint to observe:  
    - Delta rehydrate inserts any missing projects <24h old.  
    - LLM backfill respects skip rules (_has_llm_result) and writes to `analysis_results`.  
    - Monday sync pushes analysis for projects created within 30 days.  
  - Monitor job_queue to ensure scheduler-triggered tasks are logged (rehydrate → analyze → push chain).  

- **End-to-End Validation**  
  - Pick a live Monday project; change a trigger column. Track timeline: webhook receipt, Supabase update, job enqueue, analysis rerun, Monday update.  
  - Confirm Monday board reflects new predictions and Supabase `analysis_results.analysis_timestamp` increments.  
  - Review logs for queue worker and webhook processing to ensure no silent failures.

- **Observability & Rollback**  
  - Query `job_queue` for incomplete jobs; practice manually retrying by re-enqueueing `project_id`.  
  - Check `docs/automation.md` instructions match actual workflow; update gaps noted during testing.  
  - Define rollback: disable scheduler jobs (via APScheduler dashboard or config) and stop queue worker if severe issues occur.

Next steps after successful dry-run: document results, enable production webhooks, schedule initial backfill, and set up monitoring alerts on `job_queue.status != 'completed'`.