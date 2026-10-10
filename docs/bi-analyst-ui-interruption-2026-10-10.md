# Analyst UI interruption investigation — 10 October 2026

The supplied screenshot shows a saved backend run in `interrupted` state. Local
browser tests confirm that the UI can retry this state and render an answer,
chart and table when a completed result is returned. The signed-in production
page was not accessible from this session; these changes have not been deployed.

## Evidence from the supplied logs

Times below are the UTC timestamps in the attachment.

- Ordinary `/health/ready` requests took approximately 4.2 seconds.
- During the run, readiness requests took 5.041, 5.456 and 5.271 seconds.
- At 01:47:14, a model operation succeeded in 8.873 seconds.
- At 01:47:41, the query was cancelled after 6.581 seconds and the workflow
  reported failure after 99.494 seconds.
- Server shutdown began at 01:47:45; Render reported an instance restart at
  01:47:50. Startup and preflight subsequently succeeded.

[Render requires HTTP health checks to respond within five seconds](https://render.com/docs/health-checks).
A logged HTTP 200 can therefore still fail the platform's deadline. The checked-in
workflow limit is 90 seconds, and expired running jobs become `interrupted`.
The log is consistent with excessive database round trips consuming the workflow
budget and making readiness fragile. It does **not** establish the exact restart
trigger or distinguish an expired lease from a shutdown interruption for the
specific conversation: the supplied operation events have no run identifier.

## Changes

- Batch independent readiness probes with PostgreSQL pipeline mode while still
  checking all existing relations and migration versions on each request.
- Set transaction-local subject and timeout/search-path settings together.
- Honour the checkpoint driver's pipeline request inside its existing scoped
  transaction, preserving rollback and synchronous checkpoint durability.
- Read workflow ownership, permission versions and status in one transaction.
  Stream status and events now share one freshly authorised transaction.
- Remove duplicate graph checks immediately before operations that already
  verify the principal and lock/check the execution token.

Runtime limits, connection budgets, source permissions, owner checks, audit
requirements and the production configuration are unchanged.

## Verification

The latency regression uses a disposable loopback PostgreSQL database, a TCP
proxy adding 100 ms to database response batches, a real local Uvicorn HTTP
server with bounded HTTP client timeouts, and a deterministic model fixture.
It submits “Show revenue for the last completed month”, checks readiness during
execution, retrieves the saved revenue answer, and verifies its SSE replay.
Its values and model responses are synthetic; this is not a live Gemini or
production-data qualification.

The final real-HTTP run completed in **47.67 seconds**, with maximum readiness
latency **2.03 seconds**, within the test's 60-second workflow budget. The saved
answer and SSE replay were verified and the server shut down cleanly.
Earlier in-process `TestClient` diagnostics sometimes failed to return, including
one after the answer had already been saved. The final regression uses real HTTP
to avoid unbounded in-process client waits. That diagnostic behavior was not
reproduced in the final real-server test and is not established as a production
failure cause. Production end-to-end verification remains necessary.

The browser regression opens an interrupted saved conversation, retries it with
a new run, and checks that the returned answer and chart are visible. Browser
API responses are fixtures. Screenshot: `outputs/analyst-interrupted-retry.png`.

Completed checks:

- 236 backend contract/unit tests passed.
- 175 real PostgreSQL tests passed, including the latency regression, owner
  isolation, permission changes, cancellation and restart recovery. New cases
  revoke access to analytical/auth/checkpoint relations and verify readiness
  rejects the broken state and recovers after access is restored.
- 18 frontend unit tests and 11 browser tests passed. The browser suite also
  builds the production frontend and checks accessibility.
- `git diff --check` passed. Database test results are recorded in
  `outputs/analyst-ui-postgres-20261010.xml`.

An initial unit run encountered permissions on the existing Windows pytest temp
directory; rerunning with an isolated workspace temp directory passed. The
production build retains its existing large JavaScript chunk warning.

## Follow-up: confirmed workflow timeout at 02:38 UTC

A later supplied production log records a different failure from the initial
restart investigation:

- The question submission returned HTTP 202 at 02:36:58.
- Three model operations succeeded in 5.272, 11.138 and 17.930 seconds, totalling
  34.340 seconds. The database query also succeeded, in 4.634 seconds.
- At 02:38:31, the workflow explicitly reported `outcome: timeout` after
  94.260 seconds, consistent with the default 90-second workflow limit plus
  setup and cleanup. The production environment override was not accessible.
- All 25 readiness checks during this attempt returned HTTP 200 in
  1.884-3.861 seconds. These logs do not show another health-check timeout or
  restart during the question.

The UI renders the backend status. Expiration marks overdue jobs `interrupted`;
once that terminal state wins, timeout cleanup cannot overwrite it. Thus
`Interrupted` does not necessarily mean the process restarted. The supplied
operation logs still lack run identifiers, but the combined-latency regression
below reproduced this same no-answer outcome.

The original regression used immediate scripted model responses. The extended
test adds the three measured model delays, 125 ms per database response batch,
live SSE consumption, concurrent readiness/status requests, saved-answer
reopening and event replay. Before this fix it ended `interrupted` at **100.41
seconds**. Two successful runs completed with the correct synthetic revenue
answer in **84.31 and 83.40 seconds**, below the unchanged **90-second** budget;
maximum readiness was **3.05 and 2.63 seconds**. The original 100 ms database-only
case also passed twice with live SSE in **38.02 and 42.47 seconds**, below its
60-second budget. The final regression verifies the exact streamed answer
against the saved answer and all ten original progress stages in order.

The follow-up fix:

- Pipelines short state transactions, synchronizing reads before decisions and
  flushing writes inside the transaction so failures roll back before reuse.
  Analytical streaming cursors remain outside pipeline mode.
- Shares authorization/expiration/status work in a single transaction for status
  reads and each event-stream snapshot.
- Uses graph **1.1.0** with fewer checkpoints between non-resumable steps.
  Clarification still uses synchronous checkpoints; results and final answers
  retain their own durable writes. Progress stages, permissions, cancellation
  fences, evidence validation, all three model calls and existing limits remain.
- Keeps the evaluation harness aligned with the grouped planning stages.

No limits, model settings, connection budgets, production configuration or
frontend code were changed. This is a local synthetic validation, not a live
Gemini qualification or a production deployment. The signed-in production
conversation and Render settings were unavailable in the shared browser.

**Upgrade note:** the existing graph-version fence deliberately rejects resuming
or following up a pre-upgrade run. Saved answers remain readable. Start a fresh
question for old clarifications; retrying an interrupted question creates a new
compatible run. No schema migration or frontend deployment is needed.

Follow-up validation also passed **255 API/contract/PostgreSQL tests**, including
cross-user isolation, permission changes, cancellation, clarification/resume,
readiness failures, analytical read-only access and the new state-pipeline
rollback checks for SQL errors, application errors and cancellation. The analyst
wheel build and `git diff --check` passed. The editor test runner did not discover
these tests, so they were run with pytest in the existing isolated analyst
environment against a disposable loopback cluster. Both final latency scenarios
passed, bringing the focused test total to **257**. A deterministic revenue case
also passed through the updated evaluation harness. The final HTTP test servers
and disposable database shut down successfully; the temporary cluster was
removed.

## Production verification still required

Deploy the tested backend through the existing release process. Confirm timely
readiness on that deployment, sign in to the existing frontend, and use **Retry
question** on the interrupted conversation. Confirm a new run completes, its
answer survives reopening the conversation, and Render reports no further
restarts during the check. A frontend deployment is not required by this patch.
Record the deployed commit and observed latency. If delays remain high, compare
the Render and database regions and inspect actual pool wait metrics before
changing timeouts or connection budgets.
