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

## Production verification still required

Deploy the tested backend through the existing release process. Confirm timely
readiness on that deployment, sign in to the existing frontend, and use **Retry
question** on the interrupted conversation. Confirm a new run completes, its
answer survives reopening the conversation, and Render reports no further
restarts during the check. A frontend deployment is not required by this patch.
Record the deployed commit and observed latency. If delays remain high, compare
the Render and database regions and inspect actual pool wait metrics before
changing timeouts or connection budgets.
