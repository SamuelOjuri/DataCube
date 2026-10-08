# Phase 5: conversational metric workflow

Implemented in analyst package **0.5.0**, 8 October 2026. Phase 5 extends the
Phase 4 compiler and executor; it does not change metric definitions or deploy
the service. Production activation remains a separate delivery step.

The implementation plan is sound in its separation of model interpretation from
business calculations. Four details needed concrete decisions:

- Checkpoints do not schedule work or provide mutual exclusion. The application
  now owns admission, execution tokens, deadlines, cancellation and idempotency.
- A database-backed checkpointer must use the existing restricted state role and
  row security. The default unrestricted checkpointer setup is not appropriate
  for this application's ownership boundary.
- A fluent narrative is insufficient evidence. The model selects identified
  claims; application code validates the selection and renders exact numbers.
- Phase 4 reports source freshness as **unknown**. Phase 5 preserves that
  limitation and re-queries follow-ups. Query timestamps do not become ETL or
  business snapshot timestamps.

The approved retained-reportable population, archived history, source totals,
monthly revenue rules and Phase 1 owner acceptance remain the Phase 4 contracts.
MTD, arbitrary dates, fiscal periods, project-level rankings and predictive
questions that the compiler cannot express remain unsupported. Phase 5 must
clarify or reject these requests, not invent another measure.

The implementation uses one LangGraph with explicit context, interpretation,
catalogue retrieval, planning, clarification, entity resolution, query, evidence
validation, presentation and outcome nodes. Model-selected identifiers and
filters pass through the Phase 4 contracts and compiler. Versions, population,
permission scope and complete-total requirements are assigned by the server.
Follow-ups apply a typed patch to an explicitly identified completed run in the
same conversation and return material scope changes.

The current [LangGraph persistence guidance](https://docs.langchain.com/oss/python/langgraph/checkpointers)
and [interrupt guidance](https://docs.langchain.com/oss/python/langgraph/interrupts)
informed the implementation: checkpoints use synchronous durability, the
interrupt node has no effects before interruption, and authenticated replies use
`Command(resume=...)`. Small reference-based state keeps checkpoint writes
bounded. Runtime uses `langgraph==1.2.14` and
`langgraph-checkpoint-postgres==3.1.2`; the full dependency closure is pinned in
`services/bi_analyst/requirements.lock`.

`ScopedPostgresSaver` uses the pinned driver's cursor acquisition hook to borrow
a connection for each short owner-scoped transaction. It sets the run scope and
restricted search path locally. It holds no connection while waiting on Gemini
or a user. The application never calls `setup()` or uses a migration credential.
Checkpoint rows require an owned run with the current permissions version.
Datasets remain in `analyst_state.results`; graph state stores result references.

The independent provider calls the documented
[Gemini structured output API](https://ai.google.dev/gemini-api/docs/structured-output)
using `gemini-3.8-flash`, temperature zero, low thinking effort, a bounded response
and Pydantic validation. Prompt version is `bi-conversation-1.0.2`. The adapter
does not import predictive scoring, ETL configuration or Monday write clients.
Inherited LangSmith tracing is disabled around execution. Neither provider
bodies nor raw prompts are exposed in public progress events or error messages.

Numerical evidence includes the owned result, cell/comparison location, metric,
unit, resolved period and denominator where applicable. The application uses
stored decimals; the model cannot supply replacement numbers, prose claims,
formulas or Vega expressions. Unknown and empty values, incomplete coverage,
zero comparison denominators and truncation are explicit. Measured contributors
are not presented as causes. Chart intent permits only a table, number, bar or
line and approved fields; suitability is checked against the owned dataset.
Vega-Lite specification generation remains Phase 6.

Authenticated exact-ID Monday source checks remain available on registered,
running, clarification-waiting and completed runs. They retain the existing
allowlisted read-only GraphQL, account/board checks and response budgets. Live
observations stay separate from analytical results and cannot certify a metric
or establish database freshness. Model output cannot supply arbitrary GraphQL
or invoke mutations. Automatic traversal and source-data reconciliation are not
added to this graph.

The API additions are:

| Endpoint | Behaviour |
|---|---|
| `POST /v1/conversations/{id}/messages` | Submit `question`, UUID `idempotency_key`, optional completed `follow_up_to`; returns 202 with an owned workflow run |
| `GET /v1/runs/{id}/workflow` | Current outcome, clarification, validated answer, version pins and call counts |
| `POST /v1/runs/{id}/resume` | Submit `clarification_id`, UUID `idempotency_key`, and `answer`; owner and current permissions are rechecked |
| `POST /v1/runs/{id}/cancel` | Durable cancellation, also observed by an executor on another instance |
| `GET /v1/runs/{id}/events?after=N` | Authenticated fetch/SSE with monotonic event IDs, replay, heartbeats and persisted terminal events |

The earlier `/runs` registration and `/metric` execution endpoints remain
available. They do not start the graph. A graph run cannot be executed through
the direct metric endpoint. Browser integrations should use `/messages` for
conversation execution. Every endpoint still requires bearer authentication;
tokens never belong in event URLs.

Example submission (identifiers below are illustrative):

```json
{
  "question": "Give revenue for the last completed month.",
  "idempotency_key": "159ead24-cc16-491c-a202-fda48ef11f22"
}
```

Each conversation has at most one active workflow, including a pending
clarification. A database partial unique index and short transaction lock
enforce this across instances. Reusing a submission/reply key with changed
content returns 409; replaying the same content returns the existing run.
Reply receipts survive subsequent clarification rounds. Wrong clarification
IDs and changed model/prompt/catalogue/dependency versions require a new run.

Normal execution is `registered -> running -> completed/failed/cancelled`.
Clarification is `running -> awaiting_clarification -> running`. A disconnect
from SSE leaves execution running so the client can reconnect. Graceful process
shutdown marks active executions `interrupted`; a hard crash leaves a deadline
that is expired on the next authorised read/submission. There is no background
work recovery or invisible replay. Interrupted runs require a new submission
and idempotency key. A fully persisted clarification can resume after restart.
Permission changes invalidate prior runs/results/checkpoints; stale active
workflow slots are retired when the newly authorised owner next accesses them.

Defaults are two active workflows per instance, 90 seconds of cumulative active
execution time across clarification segments, eight total model attempts,
24 metric/entity calls and at most three clarification replies. A transient
provider failure can retry once per node; all attempts consume the durable run
budget. Each model response is limited to 128 KiB, input context to 64 KiB and
generation to 4,096 tokens. Phase 4 row, byte, query, connection and concurrency
limits also apply. Waiting for a user consumes no database lease or runtime
budget. Public event count and stored payload sizes are database bounded.

Apply `src/database/migrations/20261008_006_analyst_workflow.sql` after migrations
003, 004 and 005, in one reviewed administrator transaction. It adds workflow
jobs/events and the final schema of the pinned PostgreSQL checkpointer,
preserves operational grants, and applies forced row security. Its effective
privilege audit is also packaged as `permissions_workflow.sql` for startup.
Runtime rejects schema/grant drift and refuses to enable the graph without
schema version 6. Production migration and deployment were not performed as
part of this implementation.

Configure a separate `BI_ANALYST_GEMINI_API_KEY` and then explicitly set
`BI_ANALYST_WORKFLOW_ENABLED=true` after reviewing evaluation evidence and hosted
authentication. The checked-in Render blueprint and environment example leave
the feature disabled. Reuse the existing state pool; no additional database
pool is introduced. Roll back execution by setting the feature flag false while
retaining schema version 6 and package 0.5.0. Older API packages do not recognise
version 6. Do not drop run, result or checkpoint tables to roll back a service.

The isolated test suite uses real loopback PostgreSQL, actual LangGraph
checkpoints and restricted runtime roles. Deterministic provider fixtures cover
all 16 golden metric variants, entity clarification, process restart, duplicate
submissions across instances, cancellation on another instance, permission
changes during model waits, checkpoint isolation, model/runtime budgets,
unsupported chart/prose fields and fabricated evidence IDs. Provider HTTP tests
check structured responses, response limits and sanitized failures.

The live benchmark runner is
`services/bi_analyst/tests/evals/conversations.py`. It sends only catalogue
metadata and synthetic questions, with no database access, and evaluates plans
against independently authored contracts. It supports `--cases` for diagnostics
and explicit `--env-file` for a benchmark credential; the application itself never
loads the repository `.env`. Reports retain failed attempts as well as revised
prompt diagnostics. A benchmark pass is not automatic production enablement.

The final isolated run passed **361 tests**. The live
[planning benchmark](bi-analyst-phase5-model-evaluation-1.0.2.json) passed **27/27**
cases with prompt 1.0.2: all 16 catalogue variants, two clarifications, three
unsupported requests, one injection attempt and five follow-ups. Mean planning
latency in this sequential synthetic workload was **6.58 seconds**; reported
usage was 84,851 input, 2,032 candidate-output and 2,354 thinking tokens. These
are planning measurements, not end-to-end or concurrent-user targets. The
[presentation benchmark](bi-analyst-phase5-presentation-evaluation.json) passed
**8/8** synthetic cases across the five metric families, limited rows, unknown
values and percentage-point comparisons.

Earlier [prompt 1.0.0 results](bi-analyst-phase5-model-evaluation.json),
[1.0.1 diagnostics](bi-analyst-phase5-model-diagnostic.json) and
[1.0.2 diagnostics](bi-analyst-phase5-model-diagnostic-1.0.2.json) are retained.
They exposed unnecessary clarification, early rejection and a provider timeout.
The final prompt separates family routing from exact plan validation, requires
metric/period fields for new plans and makes completed-month semantics explicit.
The benchmark is representative local release evidence; it does not claim
population-wide production parity or hosted end-to-end acceptance.

Validation commands from the repository root:

```powershell
$env:BI_ANALYST_TEST_DSN='host=127.0.0.1 port=55439 dbname=postgres user=postgres'
& ./report.venv/Scripts/python.exe -m pytest services/bi_analyst/tests -q -p no:cacheprovider --basetemp=tmp/NEW_UNIQUE_TEST_DIRECTORY
& ./report.venv/Scripts/python.exe services/bi_analyst/tests/evals/conversations.py --output outputs/phase5-model-evaluation.json
```

Use a fresh temporary directory when local Windows pytest temp permissions are
restricted. Test fixtures reject non-loopback databases and create/drop only
their disposable databases. Hosted auth, browser streaming and production load
acceptance remain separate Phase 6/7 delivery work.
