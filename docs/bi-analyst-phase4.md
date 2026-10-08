# Phase 4: deterministic metric service

Implemented 8 October 2026 in analyst package 0.4.0. This is a code delivery;
no hosted database, source configuration, reference dataset or deployment was
changed. The existing dependency lock remains applicable; no dependency was added.

Package **0.4.1** adds [read-only Monday source checks](bi-analyst-monday-source-checks.md)
and connects the recorded owner acceptance to runtime metric execution.

**Later owner decision: Phase 1 is closed.** The
[8 October closure](bi-analyst-phase1-closure.md) accepts the implemented app
definitions and removes Power BI verification and Monday CRM cleanup as
prerequisites. It supersedes the earlier source/business checklist.

The corresponding [Phase 3 source/business dependency](bi-analyst-phase3.md#sourcebusiness-gate-closure-8-october-2026)
is also explicitly closed by owner acceptance with zero blockers. Infrastructure
evidence and business acceptance have separate recorded bases; neither requires
repeating the closed review before proceeding to later implementation phases.

## Assessment of the implementation plan

The Phase 4 sequence is appropriate for the implemented Phase 3 boundary:
compile typed plans over the restricted gateways, perform arithmetic in SQL or
fixed decimal utilities, and persist results with the existing owner/version
checks. LangGraph and natural-language interpretation can consume this interface
in Phase 5 without generating business formulas.

Repository inspection identified these material constraints:

- Catalogue 1.1.0 contains **16 variants across the five core metric families**.
  The sealed catalogue retains its original `pending_phase1` evidence, while
  the packaged, fingerprint-bound owner acceptance enables these variants at
  runtime. Discovery reports effective `owner_accepted` status. Any unmatched
  catalogue still returns `metric_not_certified` with a source-check capability.
  A guarded loopback-only
  evaluation mode supports implementation and acceptance testing; it cannot
  enable metrics on a hosted TEST, staging or production database.
- The later retained-reportable-population decision and revenue correction are
  authoritative for this implementation. Migration 004 must accompany the
  curated surface. The compiler uses `analyst_query` gateways, never active-only
  operational views or raw source tables. The owner confirms that the app's
  correct Order Value is implemented; the saved-schema observation is no longer
  a Phase 1 blocker. Parent and hidden inventory totals remain distinct scopes.
- The gateways publish coverage counts but no successful ingestion, rollup or
  refresh version. Results explicitly report **unknown freshness**. Query time
  is not a refresh time or historical business snapshot date. This prevents a
  cache with a trustworthy refresh key, so this release has no result cache.
- Only category and type are approved dimensions. Account/product expansion,
  arbitrary project search, fiscal periods, month-to-date and dated historical
  snapshots remain unsupported. No wider source privileges or migration are
  necessary for Phase 4.
- Hosted authentication and deployment acceptance remain later delivery work.
  Phase 1 source/business acceptance is recorded; Power BI comparison is optional.

The [8 October owner decision update](bi-analyst-business-decisions.md) resolves
currency (GBP), business timezone (Europe/London), fiscal year (1 November-31
October) and tax presentation (as stored; VAT inclusion unspecified). The
subsequent full owner closure resolves the Phase 1 gate with zero blockers.

## Interface

Every endpoint below uses the existing Monday/DataCube bearer authentication,
company-wide principal grant, permissions version, rate limit and audit path.

| Endpoint | Behavior |
|---|---|
| `GET /v1/metrics` | Candidate definitions, catalogue version/hash and limitations |
| `GET /v1/metrics/resolve?name=...` | Exact approved ID/label/alias matching; multiple candidates require clarification |
| `POST /v1/entities/resolve` | Category/type candidates on the metric's own gateway; unique exact case-insensitive matches resolve, ambiguous or partial matches require clarification |
| `POST /v1/runs/{run_id}/metric` | Execute a structured request for an owned registered run and atomically save its result |
| `POST /v1/runs/{run_id}/cancel` | Persist cancellation; an executor on any replica observes it and cancels its database task |
| `GET /v1/results/{result_id}` | Retrieve the owned persisted dataset and typed metric provenance |
| `GET /v1/results/{result_id}/export` | Export only stored/displayed rows; `X-Export-Scope: stored-result-rows` |

Create a conversation and register a run using the Phase 3 endpoints first.
The metric execution endpoint takes this JSON, for example:

```json
{
  "metric_id": "invoice_monthly_actual",
  "metric_version": "1.1.0",
  "population": "reportable",
  "period": "last_month",
  "grain": "total",
  "dimensions": ["category"],
  "filters": [],
  "comparison": {"period": "previous_month"},
  "ordering": [{"column": "value", "direction": "desc"}],
  "limit": 10,
  "include_total": true,
  "include_share": true
}
```

`metric_id`, version, population and period are required. Unqualified "order
value" and "invoiced value" resolve to multiple catalogue candidates; the caller
must choose a specific contract. Model interpretation and conversational
clarification/resume remain Phase 5.

Filters support `in` with at most 50 exact whole-classification values, or
`is_null` with no values. One filter per dimension is allowed. Comparison filters
omitted or `null` inherit the current filters; an explicit empty list compares
with the unfiltered scope. Comparisons use another period supported by the same
metric and return complete aggregate comparisons, not per-row change rankings.

`grain=month` is supported for the three monthly actuals. A calendar spine fills
empty months with zero. With dimensions, the spine covers combinations observed
in the filtered period; an entirely empty dimension population has no invented
categories. `grain=total` supports overall or approved dimension aggregates.
Ordering and a limit provide top contributors for additive metrics. Shares
require an additive grouped metric and `include_total=true` so the denominator
is visible. Limits are not pagination; the result reports both matched groups
and displayed row count.

## Results and numerical rules

The response preserves Phase 3's result envelope (`id`, `run_id`,
`permissions_version`, `columns`, `rows`, `provenance`, `created_at`). Phase 4
provides typed column descriptors and provenance, including the exact request,
metric/catalogue/compiler versions, catalogue SHA-256, query reference, source
relation/population/grain, resolved boundaries, timezone, complete filtered
total, comparison, coverage, freshness and truncation reasons.

Decimal values are JSON **strings**, including values in rows, totals, ratios
and comparisons. They remain decimal through calculation and persistence;
clients must not parse financial values through binary floating point for
further calculations. Round at catalogue precision after aggregation.

- Raw sums preserve signed amounts and NULL versus numeric zero. `source_rows`
  and `known_values` explain partially known totals; the source relation's grain
  identifies whether these count projects, children, hidden items or monthly rows.
- Conversion sums wins and eligible/closed counts before division and applies
  catalogue rounding. Zero denominators yield NULL. Five/two-year cohorts retain
  their lower-bound-only date semantics, including future-dated records.
- Gestation uses stored positive actual days. Mean and median variants remain
  separate; predicted values are never substituted.
- Absolute change is current minus baseline. Percentage change is
  `100 * (current - baseline) / abs(baseline)` and is NULL for a zero/missing
  baseline. Ratio changes also expose `100 * (current - baseline)` as percentage
  points. Missing current values produce missing changes.
- Share is group value divided by the complete filtered total; a zero/missing
  total yields NULL. Signed values can produce negative shares or shares above
  one, so these are contribution ratios, not a promise of pie-chart suitability.
- Complete totals and comparison aggregates are calculated independently of the
  displayed row limit. `dataset_scope=limited` and explicit `row_limit` or
  `byte_limit` reasons prevent a limited table from implying a complete dataset.
- Coverage counters describe the **unfiltered gateway population**. They are not
  per-filter evidence, and do not certify source membership. Freshness timestamps
  and data version remain NULL until a separately reviewed source supplies them.

## Execution and storage boundary

All analytical reads use `REPEATABLE READ READ ONLY`, a fixed `pg_catalog`
search path, the configured business timezone, database statement/lock timeouts
and bounded pools. Periods resolve from the database transaction timestamp,
matching the views' `CURRENT_DATE`. Totals, comparison, rows and coverage share
that transaction. Connections are released before result serialization/storage
and before any future user/model interaction.

Only packaged catalogue expressions and validated identifiers enter compiler
templates; every caller-supplied filter value is bound separately. There is no
client SQL/function/join interface. The broader inherited PUBLIC read/function
privileges approved in Phase 3 are not exposed by these endpoints.

Defaults: 1,000 output rows, 900,000 serialized result bytes, two concurrent
metric/entity requests per process, a 15-second total query workflow deadline,
5-second database statement timeout and 1-second lock timeout. Concurrency must
fit the read pool, and pool sizes across configured replicas must fit the
existing connection budget. Capacity exhaustion returns 429 without an
unbounded application queue. All replica/worker counts must be included in
deployment connection budgeting.

Server cursors fetch one row at a time. SQL rejects oversized text cells before
transfer; serialization also accounts for metadata/JSON escaping and trims only
the displayed dataset. An oversized metadata envelope returns a safe error.
Stored results stay below the existing 1 MiB state-table constraint.

Cancellation is checked through durable run state every 200 ms while execution
is active; disconnects also cancel. Task cancellation triggers Psycopg's SQL
cancellation and transaction cleanup, consistent with its
[async cancellation contract](https://www.psycopg.org/psycopg3/docs/advanced/async.html#interrupting-async-operations).
Database timeouts remain an independent bound. Result insertion and the
registered-to-completed transition commit together; a cancelled/completed run
cannot accept a second result. Current permissions are rechecked before storing
or retrieving results. Failures return sanitized codes without SQL, source
records or credentials. Durable scheduling/restart/resume belongs to Phase 5;
an interrupted registered request is not automatically retried.

## Local verification and configuration

The isolated tests use disposable databases and actual restricted reader/state
logins. They never load the repository `.env` or contact Monday/Supabase. The
authentication integration mocks only Monday HTTPS; bearer validation, session
storage, principals, RLS, SQL execution and result persistence are real.

```powershell
$env:BI_ANALYST_TEST_DSN = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres connect_timeout=3'
$analystTestTemp = Join-Path 'tmp' ('bi_phase4_pytest_' + [guid]::NewGuid().ToString('N'))
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests -q -p no:cacheprovider --basetemp $analystTestTemp
```

Without this explicit loopback admin DSN, PostgreSQL tests skip. Fixtures refuse
remote targets and clusters with existing analyst roles. Use a dedicated local
test cluster. Synthetic source data covers signed/blank/zero amounts, additional
charges, duplicate links, excluded parents, future creation dates and differing
business stages. Golden tests cover every catalogue variant, plus filter and
ordering injection, totals/shares, month spines, comparisons, permission changes,
cancellation, timeouts, byte/concurrency limits and consistent snapshots.

Verification on 8 October 2026: **271 tests passed**, including actual restricted
PostgreSQL logins and all five metric families through bearer authentication.
An earlier run encountered 31 fixture-setup errors in the pre-existing Windows
temporary directory; rerunning with a fresh workspace `--basetemp` resolved them.
The final entity-resolution/timeout changes also passed **3 focused PostgreSQL
checks**. No hosted acceptance or source certification is implied.

For local API evaluation, supply independent restricted DSNs,
`BI_ANALYST_ENVIRONMENT=test`, the approved `BI_ANALYST_BUSINESS_TIMEZONE`, and
`BI_ANALYST_METRIC_EVALUATION_ENABLED=true`. Settings refuse evaluation against
non-loopback hosts or staging/production. Every evaluation result is labelled
`evaluation_only=true`. The Render blueprint retains evaluation disabled.

Runtime activation now consumes the recorded owner acceptance for the exact
catalogue fingerprint. Power BI verification, CRM cleanup and repeated Phase 1
approval are not required. Read-only Monday source checks use a separate server
token and remain available even when a metric cannot execute. Hosted
authentication and deployment acceptance remain separate delivery work.
