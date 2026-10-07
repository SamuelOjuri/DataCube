# Phase 1 certification runbook

Use `services/bi_analyst/tests/evals/manage.py` from the repository root. Core
dependencies are in its `requirements.txt`; optional offline PBIX inspection uses
`requirements-pbix.txt`. No analyst API or ETL process starts.

## Capture evidence

```powershell
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py phase1-pbix --input docs --output outputs/bi_analyst_evals/powerbi_review
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py phase1-reader --output outputs/bi_analyst_evals/reader_review
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py phase1-capture --dataset bi_eval_20261007_v1 --output outputs/bi_analyst_evals/phase1_review --sample-size 3 --pbix outputs/bi_analyst_evals/powerbi_review/pbix_inventory.json --reader-audit outputs/bi_analyst_evals/reader_review/powerbi_reader_inventory.json
```

Use new output locations for new captures. Evidence files are never overwritten;
output must stay within the git-ignored `outputs/` directory. These artifacts contain
private exact IDs, financial samples and report definitions. Retain reconciliation
access restrictions. No answer key is exported. SHA-256 fingerprints bind reviews
and detect accidental edits; they are not authenticated approval signatures.

`phase1-capture` uses only `TEST_SUPABASE_*`, verifies the sealed dataset in a
read-only repeatable-read transaction, and creates `evidence.json` and `review.json`.
Optional attachments bind separate report/reader evidence to the capture. TEST
observations are never relabelled as current production observations.

Sixteen exact-ID sample categories cover charges, signed/blank/zero invoices,
missing inputs, multiple children, repeated sources, unresolved links, parent
invoice/enquiry differences, child formula mismatches, retained closed enquiries,
gestation differences, conversion wins/nonwins and placeholder exclusions.
Selections retain candidate counts and 1–20 rows per category. Linked child detail
is capped at 2,000 rows with an explicit truncation flag/count. A stored NULL cannot
establish a typed Monday blank. Missing categories remain visible with count zero.

`phase1-pbix` uses [PBIXRay](https://github.com/Hugoberry/pbixray) locally to extract
model and visual definitions. It never evaluates DAX or executes M/Python from a
report, refreshes the report, or claims numerical parity. Unsupported metadata
categories are labelled rather than treated as empty.

`phase1-reader` is the explicit live-source exception to TEST-only evaluation.
It uses `PG_ROLES=powerbi_reader`, `PG_USER`, `PG_USER_PASSWORD`, `PG_HOST`,
`PG_PORT=5432`, `PG_DATABASE=postgres`, and matching `PG_SERVER` if supplied.
The direct reader login never falls back to administrator credentials. Its fixed
allowlist covers seven PBIX sources and six Phase 1 reporting sources. Read-only
counts/EXPLAINs that fail are recorded by SQLSTATE; no permissions are changed.
Catalog visibility is that of the reader; administrative cron contents are skipped.

## Owner review

Every approved record in `review.json` requires `owner`, `value`, `reviewed_by`,
an offset-aware ISO `reviewed_at`, and nonempty `evidence` references. Software
checks completeness and consistency, not the truth of an assertion or reviewer
identity. Keep reviewed versions and identify the actual business/platform owners.

The decision register covers backup time, access, currency, tax, timezone, fiscal
calendar, current periods, provider handling, retention, historical classifications,
population, writer precedence, question review, performance and connections.
Evaluation settings must not become agreed business decisions without review.

Each metric also needs `population`, `source_contract` and `limitations`.
The packet records required semantic distinctions. Exact-ID source reviews should
link current observations of membership and typed blank/missing/unreadable inputs.
Reuse the [existing comparison workflow](order_value_monday_compare.md) with a
reviewed scope; the new commands never invoke its mutations or infer repairs.

Issues require `status: "resolved"` or `"accepted_limitation"` and `affected_scope`.
For a limitation, supply a `metrics` array and include the issue ID in each affected
metric's `limitations`. Explicitly label limited populations. A sample is not
company-wide certification.

The deployment review needs `environment: "production"` and a complete `services`
list. Each service has `name`, `start_command`, `deployed_commit`, and observed
boolean flags: `MONDAY_ARCHIVE_ENABLED`, `MONDAY_ARCHIVE_REPORTING_ENABLED`,
`MONDAY_LIFECYCLE_ENABLED`, `SCHEDULER_ENABLED`. Include every writer; describe
manual writers in the source-precedence decision. Local `.env` is not deployment
evidence.

Separate `ingestion`, `rollup`, `materialized_refresh` and `snapshot` freshness
records require `succeeded_at` and positive `maximum_age_hours`. Record successful
completion, not a schedule or newest row. The gate rejects stale/future observations.

`performance_targets.value` requires positive numbers for `p95_response_ms`,
`concurrent_users`, `concurrent_queries`, `run_timeout_seconds`,
`max_queries_per_run`, `max_rows_per_run`, `cost_per_successful_answer`, plus
`cost_currency` and `workload`. Reference captured queries and later conversation/
export cases. These are agreed targets, not measured achievements.

`connection_budget.value` requires nonnegative integer allocations for
`database_limit`, `reserved`, `etl_peak`, `other_peak`, `analyst_replicas`,
`read_pool_per_replica`, `state_pool_per_replica`, `headroom`. Limit, replicas and
both pools must be positive; the total must fit capacity. Include provider/pooler
allowances in evidence. Observed idle connections do not establish capacity.

## Independent Power BI result comparison

The existing Power BI reports connect to the **live database**. Validate their
results against independent SQL on the same live source, aligning the report's
refresh/data version, population, dates and filters. Record this as live-report
evidence, separately from frozen TEST evaluation. Identical view definitions do
not establish identical data across the two databases.

The `phase1-powerbi` command below specifically compares with the sealed TEST
answer key; it is not a live-report comparator. To use it, prepare a separate
evaluation copy of the report against the frozen TEST population with its fixed
date and filters, then export its results independently of the key. This is an
optional evaluation workflow, not a description of the existing Power BI
connection or a requirement to repoint the live reports. Live exports and older
PBIX caches cannot be submitted as frozen-dataset evidence without data alignment.

Input JSON has `dataset`, `manifest_sha256` (from evidence payload), and a
`comparisons` array. Each entry supplies:

- `reference`: `enquiry_monthly`, `order_monthly`, `invoice_monthly`,
  `conversion_five_year`, `conversion_two_year`, `gestation_five_year`, or
  `gestation_two_year`.
- Nonempty `report_name`, `report_version`, `dax`, `filters`, `population`,
  `as_of_date` strings describing the actual calculation/context.
- `columns`: PostgreSQL-compatible `{ "name": ..., "type_oid": ... }` metadata,
  matching the reference query's result columns.
- `rows`: dictionaries with exactly those columns. Use decimal strings for
  NUMERIC, integer counts, ISO dates, JSON null for blanks. Convert percentages to
  the declared reference ratio units explicitly. Binary floats for money fail.

Order is irrelevant, but duplicates, NULL/zero distinctions and numeric precision
are retained. Seven families are the minimum review gate; the existing 50 queries
cover the wider question pack.

```powershell
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py phase1-powerbi --dataset bi_eval_20261007_v1 --input outputs/powerbi_export.json --output outputs/bi_analyst_evals/comparison_review
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/evals/manage.py phase1-review --evidence outputs/bi_analyst_evals/phase1_review/evidence.json --review outputs/bi_analyst_evals/phase1_review/review.json --power-bi outputs/bi_analyst_evals/comparison_review/power_bi_comparison.json --output outputs/bi_analyst_evals/certification_review
```

Comparison reports contain definition hashes and match outcomes without numerical
answer rows. Put their exact report name/version/definition hash in the reviewed
`reports` records. Legitimate differences require an approved
`power_bi_exceptions[reference]` record explaining DAX/filter differences, bound to
`comparison_sha256`. Exceptions never modify SQL or the key. Corrected definitions
or data require a reviewed new version.

Run `phase1-review` without `--power-bi` to get a pending-evidence report. It writes
`phase1_gate.json` and `phase1_gate.md`. New-command exit codes: `0` completed/passed;
`1` invalid input/execution failure; `2` partial reader access, missing/differing
comparisons, or blocked certification. Passing means supplied owner attestations
satisfy recorded requirements; it is not automatic production certification or
permission to deploy later phases.

```powershell
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests/evals -q
```
