# Phase 1 certification runbook

**Current status (8 October 2026): `phase1_gate` is `closed_by_owner`, with no
Phase 1 blockers.** The [owner closure record](bi-analyst-phase1-closure.md)
supersedes the earlier pending checklist. Power BI verification and Monday CRM
cleanup are not prerequisites. The detailed evidence workflow below remains
available for diagnostics and future reviews; it does not reopen this closure.

Use `services/bi_analyst/tests/evals/manage.py` from the repository root. Core
dependencies are in its `requirements.txt`; optional offline PBIX inspection uses
`requirements-pbix.txt`. No analyst API or ETL process starts.

## Contract correction before certification

Apply the [8 October owner clarification](bi-analyst-implementation-plan.md#business-definition-correction-8-october-2026)
to the next reviewed release. The later owner decision restores retained
`reportable_projects` as the primary population, including genuine archived history
and excluding only effective reviewed redundant placeholders.
Monthly revenue retains positive dated invoices in completed months but has no
closed-invoiced business-stage gate. Order Value includes material plus charges
via `formula_mkncjq9`, mirrored through children to the parent.

Record these specified choices in the decision/source contracts rather than
treating them as still undecided. Catalogue 1.1.0 and migration 004 implement the
reportable population and revenue correction. The owner has accepted the
codebase corrections, including Order Value. The earlier saved-schema observation
about material-only mirror wiring is historical, not a current blocker. Population dependencies:
existing `current_projects` is active-only and must not replace the retained
analyst population. Operational archive coverage is not an eligibility gate for
retained history. Read-only Monday GraphQL may clarify source questions when needed.

Reference contract **1.1.0** now aligns the revenue SQL, question pack, independent
checks and frozen-view parity with catalogue 1.1.0. It overrides exactly
`invoice_last_month`, `invoice_previous_month` and `invoice_monthly`; conversion's
win criterion, bookings stages and signed base totals are unchanged. Manifests
without an explicit reference version remain 1.0.0. Do not overwrite the old key
or relabel its population. Owner closure does not rewrite historical measurements.

## Revised reference release and handoff

On 8 October 2026, `bi_eval_20261008_v2` was created and verified in **Supabase TEST
only**. It contains 70 scenarios, 50 reference queries and 21 sealed tables.
All 20 independent checks, 11 access checks, five protection probes and 16
curated-view parity comparisons passed. The original `bi_eval_20261007_v1` also
reverified successfully with its 50 original references, 17 independent checks
and 16 historical parity comparisons. All 115 focused evaluation/contract tests
passed. Monday was not contacted or modified;
production was not modified.

Artifacts are under
[`outputs/bi_analyst_evals/reference_1_1_20261008/`](../outputs/bi_analyst_evals/reference_1_1_20261008/):

| Directory | Contents/status |
|---|---|
| `snapshot`, `verification`, `protection` | New manifest, questions and successful verification receipts; no financial answer rows |
| `legacy_verification` | Successful 1.0.0 compatibility verification |
| `reference_review` | Reference fingerprints, pending owner form, blocked reference gate and read-only `reference-review.sql` |
| `powerbi_package` | Three raw-input Power Query imports, eight DAX queries including context, export metadata and instructions |
| `phase1_capture`, `phase1_pending` | Revised evidence, pending full owner form and blocked certification gate |

The restricted `bi_eval_20261008_v2_key.artifacts` table stores expected answers
and a `reference_changes` record containing old/new results calculated on this
**same snapshot**. Neither numerical answer rows nor the answer-key credentials
are included in the Power BI package. The older
`revenue_definition_differing_months` diagnostic still compares the original
1.0.0 calculation with the copied legacy view; it is not revised-reference parity.

### 1. Review and explicitly approve the reference answers

An authorised owner/evaluator uses `reference_review/reference-review.sql` in
Supabase TEST to inspect the restricted SQL, expected results and same-snapshot
changes. Review the two SELECT statements' result sets. Never import this private
answer key into Power BI or an evaluated agent.

Edit **only the owner form** `reference_review/reference_review.json`: provide
the actual `owner`, `value` describing the reviewed scope/limitations,
`reviewed_by`, offset-aware `reviewed_at`, supporting `evidence` references and
`status: "approved"` if approved. Keep its evidence fingerprint and
`scope: "reference_answers_only"` unchanged. Do not edit the sealed evidence or
gate files. Then run:

```powershell
$manager = 'services\bi_analyst\tests\evals\manage.py'
$work = 'outputs\bi_analyst_evals\reference_1_1_20261008'
& .\report.venv\Scripts\python.exe $manager phase1-reference-review --dataset bi_eval_20261008_v2 --review "$work\reference_review\reference_review.json" --output "$work\reference_approved"
```

Without a complete actual approval, the command exits **2** and remains blocked.
Approval binds every reference's SQL/result fingerprints, the sealed manifest
and catalogue fingerprint. It approves recorded-data reference answers only, not
source accuracy, Power BI agreement or production access. Hashes are integrity
checks, not authenticated reviewer signatures.

### 2. Optional: execute the aligned Power BI package independently

This comparison is diagnostic only. The owner withdrew it as a Phase 1 and
analyst release prerequisite because the reports will be corrected against the app.

Follow `powerbi_package/instructions.txt` in a **separate TEST evaluation report**.
An administrator must provision a dedicated TEST login with membership in
`bi_eval_20261008_v2_reader`; that restricted role is NOLOGIN. No login/password
was created, and an administrator or answer-key-owner credential must not be used
in Power BI. Do not repoint the existing production reports.

Import the three supplied raw tables, without automatic relationships, and
refresh all three. Execute all eight supplied DAX queries in actual Power BI/
DAX Studio, saving `context.csv` and the seven metric CSV files beside the
package files. Preserve blank cells and decimal text. Fill actual report
names/versions and refresh/export times in `export.json`. Do not edit DAX/M or
substitute SQL answer rows for Power BI results.

```powershell
& .\report.venv\Scripts\python.exe $manager phase1-powerbi --dataset bi_eval_20261008_v2 --input "$work\powerbi_package" --output "$work\powerbi_comparison"
```

The loader checks package integrity, all seven exports, column types, duplicates,
NULL versus zero, exact snapshot context, reportable population, contract version,
timezone and refresh/export chronology. Missing or misaligned exports cannot
pass. The generated DAX has not yet been executed in Power BI; static/unit checks
are not execution evidence. Matching this evaluation model does not certify the
production Budget/Smoothing reports.

### 3. Regenerate the Phase 1 gate

The current `phase1_capture/review.json` contains the explicit owner closure.
No Power BI or separate reference-review packet is needed to reproduce it. Use
a new output directory so historical reports remain unchanged:

```powershell
& .\report.venv\Scripts\python.exe $manager phase1-review --evidence "$work\phase1_capture\evidence.json" --review "$work\phase1_capture\review.json" --output "$work\phase1_closure_recheck"
```

The gate records `closed_by_owner`, preserves the review snapshot and lists
unperformed earlier checks as superseded, not failed Phase 1 requirements. If
provided, comparison packets are still validated against their frozen dataset;
comparison differences and alignment findings are optional diagnostics. The old
blocked gates and `awaiting_independent_Power_BI_execution` package describe the
earlier handoff. They do not override the later owner acceptance. Runtime
`require_queryable` now consumes a packaged acceptance bound to the exact
catalogue fingerprint; see the [source-check and activation runbook](bi-analyst-monday-source-checks.md).

For future releases, choose fresh dataset and output names. The capture date must
match the database day in Europe/London; existing schemas/files are not replaced:

```powershell
& .\report.venv\Scripts\python.exe $manager freeze --dataset bi_eval_YYYYMMDD_vN --as-of YYYY-MM-DD --contract-version 1.1.0
& .\report.venv\Scripts\python.exe $manager verify --dataset bi_eval_YYYYMMDD_vN
& .\report.venv\Scripts\python.exe services\bi_analyst\tests\semantic\verify_frozen.py --dataset bi_eval_YYYYMMDD_vN
& .\report.venv\Scripts\python.exe $manager phase1-reference-review --dataset bi_eval_YYYYMMDD_vN --output outputs\new_reference_review
& .\report.venv\Scripts\python.exe $manager phase1-powerbi-package --dataset bi_eval_YYYYMMDD_vN --output outputs\new_powerbi_package
```

The remaining examples below describe the original evidence workflow; continue
using explicit dataset versions, new output directories and aligned inputs.

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

**8 October owner decision update:** currency is GBP; the business timezone is
Europe/London; the fiscal year is 1 November-31 October; financial amounts are
presented as stored with VAT inclusion unspecified, under the UK tax jurisdiction.
All four decisions are approved in the current owner review. See the
[decision record and regenerated gate](bi-analyst-business-decisions.md).
Earlier pending gate reports retain their historical status and do not supersede
these later decisions or the subsequent full Phase 1 owner closure.

The following fields describe the detailed evidence-checklist path when no
explicit `owner_closure` is supplied. The current owner acceptance supersedes
this checklist for Phase 1 progression without marking unperformed checks passed.

Every approved record in `review.json` requires `owner`, `value`, `reviewed_by`,
an offset-aware ISO `reviewed_at`, and nonempty `evidence` references. Software
checks completeness and consistency, not the truth of an assertion or reviewer
identity. Keep reviewed versions and identify the actual business/platform owners.

The decision register covers backup time, access, currency, tax, timezone, fiscal
calendar, current periods, provider handling, retention, historical classifications,
population, writer precedence, question review, performance and connections.
Evaluation settings must not become agreed business decisions without review.

Each metric also needs `population`, `source_contract` and `limitations`.
The `report_population` decision's `value` and each core metric's `population`
must be exactly `reportable`; the gate rejects active-only or obsolete population
labels even if an approval record is otherwise complete. Dataset/version and any
accepted limitations remain separate evidence fields.
The packet records required semantic distinctions. Exact-ID source reviews should
link current observations of API lifecycle state, membership and typed
blank/missing/unreadable inputs. Link the 8 October clarification and identify
the exact corrected catalogue/metric version, source SQL/mirror configuration,
deployed writer commits and new reference-dataset version covered by approval.
Historical evidence hashes do not extend approval to changed definitions.
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

## Optional independent Power BI result comparison

The existing Power BI reports connect to the **live database**. If comparing them, validate their
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
are retained. The optional comparator covers seven reference outputs; the existing 50 queries
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

Run `phase1-review` with or without `--power-bi`; its absence never blocks Phase 1.
It writes `phase1_gate.json`, `phase1_gate.md` and `review_snapshot.json`.
An explicit accepted owner closure returns `closed_by_owner` with no blockers.
The gate command exits `0` for accepted/closed, `1` for invalid input/execution
failure and `2` for a blocked review without valid owner closure. The separate
optional reader/comparator commands still return `2` for incomplete access or
missing/differing comparisons; that diagnostic exit code does not reopen Phase 1.
Owner closure authorises progression, without asserting a hosted deployment.

```powershell
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests/evals -q
```
