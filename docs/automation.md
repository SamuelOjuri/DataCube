# Automation Overview

This document outlines how the automation pipeline operates, covering the core
webhook-driven flow, scheduled background jobs, and the pipeline forecast layer.

## Components

- **Task helpers** (`src/tasks/pipeline.py`)
  - Provide callable functions for rehydrate flows, LLM backfill, Monday sync, and
    project-by-ID refreshes.
- **Queue worker** (`src/services/queue_worker.py`)
  - In-process async worker that runs rehydrate → analyse → Monday push jobs.
  - Persists job status to the `job_queue` table for observability.
- **Webhook integration** (`src/webhooks/webhook_server.py`)
  - Subitem/hidden item updates enqueue rehydrate jobs for affected parent projects.
  - Parent item updates still perform immediate analysis and enqueue a Monday push,
    except order-value mirror changes, which enqueue source rehydration.
- **Postgres maintenance** (`src/tasks/postgres_maintenance.py`)
  - Materialized view refresh (conversion metrics, forecast aggregates, and smoothing artifacts).
  - Daily base forecast and smoothing snapshot creation plus retention cleanup.
- **Forecast API** (`src/api/routes/forecast.py`)
  - Read-only endpoints for Power BI and other consumers:
    - `GET /forecast/pipeline` — monthly 12-month forecast from the materialized view.
    - `GET /forecast/snapshot` — historical snapshot data with pagination.
    - `GET /forecast/smoothing/projects` — project-level smoothing scores and explanations.
    - `GET /forecast/smoothing/monthly` — smoothed monthly allocation totals.
    - `GET /forecast/smoothing/snapshot` — historical project-month smoothing allocations.
    - `GET /forecast/smoothing/snapshot/totals` — full filtered smoothing totals independent of pagination.
- **Scheduler** (`src/api/app.py`)
  - APScheduler runs the following periodic jobs:
    - **Hourly delta rehydrate** (`rehydrate_delta`) — re-syncs recently changed projects.
    - **Nightly LLM backfill** (`backfill_llm`, 02:15 UTC) — fills missing LLM analyses.
    - **25-minute Monday sync** (`sync_projects_to_monday`) — pushes updated analyses back to Monday.
    - **6-hour recent rehydrate** (`rehydrate_recent`) — broader catch-up rehydrate.
    - **30-minute materialized view refresh** (`refresh_conversion_views`) — runs `refresh_analytics_views()`, including smoothing signal refresh before smoothed monthly allocation refresh.
    - **Daily forecast snapshot maintenance** (`forecast_snapshot_maintenance`, default 03:10 UTC) — creates today's base and smoothing snapshots and deletes expired rows.
  - Queue worker is started alongside the FastAPI app.

## Customer Order Value

`projects.total_order_value` is the sum of `cust_order_value_material` plus
`cust_additional_charges` across the project's persisted subitems. Both amounts
come from the same resolved hidden item: Monday columns `numbers98__1` and
`numbers3__1`. The material component remains separately available. Order dates,
invoice values, enquiry values and reporting-stage filters are unchanged.

Order inputs use strict decimal parsing. A fetched blank numeric source is stored
as zero; a missing or invalid source is stored as NULL. A project total is withheld
if any child has an unknown component, if a hidden source is repeated in the
loaded rollup population, or if the total exceeds NUMERIC(12,2). These conditions
are logged. The existing project total is retained, not certified as current.
Fully loaded zero-value projects, including projects with no persisted subitems,
are updated to zero. Child reads use keyset pagination until an empty page.

Order webhooks queue rehydration of both current source amounts instead of
writing event values directly. Parent order mirrors cannot overwrite the rollup.
Explicit hidden IDs take precedence over name matching; unresolved explicit IDs
and multiple links do not fall back to another source. Name-based fallbacks still
require relationship verification during reconciliation.

Before the order-only backfill, ensure `cust_additional_charges NUMERIC(12,2)` exists
on both `hidden_items` and `subitems`, without a zero default for unfetched history.
Deploy the corrected sync and webhook code to all writers. The code change does
not itself backfill existing rows, refresh forecast materialized views, or restate
historical snapshots. Reconcile the backfilled totals against Monday before using
them as certified full customer order values.

Offline regression checks (the two live webhook smoke tests are excluded):

```powershell
& .\report.venv\Scripts\python.exe -m pytest tests/test_sync_service.py tests/test_webhooks.py -k 'not test_parent_update and not test_subitem_update' -q
```

### Controlled Order Backfill

For bounded correction while normal writers remain active, use the separate
[scoped order-value workflow](order-value-scopes.md). Its read-only reassessment,
reviewed dependency scopes, foreign-key-backed row locks and database commit
journal replace the global apply window for eligible existing rows. The legacy
commands below still require their documented paused-writer window.

Use `scripts/backfill_order_values.py` for the order-only historical backfill.
It does not invoke the broad rehydration pipeline, change order dates, refresh
forecast views, replay webhooks, or modify historical snapshots.

Prerequisites:

- Set `MONDAY_API_KEY` and `SUPABASE_DB_URL` in the environment or local `.env`.
  The database URL must be a PostgreSQL connection string, not the Supabase REST
  URL. Use direct PostgreSQL or session pooling and credentials permitted to read,
  lock and update these three tables. The script does not use the service-role
  REST client and does not print connection credentials.
- Keep the corrected services deployed, but pause scheduled syncs, queue consumers,
  snapshot jobs and other writers for the capture/review/apply/verify window.
  Retain incoming webhooks durably for replay; do not discard events. Coordinate
  a quiet Monday editing window too. The script cannot pause these services for
  you; `--writers-paused` is your explicit confirmation, not an automatic pause.
- Ensure there is enough local space for the baseline, source and plan artifacts.
  These contain commercially sensitive amounts and identifiers: keep them private
  and out of source control. Use a new run directory for every fresh preparation;
  only the guarded checkpoint-resume workflow below may continue an existing run.

1. Prepare a read-only dry run from the repository root:

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run1
```

`prepare` reads all database rows using keyset pagination and starts fresh Monday
board traversals, without reusing sync cursors. It checks board counts and exact
detail-response coverage. Missing pages, API failures and count changes abort
preparation; an incomplete directory has no usable manifest and cannot be applied.
Parent IDs, subitem parent/hidden links and numeric hidden-board inputs are captured.
The computed material-plus-charges amount is compared with Monday's total formula.

The run directory contains `baseline.json`, `source.json`, `plan.json`,
`review.csv` and `manifest.json`. Review the CSV's old total, material sum, charge
sum, proposed total, difference and validation status. Inspect `diagnostics` in
the plan for orphaned records and unlinked nonzero hidden orders. The manifest
prints a run ID, per-table update counts and blocked-project counts.

Missing or conflicting links, duplicate source orders, unknown amounts and formula
disagreements block the whole affected project. Unlike ordinary sync's name-based
fallbacks, this backfill requires a verified single Monday link agreeing with the
stored relationship. Resolve relationships using the corrected sync process and
prepare a new run; this script does not guess links, insert missing records or
delete stale children. A project with no stored or live children is withheld unless
individually approved when preparing a new run, for example:

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run2 --approve-empty-project 123456789
```

2. Apply the reviewed run, using its printed run ID:

```powershell
python -m scripts.backfill_order_values apply --run-dir outputs/order_value_backfill/run1 --confirm-run-id RUN_ID_FROM_PREPARE --writers-paused
```

Apply refuses unresolved projects or diagnostics by default. `--allow-blocked` is
an explicit opt-in to a PARTIAL backfill: only verified projects and their linked
order components are updated; blocked rows are untouched. This is not certification
of the full dataset. Keep the unresolved report for step 4.

The script checks artifact hashes, code/mapping versions and database target, then
fetches Monday again and rejects source drift. Inside one transaction it acquires
write-conflicting table locks with a five-second lock timeout, compares the current
database with the captured baseline, replaces order fields and verifies the complete
expected state before commit. Other captured invoice/enquiry/date fields must remain
unchanged. Database triggers may update normal metadata timestamps.

Existing nonzero totals are replaced, never incremented. Any write or reconciliation
failure rolls back the transaction. Retrying the same unchanged run after a confirmed
commit is a no-op. Each successful apply writes a separate `apply-*.json` receipt.
If the connection or receipt write fails near commit, do not assume rollback: run
`verify` before retrying. Never edit staged files to bypass a refusal.

3. Verify while writers are still paused:

```powershell
python -m scripts.backfill_order_values verify --run-dir outputs/order_value_backfill/run1
```

This read-only check compares the stored values with the reviewed post-apply state
and writes a `verify-*.json` result. It exits with code 2 on disagreement and code 1
on errors. A matching result can still have blocked projects after a partial run;
check those counts. Source or database drift requires a fresh preparation, not
overwriting the old audit files.

After verification, follow step 4's reconciliation and forecast-view refresh gates.
Replay retained events and resume writers under the deployment runbook; neither is
automated here. Preserved historical snapshots still represent their original values.

Offline backfill tests:

```powershell
python -m pytest tests/test_backfill_order_values.py -q --basetemp "$env:TEMP/order-$([guid]::NewGuid().ToString('N'))"
```

#### Reviewed Parentless Duplicate Exclusions

The reviewed discrepancy was 37,681 unique subitem-board IDs versus a reported
count of 37,677. Independent parent enumeration contained 37,677 children. The
four extra IDs were active, parentless duplicates sharing hidden sources with
these legitimate children:

| Excluded Subitem | Retained Subitem | Retained Parent | Retained Hidden Source |
|---|---|---|---|
| `2121791235` | `2119952710` | `2118634736` | `2119952498` |
| `2121791391` | `2120432332` | `2120277774` | `2120432144` |
| `2121791467` | `2120454800` | `2120150600` | `2120454642` |
| `2121791522` | `2120852454` | `2120673411` | `2120852318` |

To explicitly approve this fixed four-item contract, prepare a fresh run while
writers are paused and Monday edits are quiet, as required above:

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run3 --approve-reviewed-parentless-duplicates
```

Without this flag, the original strict count check remains in force. The flag
does not accept arbitrary IDs, a count tolerance or other parentless records.
It does not delete or reparent anything. It removes only the four duplicate
subitem entries from the captured calculation inventory; their legitimate
counterparts, hidden sources and projects remain in the normal backfill.

Preparation validates the following evidence; application always fetches it
again from Monday. A resumed preparation may reuse saved detail batches as
described under Interrupted Preparation below:

- All four excluded IDs remain active and parentless on the expected subitem
  board, each linked only to its reviewed hidden source. None exists in the
  captured database tables.
- Every retained child exactly reconciles with the independently enumerated
  active parents and their immediate children. The subitem board count equals
  the retained count, with exactly the four approved extra IDs. Parent and hidden
  board counts and detail coverage must match exactly too.
- Each reviewed counterpart, parent and hidden source is active on its expected
  board. The database relationships still match the table above, and exactly
  that counterpart references the source in both the retained live inventory
  and stored subitems.
- Each reviewed hidden source has freshly fetched material, additional charges,
  formula total and invoiced amount of zero, with blank order/invoice dates.
  Fetched blank numeric values count as zero; absent or malformed inputs do not.
  Stored counterpart/source amounts may be NULL or zero, but not nonzero, and
  stored order/invoice dates must remain NULL. Historical NULLs alone never
  establish the live zero-value condition.

`source.json` retains the excluded records, exact approved mappings, inventories,
parent metadata and financial evidence. `plan.json` and `manifest.json` contain
an `excluded_subitems` audit array, and the manifest summary reports the exclusion
count separately from blocked projects. Review these alongside `review.csv`.
No usable manifest is written if validation fails. Keep incomplete runs for audit.
Investigate validation failures and prepare a fresh run; transport interruptions
in checkpoint-enabled preparations may use the guarded resume workflow below.

After reviewing the artifacts, use the usual confirmation and verification steps
against that same new run directory:

```powershell
python -m scripts.backfill_order_values apply --run-dir outputs/order_value_backfill/run3 --confirm-run-id RUN_ID_FROM_PREPARE --writers-paused
python -m scripts.backfill_order_values verify --run-dir outputs/order_value_backfill/run3
```

Application uses the approval persisted in the manifest; there is no separate
apply-time exclusion override. It revalidates live source conditions and exact
source equality, then checks the database exclusion conditions again under the
table locks before updating or declaring an already-applied result. Changed
relationships, new amounts/dates, unexplained records, missing evidence and count
mismatches abort even with `--allow-blocked`. That flag still permits only a
partial run for unrelated blocked projects under the normal rules.

Apply and verify receipts include the exclusion audit. `verify` checks the staged
evidence and expected database state; it does not perform another Monday scan.
Exclusions are not blocked projects, nor do they certify unrelated rows. The
script/manifest version and code fingerprint changed, so old prepared manifests
must be replaced by a fresh preparation. Failed runs and diagnostic directories
are never applyable. Historical snapshots are not restated.

#### Targeted Reconciliation

Use `scripts/reconcile_order_values.py` to investigate blocked projects and stage
repairs for explicitly selected parents. It is separate from the backfill script,
so generating reports does not invalidate a prepared backfill's code fingerprint.
It never edits original run artifacts, changes Monday relationships, deletes rows,
reparents children, expands the reviewed exclusions or automatically clears empty
projects. Keep all generated reports private and out of source control.

Generate the offline reports in a new directory:

```powershell
python -m scripts.reconcile_order_values report --run-dir outputs/order_value_backfill/run4 --output-dir outputs/order_value_backfill/reconciliation_run4
```

This validates the original run and needs no database credentials or Monday
requests. If that output directory already exists, inspect it or choose another;
it is never overwritten. The output includes:

| File | Review purpose |
|---|---|
| `projects.csv` | Original blockers, child IDs, repair group, readiness and resolution steps |
| `repair-groups.csv`, `repair-groups.json` | Dependency groups, blocking stored owners and review instructions |
| `subitem-links.csv` | Stored versus live parent/hidden links; missing-row direction |
| `duplicate-sources.csv` | Shared live sources versus duplicate stored links only |
| `unlinked-orders.csv` | Nonzero source orders without a retained live child |
| `candidate-project-ids.json` | Potential candidates, not automatic write approval |
| `report.json`, `summary.json` | Structured evidence and counts |

A candidate must have a complete, active live child set with explicit single
hidden links and unambiguous source amounts. A project with missing live children,
reparenting, inactive/unknown metadata, shared live sources or no children requires
manual review. Confirm missing/inactive records in Monday before deciding their
disposition. With read-only Monday access, genuinely shared or invalid live links
remain blocked pending an approved ownership decision; the script does not alter
Monday or invent local overrides. Do not choose an owner by name or delete records
based on missing API results. Investigate unlinked nonzero
orders separately. Approve an empty-project zero only through the existing
backfill's per-project approval after business review.

Duplicate names in Monday are allowed when each child has its own verified source
ID. Neither name uniqueness nor the business status label `Archived` establishes
the relationship or lifecycle state. Missing stored links are repaired from exact
live IDs, not by matching the first name, largest quote or latest revision.

The report distinguishes 1) preliminary `rehydrate_candidate` status from 2)
`repair_readiness=prepare_candidate`, which also checks stored-owner dependencies.
The latter still requires fresh source/database checks and human review, not
automatic approval. Dependency groups containing an ineligible stored owner or
more than 25 projects remain manual. Group IDs are specific to a saved run and
must be selected again after generating a fresh baseline.

| Finding | Treatment with read-only Monday access |
|---|---|
| Missing or different stored hidden ID | Refresh from a unique verified live link, then recompute parent rollups |
| Missing stored child or hidden source | Insert only records confirmed by complete live metadata and fields |
| Duplicate stored source references | Correct all necessary owners together; never count a source twice |
| Missing parent/child in live inventory | Confirm lifecycle and history; no automatic deletion or reparenting |
| Empty project | Require an explicit business decision about preserving or clearing totals |
| Shared or invalid live link | Require reviewed ownership/allocation; a database-only override needs a separate durable sync policy |
| Unlinked nonzero hidden order | Establish its actual parent/child ownership; do not allocate by name or prefix |

Select a small reviewed group of 1-25 candidate projects. If a desired hidden
source is still referenced by another stored child, that other eligible owner's
repair may need to be included in the same group; inspect the duplicate/link
reports. A selection that would leave a source shared is refused. Candidate
status alone does not guarantee that an arbitrary subset is executable.

During the paused-writer and quiet-Monday window, stage the repair in a new directory:

```powershell
python -m scripts.reconcile_order_values prepare --run-dir outputs/order_value_backfill/run4 --repair-dir outputs/order_value_backfill/repair1 --project-id REVIEWED_PROJECT_ID
```

Replace the placeholder with a numeric Monday parent ID. Repeat `--project-id`
for a group, or use `--project-ids-file` with a JSON array of 1-25 unique string
IDs. Do not pass the entire candidate list when it exceeds that limit.

Alternatively, select the full dependency group shown in `repair-groups.csv`:

```powershell
python -m scripts.reconcile_order_values prepare --run-dir outputs/order_value_backfill/run4 --repair-dir outputs/order_value_backfill/repair1 --repair-group repair-001
```

Repeat `--repair-group` to combine independent reviewed groups, up to 25 projects
in total. Only `prepare_candidate` groups are accepted. Preparation uses the
original immutable source artifacts, not edited CSV values. Start with one small
repair to inspect all affected fields before considering larger groups.

Preparation is read-only. It requires the metadata-complete approved-exclusion
backfill run, verifies the current database against its baseline, and recaptures
the complete Monday inventory/source for drift and uniqueness checks. Reads are
therefore not limited to selected projects, even though writes are. It then fetches
only the selected parents' complete child sets and exact hidden IDs with all
extraction columns, including mirror display values. No prefix/name lookup is used
to select rows. Missing columns, incomplete details and changed relationships stop
preparation; there is no fallback to partial data.

Review `changes.csv`, `repair.json` and the repair `manifest.json`. This is a
targeted rehydration, **not a link-only update**: it refreshes the transformed hidden
and subitem fields for the selected scope, inserts confirmed missing hidden/subitem
rows, and recomputes parent order totals/dates, invoiced totals/date ranges and
enquiry totals. It does not insert parents or run analyses, snapshot jobs or Monday
pushes. Other parent fields remain untouched. Review invoice/enquiry and other
field changes alongside the hidden-link correction before approving it.

Generated columns are never staged for writing. In particular, PostgreSQL computes
`invoicing_spread_days` from the two invoice dates, and verification checks the
result instead of requiring its old value to remain unchanged. Schema metadata
rejects any other generated field accidentally introduced into a staged update.

Apply with the repair ID, not the original backfill run ID:

```powershell
python -m scripts.reconcile_order_values apply --repair-dir outputs/order_value_backfill/repair1 --confirm-repair-id REPAIR_ID_FROM_PREPARE --writers-paused
python -m scripts.reconcile_order_values verify --repair-dir outputs/order_value_backfill/repair1
```

Apply rechecks artifact hashes, code, database target, full source equality and
exact targeted source details. One transaction takes write-conflicting locks on
the three tables, checks the database baseline, selected full rows and schema,
then writes only staged identifiers. It checks both the resulting backfill fields
and selected rows before commit. Unexpected changes roll back the transaction;
an unchanged repeated apply returns `already_applied`. No `--allow-blocked` or
other validation bypass exists in this tool.

Success writes an `apply-*.json` receipt. A connection/receipt failure near commit
can leave commit status uncertain: use read-only `verify` before retrying. Verify
returns exit code 2 for a mismatch, 1 for an error, and 0 for a match; it does not
query Monday. Normal database metadata timestamps may change during updates.

After a repair, verify it and prepare a **fresh backfill and reconciliation report**
before further repairs or the order backfill. The original baseline is intentionally
stale after changed rows are committed; do not apply run4 or another repair staged
against that old baseline. Run the normal downstream analysis/forecast refresh
and event replay gates once reconciliation and backfill are verified. Historical
snapshots are not restated by this script.

Offline tests:

```powershell
python -m pytest tests/test_reconcile_order_values.py tests/test_backfill_order_values.py -q --basetemp "$env:TEMP/order-$([guid]::NewGuid().ToString('N'))"
```

#### Interrupted Preparation

A Windows `ConnectionResetError(10054)` means a transport connection was closed;
it does not establish that a Monday cursor expired. Detail batches use explicit
item IDs, not cursors. Check Monday availability and the network/proxy route
before retrying. This small authenticated read needs `MONDAY_API_KEY` but no
database connection and creates no files:

```powershell
python -m scripts.backfill_order_values check-monday
```

It returns the configured subitem board's count and a timestamp, with exit code
0 on success or 1 on failure. Success demonstrates access for that request, not
that a long capture will finish or that the board inventory is complete.

New preparations with `--approve-reviewed-parentless-duplicates` automatically
save `capture-context.json` and complete, validated detail batches under
`capture-batches/`. Each batch records its request, response hash and capture
timestamp. Missing/incomplete batches are not saved as complete. No pagination
cursors are reused across invocations. Keep these commercially sensitive files
private alongside the rest of the run artifacts.

The earlier failed `run3` contains only a baseline and cannot be resumed. Start
a new run with this code, during the paused-writer and quiet-Monday window:

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run4 --approve-reviewed-parentless-duplicates
```

After a transport interruption of that checkpoint-enabled capture, resume with
the same approvals (including any `--approve-empty-project` options):

```powershell
python -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/run4 --approve-reviewed-parentless-duplicates --resume
```

Resume checks the database against the saved baseline in a new read-only
transaction, and checks the code fingerprint, target, mappings and approvals.
It starts new board ID traversals and count checks. Only complete saved batches
matching the exact current request and response hash are reused; missing batches
are fetched. The original baseline and completed batch files are not overwritten.
Run only one preparation/resume process per directory. If the quiet window ended,
or records, code, approvals or the database changed, start a fresh run instead.

Checkpoints are draft capture evidence, not a Monday snapshot. Earlier batch data
may be stale even when IDs/counts match. All normal exclusion, relationship and
amount checks still run on the assembled evidence. The manifest records
`preparation_resumed`; review it and the plan before applying. Apply **never**
reads checkpoints: it fetches every source again and rejects any difference,
including when `--allow-blocked` is set. It then performs the existing locked
database checks before any writes. Historical snapshots remain untouched.

Resume refuses legacy runs without capture context, changed or corrupt evidence,
and runs that already contain a completed source or review artifacts. Never edit
checkpoint files or delete audit artifacts to force a retry. An interrupted run
without a manifest cannot be applied. Ordinary strict preparations and the
inventory diagnostic still require new directories after failure; `--resume`
is supported only for approved-duplicate preparation, never for apply or verify.
After an uncertain apply, use verify as described above.

#### Inventory Count Diagnostics

If preparation stops because Monday's reported count disagrees with the captured
unique IDs, investigate the inventory before repeating a full source capture.
Equal before/after counts do not establish an unchanged inventory, and fetching
more IDs does not establish completeness. Do not add a count tolerance or bypass
the backfill guard.

Run the read-only diagnostic in a new directory:

```powershell
python -m scripts.backfill_order_values diagnose-inventory --run-dir outputs/order_value_backfill/diagnostic1
```

Only `MONDAY_API_KEY` is needed. This command does not connect to Supabase, fetch
hidden-board order amounts, modify Monday, or create an applyable backfill plan.
Use a quiet Monday editing window. It cannot pause Monday users or automations.
Keep these ID and relationship artifacts private and out of source control.

The default workflow:

1. Traverse the configured subitem board twice using fresh IDs-only pagination.
   Save each completed traversal with timestamps, before/after counts, unique IDs,
   actual captured count and count difference. Log the final page count too.
2. Compare the actual ID sets. Save IDs seen only in the first or second scan,
   including changes that leave the total number of items unchanged.
3. Independently traverse the configured parent board and query each parent's
   immediate `subitems`. Compare that inventory with the union of both subitem
   scans; flag missing parents, duplicate child listings, unexpected boards,
   non-active/unknown states and inconsistent parent links.
4. Fetch board, state and parent metadata for discrepant IDs, requesting
   `exclude_nonactive: false`. Inaccessible or absent items remain explicitly
   listed as `not_returned_ids`; their absence is not proof of deletion.

Inspect `summary.json` first. The evidence files are:

| File | Contents |
|---|---|
| `inventory-1.json`, `inventory-2.json` | Complete subitem ID scans and observed counts |
| `comparison.json` | Set equality, reported counts, IDs unique to either scan |
| `parent-inventory.json` | Independently scanned parent IDs and counts |
| `parent-details.json` | Parent and immediate-child metadata returned by Monday |
| `parent-comparison.json` | Board-only/parent-only IDs, relationship map and metadata issues |
| `discrepancy-details.json` | Fresh metadata for discrepant IDs and IDs not returned |
| `summary.json` | Diagnostic outcome, written only after all requested stages finish |

To run only the two IDs-only scans, skip the parent and metadata queries explicitly:

```powershell
python -m scripts.backfill_order_values diagnose-inventory --run-dir outputs/order_value_backfill/diagnostic_ids1 --ids-only
```

Exit code 0 means the requested checks found no discrepancy; code 2 means the
diagnostic completed and found evidence requiring investigation. Code 1 means an
error interrupted the diagnostic. Completed stage files are retained, but a
missing summary means the requested workflow did not finish. Duplicate IDs,
missing/partial API responses and non-advancing cursors still stop capture.
Existing directories are never overwritten; retries require a new directory.

For connection resets (including Windows error 10054) and timeouts, each diagnostic,
connectivity-check or reviewed-exclusion capture read is attempted up to six times,
with delays of 5, 10, 20, 40 and 60 seconds. Before a retry, pooled connections are
closed so the same client establishes a new connection with its existing TLS,
authentication and adapter configuration. These are application-level attempts;
request timeouts and existing HTTP-adapter retries add to the elapsed time.
Retried pages keep the same cursor and
metadata requests keep the same ID batch. The scan is not silently restarted.
Warnings and the final failure identify the board-count, page or metadata-batch
stage without logging cursors, request headers or exception details from this
retry helper. Shared client logging is unchanged.

This retry helper applies only to diagnostic, connectivity-check and
reviewed-exclusion capture reads, never mutations or backfill writes. Certificate
failures, HTTP errors, GraphQL errors and invalid data are not retried by the
helper, and TLS verification remains enabled. Persistent
resets still stop capture: retries cannot fix an unavailable service,
blocked proxy/firewall route or environment-specific network issue. Check those
before another run. Previously completed evidence files remain; if the first
request fails, the diagnostic directory can be empty.

Interpret `classification` in the summary:

- `inventory_changed`: the two ID sets differ, even if their sizes match.
- `reported_count_changed`: sets match, but the four reported count observations
  are not all equal.
- `stable_inventory_count_mismatch`: both sets match and reported counts are
  stable, but the reported count disagrees with the returned inventory.
- `counts_and_ids_match`: the two scans' IDs and counts agree; inspect the parent
  cross-check separately when performed.

A count discrepancy alone cannot identify particular "extra" items. If all
independent ID sets agree but the metadata count still differs, retain the evidence
for investigation with Monday before changing the completeness rule. This is a
diagnostic, not a transactional snapshot or certification: API visibility, item
state semantics and edits between requests can still affect results. The parent
cross-check targets this project's separate parent/subitem boards and immediate
children, not arbitrary multi-level board hierarchies.

No diagnostic result changes the `prepare`/`apply` safeguards. Resolve or explain
the discrepancy before preparing a fresh backfill. Neither failed `run1`/`run2`
captures nor diagnostic directories can be applied. Changing this script also
invalidates the code fingerprint of previously prepared backfill manifests.

## Scheduled Jobs Summary

| Job | Schedule | Function | Description |
|---|---|---|---|
| Delta rehydrate | Every 1 hour | `rehydrate_delta` | Re-sync projects changed in the last 3 days |
| LLM backfill | Cron 02:15 UTC | `backfill_llm` | Fill missing LLM analyses for recent projects |
| Monday sync | Every 25 minutes | `sync_projects_to_monday` | Push updated analysis results to Monday.com |
| Recent rehydrate | Every 6 hours | `rehydrate_recent` | Broader catch-up rehydrate of recent changes |
| Materialized view refresh | Every 30 minutes | `refresh_conversion_views` | Refresh analytics, forecast, smoothing signal, and smoothed monthly artifacts where deployed |
| Forecast snapshot maintenance | Cron (default 03:10 UTC) | `run_daily_forecast_snapshot_maintenance` | Insert daily base + smoothing snapshots and clean up expired rows |

## Pipeline Forecast Layer

The forecast layer runs in parallel to the Monday push flow and does not interfere with it. It produces SQL-based forecast artifacts that Power BI (or any Postgres/API consumer) can query.

### Architecture

```
Monday Sync + Webhooks
        │
        ▼
  projects + analysis_results  (core data)
        │
        ├──► vw_pipeline_forecast_project_v1    (real-time project-level forecast view)
        │         │
        │         ├──► mv_pipeline_forecast_monthly_12m_v1  (monthly aggregate, refreshed every 30 min)
        │         │
      │         ├──► pipeline_forecast_snapshot            (daily snapshot table)
      │         │
      │         └──► vw_pipeline_smoothing_score_v1        (project smoothing scores)
      │                   │
      │                   ├──► mv_pipeline_smoothed_revenue_monthly_12m_v1
      │                   │
      │                   └──► pipeline_smoothing_forecast_snapshot
      │
      ├──► mv_invoice_smoothing_signal_v1      (invoice timing signals)
        │
        └──► Monday push  (unchanged)
```

### Forecast Artifacts

- **`vw_pipeline_forecast_project_v1`** (view) — Joins `projects` to the latest `analysis_results` and computes stage bucket, contract value (with practical fallback), probability (with precedence rules), forecast date, and committed/expected/best-case/worst-case value bands. This is the single source of truth for all forecast formulas.

- **`mv_pipeline_forecast_monthly_12m_v1`** (materialized view) — Aggregates the project-level view into monthly totals by stage bucket for the next 12 months. Refreshed concurrently every 30 minutes by the scheduler. Required unique index ensures concurrent refresh works without downtime.

- **`pipeline_forecast_snapshot`** (table) — Stores one daily snapshot set of all project-level forecasts within the 12-month window. Populated by the `create_pipeline_forecast_snapshot()` SQL function. The snapshot is idempotent (re-running for the same date replaces that day's rows). Primary key: `(snapshot_date, project_id, forecast_month)`.

- **`pipeline_smoothing_forecast_snapshot`** (table) — Stores daily project-month smoothing allocation rows. One project can appear in multiple forecast months, so `expected_value` is repeated on each project-month row as project context. Do not sum `expected_value` across smoothing snapshot rows for reporting totals. Use `allocated_expected_value` instead; in the REST API this is exposed as `allocated_monthly_value`, with `smoothed_allocated_value` and `unsmoothed_allocated_value` providing the component split. Primary key: `(snapshot_date, project_id, forecast_month)`.

- **`mv_invoice_smoothing_signal_v1`** (materialized view) — Stores live Empirical-Bayes invoice smoothing signals by dimension and group. It is refreshed through `refresh_invoice_smoothing_signal_v1(CURRENT_DATE)` before smoothed monthly allocation is refreshed.

- **`vw_pipeline_smoothing_score_v1`** (view) — Scores live forecast projects with Category 30%, Type 10%, Product 30%, Account 30% smoothing weights, global fallback values, risk bands, confidence counts, and treatment recommendations.

- **`mv_pipeline_smoothed_revenue_monthly_12m_v1`** (materialized view) — Allocates expected value into unsmoothed and smoothed components across the current 12-month window using day-weighted month overlap.

### Snapshot Retention

Old base forecast and smoothing snapshot rows are automatically deleted after the daily insert. The default retention window is **730 days** (approximately 2 years).

**Configuration:**

| Environment Variable | Default | Description |
|---|---|---|
| `FORECAST_SNAPSHOT_RETENTION_DAYS` | `730` | Days of snapshot history to retain |
| `FORECAST_SNAPSHOT_CRON_HOUR_UTC` | `3` | Hour (UTC) for the daily snapshot job |
| `FORECAST_SNAPSHOT_CRON_MINUTE_UTC` | `10` | Minute for the daily snapshot job |

To change the retention window without redeploying, set the `FORECAST_SNAPSHOT_RETENTION_DAYS` environment variable and restart the service.

To run snapshot maintenance manually (e.g., to seed the first snapshot or re-run after a failure):

```python
from src.tasks.postgres_maintenance import run_daily_forecast_snapshot_maintenance
import logging
run_daily_forecast_snapshot_maintenance(logger=logging.getLogger("manual"))
```

This wrapper refreshes analytics and smoothing materialized views, creates the base forecast snapshot, cleans old base rows, creates the smoothing snapshot, and cleans old smoothing rows.

Or target a specific date:

```python
from datetime import date
from src.tasks.postgres_maintenance import create_pipeline_forecast_snapshot, create_pipeline_smoothing_forecast_snapshot
import logging
create_pipeline_forecast_snapshot(task_logger=logging.getLogger("manual"), snapshot_date=date(2026, 2, 15))
create_pipeline_smoothing_forecast_snapshot(task_logger=logging.getLogger("manual"), snapshot_date=date(2026, 2, 15))
```

### Power BI Consumption

Power BI connects to the forecast layer in one of two ways:

**1. Direct Database Connection (recommended)**

- Connect Power BI Desktop to the Supabase PostgreSQL instance.
- Import or DirectQuery the following:
  - `pipeline_forecast_snapshot` — use `snapshot_date` as the incremental refresh partition key.
  - `pipeline_smoothing_forecast_snapshot` — use `snapshot_date` as the incremental refresh partition key and `allocated_expected_value` as the additive forecast value.
  - `mv_pipeline_forecast_monthly_12m_v1` — current-state monthly summary.
  - `mv_pipeline_smoothed_revenue_monthly_12m_v1` — current-state smoothed monthly revenue allocation.
  - `vw_pipeline_forecast_project_v1` — project-level drill-down.
  - `vw_pipeline_smoothing_score_v1` — project-level smoothing explanations, risk bands, confidence, and treatment fields.
- For incremental refresh on the snapshot table, configure a daily refresh with a rolling retention window (e.g., 24 months) keyed on `snapshot_date`.

**2. REST API**

- `GET /forecast/pipeline?months=12` — monthly aggregates.
- `GET /forecast/snapshot?snapshot_date=2026-02-15` — snapshot rows for a specific date, with pagination (`offset`, `limit`).
- `GET /forecast/smoothing/projects` — project-level smoothing scores and explanation fields.
- `GET /forecast/smoothing/monthly?months=12` — current smoothed monthly allocation totals.
- `GET /forecast/smoothing/snapshot` — paginated project-month smoothing snapshot rows. Row-level `expected_value` is the original project expected value and may repeat for projects allocated across multiple months.
- `GET /forecast/smoothing/snapshot/totals` — full filtered smoothing snapshot totals independent of pagination. Use `totals.allocated_monthly_value` for the additive forecast total; `totals.expected_value` is explanatory and should not be summed across project-month rows.

### Validation

Run the automated forecast validation checks:

```bash
# Pytest suite — validates SQL invariants against live data
pytest tests/test_pipeline_forecast_service.py -q

# Standalone CLI validation script
python scripts/validate_forecast_sql.py

# Focused Phase 7 smoothing SQL checks
python scripts/validate_smoothing_phase7_sql.py

# Full smoothing SQL health check
python scripts/validate_smoothing_sql.py

# Manual smoothing refresh plus validation after deployment/backfill
python scripts/validate_smoothing_sql.py --refresh

# Smoothing SQL/integration tests
pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py -q

# Backtest against invoiced actuals (requires accumulated snapshot history)
python scripts/forecast_backtest.py --months-back 6
```

The backtest script compares historical snapshots against invoiced subitems and generates a markdown report with WAPE, bias ratio, band coverage rate, and calibration recommendations.

## Operations

### Monitoring Jobs

The `job_queue` table records every queue task with status transitions (`queued`,
`running`, `completed`, `failed`). Use Supabase SQL or dashboards to monitor queue
health.

Forecast-specific monitoring:

- Check the latest snapshot date: `SELECT MAX(snapshot_date) FROM pipeline_forecast_snapshot;`
- Count rows per snapshot: `SELECT snapshot_date, COUNT(*) FROM pipeline_forecast_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 7;`
- Check the latest smoothing snapshot date: `SELECT MAX(snapshot_date) FROM pipeline_smoothing_forecast_snapshot;`
- Count smoothing rows per snapshot: `SELECT snapshot_date, COUNT(*) FROM pipeline_smoothing_forecast_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 7;`
- Check smoothing signal date: `SELECT as_of_date FROM mv_invoice_smoothing_signal_v1 WHERE dimension = 'global' AND group_key = '__global__';`
- Compare smoothing monthly and snapshot totals: `SELECT ROUND(SUM(allocated_expected_value), 2) FROM mv_pipeline_smoothed_revenue_monthly_12m_v1;` and latest `pipeline_smoothing_forecast_snapshot` totals.
- Verify materialized view freshness by comparing row counts to the live view.

### Running Manually

All previous scripts still exist as thin wrappers around the task helpers. They can
be invoked manually if required for smoke tests or emergency replays.

Forecast-specific manual operations:

- **Force snapshot re-creation**: Call `create_pipeline_forecast_snapshot()` with a target date.
- **Force smoothing snapshot re-creation**: Call `create_pipeline_smoothing_forecast_snapshot()` with a target date.
- **Force materialized view refresh**: Call `refresh_conversion_views()` from `postgres_maintenance.py`; this uses `refresh_analytics_views()` and refreshes smoothing signals before smoothed monthly allocation where deployed.
- **Adjust retention**: Call `cleanup_old_pipeline_forecast_snapshots(retain_days=N)` directly.
- **Adjust smoothing retention**: Call `cleanup_old_pipeline_smoothing_forecast_snapshots(retain_days=N)` directly.

Smoothing-specific runbook after SQL deployment, invoice-date backfill, or stale materialized-view validation failures:

```powershell
python scripts/backfill_invoice_dates.py --dry-run
python scripts/backfill_invoice_dates.py
python scripts/validate_smoothing_phase7_sql.py
python scripts/validate_smoothing_sql.py --refresh
python scripts/validate_smoothing_sql.py
python -m pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py -q
```

Use `--refresh` as an operator action after deployment/backfill or when a health check indicates stale smoothing materialized views. Routine production refresh should come from the APScheduler `refresh_conversion_views` job and daily snapshot maintenance.

### Smoothing Deployment Order

1. Apply `src/database/schema/schema.sql` so invoice rollup columns, smoothing signal functions/views, scoring views, monthly allocation, and snapshot table exist.
2. Apply `src/database/schema/functions.sql` so `refresh_analytics_views()`, smoothing snapshot creation, and cleanup functions are current.
3. Run `python scripts/backfill_invoice_dates.py --dry-run`, review prepared updates, then run `python scripts/backfill_invoice_dates.py`.
4. Run `python scripts/validate_smoothing_sql.py --refresh` to rebuild smoothing signal, monthly allocation, and today's smoothing snapshot.
5. Run `python scripts/validate_smoothing_phase7_sql.py` and `python scripts/validate_smoothing_sql.py`.
6. Run `python -m pytest tests/test_smoothing_phase7_sql.py tests/test_smoothing_forecast_service.py tests/test_pipeline_forecast_service.py tests/test_postgres_maintenance_phase5.py -q`.
7. Deploy or restart the FastAPI service so APScheduler and `/forecast/smoothing/*` endpoints use the current code.
8. Smoke test `/forecast/smoothing/projects`, `/forecast/smoothing/monthly`, `/forecast/smoothing/snapshot`, and `/forecast/smoothing/snapshot/totals`.

### First Production Smoothing Validation

First production validation should verify live database values, not hard-coded workbook fixture outputs:

- invoice rollups match subitem invoice dates;
- mature-cohort eligibility uses the deployed `as_of_date`;
- global fallback rate/spread are recomputed from the live mature cohort;
- shrinkage formulas use `k = 20`;
- scoring uses Category 30%, Type 10%, Product 30%, Account 30%;
- risk bands and treatment fields match the implementation contract;
- smoothed monthly allocation sums back to project expected value within rounding tolerance;
- latest smoothing snapshot rows are project-month rows with project-level explanation fields;
- Power BI uses `allocated_expected_value` or API `allocated_monthly_value` for additive smoothing snapshot totals.

Only compare against workbook metrics in a deliberate fixture/parity run using frozen workbook-style data and `as_of_date = 2026-04-28`; live production values are expected to differ.

### Adding New Tasks

1. Implement the core logic inside `src/tasks/pipeline.py` or `src/tasks/postgres_maintenance.py`.
2. Add a queue handler in `queue_worker.py` and expose enqueue helpers.
3. Wire the new tasks via webhooks or scheduler as appropriate.

### Monday Push — Unchanged

The existing webhook → rehydrate → analyse → Monday push flow remains active and is
completely independent of the forecast layer. Forecast artifacts are a parallel,
read-only output path that does not write back to Monday.com.
