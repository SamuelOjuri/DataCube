# Compare the remaining manual-review projects with Monday

Monday is the authority. Finance corrects business data in Monday; this workflow
copies confirmed differences into existing Supabase rows. It never writes to
Monday, infers relationships from item names, excludes a business status such as
`Archived`, deduplicates financial contributions, or repairs Monday formulas.

## Stage a fresh comparison

From the repository root in PowerShell:

```powershell
$runDir = 'outputs/order_value_backfill/monday_compare_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\scripts\run_order_value_monday_compare.ps1 -Step Stage -RunDir $runDir
```

The default selection is `manual_review` in
`outputs/order_value_backfill/blocked_review_20261002/projects.csv`, the later
156-project report. The older `reconciliation_run4/projects.csv` also has 156 but
contains a different set of IDs. Use `-Report <path>` to explicitly select another
report. Both `status` and historical `action` columns are supported. Selection is
by Monday ID; empty parents and nonnumeric item names remain eligible.

Stage uses read-only SQL transactions and GraphQL queries for selected IDs,
their current children, stored extra children, and explicitly linked sources.
It performs no full board or table traversal. It requires the existing privileged
`SUPABASE_DB_URL`, `MONDAY_API_KEY`, PostgreSQL 17+, validated foreign keys and the
already-installed `order_value_scope_commits` journal. It never installs schema.

Review these local artifacts:

- `changes.csv`: only actual field differences proposed for update.
- `projects.csv`: every compared project's state, scope and unresolved count.
- `unresolved.csv`: field-level limitations, including lifecycle and missing rows.
- `comparison.json`: full before/after rows, Monday evidence, connected scopes,
  report provenance and any whole-scope deferrals.
- `manifest.json`: selected count, change count, hashes and database target.

Missing/incomplete GraphQL responses are rejected, not interpreted as zero.
Unreadable individual fields are withheld while independently proven fields can
still be staged. A zero-change result with unresolved fields does **not** mean
the project fully matches Monday.

If Monday returns `INTERNAL_SERVER_ERROR` / `DOWNSTREAM_SERVICE_ERROR` during
Stage, the comparison has not completed and **this Stage command has made no
Supabase changes**. No apply or verification is needed for that failed staging
attempt. The run directory is created only when the fresh comparison is ready
to save, so an early API failure normally leaves no review artifacts.

The comparison transport reads 10 IDs per batch. It queries mirror linked-item
IDs, source-board IDs and column settings, then reads the referenced values in
separate exact-ID requests. It never requests nested `mirrored_value` fields.
Source values are joined locally using the returned IDs and column settings;
raw observations remain unchanged in the audit evidence. Transient GraphQL/HTTP
errors receive up to three attempts with
backoff. Persistent server/timeout errors can split that failed batch into smaller
exact-ID requests, down to one item, within a fixed request budget. A failing
singleton stops capture; it is never silently skipped. Rate limits honor server
retry hints and never trigger batch splitting; a requested cooldown over 30 s
stops the operation with instructions to wait before retrying. Permissions,
validation and certificate errors are not retried. No partial error response is
used as evidence. Progress shows the board role and batch; failures retain error
codes and the request ID when supplied by Monday.

This handling is local to the comparison script. It does not alter the shared
production client or financial projection rules. Monday documents that GraphQL
errors can accompany HTTP 200 and partial data; transport retries alone therefore
do not cover them ([Monday error handling](https://developer.monday.com/api-reference/docs/error-handling)).
An internal error alone does not establish whether the cause is a temporary
outage, a resolver failure on one item or the request's size/shape. If the new
bounded retries still fail, keep the reported IDs/codes/request ID for diagnosis.
Use the Stage command above with a **new** timestamped directory to retry after
the reported cooldown. Avoid applying a previously staged comparison after a
code change: its fingerprint deliberately requires a fresh Stage.

### Diagnosed nested-mirror failure on 4 October 2026

Live read-only probes for parent `1770216092` (project 16312) established:

- Item metadata, child membership, displayed values, mirror settings and linked
  child IDs all returned successfully.
- Expanding the parent order mirror through a nested `MirrorValue` returned
  `INTERNAL_SERVER_ERROR` / `DOWNSTREAM_SERVICE_ERROR`, even for one item and
  without requesting column settings.
- Reading the enquiry formula through the nested mirror returned `FORBIDDEN`.
  Direct reads of the sampled child formula and hidden-source values succeeded.
- The corrected direct-read capture succeeded for the parent, all nine current
  children and nine linked hidden sources, resolving order value **7570.64**.
  Its enquiry aggregate was withheld because the mirror lacked an explicit SUM
  setting. No Supabase connection or update was performed during diagnosis.

The successful capture is saved under
`outputs/order_value_backfill/diagnose_16312_20261004g/`. The earlier probes are
under `diagnose_16312_20261004b/` and `diagnose_16312_20261004c/`. This validates one
project's read path; it is not a completed comparison or verification of all 156.
The source column is read from the live mirror settings: for example, the tested
order mirror pointed to the material field, so the workflow must not substitute
a different formula or add charges on its own.

To reproduce a single-parent read without connecting to Supabase:

```powershell
$diagnosticDir = 'outputs/order_value_backfill/monday_diagnostic_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.diagnose_order_value_monday --project-id 1770216092 --output-dir $diagnosticDir --capture-only
```

The diagnostic module also offers individually selectable `--probe` queries for
isolating an API failure. Diagnostic partial error responses are saved only for
investigation and are never used to stage corrections.

## Apply the reviewed differences and verify

Keep the same `$runDir` value (or set it to the existing reviewed directory):

```powershell
& .\scripts\run_order_value_monday_compare.ps1 -Step ApplyAndVerify -RunDir $runDir
```

The underlying CLI also permits separate steps:

```powershell
$manifest = Get-Content -LiteralPath (Join-Path $runDir 'manifest.json') -Raw | ConvertFrom-Json
& .\report.venv\Scripts\python.exe -m scripts.order_value_monday_compare apply --run-dir $runDir --confirm-run-id $manifest.run_id
& .\report.venv\Scripts\python.exe -m scripts.order_value_monday_compare verify --run-dir $runDir
```

Apply re-reads each changed scope's Monday evidence before committing. Related
records commit together, up to 25 parents / 500 SQL rows, under brief table write
locks, a 750 ms lock timeout and a 10 s transaction timeout. No HTTP requests occur
inside the SQL transaction. Full SQL before-state checks protect against sync and
webhook changes; full after-state checks and the atomic journal detect unexpected
trigger effects and allow safe resume after a lost response. Hash checks protect
the reviewed artifacts and code. Scopes with no differences are never written or
journaled, but verification still checks their fresh Monday and SQL state.

The expected SQL after-state includes the database-generated `status_category`
using the exact CASE expression in `src/database/schema/schema.sql`:
`Won - Closed (Invoiced)` becomes `Won`, `Lost` becomes `Lost`, and every other
value (including NULL and `Won - Open (Order Received)`) becomes `Open`.
Matching is exact: no trimming, case folding or alternate status labels. The
workflow writes `pipeline_stage` only; PostgreSQL generates `status_category`.
The generated category remains part of the strict after-state and journal checks.

Runs staged before this generated-category fix may retain `Open` in their
expected after-state when Monday changes the pipeline to `Won - Closed (Invoiced)`.
Their affected transactions roll back with `Post-write values differ from
reviewed result`. Preserve those runs and receipts. The code fingerprint changes
with this fix, so stage a new run for only the uncommitted scopes' project IDs;
do not edit the old manifest or expected values to bypass validation.

For run `3ae1dbae-54fa-49bb-b545-9baa93ada075`, the locally prepared selection
`outputs/order_value_backfill/retry_uncommitted_3ae1dbae/projects.csv` contains
only the 50 projects from its two uncommitted scopes. `selection.json` beside
it records the source receipt hashes and the two corrected expected categories.
This selection file is not an apply plan. Obtain fresh evidence with:

```powershell
$runDir = 'outputs/order_value_backfill/monday_compare_retry_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\scripts\run_order_value_monday_compare.ps1 `
    -Step Stage `
    -RunDir $runDir `
    -Report 'outputs/order_value_backfill/retry_uncommitted_3ae1dbae/projects.csv'
```

Review the new manifest, changes and unresolved entries, then use `ApplyAndVerify`
with that new directory. The previously committed scopes retain their original
receipts and are not included in this retry selection.

Monday cannot participate in a PostgreSQL transaction. A second source read after
commit detects intervening Finance edits; it reports reassessment rather than
claiming success or undoing Finance's newer changes. Keep a quiet window where
practical. Application may partially commit; preserve the directory and inspect
the receipts before retrying. A retry skips journaled scopes; always verify it.

The wrapper always attempts verification after application, including after an
application error. Exit `0` means the mapped checks passed without outstanding
deferrals; `2` means unresolved fields, deferred scopes or failed verification;
`1` means the operation stopped. Inspect the latest `verify-*.json`, especially
`staged_changes_successful`, `counts` and `remaining_uncommitted`. The report never
certifies every column in the database.

## What is compared, and what remains separate

| Table | Fields compared |
|---|---|
| projects | Item name, project name, pipeline stage, order total, enquiry mirror, derived invoice total |
| subitems | Item name, representable parent/source links, material and additional charges from the explicit single hidden source, quote, invoice, invoice/order dates, mapped order status |
| hidden_items | Item name, material, additional charges, quote, invoice, invoice/order dates, status |

Parent order and enquiry values come from directly read typed source values,
following the mirror's exact linked IDs, source-column settings and configured
SUM aggregation. Unknown aggregation, formatted numbers, unreadable contributions
or cyclic/excessively deep references are explicitly deferred.
The code does not parse comma-separated mirror display text as a number: Monday
documents that text as a summary and provides `mirrored_items` references
([Monday mirror API](https://developer.monday.com/api-reference/reference/mirror)).
Duplicate links in Monday remain duplicate child contributions; the underlying
hidden row is updated once. Project 16312's 2,104.88 archived-status contribution
and 5,465.76 won contribution therefore remain 7,570.64 when Monday says so.

There is no configured parent invoice-total column. `total_amount_invoiced` is
explicitly a derived sum of the complete current Monday child invoice mirrors,
without status filtering. All-blank invoice values produce NULL. Explicit blank
source numbers stay NULL; explicit numeric zero stays zero. Empty parent mirrors
stay blank/NULL when their typed API evidence is blank; old SQL children are not
used to infer current totals.

This is an **update-only financial comparison**, not a replacement of the full
production sync. It reports and retains these cases for separate workflows:

- Missing Supabase rows: use reviewed rehydration. No partial-row inserts here.
- Actual API archived/deleted/unavailable items: lifecycle/retirement review.
  A business `Archived` status is different from API `state=archived`.
- Stored children absent from the current parent list: retain their history and
  report their fresh observed state; absence alone is not proof of deletion.
- Zero/multiple hidden links: do not guess a scalar relationship or material
  amount. Multi-link fidelity needs schema support; independent readable fields
  may still be corrected.
- Reparenting: compare both affected parents before moving the stored child.
- Sources with stored owners outside the selection: identify those owners for a
  further comparison; do not claim their denormalized values were refreshed.

The existing production sync/webhook writers are unchanged by this script.
In particular, persisted-child rollups and duplicate-source withholding can still
disagree with these authoritative current-parent totals until those production
paths are redesigned. Do not treat this one-off backfill as a guarantee of ongoing
replication. After Finance edits Monday, run a new Stage in a new directory to
capture current differences; reapplying an old plan deliberately cannot overwrite
the newer evidence. The same report IDs can be rechecked even after earlier
differences have been applied.
