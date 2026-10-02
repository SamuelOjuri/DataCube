# Scoped reads and all-pending order corrections

The new entry point is `python -m scripts.order_value_scopes_targeted`.
It leaves the original `order_value_scopes` entry point and every source file in
its fingerprint unchanged. An apply/verify already started with the original
entry point should finish using that entry point and its existing run directory.
Do not switch the current pilot (`1770290281`) to the new command mid-run.

This implementation has offline regression coverage. Its new Monday page query
has not yet been exercised against the live boards. No live stage, preflight,
apply, verify, or diagnostic was run while developing it. The commands below are
for a later authorized window, after the current pilot finishes.

## What gets read

| Step | Monday | PostgreSQL |
| --- | --- | --- |
| Stage orders | Saved capture only, validated offline | Selected parents, selected children, old/new source keys, and every stored owner of those sources; one repeatable-read snapshot |
| Stage repair | Saved capture plus exact-ID repair inputs | The same dependency boundary, including the broader fields reviewed for repair |
| Apply | One complete **subitem relationship** scan per invocation; exact-ID financial and complete parent/child reads immediately before each scope | Schema, run journal, four reviewed exception boundaries, and the locked boundary for each scope |
| Verify | One new subitem relationship scan, then exact-ID reads for committed scopes | Journal, exception boundaries and each committed scope's expected after-state |

Neither apply nor verify calls the legacy full financial capture or full database
baseline reader. The remaining global scan reads only IDs, state, board/parent
metadata and the source-link column. It does not scan the parent board or hidden
financial board. IDs and source links arrive together in each page, avoiding a
second per-ID detail pass. SQL predicates restrict the rows requested; physical
query performance still depends on the database's indexes and query plans.

The four reviewed parentless duplicates retain their exact IDs, relationships,
zero order/invoice values, blank dates, absence from stored business tables and
exclusive retained owner checks. Their four parent/source records and all stored
owners are fetched by targeted predicates. New count anomalies, unknown
parentless items, unreadable relationships, duplicate pages, missing cursors and
incomplete responses stop the invocation before any new commits.

The scanner is complete for the active subitem inventory exposed by the same
Monday credentials. Its count checks retain the existing four-item anomaly
contract. Financial reads request known IDs with `exclude_nonactive: false` and
reject inactive or misplaced records. This does not certify archived/deleted
history or overcome API visibility restrictions.

## Stage and review a new run

Use a **new directory**. Old staged runs are deliberately rejected by this entry
point; new runs contain a separate workflow identifier and fingerprint covering
both new modules and the unchanged transaction implementation. Existing saved
backfill captures remain compatible because their fingerprinted files were not
changed. Loading one validates all its saved artifacts locally; it does not make
a new Monday traversal. Eligibility is from that capture, not a claim that all
projects are still eligible today.

For another small pilot:

```powershell
$runDir = 'outputs/order_value_backfill/targeted_pilot_NEW'
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes_targeted stage `
  --capture-dir outputs/order_value_backfill/reassess_20261001 `
  --run-dir $runDir --mode orders --project-id REVIEWED_PROJECT_ID
if ($LASTEXITCODE -ne 0) { throw 'Staging failed or deferred a scope; review the result.' }
```

For all verified order projects in the capture, replace `--project-id ...` with
`--all-verified` and use a different new directory. Stage all eligible projects
once and review `changes.csv`, `scopes.json` (including `deferred`) and
`manifest.json`. Already corrected rows are read in their current database state;
the new plan does not replay the old captured before-values. Unchanged projects
can remain in a scope so its full membership is checked. Source changes since
the capture still defer apply; they are never silently rebased.

Relationship repairs remain separate: use `--mode repair` and explicitly select
every project in the reviewed dependency group. No missing-row insertion,
arbitrary blocked-project inclusion, or automatic manual resolution is added.

## Apply every reviewed pending scope in one invocation

After reviewing the new artifacts, use the manifest's exact run ID:

```powershell
$runDir = 'outputs/order_value_backfill/targeted_orders_NEW'
$runId = (Get-Content -LiteralPath (Join-Path $runDir 'manifest.json') -Raw | ConvertFrom-Json).run_id
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes_targeted apply `
  --run-dir $runDir --confirm-run-id $runId --allow-partial --all-pending
$applyExit = $LASTEXITCODE

# Verify committed scopes even when some scopes deferred (apply exit code 2).
# A transport failure may also leave a committed journal entry to check.
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes_targeted verify --run-dir $runDir
$verifyExit = $LASTEXITCODE
Write-Host "Apply exit: $applyExit; verify exit: $verifyExit"
```

For a new pilot use `--limit 1` in place of `--all-pending`. These options are
mutually exclusive. `--limit` still defaults to 10 scopes and allows at most 100;
`--all-pending` explicitly removes the invocation cap. It does **not** increase
the 25-project/500-row scope limits or the ten-second transaction deadline.
`--scope-id` can further restrict the reviewed selection. Repair apply still
requires `--allow-repair-fields`.

The existing FK-protected lock order, before/after comparisons, field restrictions,
schema checks, timeouts and atomic journal are reused unchanged. Scopes commit
independently. A deferred scope does not prevent attempting the other selected
scopes. A connection failure stops execution; rerunning the same reviewed run
uses the database journal to skip existing commits, then verify again. No retries
restore old values over later writer changes.

One invocation can handle all staged eligible scopes during a quiet Monday
window. It is not one giant SQL transaction or an atomic snapshot spanning Monday
and PostgreSQL. Ownership is observed once per invocation; selected membership
and values are reread immediately before each transaction. Keep Monday source
editing quiet throughout apply **and** verify. A new external source owner arriving
after the ownership scan is detected by the next ownership verification, not
atomically prevented in Monday. Subsequent changes remain normal sync's job.

`ownership-apply-*.json` and `ownership-verify-*.json` preserve the observed links,
counts and observation times. Per-scope and summary receipts refer to that
evidence. `remaining_uncommitted` includes deferred scopes; `remaining_unattempted`
does not. Verification with no committed scopes performs no Monday reads.
An API failure leaves commits unverified; `complete` never certifies the whole
dataset or hides projects deferred at staging.

## Filtered-owner query diagnostic (later, read-only)

Monday documents `items_page` filtering on a Connect Boards column using
`any_of` with numeric linked item IDs:
[Connect Boards filtering](https://developer.monday.com/api-reference/reference/connect).
The implementation paginates this query and compares all returned owners and
metadata with the complete relationship scan for the staged sources **plus all
four exception sources**.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes_targeted check-owner-filter `
  --run-dir $runDir --output outputs/order_value_backfill/owner_filter_check_NEW.json
```

This diagnostic does make a fresh relationship traversal and extra filtered
Monday queries. It makes no database connection or business writes. It is **not
required** for the default apply workflow and has not been run during development.
A match is diagnostic evidence only: it does not enable a bypass or cache an old
owner list for later applies. Before making filtered reads authoritative, review
actual API-version behavior, pagination, visibility and exception coverage. The
default relationship scan remains in place until that live validation supports
a separately tested change.

## Offline tests

```powershell
& .\report.venv\Scripts\python.exe -m pytest `
  tests/test_order_value_scopes_targeted.py tests/test_order_value_scopes.py `
  tests/test_backfill_order_values.py tests/test_reconcile_order_values.py -q
```

The new test module forbids HTTP and database connections. It exercises paginated
ownership evidence, all four exclusions, drift, partial progress, over 100 scopes,
resume behavior and reviewed-artifact validation. The unchanged commit protocol
also has the existing disposable-local-PostgreSQL concurrency suite documented in
`order-value-scopes.md`; it is not needed to inspect or run the offline tests.
