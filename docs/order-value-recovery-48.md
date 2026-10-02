# Recovery of the 48 projects identified on 2 October 2026

Monday CRM supplies the proposed order values. The selection is fixed in
`docs/order-value-recovery-48-projects.txt`; no amounts are hardcoded. Supabase
supplies the current before-values and stored relationships used for concurrency
checks. The supplied afternoon snapshots suggested 502 field changes; the actual
count is recalculated from fresh reads and can differ, including zero.

The new read-only stager, `scripts.order_value_scopes_refresh`, validates the
previous targeted run, reads current Monday parents/children/source inputs, and
writes a **new** standard targeted run. It can refresh changed source amounts and
new child membership when the corresponding database rows and links already
exist. Missing rows, conflicting relationships, duplicate sources, invalid
formulas, and empty parents remain blocked. It changes only the two order
components in hidden sources/subitems and parent total order values when applied.

The previous run and the original stage/apply/verify source files remain intact.
The existing targeted loader validates the new plan, including arithmetic and
field restrictions. The helper's hash is saved as staging provenance; apply and
verify depend on the unchanged targeted workflow fingerprint and never execute
the refresh helper. Raw Monday evidence is preserved in `monday-refresh.json`;
the reviewed canonical source evidence is inside fingerprinted `scopes.json`.

## Commands

Run from the repository root in PowerShell with the usual `.env` credentials,
including `SUPABASE_DB_URL` and the Monday credentials used by the existing client.
Keep Monday source editing quiet while applying and verifying.

To stage, apply all selected scopes, and verify in one invocation:

```powershell
$runDir = 'outputs/order_value_backfill/recovery_48_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\scripts\run_order_value_recovery_48.ps1 -RunDir $runDir -Step All
```

**`-Step All` writes the freshly staged order corrections to Supabase.** The
wrapper proceeds only after successful staging of all 48 selected projects with
zero deferrals. It uses one apply invocation for all pending scopes, then runs
verification even when apply returns an error or a partial result. It does not
ask for an interactive confirmation. Stage uses a new directory and will refuse
to replace an existing one.

For a separate review before application:

```powershell
$runDir = 'outputs/order_value_backfill/recovery_48_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\scripts\run_order_value_recovery_48.ps1 -RunDir $runDir -Step Stage
if (-not $?) { throw 'Staging did not succeed.' }

# Review changes.csv and scopes.json in $runDir, then run:
& .\scripts\run_order_value_recovery_48.ps1 -RunDir $runDir -Step ApplyAndVerify
```

To verify again without applying:

```powershell
& .\scripts\run_order_value_recovery_48.ps1 -RunDir $runDir -Step Verify
```

After a connection failure, use **the same recovery directory** with
`ApplyAndVerify` to resume. The database journal skips committed scopes. Source
or database drift that caused a deferral requires reassessment/fresh staging;
resume is not a bypass for changed evidence. Run `All` only when preparing a new
run. Save the displayed directory if you need to resume in another terminal.

## Read and write boundaries

- Stage: exact-ID reads for 48 Monday projects, all their children and their
  linked financial sources; one repeatable-read SQL snapshot with selected rows
  and all stored owners. No whole-board or whole-table staging traversal.
- Apply: the existing single board-wide **subitem relationship** inventory,
  the four reviewed duplicate-exception checks, fresh exact-ID scope checks,
  then bounded transactions. No full financial-board capture is added.
- Verify: one new relationship inventory and fresh selected Monday/SQL reads.

Staging's expected source owners are the selected children, not a certification
of global ownership. Apply independently proves that the complete relationship
inventory has exactly those owners before attempting any scope's transaction.
A newly discovered outside owner defers the scope. The existing 25-project /
500-row limits, FK locks, before-state checks, timeouts and journal remain in
force. The 48 projects normally fit into two scopes; new child counts can alter
that grouping. Scopes commit independently, so a later deferral can leave earlier
scopes committed and awaiting final verification.

Success means the final verification receipt has `complete: true` and all 48
selected projects are covered. It does not certify other historical blockers or
guarantee that Monday will remain unchanged after the checks.

## Offline validation

```powershell
& .\report.venv\Scripts\python.exe -m pytest `
  tests/test_order_value_recovery_runner.py tests/test_order_value_scopes_refresh.py `
  tests/test_order_value_scopes_targeted.py `
  tests/test_order_value_scopes.py tests/test_backfill_order_values.py `
  tests/test_reconcile_order_values.py -q
```

Tests forbid live connections. Developing this recovery workflow does not run
the commands against Monday or Supabase.
