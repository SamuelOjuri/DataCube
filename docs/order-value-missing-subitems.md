# Rehydrate the two missing subitems without changing existing rows

Use `scripts/run_order_value_missing_rehydration.ps1`. This separate workflow
inserts only the explicit subitems in `scripts/order_value_missing_subitems_2.json`:

| Project | Parent Monday ID | Subitem Monday ID | Hidden-source Monday ID |
|---|---|---|---|
| 14833 | 1772110252 | 2828149014 | 2727538504 |
| 18408 | 2964337986 | 3123480558 | 3121757431 |

These IDs select targets; fresh Monday evidence must still prove the parent
membership and exactly one matching hidden-source link. Names are not used to
infer relationships. Actual inactive/unavailable targets block staging; a
business status such as `Archived` does not exclude an API-active item.

## Stage

From the DataCube directory, with the normal environment configured:

```powershell
$runDir = 'outputs/order_value_backfill/rehydrate_missing_2_' +
    (Get-Date -Format 'yyyyMMdd_HHmmss')

& .\scripts\run_order_value_missing_rehydration.ps1 `
    -Step Stage `
    -RunDir $runDir

if (-not $?) { throw 'Staging failed; do not apply.' }
Get-Content -LiteralPath (Join-Path $runDir 'manifest.json')
```

Stage reads only the two parents' metadata/membership, the two named subitems'
production extraction columns, and the two explicit hidden sources. It does not
traverse boards or request parent financial mirrors. SQL reads are restricted to
the two parent/source keys, target subitems, and stored children/owners associated
with those keys. No application rows, schema, or journal entries are written.

Review `manifest.json`, `changes.csv`, and `inserts.json`. If both subitems are
still missing, expect two subitem inserts, zero parent/hidden inserts, zero updates,
and one atomic scope. Changes.csv counts inserted fields, not row updates.
Database defaults (UUID and timestamps) are supplied by PostgreSQL.

Already-present targets with matching mapped values are reported as no-ops.
Already-present targets with different values block staging for a separate
comparison; they are never overwritten. Missing parent or hidden-source rows also
block this workflow rather than creating additional rows.

## Apply and verify

After reviewing the new insertion plan, use the same `$runDir`:

```powershell
& .\scripts\run_order_value_missing_rehydration.ps1 `
    -Step ApplyAndVerify `
    -RunDir $runDir `
    -AllowRehydration
```

Production hidden/subitem transformations populate the new rows. No production
parent rollup method is called. Material and additional charges are resolved from
the exact hidden source; quote, invoiced amount, enquiry formula, order status and
invoice/order dates use the comparison workflow's typed source resolution.
Blank financial values stay NULL and numeric values use the captured SQL scale.
Other fields use normal production extraction and metadata transformations.

All existing scoped rows must remain unchanged, including parent totals and
the generated `status_category`. PostgreSQL retains its exact generated-column
definition; this command never writes a generated category or changes the schema.
The older `run_order_value_rehydration.ps1` remains a different workflow that also
updates existing rows and recalculates parent totals. Do not interchange runners.

Both inserts commit together under the existing 500-row boundary limit, a 750 ms
lock timeout, 4 s statement timeout, and 10 s transaction timeout. Brief table
write locks protect target absence and concurrent relinks. Reads remain available.
There are no HTTP requests inside the SQL transaction. Unexpected trigger effects
on existing scoped rows roll back the entire insertion transaction. The commit
journal is written atomically with the inserts. No upserts or conflict-ignore
clauses can silently replace or skip a newly created row.

Source evidence is compared again before and after committing. Finance changes
after commit are reported for reassessment; a committed transaction is not
reported as rolled back. A retry skips journaled inserts and the wrapper always
attempts verification after apply. Keep the original run and receipts.

## Check the result

```powershell
$verifyFile = Get-ChildItem -LiteralPath $runDir -Filter 'verify-*.json' |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
if (-not $verifyFile) { throw 'No verification summary; inspect output and receipts.' }
$verification = Get-Content -LiteralPath $verifyFile.FullName -Raw | ConvertFrom-Json
$verification | Select-Object staged_changes_successful, remaining_uncommitted | Format-List
$verification.counts | Format-List
```

Expect `staged_changes_successful: True`, `remaining_uncommitted: 0`, and
`verified: 1` when inserts were staged. If both targets already matched at Stage,
the status is `verified_no_changes: 1` and no commit journal is created.
Unlike the broader comparison run, exit `2` here means this insertion run needs
reassessment, not merely that the earlier 196 review entries remain outstanding.

To rerun verification without applying:

```powershell
& .\scripts\run_order_value_missing_rehydration.ps1 -Step Verify -RunDir $runDir
```

This certifies the selected insertions and existing rows within their read
boundary, not the entire database or the remaining mirror/lifecycle review cases.
