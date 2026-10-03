# Missing-row rehydration for the 27 reviewed projects

The repair run `f2f8adbb-1a7f-4019-9867-6dd64c2fb0c4` committed all 20 scopes
covering 414 projects. Its fresh-source verification completed on 2 October 2026
at 22:31:22 BST with `complete: true`, 20 verified scopes and no remaining
uncommitted scopes. The historical `blocked_projects: 599` in that receipt is
original-capture context, not a new failure count. This success covers the selected
414 projects, not every project in Supabase.

The separate 27-project group had 47 missing `subitems` rows and two missing
`hidden_items` rows in the capture completed at 18:05:24 BST. No parent project
inserts were needed. The two missing hidden IDs were `2825748841` and `2825807081`,
both linked to children of project `2807630507`.

An offline preview using the new planner accepts all 27 in two scopes, containing
208 current children and 208 hidden sources. Their boundaries contain 425 and 18
rows after insertion. This is saved-snapshot validation only. Staging reads current
Monday and SQL evidence again, so actual insertion counts and eligibility can change.

## Relationship to production rehydration

`src/tasks/pipeline.py:rehydrate_projects_by_ids` calls
`RecentRehydrationManager._warm_hidden_cache_targeted` and `_rehydrate_batches`.
Production transforms/upserts hidden sources, transforms/upserts subitems, updates
projects and refreshes order/invoice rollups. Its hidden lookup uses name prefixes
and can fall back to scanning the hidden board; its writes span separate batches.

`scripts/order_value_rehydrate.py` uses the same production `DataSyncService`
hidden/subitem transformations and rollups through
`reconcile_order_values.transform_exact_rows`. For this reviewed correction it
requires the explicit Monday `connect_boards8__1` relationship and fetches sources
by ID. It does not enqueue jobs, run LLM analysis or write to Monday.

The script rehydrates the complete selected child/source sets, inserts only missing
hidden/subitem rows, repairs existing child source links/fields where required, and
refreshes six existing project fields: total order value, order date, invoice total,
enquiry total, first invoice date and last invoice date. PostgreSQL computes the
generated invoice spread. Other project metadata is not part of this correction.

## Run from the repository root

Stage current values without business-table writes:

```powershell
$runDir = 'outputs/order_value_backfill/rehydrate_27_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\scripts\run_order_value_rehydration.ps1 -Step Stage -RunDir $runDir
if (-not $?) { throw 'Staging failed or deferred projects; stop and inspect its output.' }
Get-Content -LiteralPath (Join-Path $runDir 'manifest.json')
Import-Csv -LiteralPath (Join-Path $runDir 'changes.csv') |
    Group-Object operation, table | Select-Object Name, Count
Write-Host "Review directory: $runDir"
```

The default projects file is `scripts/order_value_rehydrate_projects_27.txt`,
containing exactly the previously reviewed IDs. `-ProjectsFile` can select an
explicit subset for a later reassessment. The file contains IDs only: values and
membership always come from current Monday evidence.

Review `manifest.json` for project/scope/deferred counts and `insert_rows`.
Review `changes.csv` for individual insert fields, existing-row changes and parent
totals. CSV counts are field entries, not row counts; `insert_rows` gives row counts.
`scopes.json` contains complete raw Monday evidence, SQL before-state, expected
business fields, schema/default metadata and explicit deferral reasons.
Database-generated IDs/default timestamps are assigned at insertion.

After reviewing, run all pending scopes and fresh verification:

```powershell
& .\scripts\run_order_value_rehydration.ps1 `
    -Step ApplyAndVerify -RunDir $runDir -AllowRehydration
if (-not $?) { throw 'Apply or verification is incomplete; inspect the saved receipts.' }
```

`-AllowRehydration` acknowledges inserts, repair fields, partial scope commits and
brief table write locks. The runner requires no staging deferrals. It runs
verification even if apply returns a failure, because earlier scopes may have
committed. Success requires both exit codes to be zero.

Verification only:

```powershell
& .\scripts\run_order_value_rehydration.ps1 -Step Verify -RunDir $runDir
```

If using a new terminal, set `$runDir` to the existing run directory. Never generate
a new timestamp to resume an already staged run.

## Safeguards and read costs

- Monday is authoritative for values and explicit relationships. No source is
  selected by matching a project or subitem name. Inactive/missing metadata,
  ambiguous links, missing columns, inconsistent formulas and outside owners block
  the affected work. Missing parent rows and stale SQL children remain separate
  review cases. Existing children cannot be moved between parents by this script.
- Staging reads the selected parents, all their current children, their linked
  sources and targeted SQL boundaries. SQL includes incoming owners and old source
  keys. It does not traverse whole Supabase tables or all financial board records.
  Global Monday ownership is deliberately not certified at staging.
- Apply scans the narrow global subitem relationship inventory once, validates the
  four previously reviewed parentless duplicate exclusions, then refreshes each
  scope's full evidence before committing. Verification performs a second fresh
  relationship scan and fresh per-scope reads. The scans are per invocation, not
  per project. A quiet Monday window remains advisable: there is no atomic
  transaction spanning Monday and PostgreSQL.
- Each transaction contains at most 25 projects and 500 boundary rows after
  insertion. Related old/new source dependencies stay in the same transaction.
  It takes `SHARE ROW EXCLUSIVE` locks on `projects`, `hidden_items` and `subitems`
  because missing keys cannot be row-locked. These locks briefly delay writers to
  those tables; ordinary reads continue. Locks wait at most 750 ms, statements
  at most four seconds, and the transaction at most ten seconds. No Monday reads
  occur while the database locks are held. Busy scopes defer rather than waiting
  indefinitely.
- The transaction checks schema/defaults and the exact staged SQL before-state,
  inserts missing hidden rows then missing children, updates existing rows and
  aggregates, reconciles the result, and writes its journal entry atomically.
  Inserts have no conflict-overwrite clause. A concurrent creation after staging
  requires reassessment, even if its values appear compatible.
- Existing fields outside the planned updates must remain unchanged. New rows may
  receive database-generated defaults; the full resulting state is hashed into
  the existing `order_value_scope_commits` journal using mode `repair`. Verification
  checks that hash plus expected business values and fresh Monday evidence.
- Run IDs, target, code/mappings, plan and review hashes are checked. Historical
  order-only/repair workflows are unchanged and continue to reject missing rows.
  No deployment or new journal migration is required for this local script.

## If a run is incomplete

Keep its directory and inspect `apply-*.json`, `verify-*.json` and per-scope receipts.
A lock timeout can be retried with the same `ApplyAndVerify` command: the journal
skips committed scopes. A lost local receipt is also resolved from that journal.
Changed Monday values, SQL values/ownership or schema need fresh staging in a new
directory for the affected project IDs. Do not automatically force a stale plan
through, or interpret partial success as completion of all 27 projects.

The other 156 manual-review projects and 47 unlinked nonzero hidden sources from
the earlier assessment are outside this 27-project workflow. Counts reflect that
assessment, not a fresh global reassessment.

## Validation performed

39 new checks passed: 17 offline source/boundary/artifact checks, six PowerShell
runner checks and 16 real PostgreSQL transaction tests on an isolated loopback
server. Another 121 existing workflow regression checks passed. Tests cover atomic
rollback, concurrent creation/relinking, generated defaults, journal idempotency,
lost receipts, fresh source ownership changes and read-only staging. The final
planner also passed against the saved 27-project capture. These checks made no
production writes and performed no new live Monday or Supabase traversal.
