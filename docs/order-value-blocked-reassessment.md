# Current Monday reassessment of historical order blockers

Monday CRM is the source for relationships and financial inputs. Historical
captures select the projects to revisit; their old amounts are never applied as
current values. Supabase supplies only the current before-state, stored owner
dependencies and schema constraints. The existing apply/verify implementation
and its code fingerprints remain unchanged.

## Capture and stage

`scripts.order_value_blocked_capture` is read-only. It validates the historical
capture, reads one complete Monday subitem relationship inventory, rechecks the
four specific excluded duplicates, then requests current details for all formerly
blocked parents and their relevant children/hidden sources. It also revisits
previously unlinked nonzero sources. Two scoped SQL snapshots discover dependencies
and capture the current full before-state; no complete Supabase table traversal
or business write is performed.

```powershell
$captureDir = 'outputs/order_value_backfill/blocked_current_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
$reviewDir = $captureDir + '_review'

& .\report.venv\Scripts\python.exe -m scripts.order_value_blocked_capture `
    --previous-capture outputs/order_value_backfill/reassess_20261001 `
    --output-dir $captureDir
if ($LASTEXITCODE -ne 0) { throw 'Current evidence capture is incomplete.' }

& .\report.venv\Scripts\python.exe -m scripts.order_value_blocked_review `
    --capture-dir $captureDir --output-dir $reviewDir
if ($LASTEXITCODE -ne 0) { throw 'Reassessment/staging failed.' }
```

The second command is offline. It checks evidence hashes, metadata, formula
arithmetic, complete parent membership and observed source ownership, then stages
eligible groups in the existing targeted run format. Dependent projects sharing
old or new stored sources remain together; independent groups are packed within
the existing 25-project/500-row limits. Apply revalidates each plan using its
original loader and fetches current evidence before every transaction.

`context.json` records the initial request; `completed-context.json` records a
finished capture. If all capture files were saved but the final receipt failed,
rerunning the same capture command can finish that receipt with a read-only schema
check and no repeated Monday traversal. Other partial captures require a new
directory. A finished capture is never overwritten.

## Review the resulting scope

- `summary.json`: current outcome counts and the staged run IDs/hashes.
- `projects.csv`: disposition of every historical blocked project.
- `unresolved-projects.csv`: precise reasons a project cannot be corrected by the
  existing update-only transaction protocol.
- `orders-run/changes.csv`: only the two order components and parent order total.
- `repair-run/changes.csv`: explicit source links and the full related hidden/
  child fields and parent financial/date rollups produced by the existing repair
  transformation. Repair can refresh invoice/enquiry values, dates, descriptions
  and other mapped source fields as well as order values; review this wider scope.
- `unlinked-nonzero-sources.csv`: observed nonzero sources without an active owner
  in the reviewed source population. Names do not assign them to a project.

Each ready group must have current, readable and unambiguous Monday evidence,
existing referenced database rows, matching child parents and a safe final owner
set. Missing rows remain separate rehydration cases; this script does not insert
them, delete historical rows, move children, allocate a shared source or infer
zero for an empty/unavailable parent. Archived and unavailable projects are
reported with their observed Monday states. A non-returned item is not asserted
to be deleted.

The four parentless duplicates remain excluded only if the current counterpart,
zero order/invoice/date conditions, database absence and exclusive retained owner
checks all pass. Their continued presence is evidence to report, not authorization
to insert them into Supabase.

## Apply and verify reviewed eligible corrections

Use the review directory produced above. During a quiet Monday window:

```powershell
& .\scripts\run_order_value_blocked_updates.ps1 `
    -ReviewDir $reviewDir -Step ApplyAndVerify -AllowRepairFields
```

This writes to Supabase. It selects every pending scope in each staged mode and
always attempts verification afterward, including after partial/failed applies.
The `-AllowRepairFields` switch acknowledges the wider repair CSV described above.
The final success message covers the staged eligible projects; unresolved cases
remain explicitly outside those runs. `verify` receipts provide the actual result.

Apply and verification each retain one global relationship scan per mode/run,
the exception checks and exact-ID financial reads. They reuse the existing
FK-backed row locks, schema and before-state checks, bounded transactions,
rollback reconciliation and atomic commit journal. An unexpected change defers
a scope. No API calls occur while its database transaction holds write locks.

After a connection failure, rerun the same command against the same review
directory. The journal skips committed scopes. To verify without writes:

```powershell
& .\scripts\run_order_value_blocked_updates.ps1 -ReviewDir $reviewDir -Step Verify
```

New source/database drift needs a new capture and reviewed plan; resume does not
rebase values or bypass checks. Do not rerun the completed 48-project recovery.
