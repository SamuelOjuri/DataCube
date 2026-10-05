# Follow-up cleanup: audit the completed run and recover two old IDs

The original 31 deletions are complete. Projects 10306, 18466 and 17529 had
broader refreshes, so audit these without rerunning the deletions or
automatically restoring old values. The historical audit has before-snapshots
but no exact after-snapshots. A current difference may include later edits.
New worker writes now record both after-snapshots and field changes.

The separate `linked2` batch selects only these old IDs:

| Project | Parent Monday ID | Old ID to remove from DataCube | Current ID to preserve |
|---|---|---|---|
| 18326 — Cospace Harlow | 2915886260 | 2916255740 | 2916256531 |
| 15957 — 119 High Street Southampton | 1771987887 | 2902522882 | 2902535739 |

Their deletion events lack parent fields. This batch requires the exact old
ID's creation event to establish its parent, an explicit later deletion,
complete available activity without intervening lifecycle ambiguity, and
fresh parent membership checks. Both events are fingerprinted and checked
again by the worker. Names never establish identity. A missing/replaced
surviving ID, returned old ID, changed stored row or ambiguous history defers
the operation. Five other unconfirmed missing IDs remain excluded.

For parents whose generated `status_category` is `Open`, sum the New Enquiry
Value of current children whose API lifecycle state is `active`. Visible
Archived/Won Closed/Lost labels do not filter children. Won/Lost parents keep
their existing enquiry values. The fresh Monday pipeline classification must
agree with the stored generated category. This is the requested business rule;
Monday's hidden mirror aggregation setting has not been proven.

## Read-only audit and preview

Run these from the DataCube directory with the existing environment settings.
They read Monday/PostgreSQL and write only local review artifacts.

```powershell
$auditRun = 'outputs/monday_lifecycle/scope_audit_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup audit `
    --run-dir outputs/monday_lifecycle/review31_20261005_active_open_preview `
    --output-dir $auditRun

$cleanupRun = 'outputs/monday_lifecycle/linked2_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup stage `
    --batch linked2 --run-dir $cleanupRun
if ($LASTEXITCODE -ne 0) { throw 'Review deferred reasons before proceeding.' }
Import-Csv -LiteralPath (Join-Path $cleanupRun 'review.csv') | Format-Table
$cleanupManifest = Get-Content -Raw -LiteralPath (Join-Path $cleanupRun 'manifest.json') | ConvertFrom-Json
```

The audit writes `audit.json` with scope violations and before/current
differences, plus a short `audit.md`. A successful audit command means the
report was produced; inspect `scope_compliant` for the assessment. The preview
writes `review.csv`, `plan.json`, per-deletion evidence and an integrity manifest.
Both deletions and both parent previews must be ready. The manifest separately
reports whether the database guard is installed; read-only staging can run
before installation, but queueing cannot.

## Install the guard before any new cleanup writes

Apply `src/database/schema/monday_lifecycle_scoped_cleanup.sql` in the Supabase
SQL Editor, after the existing `monday_lifecycle.sql` migration. It adds an
idempotent trigger/function, with bounded lock and statement waits; it does
not alter business rows or requeue historical jobs. This migration is not
automatically installed by the CLI.

Deploy these code changes and restart every lifecycle worker, including any
remote/background application workers. Newly scoped jobs require the worker
protocol set by the updated `claim()` function. Older workers are rejected
when they try to claim them; leaving old workers running can stall queue work.

The database carries the enquiry-only policy from the root deletion to all
follow-ups even if an enqueue call omits it, rejects a conflicting refresh
mode and rejects targets outside the reviewed parent/old subitem. Existing
unscoped lifecycle jobs keep their existing behavior. Scope is not retrofitted
onto already processed historical jobs.

## Queue and process the reviewed two-ID run

These commands write to DataCube. Monday remains read-only. Use the same
reviewed `$cleanupRun` and manifest after installing the guard and restarting
workers. Code or artifact changes require a new stage.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup queue `
    --run-dir $cleanupRun --confirm-run-id $cleanupManifest.run_id
if ($LASTEXITCODE -ne 0) { throw 'Queueing failed; inspect the error.' }
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup process --run-dir $cleanupRun
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup status --run-dir $cleanupRun
```

The queue transaction covers both roots. Each worker rechecks its evidence
before deletion and writes a tombstone/audit trail, followed by enquiry-only
refresh and verification. The same-name current subitems and hidden items
are preserved. `process` is restricted to this run's jobs. A returned old ID
goes to review instead of triggering a broader restore within this cleanup.

`status.json` separates `jobs_complete` and `scope_compliant`. `complete` is
true only when both are true. Status/process exit 2 when work remains or an
audit violation needs review. Do not treat completed deletions as authority
to roll back other fields or rerun an already completed batch.
