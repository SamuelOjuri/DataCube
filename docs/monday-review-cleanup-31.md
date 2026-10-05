# Cleanup of 31 confirmed deleted subitems

**Revised rule (5 October 2026):** the user clarified that eligibility means
the subitem's **API lifecycle state is `active`** and its **parent project's
`status_category` is `Open`**. The visible business Status is not the active
filter. Earlier all-parent previews are superseded and must be staged again.
This is the user's requested calculation, not proof of the parent mirror's
undocumented aggregation setting. No cleanup has been applied.

`python -m scripts.monday_review_cleanup` stages and queues the 31 exact
subitem IDs in `scripts/monday_review_cleanup_31_targets.json`. These belong to
27 projects from the 5 October 2026 read-only investigation. The two deletion
events without parent details and the five unconfirmed missing records are
excluded. Monday is always read-only.

For Open parents, **New Enq Value = SUM of New Enquiry Value for current
API-active subitems**. Won/Lost parents are explicitly skipped and keep their
existing enquiry value; they are not zeroed. Deletion cleanup still covers the
31 confirmed deleted IDs, including IDs belonging to non-Open parents.

Parent category is derived from the fresh Monday pipeline stage with the exact
SQL CASE: `Won - Closed (Invoiced)` -> `Won`, `Lost` -> `Lost`, everything else
(including NULL) -> `Open`. The targeted workflow requires this to agree with
the stored generated `status_category`; a mismatch defers the parent for stage
reconciliation. The category is never written directly.

The calculation reads each eligible child's `formula_mkqa31kh` value. Stored
extras, extra mirror links and API-archived/deleted children are excluded.
Business status labels such as Archived do not exclude API-active children.
Blank typed values and an empty eligible membership sum to zero. Missing,
moved, duplicate, unknown-state or unreadable evidence withholds the total.
Decimal arithmetic is used and the result is rounded once to database precision.

This is a field-specific business rule, not a general assumption that every
Monday mirror uses SUM. Generic mirrors retain their existing strict checks.
See the [Monday mirror API](https://developer.monday.com/api-reference/reference/mirror)
for the distinction between mirror settings, display text and typed contributions.

## Prepare a read-only review

Use the existing `MONDAY_API_KEY` and `SUPABASE_DB_URL` configuration. PostgreSQL
17+, the existing lifecycle migration, enabled deletion guards and validated
foreign keys are checked before staging. No migration is installed automatically.

```powershell
$cleanupRun = 'outputs/monday_lifecycle/review31_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup stage --run-dir $cleanupRun
if ($LASTEXITCODE -ne 0) { throw 'Staging incomplete; inspect the saved review and deferred reasons.' }
Import-Csv -LiteralPath (Join-Path $cleanupRun 'review.csv') | Format-Table
$cleanupManifest = Get-Content -Raw -LiteralPath (Join-Path $cleanupRun 'manifest.json') | ConvertFrom-Json
```

Staging writes only local files. It runs the existing single-subitem activity
recovery checks independently for each target: exact deletion event, matching
parent/boards, exhaustive available later activity, and current membership
before and after the history read. A returned active/archived item, later
restore/move, missing event, changed parent, unavailable board or incomplete
history defers the deletion. It also evaluates eligibility and previews values
for all 27 parents, including explicit unchanged outcomes for Won/Lost parents.

A later parent `Subitems` change is allowed only when its typed before/after
membership explicitly removes the deleted ID, adds no IDs, and references that
ID solely in the previous membership. This records the deletion being reflected
on the parent; it is not a restoration. The complete event remains in the audit.
Metadata-only requests skip column values explicitly, since Monday interprets
an empty column-ID list as all columns.

Artifacts include a consolidated `review.csv`, before/after enquiry totals and
raw evidence in `plan.json`, child recovery artifacts under `deletions/<id>`,
and an integrity manifest. All 31 deletions and 27 enquiry previews must be
ready before queueing; there is no automatic partial-cleanup option. Source
read failures stop staging; preserve the partial folder and use a new folder
for a fresh run.

## Queue and process the reviewed plan

These commands **write to DataCube**. Deploy the updated lifecycle worker and
comparison code before queueing if a remote lifecycle worker is enabled. An
enabled worker may process a newly queued job immediately. The updated worker
understands the enquiry-only refresh mode used by this batch.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup queue `
    --run-dir $cleanupRun --confirm-run-id $cleanupManifest.run_id
if ($LASTEXITCODE -ne 0) { throw 'Queueing failed; inspect the error.' }

& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup process --run-dir $cleanupRun
& .\report.venv\Scripts\python.exe -m scripts.monday_review_cleanup status --run-dir $cleanupRun
```

Queueing rechecks the reviewed totals and database baseline, then queues all
31 root jobs in one transaction. It does not report deletion as completed.
`process` claims only this run's jobs and their follow-ups; it does not process
unrelated pending work or schedule unrelated tombstone checks. It defaults to
500 job attempts, with a maximum of 1,000 per invocation. If a worker has a job
in progress or a retry is waiting, run `process`/`status` again later.

Each worker deletion revalidates the saved event and stored row, records an
audit snapshot, deletes only the selected subitem, installs the existing
stale-write guard, and queues source verification. The batch's parent refresh
updates **only `projects.new_enquiry_value`** for eligible Open parents from
fresh API-active child evidence; Won/Lost parents are recorded as skipped;
parent identity, other parent fields, siblings and hidden sources are retained.
Changed enquiry values get a durable verification refresh. Other lifecycle
jobs continue to use their existing full refresh behavior.

`status.json` lists each job, outstanding records and current parent values.
Completion requires all 31 deletions processed and verified, all 27 parent
refreshes successful (including explicit non-Open skips), no selected subitems
left, and no pending/retry/review
jobs in this run. Exit code 2 means incomplete/review required, not success.
Individual review reasons remain visible; a successful deletion does not hide
a failed enquiry refresh. Queueing the same intact run again is idempotent.

Current Monday facts can change after staging. The worker recomputes totals
from fresh evidence and does not force the old preview onto newer Finance data.
Code or reviewed-artifact changes require a new stage. Do not hand-edit the
allowlist or plan to expand the selection or bypass an evidence failure.

The original 96-project review CSVs are historic snapshots and are not edited
by this script. A later comparison will reflect completed cleanup and the
confirmed enquiry calculation rule.
