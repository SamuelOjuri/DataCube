# Monday deletion and restoration replication

Finance owns Monday data. Deleting a Monday item now has a durable, exact-ID path
to deleting its Supabase counterpart. This release includes code and a migration;
deploying Python alone does not enable the feature.

## Deployment order

1. Apply `src/database/schema/monday_lifecycle.sql` in Supabase SQL Editor as the
   database owner. It creates the durable inbox, audit history, lifecycle markers,
   indexes and write guards. It does **not** delete existing business records or
   change generated columns. Existing immediate foreign keys must provide project
   CASCADE and hidden-source SET NULL behavior. PostgreSQL 17+ is required.
2. Configure Render's environment with `SUPABASE_DB_URL` (a privileged PostgreSQL
   connection, supplied as a secret) and `MONDAY_LIFECYCLE_ENABLED=true`. Retain the
   existing Monday API, Supabase service key and webhook authentication settings.
   Run migrations before rolling out the enabled code. The worker requires the
   PostgreSQL URL in addition to the Supabase REST URL/service key.
3. Deploy this code. The worker starts with either `src.webhooks.webhook_server:app`
   or `src.api.app:app`. Multiple processes can run workers: leases and transaction
   checks prevent an expired worker from committing. Follow-up jobs survive restarts.
4. Set the local `MONDAY_WEBHOOK_URL` to the deployed endpoint, then register the
   subscriptions below. Subscriptions recorded by this setup command are reused. Errors now
   produce a nonzero exit instead of a success-looking partial setup.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.setup_webhooks `
    --boards 1825117125 --events item_deleted subitem_deleted item_restored
if ($LASTEXITCODE -ne 0) { throw 'Project webhook registration failed.' }

& .\report.venv\Scripts\python.exe -m scripts.setup_webhooks `
    --boards 1825138260 --events item_deleted item_restored
if ($LASTEXITCODE -ne 0) { throw 'Hidden-source webhook registration failed.' }
```

Monday registers subitem events on the **parent** board. A subitem delivery must
identify its actual subitem board; conflicting IDs or unexpected boards fail
closed. See [Monday webhook documentation](https://developer.monday.com/api-reference/reference/webhooks).

Keep `outputs/monday_webhooks/registry.json` (or pass `--registry-file`). The
documented webhook read fields omit the destination URL, so the setup command
records which subscription IDs it created for your URL and checks that those IDs
still exist before reusing them. On the first run without this registry it creates
its own subscriptions, even if another same-event subscription exists. It never
deletes an existing subscription. Concurrent/redundant deliveries remain safe.

The receiver supports `delete_pulse`, `delete_item`, `item_deleted`,
`delete_subitem`, `subitem_deleted` and restoration aliases. It acknowledges a
deletion/restoration only after storing the event. Failed storage returns HTTP 503.
The ordinary create/change webhook path remains separate. Before enablement,
lifecycle requests return 503 rather than use the former blind-delete path.

Provider retries are finite: a deletion never delivered during a prolonged outage
has no durable inbox entry. Use targeted historical comparison/staging for such
missed deletions; the daily marker check covers already known deletions/restores.

## Processing and safeguards

- The default path requires fresh, successfully returned `state=deleted` evidence
  on the expected board. An explicit single-subitem activity-log recovery is also
  available below. Missing results alone, API errors, moved items, and API archives
  never authorize deletion. The business Status label `Archived` is not a deletion signal.
- Delete transactions contain marker changes, before-row audit history, actual
  deletion, durable refresh jobs and a durable post-deletion source verification.
  They use 750 ms lock, 4 second statement and 10 second transaction timeouts.
  Table write locks last only for the bounded SQL transaction; no HTTP runs there.
- A deleted project cascades to stored subitems only after **every affected child**
  is independently confirmed deleted in Monday. Moved or unreadable children stop
  the operation for review. Independently existing hidden sources are retained.
- A deleted subitem leaves its parent, siblings and hidden source intact. A deleted
  hidden source leaves its owners intact with null links; every exact stored owner
  is queued for a fresh project comparison. Surviving rows are checked in-transaction.
- Triggers on all three core tables prevent stale inserts/upserts from recreating
  a deleted row or referencing a deleted parent/source. Other valid rows in an
  import batch continue. No importer can clear a deletion marker implicitly.
- Current active Monday evidence allows restoration. Markers are cleared in the
  same transaction that writes freshly rehydrated rows, using production metadata
  extraction with all name/prefix fallback caches disabled. Generated fields,
  including `status_category`, are left to the database's defined expressions.
- Refresh reads one current parent, its exact children and mirror/source IDs.
  Order total follows the parent's actual mirror; invoice total follows its current
  Monday children. It does not sum stale Supabase children or discard amounts based
  on business status. Blank source links clear the relation and hidden-only amounts.
- Rehydration captures current source evidence twice before writing and compares
  SQL before-state under lock. Representable differences are checked after writing.
  A durable follow-up compares again; continuing drift is reported for review.
  Unsupported mirror aggregation and multi-source scalar links remain explicit
  issues, rather than guessed values. Refresh `review` does not undo a successfully
  committed deletion: inspect the deletion and refresh job results separately.
- Each operation is capped at 500 related rows and the Monday transport has a
  request/time budget. Worker errors retry with backoff, then remain in `review`
  after 12 attempts. An interrupted claim can be reclaimed after its 20-minute
  lease. The worker checks the lease token before writing.
- When idle, the worker schedules at most ten indexed tombstones for rechecking;
  each marker is checked at most once per day. This recovers missed restoration
  events, including subitem restorations, without scanning business tables/boards.

Monday and PostgreSQL cannot share one transaction. A Finance edit during the
small interval after a source check is handled by verification and subsequent
events. The feature protects deleted IDs across all writers; it does not replace
every existing sync field mapping or promise that an unrelated writer cannot
subsequently change an ordinary value. Repeated differences appear in job review.

## Historical recovery: project 18824

The known candidate is deleted subitem `3242789524`, not active same-name subitem
`3242816565`. Stage only the candidate below. Stage is read-only for both systems.

```powershell
$runDir = 'outputs/monday_lifecycle/18824_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle stage `
    --board 1825117144 --item-id 3242789524 --run-dir $runDir
if ($LASTEXITCODE -ne 0) { throw 'Lifecycle staging failed.' }
Get-Content -LiteralPath (Join-Path $runDir 'manifest.json')
Import-Csv -LiteralPath (Join-Path $runDir 'review.csv') | Format-Table
```

After reviewing the staged ID and dependencies, enqueue it for the worker. This
command writes an event, and the enabled worker will apply the confirmed deletion.
The worker rechecks current Monday evidence, including any dependent children;
the stage file alone never authorizes an unchecked delete.

```powershell
$manifest = Get-Content -LiteralPath (Join-Path $runDir 'manifest.json') -Raw | ConvertFrom-Json
if ($manifest.selected -lt 1) { throw 'No confirmed deletions were staged.' }
& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle queue `
    --run-dir $runDir --confirm-run-id $manifest.run_id
if ($LASTEXITCODE -ne 0) { throw 'Lifecycle queueing failed.' }

& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle status --run-id $manifest.run_id
```

`status --run-id` also includes follow-ups whose keys descend from the recovery
event. Inspect `processed`, `review` and `retry` separately; an empty queue view
or a successful enqueue is not evidence that deletion verification has completed.

To stage only confirmed-deletion candidates from the saved review report:

```powershell
$runDir = 'outputs/monday_lifecycle/review_deleted_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle stage `
    --review-csv outputs/order_value_backfill/manual_review_96_20261005/review_issues_176.csv `
    --run-dir $runDir
```

Only reasons containing `state=deleted` are selected; archives and unavailable
items remain excluded from deletion. Each selected ID receives a fresh Monday
check. Review `plan.json` for deferred IDs, then use the same queue/status steps.

## Activity-log recovery: missing subitem in project 18747

Use this explicit exception only for one stored subitem whose exact deletion
event can still be read from Monday. It does not apply to projects, hidden sources,
bulk CSV selections, or items currently returned by Monday. The ordinary staging
and webhook paths never infer deletion from absence or automatically enable it.

For subitem `3201675663` (`18747_26.01 - A-FB`), the known deletion event is
`c544b619-f673-463c-87df-fec7efc46d2d`, recorded on 2 September 2026 at 14:28:01 UTC.
The parent is `3199168336`. Active sibling `3200638626` is a separate record.
Run from the repository root, with the existing Monday and PostgreSQL credentials:

```powershell
$activityRunDir = 'outputs/monday_lifecycle/18747_activity_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle stage `
    --board 1825117144 --item-id 3201675663 --parent-id 3199168336 `
    --activity-log-id c544b619-f673-463c-87df-fec7efc46d2d `
    --activity-log-from '2026-09-01T00:00:00Z' --run-dir $activityRunDir
if ($LASTEXITCODE -ne 0) { throw 'Activity recovery staging failed.' }
Get-Content -LiteralPath (Join-Path $activityRunDir 'manifest.json')
Import-Csv -LiteralPath (Join-Path $activityRunDir 'review.csv') | Format-Table
```

Stage is read-only for both remote systems. A successful selection creates:

- `review.csv`: exact subitem and parent IDs, evidence basis, deletion event ID
  and UTC timestamp, and the number of stored rows to delete.
- `plan.json`: the complete stored row, the deletion event, fresh activity history,
  and the parent membership checks made before and after reading that history.
- `manifest.json`: target database identity, code/artifact fingerprints, run ID,
  and selected/deferred counts. `selected=1, deferred=0` means eligible for review,
  not that anything has been queued or deleted.

The reader exhausts item-filtered activity pages on both the subitem and parent
boards from the supplied start through the current check. It requires accessible,
active boards, an active parent on the expected board, and absence of the selected
ID from both direct reads and the parent's complete current child list. The exact
deletion must identify the selected item, subitem board, parent and parent board.
Any later or simultaneous activity for the subitem stops recovery, including
unknown event types. Later parent lifecycle or unfamiliar actions also stop it;
ordinary parent field/name edits and subscriptions are allowed. API/permission
errors, missing events, malformed data, inconsistent/duplicate pagination, changed
membership and the ten-page-per-board limit all fail closed.

Activity logs are queried using the documented [board activity log API](https://developer.monday.com/api-reference/reference/activity-logs).
They cover available history on these two boards, not a global account-wide audit.
There is no atomic transaction shared by Monday and PostgreSQL. Incomplete history
or an inaccessible source requires manual investigation, not a forced deletion.

After reviewing the single-row plan, deploy the updated worker code **before**
queueing it. No additional database migration is needed when the existing lifecycle
migration is installed. Queueing is a database write and an enabled worker may
process it immediately:

```powershell
$activityManifest = Get-Content -Raw -LiteralPath (Join-Path $activityRunDir 'manifest.json') | ConvertFrom-Json
if ($activityManifest.selected -ne 1 -or $activityManifest.deferred -ne 0) {
    throw 'Review the deferred reason in plan.json; no deletion is ready.'
}
& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle queue `
    --run-dir $activityRunDir --confirm-run-id $activityManifest.run_id
if ($LASTEXITCODE -ne 0) { throw 'Activity recovery queueing failed.' }
& .\report.venv\Scripts\python.exe -m scripts.monday_lifecycle status --run-id $activityManifest.run_id
```

The worker re-reads the same event and later history, rechecks current membership,
and rejects a stored row that changed since review. It never fabricates a current
Monday `state=deleted` response. The transaction records the original row and
fresh activity evidence, deletes only that subitem, installs the normal stale-write
guard, and queues parent refresh and source verification. All writes roll back
together on failure. Parent, sibling and hidden-source records are retained.

Immediate and daily verification jobs retain the reviewed activity reference and
re-read it when the item remains missing. A returned active item uses the normal
restoration path. If the historical event expires or becomes inaccessible, the
check remains in `review` with the deletion marker intact. A failed parent refresh
also remains visible separately; do not treat a processed deletion as proof that
every follow-up succeeded. Restage after changing the code, stored row or reviewed
event; do not edit artifacts or manually insert a replacement job to bypass checks.

## Operations and verification

```sql
SELECT event_key, kind, item_id, status, attempts, last_error, result
FROM public.monday_lifecycle_events
WHERE item_id IN ('3242789524', '3240803517')
ORDER BY received_at DESC;

SELECT monday_id, parent_monday_id, item_name
FROM public.subitems
WHERE monday_id IN ('3242789524', '3242816565');
-- After verified recovery: only 3242816565 remains from these two IDs.

SELECT table_name, monday_id, blocked, last_event_key
FROM public.monday_item_lifecycle
WHERE monday_id = '3242789524';
-- blocked remains true unless fresh Monday evidence confirms restoration.
```

Inspect a failed/review event, correct its cause in Monday or the integration,
then use `python -m scripts.monday_lifecycle requeue --event-key <exact-key>`.
This accepts only retry/review jobs and preserves their audit history. To request
an immediate fresh lifecycle check for a restored item, use `reconcile --board
<board-id> --item-id <item-id>`; this can enqueue/apply a confirmed deletion or
restoration and is not a read-only command.

A separate Render worker process may use `python -m scripts.monday_lifecycle worker
--loop`. For a controlled local pass use `worker --max-jobs 10`; it processes real
queued work and must not be used as a dry-run. Monitor `status` / inbox SQL for
retries and review items. Preserve lifecycle markers/audit history when cleaning
ordinary webhook logs; they deliberately have no cascading FK to business rows.

## Local validation

Unit tests prohibit HTTP. Database tests create random disposable databases only
on an explicitly supplied loopback PostgreSQL server:

```powershell
$env:ORDER_SCOPE_TEST_DSN = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres'
& .\report.venv\Scripts\python.exe -m pytest tests/test_monday_lifecycle.py tests/test_monday_lifecycle_activity.py tests/test_monday_lifecycle_postgres.py tests/test_monday_lifecycle_activity_postgres.py -q
```
