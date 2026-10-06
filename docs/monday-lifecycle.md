# Monday lifecycle replication

Finance owns Monday data. Deleting a Monday item now has a durable, exact-ID path
to deleting its Supabase counterpart. This release includes code and a migration;
deploying Python alone does not enable the feature.

## Verified archive handling: staged rollout

Monday API `state=archived` is a lifecycle state. Board labels such as **Archive**
or **Archived** are administrative/business data and never establish lifecycle
eligibility. All archive reads use the exact-ID, query-only comparison client.
No Monday mutations or new webhook subscriptions are needed for this rollout.

After the preparatory migration below, apply these complete files in order:

1. [Archive runtime SQL](../src/database/schema/monday_lifecycle_archive_runtime.sql):
   archive write protection, an archive-capable worker fence, and backend-only
   `current_projects`, `current_subitems`, and `current_hidden_items` views.
2. [Archive reporting SQL](../src/database/schema/monday_archive_reporting.sql):
   current forecast/smoothing views and new snapshot functions. This copies the
   **installed** forecasting formulas, replacing only their current population
   source. Unexpected dependencies/signatures abort the migration. Reapply this
   migration after future changes to the original forecasting formulas.

Neither file backfills lifecycle states, changes business rows, rewrites existing
snapshots, or switches existing reporting consumers. Do not run the historical
full schema instead of these migrations.

There are two switches, both **false by default**:

| Switch | Purpose |
|---|---|
| `MONDAY_ARCHIVE_ENABLED` | Verified archive processing, exact-ID sync gates and source-based current financial refreshes |
| `MONDAY_ARCHIVE_REPORTING_ENABLED` | Switch current analysis/forecasting consumers to verified-active populations |

Deployment sequence:

1. Leave both switches false while applying SQL and deploying the code.
2. Stop/drain old sync and lifecycle processes before enabling archive ingestion
   on **all** replacement writers. Keep `MONDAY_LIFECYCLE_ENABLED=true` for the
   existing lifecycle worker. Do not mix archive-enabled and legacy writers:
   database guards reject their attempts to update archived rows.
   All archive-enabled sync, analysis and reporting processes need the
   `SUPABASE_DB_URL` server-side secret, not just the lifecycle worker.
3. Run normal synchronisation/rehydration to establish fresh active-state,
   membership and financial coverage. Missing source responses are explicit
   errors/review cases, not active/archive/deletion defaults.
4. Stage the curated, non-excluded archive review CSV using the existing CLI:

   ```powershell
   python -m scripts.monday_lifecycle stage --state archived --review-csv <curated-review.csv> --run-dir <new-run-directory>
   ```

   Staging is read-only in **both** systems. Its plan retains exact IDs and
   before-rows, and includes a hypothetical financial preview for affected
   active parents. Use the remaining non-excluded review export, not the
   original historical list containing New project/FREE records or audit holds.
   Review its selected/deferred counts and financial issues before queueing.

5. Queue the reviewed run with `queue --run-dir ... --confirm-run-id ...`.
   Queueing requires the runtime migration and enabled archive code. Workers
   recheck API state and the reviewed before-rows; changed evidence returns to
   review. A queued archive can never become a deletion merely because the
   source changes state. Drain the scoped archive/refresh/verification jobs,
   then run another ordinary sync and fresh comparison.
6. Check coverage before switching reports:

   ```python
   from src.services import monday_lifecycle as life, monday_archive as archive
   with life.connect() as connection:
       with connection.transaction():
           connection.execute("SET TRANSACTION READ ONLY")
           print(archive.coverage(connection))
   ```

   All counts must be zero. They cover unverified projects, children, source
   links, current financial/membership evidence, and unresolved archive jobs.
   A failed current-value check cannot silently publish an incomplete total.
   Routine pending periodic state checks alone do not disable reporting.
7. Only then enable `MONDAY_ARCHIVE_REPORTING_ENABLED`. Current application
   readers also check coverage and fail explicitly if it becomes incomplete.
   External SQL/Power BI consumers must explicitly adopt the `current_*` views
   **after** the same coverage check. Those low-level views select verified
   populations; they are not a substitute for the readiness check.

### Guarded initial coverage pilot

Scheduled rehydration uses a three-day creation-date window, so it does not
establish coverage for the full historical population. The ordinary by-ID
rehydrator also warms hidden records by name prefix; **do not use it for an
exact approved pilot boundary**.

Deploy [the pilot runner](../scripts/monday_archive_pilot.py) and its
[pinned approval](../scripts/monday_archive_pilot_targets.json) first. This
approval contains ten parents, twelve children and twelve hidden sources from
the reviewed initial coverage inventory. It excludes New project/FREE records,
archived/missing records and unresolved audit holds. It is not approval to run
the remaining 15,143 initial-scope projects.

Keep `MONDAY_ARCHIVE_ENABLED=true`, `MONDAY_LIFECYCLE_ENABLED=true` and
`MONDAY_ARCHIVE_REPORTING_ENABLED=false` on all replacement writers. The CLI
requires `SUPABASE_DB_URL` with full visibility, normal foreign-key enforcement,
PostgreSQL 17+ and the already-installed archive migrations. No additional SQL
migration is required for this runner.

From the repository root (including the Render shell):

```bash
python -m scripts.monday_archive_pilot stage --run-dir archive_pilot_01
```

Staging is read-only in both systems. Review `review.csv` for **every** proposed
field change (`null` means NULL, not zero), and retain `plan.json` and
`manifest.json`. Even if no business values change, the apply establishes audited
active-state and financial coverage. Copy the UUID printed as `run_id` only
after approving the preview:

```bash
python -m scripts.monday_archive_pilot apply --run-dir archive_pilot_01 --confirm-run-id "UUID_FROM_STAGE"
```

Apply rechecks all ten projects before starting, then repeats the source reads
for each project. Each project has its own bounded transaction and a fresh
post-write verification. A failure stops the run; earlier verified projects
remain committed. Source/schema/lifecycle/SQL drift, missing or non-active items,
changed membership/links, outside stored owners, and changed placeholder/FREE
names stop the operation without guessed repairs. There is no prefix warm-up,
Monday mutation, deletion, archive, restoration or queued refresh fan-out.
SQL business rows and lifecycle metadata are locked only during short write
transactions; no network reads occur while locked.

`result.json` must report `complete: true`, ten `verified_projects`, and
`expected_projects: 10`. Supabase retains authoritative audit receipts keyed
`archive-pilot:<run-id>:<project-id>`. Receipts are never queued: only `review`
or `processed` states are committed, so even older workers cannot claim them.
A `review` receipt means operator completion is still required and blocks
archive-reporting readiness. Financial verification is set only after the fresh
post-write source projection agrees with checked SQL.

For an interruption, retain the **same** directory, code and UUID. Rerun
`apply` to resume unfinished projects without replaying verified writes.
If all business writes finished and only verification remains, use:

```bash
python -m scripts.monday_archive_pilot verify --run-dir archive_pilot_01 --confirm-run-id "UUID_FROM_STAGE"
```

`verify` can certify lifecycle metadata and close the operator receipt; it does
not rewrite business values. New applies expire after 24 hours; each write also
requires a matching source capture less than five minutes old. Verification
still requires unchanged source, SQL, lifecycle, schema, code and approval.
If these guards reject a partial run, stop for review rather than restaging
blindly or requeueing operator receipts to ordinary workers. A disconnected
commit has an uncertain outcome until the retained database receipt is checked.
Download/copy the private run directory before a Render restart or redeploy
unless it is on a persistent disk. Preview files are not automatically deployed.

The ten-project result is **not** full reporting readiness. Rerun the coverage
query above afterward and leave reporting disabled while other projects remain
unverified or held.

Operational guarantees:

- Archiving retains business rows, their financial values and historical parent
  links. An archived parent does not change any child's own API state.
- Each observation is audited with its previous metadata, exact source evidence,
  correlation key, and verification time (not a claimed archive date).
- Normal upserts use fresh, repeated exact-ID evidence. Archived rows are not
  upserted. Restored/reparented rows go through full rehydration; the old and new
  parent are checked, the old link is audited, and affected parents are refreshed.
- Confirmed active and archived IDs are periodically rechecked, ten per idle
  scheduling pass, so pagination absence is never used to infer lifecycle.
- Archive-enabled parent totals do **not** use sums of retained SQL children.
  New Enquiry Value uses current API-active children for Open parents; Won/Lost
  enquiry values are retained. Order value remains the actual typed Monday
  **parent mirror**, not an invented material-plus-charges total. Invoice value
  uses current child invoice mirrors; all blank remains NULL and explicit zero
  remains zero. No separate current-order measure is silently substituted.
  Inactive mirror dependencies withhold the affected current fields for
  source-link review; they do not turn historical amounts into zero.
- Historical `reportable_projects`, training/cohort readers and saved snapshots
  retain their populations. Current forecasts use separate live views rather
  than stale legacy materializations. The new snapshot functions accept today
  only and refuse to replace a snapshot that already exists; activate future
  snapshot scheduling after the previous day's run has completed.
- Comparison runs recognise an archive only when the exact current API state
  agrees with retained, audited lifecycle metadata. Missing/deleted/unverified
  records and unrelated source-link/audit concerns remain review cases.

Keep reporting disabled if coverage is incomplete; do not classify missing
records as active to make the counts pass. After archive observations exist,
rollback means pausing writers, not restarting old code against the guards.
Keep the guards and retained history in place.

## Archive support: preparatory database migration

Apply [monday_lifecycle_archive_state.sql](../src/database/schema/monday_lifecycle_archive_state.sql)
in the Supabase SQL Editor as the database owner, after the existing
`monday_lifecycle.sql` migration. Run the entire file, including `BEGIN` and
`COMMIT`. PostgreSQL 17+ and the existing lifecycle tables with RLS enabled are
required. Do not run the full historical `schema.sql`.

This is schema preparation, not activation of archive handling. It adds four
nullable columns to `monday_item_lifecycle`:

| Column | Meaning |
|---|---|
| `monday_state` | Verified API `active`, `archived` or `deleted`; NULL means unverified |
| `state_verified_at` | When the state was checked, not when the archive occurred |
| `state_event_key` | Durable lifecycle event/audit correlation key for the observation |
| `state_evidence` | Nonempty JSON object containing the observation evidence |

An observation must supply all four fields together. Business labels such as
Archive/Archived, an absent API result, and `blocked=false` do not establish an
API state. The old deletion marker is deliberately independent: this migration
neither derives states from it nor changes its meaning. Event keys have no
cascading foreign key, so audit correlation can survive queue retention.

There are no business-data updates, state backfills, new grants, archive jobs,
new worker protocols, reporting filters or financial recalculations in this
migration. Existing deletion guards, scope guards, queue constraints and audit
history are unchanged. The partial index prepares future exact-ID archive
rechecks using the existing `recheck_after` scheduling column.

The migration can be rerun. Unexpected preexisting column definitions or
invalid observations stop it and roll back its changes. Lock waits are bounded
to five seconds and statements to thirty seconds; a timeout means retry the
whole file in a quieter window, not remove its safeguards.
If the editor leaves a transaction open or aborted after an error, run
`ROLLBACK;` before retrying. Do not continue with individual statements.

After applying, these read-only checks should return four nullable columns
without defaults, one validated constraint, and one valid index:

```sql
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'monday_item_lifecycle'
  AND column_name IN
      ('monday_state', 'state_verified_at', 'state_event_key', 'state_evidence')
ORDER BY column_name;

SELECT conname, convalidated
FROM pg_constraint
WHERE conrelid = 'public.monday_item_lifecycle'::regclass
  AND conname = 'monday_item_lifecycle_state_observation_check';

SELECT c.relname, i.indisvalid
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.oid = to_regclass('public.monday_item_lifecycle_archived_rechecks');
```

Do not manually populate the new fields or equate NULL with active. The runtime
release described above validates fresh exact-ID API evidence, updates lifecycle
metadata and transition audits atomically, handles archive/reactivation, and
applies lifecycle-aware current membership across sync and rollups.
Historical values and reports must remain available. Deploy all relevant
workers before enabling that behaviour; applying this migration alone does not
resolve any manual-review archive cases.

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
Later or simultaneous activity for the subitem stops recovery, including
unknown event types. One narrowly checked exception is a later parent Subitems
update whose typed membership removes the deleted ID, adds no IDs, and refers
to that ID only in its previous membership. Its full event remains in the audit.
This observation reflects the deletion; it cannot authorize a restore or move.
Later parent lifecycle or unfamiliar actions also stop recovery;
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

For the explicit 31-subitem selection reviewed on 5 October 2026, use the
[batch cleanup workflow](monday-review-cleanup-31.md). It stages the existing
single-subitem proof independently for each reviewed ID and performs a targeted
parent enquiry-value refresh using the confirmed current-subitem SUM rule.

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

The preparatory archive migration has focused idempotency, data-preservation,
constraint, RLS and existing-worker compatibility coverage:

```powershell
& .\report.venv\Scripts\python.exe -m pytest tests\test_monday_archive_schema_postgres.py -q
```

## Worker monitoring

The application worker and standalone `worker --loop` publish instance heartbeats.
Apply the worker-operations migration before deploying this version and configure
the independent watchdog as described in [Worker monitoring](worker-monitoring.md).
Lifecycle claims, evidence checks and deletion safeguards remain in effect.
