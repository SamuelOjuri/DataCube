# Scoped order-value correction

`scripts/order_value_scopes.py` adds a separate online workflow. The existing
`backfill_order_values.py` and `reconcile_order_values.py` apply commands retain
their maintenance-window requirements and their original artifact fingerprints.
Do not remove their drift checks or pass `--writers-paused` while writers run.

## Reassess and review

Create a fresh, read-only Monday/Supabase capture. Normal activity may invalidate
an inventory scan; a failed or incomplete capture cannot be applied. The reviewed
four-duplicate exception still requires its complete zero-value and ownership
evidence. It is not a general exclusion switch.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.backfill_order_values prepare --run-dir outputs/order_value_backfill/reassessment_NEW --approve-reviewed-parentless-duplicates
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes reassess --previous-run outputs/order_value_backfill/run4 --capture-dir outputs/order_value_backfill/reassessment_NEW --output-dir outputs/order_value_backfill/review_NEW
```

The second command is entirely offline. `reassessment.json` compares blocked
statuses between captures; `blocked-changes.csv` lists project-level changes.
The usual reconciliation reports include the complete repair dependency groups,
manual blockers, duplicate sources and unlinked nonzero orders. Counts are
observations at capture time, not certification of the current database.

`online-readiness.csv` distinguishes existing-row groups that can be staged in
online repair mode from candidates that first need missing-row rehydration or
manual review. Use its workflow guidance for the online path; the legacy
reconciliation report's resolution text still describes its maintenance workflow.

Review null-to-zero changes and decreases explicitly. A fetched blank numeric
input is a certified zero under the existing source contract; an absent or
invalid input blocks correction. Empty projects are deferred by the online
workflow even if an older maintenance plan approved clearing them.

## Schema prerequisite

Run the read-only preflight:

```powershell
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes preflight
```

This requires PostgreSQL 17+, a database role with full row visibility, normal
constraint enforcement, and validated, nondeferrable foreign keys from
`subitems.parent_monday_id` to `projects.monday_id` and from
`subitems.hidden_item_id` to `hidden_items.monday_id`. The FK triggers must be
enabled. These requirements are checked again inside every write transaction.

Before applying, have the database operator review and install
[`order_value_scope_commits.sql`](../src/database/schema/order_value_scope_commits.sql)
using the same privileged database role used by the CLI. It creates an isolated
commit journal, enables RLS and revokes API-role access. It does not alter the
three business tables or their data. Staging can run before this installation;
apply refuses an absent journal. The CLI never installs schema automatically.

Use a direct PostgreSQL connection or session pooling. Keep credentials in the
environment; never place the DSN in a saved command or report. Confirm the
corrected sync/webhook code is deployed on every writer before a production pilot.

## Stage existing, verified order rows

```powershell
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes stage --capture-dir outputs/order_value_backfill/reassessment_NEW --run-dir outputs/order_value_backfill/orders_pilot_NEW --mode orders --project-id PROJECT_ID
```

Repeat `--project-id` for additional projects. `--all-verified` explicitly selects
all verified projects in the capture instead. Preparation reads current database
values in a repeatable-read snapshot; it does not require the unrelated database
population to equal the old capture. It rebuilds the arithmetic and eligibility
for each selected scope against that current state.

Order-only scopes contain up to 25 projects and 500 total database rows,
including source-sharing dependencies. Each changes only the two order inputs
on hidden/subitem rows and `projects.total_order_value`. Existing totals are
replaced, never incremented. Invoice/enquiry/date values are checked and preserved.
The plan includes all children of each selected parent, including unchanged ones.

Review `changes.csv`, `scopes.json` (including `deferred`), and `manifest.json`.
The CSV contains exact before/after fields; its hash is verified at apply time.
Staged records include current database state, expected state, Monday evidence,
global source owners and database schema. Code/mapping changes invalidate staging.

## Stage relationship repairs separately

```powershell
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes stage --capture-dir outputs/order_value_backfill/reassessment_NEW --run-dir outputs/order_value_backfill/repair_pilot_NEW --mode repair --project-id REVIEWED_PROJECT_ID
```

Select **every project in a repair dependency group**, repeating `--project-id`.
Manual-review groups cannot be staged. The source is fetched by exact IDs, with
complete parent membership and source ownership checks; names do not establish
relationships. Repairs refresh the selected hidden/subitem records and the
parent's order, invoice, enquiry and date aggregates. Review these broader fields
independently of the order-only correction.

Missing database rows, changed child parents, incomplete child sets and oversized
groups are deferred. The online workflow does not insert absent rows, move children
between parents, delete history or infer zero for an empty parent. Missing rows
need separate reviewed rehydration followed by a new capture. This restriction is
intentional: a missing referenced key cannot provide the required row-lock guard.

## Apply a bounded batch

```powershell
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes apply --run-dir outputs/order_value_backfill/orders_pilot_NEW --confirm-run-id REVIEWED_RUN_UUID --allow-partial --limit 1
```

Repair runs additionally require `--allow-repair-fields`. `--allow-partial` is
required for both modes: each scope commits separately and blocked/deferred
projects remain unresolved. The default batch limit is 10 scopes; the maximum is
100. A pilot should start with one small, explicitly selected project.

Apply makes a new complete Monday capture without reusing saved detail batches.
It checks only the selected scope's evidence against its review, while retaining
global ownership checks for every selected source. It then fetches the selected
parents, children and inputs again immediately before each transaction. An
unrelated source change does not invalidate the selected scope. API/inventory
validation failures abort rather than treating missing evidence as zero.

Inside each database transaction:

1. Take an advisory lock for this run/scope and consult the commit journal.
2. Check schema and FK enforcement. Acquire `FOR UPDATE` locks on the selected
   parents and old/new hidden sources, then read/lock their complete child and
   source-owner population. Ordinary `ROW SHARE` table locks only prevent
   incompatible schema changes; they allow unrelated data writers.
3. Compare that population with the reviewed before-state. FK `KEY SHARE` checks
   on incoming inserts/relinks conflict with these referenced-key locks, so new
   children and new source owners cannot slip between comparison and commit.
4. Update rows with set-based statements and reconcile the entire expected scope.
5. Insert the journal record in the same transaction as the business updates.

Lock acquisition waits at most 750 ms, each statement at most four seconds, and
each database transaction at most ten seconds. There are no Monday calls while
database locks are held. Contention, changed scoped values, changed membership
or changed ownership defers the entire scope. A failure rolls back its changes
and its journal entry together; earlier successful scopes remain committed.

These guarantees assume normal FK-enforced writers. Do not run concurrent DDL,
disable constraint triggers, or use replication sessions that bypass FKs during
correction. Ordinary reads and unrelated writers can continue.

## Resume, verify and resolve drift

Rerunning the same apply command skips scopes already present in the database
journal, **even if normal activity has changed them since commit**. It never
restores old values merely to make a retry match the review. This also resolves a
lost connection at commit or a failed local receipt write. A connection failure
stops the batch; do not infer rollback from the absence of `scope-*.json` files.

Conflicts are not automatically rebased onto newer amounts. Restage conflicted
projects from fresh evidence and review the new differences. To progress past a
deferred scope in an existing run, select other reviewed IDs with repeated
`--scope-id SCOPE_ID` flags. The scope IDs and project membership are in
`scopes.json`; unknown IDs are refused.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.order_value_scopes verify --run-dir outputs/order_value_backfill/orders_pilot_NEW
```

Verification is read-only. It checks committed journal entries, makes a second
complete Monday capture (including global owners), refetches selected evidence,
and compares each scope with its expected database state. Results distinguish
`verified`, `not_committed` and `changed_requires_reassessment`. A failed source
capture leaves the commits unverified. Save the resulting `verify-*.json` file.
`complete` refers only to all scopes selected in this run; the result explicitly
does not certify the entire dataset and retains the original blocked counts.

Monday and PostgreSQL cannot be committed atomically. A Monday change after a
source read remains possible; post-commit verification detects observed drift,
and subsequent edits remain the corrected normal sync's responsibility. Do not
describe this workflow as a point-in-time transaction spanning both systems.
The complete ownership captures can take substantial time, but they hold no
database write locks. Small database transactions do not imply a short overall
capture/verification runtime.

After the pilot is verified, expand batches and track deferred/manual projects to
resolution. Refresh affected reporting materialized views after verified batches.
Coordinate snapshot timing so published snapshots are clearly before/after the
correction. Historical snapshots are unchanged unless separately restated.

## Regression checks

```powershell
& .\report.venv\Scripts\python.exe -m pytest tests/test_order_value_scopes.py tests/test_backfill_order_values.py tests/test_reconcile_order_values.py -q
```

The real PostgreSQL tests require a disposable local PostgreSQL 17+ server:

```powershell
$env:ORDER_SCOPE_TEST_DSN = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres'
& .\report.venv\Scripts\python.exe -m pytest tests/test_order_value_scopes_postgres.py -q
```

They only accept a loopback host and create/drop randomly named test databases.
They never fall back to `SUPABASE_DB_URL`. They exercise incoming children/shared
sources, relinks, independent writers, scope drift, concurrent applicators,
rollback, server timeouts and local receipt failure against real PostgreSQL.
