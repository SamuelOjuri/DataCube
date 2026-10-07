# Redundant project placeholders

## Scope clarification: 8 October 2026

The owner confirms this feature is not fully implemented and must not define the
BI analyst's eligibility. The [corrected analyst plan](bi-analyst-implementation-plan.md#business-definition-correction-8-october-2026)
uses current Monday API `state = active`; an otherwise active item is not excluded
solely by a placeholder classification. This also applies to monthly revenue,
without requiring the manually maintained closed-invoiced business label.

The workflow and deployment details below describe the existing classification
feature, not proof that its reporting population is approved for the analyst.
Existing `current_projects` also derives from `reportable_projects`, so the
analyst implementation must address indirect exclusions, not merely rename its
source. This documentation correction does not change existing consumers,
classification decisions, Monday items or historical evidence.

`projects` remains the complete synced source table. The classification-based
business reporting and model-selection paths use `reportable_projects`.
Operational inventory, rehydration, lifecycle checks, and historical snapshots
keep their existing populations in this implementation.

An explicit decision in `project_reporting_classifications` excludes one exact
Monday ID only while its item name is `New project`, its business fields remain
empty/zero, its stage remains blank/Open Enquiry, and it has no stored subitems.
Missing amounts remain missing; this classification does not certify financial
zeroes. Negative amounts, meaningful names/accounts/categories/products/dates,
nonzero amounts or probabilities, and any subitem make the project reportable
again immediately. A new default-name record is an unreviewed candidate and is
included until reviewed. `FREE` is held for review and remains reportable.

Decisions record reviewer, time, reason and source evidence. Their audit history
survives project deletion. No pipeline stage, generated status, raw business row,
existing analysis or deletion marker is changed by classification. The metadata
is independent of sync, so upserts cannot erase it.

`project_reporting_review` shows current candidates, recorded decisions, effective
exclusions and `needs_review_changed_record`. An effective exclusion is suspended
when data changes; if the project becomes empty again the recorded exclusion
applies again. Use an explicit `released` decision for permanent inclusion.
For example, with the privileged database connection:

```sql
UPDATE public.project_reporting_classifications
SET classification = 'released', reason = 'Reviewed as a genuine project',
    reviewed_by = 'Reviewer name', reviewed_at = now()
WHERE monday_id = 'EXACT_REVIEWED_ID';
```

The audit trigger records both decisions. Never use this metadata to forge a
Monday deletion marker or classify a real project as Lost.

## Deployment

Do not run the full historical `schema.sql` against an existing database.
The migration tool stages the deployed reporting definitions and replaces their
project source while retaining their existing formulas. It preserves dependent
views, materialized-view indexes, grants, owners and comments, and fails if the
catalog or staged SQL changes. Drops use no CASCADE. The apply is transactional.
`before.json` retains the previous definitions for recovery.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.project_reporting stage --run-dir outputs/project_placeholders/migration_run
& .\report.venv\Scripts\python.exe -m scripts.project_reporting apply --run-dir outputs/project_placeholders/migration_run
```

Deploy the Python changes to both Render services described in
[worker-monitoring.md](worker-monitoring.md). Scheduled analysis selection,
training/segment samples, direct analysis and Monday prediction pushes now use
the reporting classification. Rehydration and source sync continue using raw
projects. Deploy the migration before the Python release; missing classification
objects must fail rather than silently include excluded records.

Power BI consumers using existing reporting views get the exclusions through the
migration. Consumers reading `projects` directly must change their source to
`reportable_projects` and refresh their imported datasets. Authenticated readers
retain project RLS; classification evidence and audit are backend-only.

Current materialized aggregates are refreshed during migration/classification.
Later source changes appear immediately in ordinary views and on the next
materialized-view refresh. Historical forecast snapshots are not rewritten.

## Reviewed 5 October 2026 batch

The tracked `scripts/project_placeholder_review_20261005.json` names the 28 IDs
and holds `3011456213` (`FREE`) for review. The original CSVs are evidence, not
instructions or deletion authorizations.

```powershell
& .\report.venv\Scripts\python.exe -m scripts.project_placeholders stage --run-dir outputs/project_placeholders/review_run
& .\report.venv\Scripts\python.exe -m scripts.project_placeholders classify --run-dir outputs/project_placeholders/review_run
& .\report.venv\Scripts\python.exe -m scripts.project_placeholders archive --run-dir outputs/project_placeholders/review_run
& .\report.venv\Scripts\python.exe -m scripts.project_placeholders verify --run-dir outputs/project_placeholders/review_run --require-archived
& .\report.venv\Scripts\python.exe -m scripts.project_placeholders export --run-dir outputs/project_placeholders/review_run
```

Stage reads fresh Monday and database evidence. Classify rereads source evidence
and locks the selected database parents before checking empty values and child
membership. Existing different decisions cannot be overwritten by rerunning a
batch. Archive rechecks each exact active item, then uses Monday's reversible
[`archive_item`](https://developer.monday.com/api-reference/reference/items#archive-item)
mutation with before/after evidence and a database audit entry. It never deletes.
An ambiguous response stops for rechecking; archived items are safe to resume.
There is no distributed transaction between Monday and PostgreSQL, so a concurrent
edit requires post-operation review. Source drift suspends the exclusion.

Unreturned items can receive the user's local redundancy decision but cannot be
archived or declared deleted. Their absence remains unresolved. API errors are
not accepted as missing-item evidence. Archived formula `display_value="null"`
is retained as missing evidence and is never certified as a zero value.

For reporting-only treatment, omit `archive` and `--require-archived`. Verification
then records pending archive IDs without treating them as a reporting failure.
The 5 October implementation's archive execution approval was declined: its 14
active Monday items were left unchanged. Reporting exclusions remain effective.

Exports leave the original 96-project/176-issue CSVs intact. They partition all
28 selected IDs (14 appear in those CSVs) into a separate placeholder queue,
including the held FREE case. The other queue has 82 projects and 162 issues.
Original issue descriptions remain unchanged; separation does not resolve or
recertify those lifecycle issues.

## Validation

```powershell
$env:ORDER_SCOPE_TEST_DSN = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres connect_timeout=15'
& .\report.venv\Scripts\python.exe -m pytest tests/test_project_reporting.py tests/test_project_reporting_postgres.py tests/test_analysis_service.py tests/test_worker_monitoring.py tests/test_recent_rehydrate.py -q --tb=short
```

PostgreSQL tests create isolated randomly named loopback databases. They verify
retention, automatic re-entry, release/audit behavior, migration dependencies,
ACL/index preservation and stable historical snapshots. Unit tests verify that
excluded projects do not trigger numeric/LLM analysis or Monday pushes and that
missing or changed source evidence cannot authorize archival.
