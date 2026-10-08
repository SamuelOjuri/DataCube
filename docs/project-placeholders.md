# Redundant project placeholders

## Scope clarification: 8 October 2026

The owner's later 8 October decision supersedes the earlier API-active analyst
population: genuine archived projects must remain in historical reporting.
The [analyst plan](bi-analyst-implementation-plan.md#business-definition-correction-8-october-2026)
therefore uses retained `reportable_projects`, excluding only effective reviewed
redundant placeholders. Lifecycle state alone does not exclude retained records.
The analyst must not substitute active-only `current_*` views for this population.

Monthly revenue uses reportable parents without requiring the manually maintained
closed-invoiced business label. Catalogue 1.1.0 and analyst migration 004 implement
that correction. Source reconciliation and deployment certification remain
separate from agreement on this population. No approval to archive or delete
Monday items is implied; existing lifecycle processing and historical snapshots
retain their independent operational contracts.

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
& .\report.venv\Scripts\python.exe -m scripts.project_reporting verify --run-dir outputs/project_placeholders/migration_run
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

Run these ETL tests separately from the isolated analyst suite, whose import
checks deliberately reject an ETL-loaded process. For reporting-only completion,
the migration verifier must report no unfiltered reporting objects, and the batch
verifier must report matching expected exclusions, no reporting/forecast leaks
and no changed business records. Pending archive IDs are not a failure unless
`--require-archived` was explicitly requested. Confirm external Power BI sources
and refreshes separately; the migration verifier does not inspect external reports.

## Verified rollout: 8 October 2026

The reporting migration was staged, applied and verified in TEST, then in
production after explicit owner approval. Both reporting plans required no
existing reporting-view rewrites. The existing 27 exclusions already matched
fresh staged evidence, so no classification decision was overwritten.

| Check | TEST | Production |
|---|---:|---:|
| Retained projects | 15,232 | 15,237 |
| Reportable/analyst projects | 15,205 | 15,210 |
| Effective reviewed exclusions | 27 | 27 |
| Held records in the reviewed batch | 1 | 1 |
| Reviewed project and analysis rows retained | 28 each | 28 each |
| Reporting/forecast leaks in the reviewed batch | 0 | 0 |

Analyst migrations 001 and 004 were also applied in both databases. Post-commit
schema checks passed, including exact project, child and invoice ID comparisons.
Source-table and historical-snapshot fingerprints were unchanged by the analyst
migrations. The revised invoice view includes 290 TEST and 289 production
eligible child invoices whose parents lack the closed-invoiced label.

The scoped Monday reads observed six archived, fourteen active and eight
unreturned items. No archive/delete command ran; unreturned items remain
unresolved lifecycle evidence, not deletion or zero-value proof. Both database
lifecycle ledgers identified zero reportable projects as archived at this check;
archived-history retention is covered by real PostgreSQL regression cases,
not a claim of complete live lifecycle evidence.

Validation passed: 95 isolated analyst tests and 35 placeholder tests, including
PostgreSQL integration tests with no skips. All 16 historical 1.0.0 parity queries
still matched the sealed TEST answers. Those passes do not certify revised
financial definitions, live mirror wiring, external Power BI refreshes or a
new reference-answer version.

Private evidence is retained in git-ignored run directories:

- `outputs/project_placeholders/test_population_20261008_migration`
- `outputs/project_placeholders/test_population_20261008_review`
- `outputs/project_placeholders/production_population_20261008_migration`
- `outputs/project_placeholders/production_population_20261008_review`

Each migration run includes staged SQL, its manifest, verification and
`analyst-rollout.json`; each review run retains the staged source evidence and
batch verification. The analyst permission bootstrap, application deployment and
business certification were not performed by this rollout.
