# Phase 2: assessment and runbook

Prepared: 7 October 2026. This document describes the Phase 2 code available in
the DataCube workspace and how to validate and review it for deployment.

Updated: 8 October 2026 to distinguish the
[corrected business requirements](bi-analyst-implementation-plan.md#business-definition-correction-8-october-2026)
and document catalogue 1.1.0. Migration 004 retains reportable-project history
and removes the monthly revenue business-stage gate; migration 001 and the sealed
1.0.0 references remain unchanged as historical evidence.

## Assessment

The implementation plan has an appropriate dependency order: certify source
definitions and reporting populations, publish stable analytical contracts, then
build the authenticated application. Phase 1 engineering is implemented, but its
[assessment](bi-analyst-phase1-assessment.md) records outstanding source/deployment
verification. The owner now selects retained reportable projects, including genuine
archived history, superseding API-active eligibility. Monthly revenue
without a closed-invoiced-stage gate, and material-plus-charges Order Value mirrored
to the parent remain required. Source alignment and certification remain release prerequisites.

Phase 2 supplies candidate contracts. Catalogue validation and frozen reference
parity establish reproducibility; they do not certify the live reporting population.
Every metric currently carries `pending_phase1`, and `require_queryable` rejects
application queries until a subsequent reviewed release integrates certification.

## Required alignment after the owner clarification

| Area | Required contract | Implementation and remaining evidence |
|---|---|---|
| Project population | Retained `reportable_projects`; genuine archived and held/unreviewed records remain included, effective reviewed placeholders are excluded | Existing project views retain this source. Schema checks compare exact project and child IDs; deployment, reviewed classifications and external consumers still require verification. |
| Monthly revenue | Positive dated invoices for retained reportable parents in completed months, regardless of business-stage label or API lifecycle state | Migration 004 removes the old stage gate from `invoice_reporting_facts_v1`; metric version 1.1.0 requires new reference approval. |
| Order Value | Hidden `formula_mkncjq9 = numbers98__1 + numbers3__1`, mirrored through children to the parent | The candidate accepts the configured parent mirror without establishing that it includes charges. The supplied schema's parent chain points to material only; verify and reconcile live wiring. |
| Reference evidence | New versioned revenue references, retained-history/exclusion tests, aligned Power BI comparison and version-bound owner review | `bi_eval_20261008_v2` passes all 50 references and 16 parity checks under 1.1.0; historical 1.0.0 still verifies unchanged. [Owner review and Power BI package](bi-analyst-phase1-certification.md#revised-reference-release-and-handoff) are prepared, not yet approved/executed. |

API lifecycle state is not an analyst eligibility filter. Operational lifecycle
checks remain unchanged; source freshness, financial and contributing-membership
uncertainties still require explicit review. The current clarification does not
change conversion's win criterion, separate bookings stage rules, signed base
invoice totals, or saved historical snapshots.

The new analyst migration does not change source writers, Monday configuration,
classification decisions, source business rows or saved historical answers.

## Implementation locations

| Artifact | Location relative to the repository root |
|---|---|
| Independent package and dependencies | `services/bi_analyst/pyproject.toml`, `services/bi_analyst/requirements.lock` |
| Catalogue 1.1.0 | `services/bi_analyst/bi_analyst/semantic/catalogue.json` |
| Pydantic contracts and consistency validation | `services/bi_analyst/bi_analyst/semantic/catalogue.py` |
| Explicit period resolution | `services/bi_analyst/bi_analyst/semantic/periods.py` |
| Installed-schema checker | `services/bi_analyst/bi_analyst/semantic/check.py` |
| Core analytical migration | `src/database/migrations/20261007_001_analytics_contracts.sql` |
| Reportable-history/revenue alignment | `src/database/migrations/20261008_004_analyst_reportable_population.sql` |
| Optional archive coverage migration | `src/database/migrations/20261007_002_analytics_archive_coverage.sql` |
| Isolated contract and PostgreSQL tests | `services/bi_analyst/tests/semantic/` |
| Frozen parity verifier | `services/bi_analyst/tests/semantic/verify_frozen.py` |

The catalogue defines 16 metric variants, 11 core relations and one optional
archive coverage relation, five populations, two permitted dimensions and three
approved joins. Importing the independent package does not start ETL or connect
to a database. The API, identity and permission design belong to later phases.

## Metric definitions

The following table describes **catalogue 1.1.0 as implemented**. Its retained
reportable population is agreed; deployed source/mirror and reference certification
remain subject to the evidence requirements above.

Every entry records a stable ID, version, label and aliases; Monday board/columns;
database lineage and source priorities; SQL expression and aggregation; numerator
and denominator where applicable; dates, population and status filters; units,
precision and NULL/zero/negative rules; dimensions, coverage requirements,
limitations and examples. Grain and key come from its named relation.

| Metric IDs | Contract |
|---|---|
| `new_enquiry_value` | Stored raw enquiry on reportable projects; retain Open-parent current-membership refresh and stored Won/Lost source semantics. |
| `order_parent_value` | Configured Monday parent order mirror; not yet verified against the required material-plus-charges formula chain. |
| `order_hidden_complete_subtotal` | Hidden material plus customer additional charges when both inputs are numeric; accompany the labelled subtotal with incomplete-input counts. |
| `invoice_project_value` | Stored derived parent invoice total, retaining signed values and NULL versus zero. |
| `invoice_hidden_value` | Signed hidden-board Amount Invoiced, with independent hidden-inventory scope. |
| `invoice_stored_child_value` | Signed persisted-child invoice sums for reportable parents; current membership still requires source evidence. |
| `enquiry_monthly_actual` | Existing monthly raw enquiry: positive values, creation month and completed months. |
| `bookings_monthly_actual` | Existing monthly bookings: positive parent order mirror, order-received month and the existing three won stages. |
| `invoice_monthly_actual` | Version 1.1.0: positive dated child invoices for retained reportable parents at any business stage, including genuine archived parents, completed months only. |
| `conversion_five_year`, `conversion_two_year` | Closed-invoiced wins divided by all eligible projects; three-decimal rounding after aggregation. |
| `conversion_closed_five_year`, `conversion_closed_two_year` | Wins divided by wins plus Lost, with the same creation cohorts. |
| `gestation_five_year`, `gestation_two_year` | Mean positive stored actual gestation, retaining source/fallback semantics. |
| `gestation_median_five_year` | Median positive stored actual gestation in the five-year cohort. |

Unqualified “order value” and “invoiced value” require scope clarification.
Currency remains `source_currency`; currency and tax presentation need owner
approval. Financial calculations retain PostgreSQL NUMERIC precision. Raw empty
sums remain NULL; monthly bins use zero where the monthly contract calls for it.
Zero denominators yield NULL. Negative invoices remain in source totals and are
excluded from the positive monthly reporting variant.

The exact enquiry CASE formula is exposed in `children_v1` for reconciliation.
`child_totals_v1` pre-aggregates stored children and provides diagnostic totals;
it does not replace stored parent values. A stored NULL alone does not establish
whether Monday supplied a typed blank or missing/unreadable evidence.

## Revenue, population and predictions

`revenue_monthly_baseline_v1` exposes the deployed revenue definition for
comparison under the unavailable `legacy_revenue` population. The frozen source
differs from the original candidate's reportable-parent and closed-invoiced restrictions.
No metric uses this diagnostic view. After migration 004,
`invoice_reporting_facts_v1` retains the reportable-parent, date, positive-amount
and completed-month rules without a business-stage or API lifecycle filter.
The relation's column interface remains v1; the changed invoice metric and
catalogue are version 1.1.0. Preserve both old calculations for comparison.

Project contracts intentionally inherit `public.reportable_projects` exclusions.
Hidden inventory remains a distinct grain; project attribution requires verified
links and source multiplicity. The unavailable verified-active population labels
operational diagnostics only, not an alternative V1 project population.
Historical-snapshot contracts remain deferred. Do not rewrite sealed answers.

The optional archive interface retains the five existing `monday_archive.coverage`
counts. It exposes aggregate counts without raw lifecycle payloads or maintenance
actions. Zero counts gate the separate active-only operational rollout, not
retention of archived analyst history. Financial findings still require review.

`latest_analysis_v1` selects one analysis per reportable project using timestamp
descending, NULL timestamps last, then analysis UUID descending. Its expected
conversion and gestation fields remain predictions. The existing chart alias
`vw_enquiry_value_forecast_chart_v1.actual_enquiry_value` means weighted enquiry
(`actual_pipeline_value`); `gross_enquiry_value` is raw enquiry. The catalogue
documents this naming discrepancy.

## Joins, dimensions and periods

Project monetary joins are permitted only to one pre-aggregated child-total row
or one deterministic latest-analysis row. Child-to-parent joins support child
detail; summing parent amounts at child grain would amplify values. Repeated
hidden-source links are retained and counted as coverage evidence rather than
automatically deduplicated.

Category and type use the whole stored classification after whitespace/empty
normalization. Monthly aggregate wrappers allow no finer dimensions. Account and
product expansion is unavailable until attributable detail or explicitly labelled
overlapping membership totals are certified.

`resolve_period` requires an aware timestamp and explicit IANA business timezone.
It retains the local as-of date, date boundaries, UTC boundaries and resolution
timestamp. Supported periods are last month, previous month, the last 12 completed
months and the two/five-year creation cohorts. Month-to-date and fiscal periods
remain unsupported. Cohorts retain the existing lower-bound-only date rule.

Views use PostgreSQL `CURRENT_DATE`. The future executor must set the reviewed
business timezone and resolve periods from the database transaction timestamp so
the plan and SQL share the same local date. Europe/London is the frozen evaluation
assumption; the business-owner decision remains outstanding.

## Validation commands

Run these commands from the DataCube repository root. The existing `report.venv`
contains the dependencies used during implementation. For a new environment,
install `services/bi_analyst/requirements.lock` and the independent package.

Validate the catalogue without installing the package:

```powershell
$env:PYTHONPATH = 'services/bi_analyst'
& .\report.venv\Scripts\python.exe -m bi_analyst.semantic.check
```

Run the isolated analyst tests:

```powershell
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests -q
```

PostgreSQL tests skip unless `BI_ANALYST_TEST_DSN` specifies the `postgres`
administrative database on an explicit loopback server. They create and drop
randomly named disposable `bi_semantic_test_*` databases.

```powershell
$env:BI_ANALYST_TEST_DSN = 'host=127.0.0.1 port=55439 dbname=postgres user=postgres connect_timeout=3'
& .\report.venv\Scripts\python.exe -m pytest services/bi_analyst/tests/semantic -q
```

The historical 1.0.0 frozen parity command uses only the existing `TEST_SUPABASE_*` configuration
and target-isolation checks. It expands migration SELECTs as CTEs over frozen
sources, with the fixed dataset date, then compares typed results as the restricted
dataset reader. It does not install migrations or modify the sealed answers.

```powershell
& .\report.venv\Scripts\python.exe services/bi_analyst/tests/semantic/verify_frozen.py --dataset bi_eval_20261007_v1
```

After reviewed migration installation, configure `BI_ANALYST_CHECK_DSN` explicitly
and run the schema checker. It checks relation names, exact columns/types, comments,
declared-key uniqueness, view security settings and exact project/child/invoice
eligibility in a read-only transaction. Equal counts with different IDs fail.
The optional archive relation is checked if present.

```powershell
$env:PYTHONPATH = 'services/bi_analyst'
& .\report.venv\Scripts\python.exe -m bi_analyst.semantic.check --database
```

## Deployment and recovery

1. Incorporate the later 8 October owner decision into reviewed catalogue/source
   versions: retained reportable projects including archived history, no monthly
   revenue business-stage gate, and the total-order formula mirrored to parents.
   Verify effective exclusions, retained-history/membership evidence, live mirror wiring and deployed writers;
   approve the revised references and complete remaining Phase 1 evidence/decisions. Bind approval
   to those exact versions, not the superseded 1.0.0 definitions.
2. Verify PostgreSQL 15+ and source dependencies on staging. Apply migration 001
   inside one transaction using `psql --single-transaction --set ON_ERROR_STOP=1
   --file src/database/migrations/20261007_001_analytics_contracts.sql` with an
   explicitly configured staging connection. Then apply migration
   `20261008_004_analyst_reportable_population.sql` in its own reviewed transaction.
   Existing installations of 001 need only 004 for this analyst change. Do not
   replay 001 alone after 004: that would restore the superseded revenue filter.
3. Apply migration 002 separately only where the archive runtime dependencies
   exist. This adds coverage visibility and does not enable active reporting.
4. Run the installed-schema check, review underlying privileges and RLS, and
   establish same-snapshot parity before granting analyst access in Phase 3.
5. Deploy consumers after certification and version review. Use new relation and
   metric versions for incompatible changes.

The migrations are additive, document relations/columns and grant no application
access. Views use `security_invoker` and `security_barrier`; the caller still needs
the privileges required by underlying invoker views and functions. Application
authorization and restricted database roles remain Phase 3 work.

Application rollback normally retains these additive views. If removal becomes
necessary, review dependent consumers and drop only the new views in reverse
dependency order inside a transaction, without CASCADE or source-table changes.
Migrations 001 and 004 were applied and independently checked in TEST and then
owner-approved production on 8 October. See the
[population rollout evidence](project-placeholders.md#verified-rollout-8-october-2026).
The Phase 3 permission bootstrap and Render application deployment were not
performed by that rollout.

## Evidence and remaining exit gate

The existing private evidence file
`outputs/bi_analyst_evals/phase2_20261007/parity.json` records a check at
17:47:32 Europe/London on 7 October 2026. All 16 curated acceptance queries matched
the sealed TEST answers, including PostgreSQL result types. The run first
reverified 50 original references, 21 frozen table signatures, 17 independent
checks and 11 access checks.

The recorded catalogue, migration 001 and parity-SQL hashes matched the workspace
versions at that check. This establishes frozen-reference parity for the original
contracts; it does not validate the 8 October correction or live Power BI results.
Catalogue 1.1.0 population/revenue implementation now has 130 passing focused
tests, including real PostgreSQL integration, and post-commit checks on TEST and
production. The Phase 2 release exit gate remains open until source/mirror
contracts, newly versioned revenue references and aligned Power BI results are
reviewed and certified with explicit remaining limitations.

The subsequent 1.1.0 reference run on `bi_eval_20261008_v2` passed all 50 queries,
20 independent checks and 16 parity comparisons; 115 focused evaluation/contract
tests also passed. The original dataset still verifies under 1.0.0.
The [review/export handoff](bi-analyst-phase1-certification.md#revised-reference-release-and-handoff)
records the pending actual owner approval and Power BI execution. Those passes
do not establish live source/mirror accuracy or enable the analyst API.
