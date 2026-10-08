# Phase 1 implementation assessment

The plan has a sound dependency order: certify source definitions and populations,
then publish the semantic interface, then enable the application. The existing
70-scenario dataset is a reproducible baseline, but does not establish source
accuracy or business approval. Phase 1 therefore remains open.

## Owner clarification: 8 October 2026

The [corrected implementation plan](bi-analyst-implementation-plan.md#business-definition-correction-8-october-2026)
settles three previously ambiguous business requirements:

- The later owner decision supersedes API-active eligibility: retained
  `reportable_projects` are authoritative, including genuine archived history.
  Only effective reviewed redundant-placeholder exclusions remove records.
- Monthly revenue must not require `Won - Closed (Invoiced)`: a reportable
  parent with an eligible invoice qualifies even when that manual label is absent.
  Positive amounts, invoice dates and completed months remain the monthly rules.
- Order Value must include material plus additional charges via hidden
  `formula_mkncjq9`, mirrored through children to the parent. A material-only
  parent mirror is a wiring discrepancy, not a second approved definition.

These choices no longer await a definition decision. Source/deployment alignment,
reportable-population and membership evidence, revised reference results and
version-bound certification still remain. The supplied board schema's saved
parent mirror targets material only; live wiring needs verification. Existing
`current_projects` narrows the retained population to active records and must not
replace it in the analyst. Operational lifecycle checks remain separate.

The findings and test counts below describe the 7 October baseline. They have not
been rerun for this correction. Preserve sealed answers and saved historical
populations; use a new reviewed dataset/catalogue version for corrected results.

## Existing evidence and remaining work

The remaining engineering work now has executable tooling under
[`services/bi_analyst/tests/evals`](../services/bi_analyst/tests/evals/README.md).
The [certification runbook](bi-analyst-phase1-certification.md) describes its use.

| Requirement | Implemented | Remaining evidence |
|---|---|---|
| Database inventory | TEST catalog and separate live reader audit; definitions, grants, RLS policies, owners, indexes, role memberships, function signatures/configuration and observed connections | Render configuration, deployed commits, complete connection allocation |
| Metric reconciliation | Exact-ID selections across 16 categories, stored project/child/hidden values, preserved multiplicity, writer source hashes and function locations | Current Monday typed source evidence and population-wide metric-owner review |
| Coverage | Frozen classifications, archive coverage, missing relationships and discrepancies carried into review | Resolve findings or approve explicitly labelled limitations |
| Freshness | Separate ingestion, worker-success and snapshot evidence; distinct rollup/refresh reviews | Successful deployed operations tied to code versions and approved age limits |
| Reference answers | Existing 70 cases, 50 queries and 21 frozen tables preserved and re-verified | Independent question/answer review and same-snapshot Power BI outputs |
| Power BI | Both supplied PBIX files inspected: 68 measures, M sources, calculated expressions, relationships, report layouts/filters and refresh metadata | Supplied reports do not cover all five core metric contracts |
| Decisions and gate | Evidence-bound review register, numeric performance targets, connection allocations, blocked/pass report | Business/platform decisions and owner sign-offs |

## Findings from the supplied Power BI reports

Power BI is connected to the **live database**, as confirmed by the user. The
`PG_*` reader audit targeted that live source. The `TEST_SUPABASE_*` checks targeted
the separate frozen evaluation dataset. Live report results are not expected to
equal TEST answers unless their underlying data and reporting context are aligned.

The Budget report contains six pages and 33 measures. It reads monthly/project
forecasts, budget combination/comparison views and weighted enquiries. The enquiry
M query contains a Python forecasting step; the inspector records its text without
running it. `Actual Weighted Enquiry Value` filters `series_type = "Actual"` and
must not become the raw, unweighted New Enquiry Value contract.

The Smoothing report contains nine pages and 35 measures. Its sources are monthly
forecast, monthly smoothed revenue and project smoothing-score views.
`Smoothed Forecast Value` sums `allocated_expected_value`, consistent with the
plan's additive smoothing requirement. Average probabilities and expected spread
days are predictive measures, not observed conversion or gestation.

Imported-partition metadata records refreshes on **29 April 2026** and
**23 June 2026**, respectively. The extracted values carry no timezone. These
are PBIX metadata timestamps, not ingestion freshness; cached results cannot
verify the frozen 7 October dataset. They describe the supplied local files, not
the latest refresh of the live-connected reports or published Power BI service.
Calendar-derived columns also do not certify
an organisation-wide fiscal calendar. Currency/tax/access decisions remain open.

A direct `powerbi_reader` login using the supplied `PG_*` settings completed the
read-only source audit against the live database. Of 13 checked sources:

- Readable: `vw_actual_revenue_monthly_v1`,
  `vw_pipeline_forecast_monthly_12m_v1`, and
  `vw_pipeline_smoothed_revenue_monthly_12m_v1`.
- Ten returned SQLSTATE `42501` (insufficient privilege), including five of the
  seven unique sources used by the PBIX files. Several have explicit SELECT
  grants, so inspecting grants alone does not establish usable access through
  their dependency chains.

Failures are recorded per relation using savepoints. The audit does not grant
access or weaken view/RLS settings. The platform owner must resolve the access
design before relying on current report refreshes or reusing this reader role.

## Source and deployment assessment

Ordinary sync already delegates parent refresh to `archive.refresh_parents` when
archive processing is enabled; otherwise it uses persisted-child rollups.
Comparison/archive workflows preserve the configured parent mirror and verify
current membership. This describes existing behaviour, not proof that the mirror
implements the required material-plus-charges formula. Reconcile its live chain
and all writer paths against the corrected contract. Local code cannot establish
deployed flags or versions.
The gate rejects observed mixed archive writer modes and inconsistent reporting
flags, and requires a reviewed source-precedence contract covering manual writers.

The revenue implementation/population mismatch, invoice/enquiry discrepancies,
gestation fallback cases and archive coverage remain findings. Placeholder review
now defines analyst exclusions, but its deployed decisions, automatic re-entry
and archived-history retention require verification. Sample success cannot settle
these for every company record.
No financial value or sealed answer has been changed to force parity.

Phase 1's performance work is to agree measurable targets and a workload. Seven
query plans are captured. The gate requires numeric targets and a connection
allocation including replicas, both read/state pools, ETL, other clients, reserved
slots and headroom; it does not claim unmeasured latency or cost achievements.

The plan defers Microsoft sign-in while retaining later authentication/ownership
requirements. Before Phase 3, the access decision must identify how Version 1
will authenticate permitted users.

## Validation and artifacts

59 offline tests passed, covering certification failures, freshness, conflicting
writers, budgets, provenance, Decimal parity, duplicates, NULL/zero, credential
isolation and artifact integrity. The TEST capture passed all 50 reference queries,
21 table fingerprints, 17 independent checks and 11 access checks.
All 37 public view definitions present in both the frozen inventory and live reader
inventory matched. This establishes SQL-definition parity, not result freshness
or permission to read every view.

Private artifacts under `outputs/bi_analyst_evals/`:

- `phase1_20261007_v2/evidence.json`: final TEST evidence with report/reader
  attachments and copied-versus-live view-definition comparisons.
- `phase1_20261007_v2/review.json`: pending owner decision and sign-off register.
- `phase1_20261007_v2/review_result/phase1_gate.md`: outstanding evidence report.
- `powerbi_20261007/pbix_inventory.json`: extracted model/report definitions.
- `powerbi_reader_20261007_v2/powerbi_reader_inventory.json`: live reader audit.

These private artifacts remain git-ignored. The PBIX inputs, TEST data/answer key,
production grants and source financial rows were not modified.
