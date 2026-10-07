# Phase 1 evaluation dataset: bi_eval_20261007_v1

Created on 7 October 2026 in **TPID Data Cube – TEST**, using only the
`TEST_SUPABASE_*` connection settings. Production was not contacted. Existing test
source tables, schedules, financial values and reporting views were not changed.

The dataset is ready for reference-query and evaluation development. It is **not
business-certified**, and its creation does not complete the Phase 1 exit gate.

## Dataset and answer key

| Item | Recorded value |
|---|---|
| Frozen data schema | `bi_eval_20261007_v1` |
| Restricted answer-key schema | `bi_eval_20261007_v1_key` |
| Reader capability | `bi_eval_20261007_v1_reader` (NOLOGIN, restricted SELECT) |
| Snapshot transaction started | 7 October 2026, 15:38:29 Europe/London (14:38:29 UTC) |
| Fixed reporting date | 7 October 2026 |
| Evaluation timezone | Europe/London; business-owner confirmation pending |
| Original clone backup timestamp | Not supplied; must be recorded by the project owner |
| Frozen tables | 21, including separate synthetic fixtures and copied baseline view results |
| Raw projects | 15,232 |
| Reportable projects | 15,205; the deployed view excludes 27 projects |
| Subitems | 37,796 |
| Hidden items | 38,022 |
| Copied analysis rows | 15,230, with numerical predictions and provenance only |
| Questions | 70: 40 core metric cases, 10 follow-ups, 10 ambiguous cases, 10 synthetic cases |
| Independent reference SQL | 50 queries; follow-up turns reuse the corresponding reference answers |

The source inventory includes the restored database's relation definitions,
functions relevant to reporting, columns, ownership, grants, indexes, RLS,
extensions and triggers. No `pg_cron` or `pg_net` extension was installed in the
inspected TEST database. This does not inspect live Render configuration.

See the [evaluation runbook](../services/bi_analyst/tests/evals/README.md),
[question list](../services/bi_analyst/tests/evals/questions.md),
[reference SQL](../services/bi_analyst/tests/evals/reference.sql), and
[synthetic reference SQL](../services/bi_analyst/tests/evals/fixture_reference.sql).
The [local manifest](../outputs/bi_analyst_evals/bi_eval_20261007_v1/manifest.json)
contains transaction provenance and per-table SHA-256 fingerprints.

Numerical expected answers, their SQL and PostgreSQL result types are held in
`bi_eval_20261007_v1_key.artifacts`, under the `expected` record. The reader cannot
access that schema. Financial result rows are not exported into source control.
This remains sensitive copied business data; the synthetic cases are the only
invented records and never enter business totals.

## Verification completed

- All 50 reference queries matched their stored expected answers from a fresh
  connection, running as the restricted reader.
- All 21 frozen table fingerprints and row counts matched.
- Seventeen independent result checks passed: seven Python Decimal/count checks
  across parent totals, conversion cohorts and gestation means, plus ten
  hand-calculated synthetic cases.
- Eleven access checks passed, including blocked source/answer-key access for the
  reader and denied schema access for PUBLIC-facing Supabase roles.
- Five live protection probes passed: source read denied, answer-key read denied,
  reader update denied, and owner-level zero-row updates rejected by the frozen
  data and answer-key mutation guards.
- Eighteen offline tests passed, covering target isolation, identifier validation,
  question/reference completeness, decimals, date serialization and safe imports.

The future application does not exist yet. No model conversations, frontend,
per-user ownership rules, or end-to-end authentication were evaluated. The
question pack defines expected behaviour for those later implementation phases.

## Findings requiring review

These are findings in the restored TEST data, not assertions about today's
production state. Stored-versus-recomputed differences may have valid membership,
source-precedence or fallback explanations and must not trigger automatic repairs.

| Finding | Evidence | Required next decision |
|---|---|---|
| Monthly revenue contract differs from the deployed view | The copied view sums positive dated child invoices without the plan's reportable-parent and closed-invoiced-stage filters. Plan-versus-view results differ in 37 months. | BI/business owner to reconcile the deployed SQL, Power BI DAX and intended reporting definition. Both calculations are preserved. |
| Project invoice totals differ from all stored-child sums | 961 reportable projects differ; 60 reportable projects have no stored children. | Reconcile current membership, blank/zero treatment and source-authoritative writer paths. These counts do not prove all 961 stored totals are wrong. |
| Enquiry reconciliation remains incomplete | 28 Open parents differ from the all-stored-child formula sum; two children differ from the exact-reason quote formula. | Check current API-active membership and source evidence. Won/Lost retained enquiry values are not automatically recalculated. |
| Actual gestation differs from date subtraction | 17 of 902 projects with both dates differ. | Review stored actual/source fallback semantics before changing any value. |
| Archive coverage is incomplete | 11,264 unverified reportable projects, 11 unverified current-value cases, and 15 unresolved archive jobs. The same coverage checks report zero unverified subitems and hidden sources within their scoped verified-active population. | Do not certify company-wide verified-active reporting from this snapshot or substitute `current_*` for every historical population. |
| Relationships need explicit treatment | 23 children have no hidden-source link; 25 hidden sources are reused. No stored child has a missing parent or a non-null link to an absent hidden row. | Preserve known missing links and verified source multiplicity; do not silently deduplicate financial contributions. |
| A reporting classification remains under review | One `needs_review` classification; the deployed reportable view excludes 27 records. | Business/data owner to resolve the review with source evidence. |

Gross enquiry and bookings reference calculations match their copied monthly
views with zero differing months. Recomputed five-year and two-year conversion
cohort counts match the copied materialised counts: 7,866 and 2,753 respectively.
Those checks establish parity with this copied baseline, not full metric accuracy.

The snapshot preserves the published three-decimal conversion ratios and the
existing lower-bound-only cohorts. It uses positive stored actual gestation for
historical means/percentiles. The hidden-order reference is explicitly a subtotal
of records with both numeric inputs, accompanied by an incomplete-input count;
it is not presented as a certified full hidden-board total.

## Freshness and decisions still outstanding

Copied worker evidence records a successful conversion-view refresh at
7 October 2026, 01:00:07 Europe/London. The latest copied forecast snapshot date
is 6 October 2026. One recent-rehydrate completion is later than its last recorded
success. Full copied freshness records are retained in the answer key and the
[aggregate review report](../outputs/bi_analyst_evals/bi_eval_20261007_v1/review_summary.json).
An evaluation capture timestamp is not an ingestion/rollup timestamp or proof of
the original backup time.

| Decision/evidence | Status | Owner |
|---|---|---|
| Original Supabase backup timestamp | Pending | Platform owner |
| Source-authoritative reconciliation for all five metrics | Pending | BI/data engineer and metric owner |
| Power BI comparison on the same frozen data and filters | Pending; no PBIX/DAX/reference exports supplied | BI report owner |
| Currency, tax basis and fiscal calendar | Pending; no assumptions used to answer fiscal questions | Finance/business owner |
| Business timezone | Europe/London used reproducibly for this evaluation; confirmation pending | Business owner |
| Per-user access scope, provider data handling and retention | Pending; this snapshot is restricted to evaluation readers/admins | Platform/business owner |
| Deployed Render writer paths and archive feature flags | Not inferred from local `.env` or the copied database | Platform owner |
| Formal question and expected-result review | Pending; development/holdout scenarios are labelled | Independent BI/business reviewer |

Approve a new version for corrected definitions or data. The existing answer key
is sealed and must not be overwritten merely because a later implementation
produces different results.
