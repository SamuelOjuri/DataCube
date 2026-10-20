# Analyst business decisions: 8 October 2026

Source: Owner's instruction in the implementation conversation on 8 October 2026.
Currency/timezone recorded at 12:36:43 UTC (13:36:43 Europe/London); fiscal/tax
clarifications recorded at 12:39:35 UTC (13:39:35 Europe/London). These
instructions approve business decisions; they do not attest source reconciliation
or deployment.

| Decision | Owner instruction | Recorded value | Review status |
|---|---|---|---|
| Currency | "Currency is British Pounds" | `GBP` (British pounds sterling) | Approved |
| Business timezone | "Business timezone is timezone for London United Kingdom" | `Europe/London`, including its seasonal clock changes | Approved |
| Fiscal calendar | Clarified: "1 November-31 October (year ends in October)" | 1 November through 31 October | Approved |
| Tax basis | Clarified: "As stored; VAT inclusion unspecified" | UK tax jurisdiction; present amounts as stored, with VAT inclusion unspecified | Approved |

The fiscal clarification supersedes the initial "September to October" wording.
The tax decision resolves presentation policy by preserving source values without
claiming they include or exclude VAT. It does not approve a tax rate, tax
calculation or conversion of stored amounts.

The current owner review is
[`phase1_capture/review.json`](../outputs/bi_analyst_evals/reference_1_1_20261008/phase1_capture/review.json).
Approved decisions use the existing gate's `approved` status, which resolves
their decision blockers. Historical evidence and reference snapshots are retained.

The historical four-decision gate is
[`phase1_owner_decisions_20261008/phase1_gate.md`](../outputs/bi_analyst_evals/reference_1_1_20261008/phase1_owner_decisions_20261008/phase1_gate.md),
with machine-readable output in the adjacent `phase1_gate.json`. Both it and
`phase1_pending` are superseded by the [full owner closure](bi-analyst-phase1-closure.md).
The current `phase1_closed_20261008/phase1_gate` is `closed_by_owner` with no
Phase 1 blockers. Power BI verification and Monday CRM cleanup are not prerequisites.
This decision update does not rewrite the sealed catalogue, source values or reference
dataset, enable fiscal query support, or activate metric execution.

## Derived project fields in archive mode: 20 October 2026

Source: Sam's instruction in the implementation conversation, recorded at
12:24:42 UTC (13:24:42 Europe/London) on 20 October 2026:

> I approve the derived project fields rule for archive mode, as implemented in commit 75484d3:
>
> 1. `first_date_designed`, `last_date_designed`, `first_date_invoiced`, `last_date_invoiced` and `date_order_received` are derived from the project's stored subitems. A stored value is never replaced by a missing one.
> 2. Subitems recorded as archived or deleted are excluded; subitems without a verified state are included.
> 3. Stored `gestation_period` is 0 when a project is designed but not yet invoiced, otherwise the days from first design to first invoice (never negative). Projects without a design date keep their stored value.

| Decision | Recorded value | Review status |
|---|---|---|
| Writer/source precedence for derived project fields | In archive mode, [project_rollups.py](../src/services/project_rollups.py) is the only writer of the five date fields and `gestation_period`. It runs after subitems are saved; the ordinary project upsert no longer writes these fields. Precedence for other fields is outside this approval. | Approved |
| Children used | Subitems recorded as archived or deleted are excluded; subitems without a verified lifecycle state are included. A field only changes when a live subitem supplies a value. | Approved |
| Gestation source rule | 0 when designed but not yet invoiced; otherwise days from first design to first invoice, never negative. Projects without a design date keep their stored value. | Approved |

These are recorded in the owner register
[`phase1_capture/review.json`](../outputs/bi_analyst_evals/reference_1_1_20261008/phase1_capture/review.json)
as `writer_source_precedence` and the `gestation` metric, both `approved`.
The gestation metric formulas are unchanged: catalogue 1.1.0 still averages (or
takes the median of) positive stored `gestation_period` by creation cohort, so
the catalogue and its hash-bound [acceptance](../services/bi_analyst/bi_analyst/semantic/acceptance.json)
are not rewritten.

This approval covers the rule and its implementation. It does not attest that the
[derived-fields backfill](../scripts/backfill_project_derived_dates.py) has been
run. Values stored before 20 October keep their earlier source/fallback rules
until it is, so the `gestation_fallback` finding and its source sample stay open
until the backfill is applied and checked.
