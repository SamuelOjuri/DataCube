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
