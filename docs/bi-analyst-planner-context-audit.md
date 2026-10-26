# Planner business-context audit

Audited on 24 October 2026 against the owner-accepted catalogue 1.1.0. Planner
prompt version: `bi-conversation-1.2.0`.

The previous planning payload supplied names, aliases, periods, dimensions,
status filters and limitations, but omitted most calculation semantics, source
precedence, population descriptions and compiler capabilities. That omission
was reproduced in the five-year closed-only conversion question: the model
requested generic scope clarification before querying any data.

`workflow/planning_context.py` now builds the context from the permitted metric
entries, their relations and populations, runtime owner notices, existing column
notes and compiler rules. The catalogue and its acceptance fingerprint are
unchanged. Only authorised entries and their dimensions, populations and periods
are supplied.

Each entry includes the definition and source precedence; full calculation and
NULL/zero/negative/empty rules; source grain and key; date basis and status rules;
unit and precision; supported grains, filters, ordering and shares; available
result fields; examples and limitations. Shared context includes exact
classification labels, population descriptions, business timezone, resolved
period boundaries and the row limit. Execution resolves periods again at query
time.

| Defined metric | Business distinction preserved |
| --- | --- |
| `new_enquiry_value` | Stored unweighted parent enquiry; retain source precedence for Open versus Won/Lost projects. Signed values and unknowns remain distinct. |
| `order_parent_value` | Configured stored parent mirror; preserve recorded contributions rather than replace them with hidden-board totals. |
| `order_hidden_complete_subtotal` | Independent hidden inventory; material plus additional charges only when both inputs are numeric. Show incomplete-input coverage. |
| `invoice_project_value` | Signed derived parent invoice total; distinguish current membership/source precedence from persisted-child sums. |
| `invoice_hidden_value` | Signed hidden-board Amount Invoiced; no project allocation is implied. |
| `invoice_stored_child_value` | Signed persisted-child mirrors for reportable parents; preserve recorded multiplicity. |
| `enquiry_monthly_actual` | Positive creation-month enquiry in completed months; no category/type filtering or breakdown in this aggregate. |
| `bookings_monthly_actual` | Positive order-received-month bookings with the three listed won stages; no category/type filtering or breakdown in this aggregate. |
| `invoice_monthly_actual` | Positive dated child invoices for retained reportable parents at all business stages; completed months; category/type grouping is supported. |
| `conversion_five_year` | Invoiced wins divided by all eligible projects in the five-year creation cohort. |
| `conversion_two_year` | The same inclusive definition in the two-year creation cohort. |
| `conversion_closed_five_year` | Invoiced wins divided by wins plus losses in the five-year creation cohort; open projects excluded from the denominator. |
| `conversion_closed_two_year` | The same closed-only definition in the two-year creation cohort. |
| `gestation_five_year` | Mean positive stored actual design-to-first-invoice gestation in the five-year creation cohort. |
| `gestation_two_year` | The same actual mean in the two-year creation cohort. |
| `gestation_median_five_year` | Median positive stored actual gestation in the five-year creation cohort. |

Creation cohorts retain the accepted lower-bound-only rule, including future
creation dates. Genuine archived projects remain reportable. Currency is GBP
with VAT inclusion unspecified. Hidden inventory, verified-active diagnostics
and historical snapshots are not interchangeable populations.

Group comparisons use the requested dimension and values. Baseline comparisons
use the same metric and one of that metric's supported periods. Source-row and
known-value counts are available with every result; conversion additionally
provides its numerator and denominator. Source rows follow source grain and are
not automatically distinct project counts. Shares are opt-in and require an
additive grouped measure.

Unambiguous requests outside the metric-plan capabilities route to exploration:
project rankings, account/product grouping, unsupported monthly breakdowns,
custom dates or percentiles, different cohort metric comparisons, alternative
date bases and signed monthly invoice analysis. This routing does not promise
that the governed data can answer them. Predictions, historical snapshots and
verified-active-only requests must not silently become observed retained-history
answers. Genuinely ambiguous definitions or periods still require clarification.

The 16-entry context is 32,480 bytes using the provider's ASCII JSON
encoding. An 8,000-character ASCII question, three 2,000-character ASCII replies
and a representative prior plan fit within the 64 KB provider input limit.
The existing size guard remains in force for larger encoded payloads.

The broader benchmark also caught an unrequested share calculation on a top-category
ranking. Shares are now explicitly opt-in. Comparison checks accept inherited
and explicitly identical filters as equivalent, while retaining the distinction
between an inherited filtered baseline and an explicitly unfiltered baseline.

Automated validation passed: 495 tests across API, semantic contracts and
benchmark validation. Another 244 checks were skipped because an isolated
PostgreSQL test database or opt-in live-source configuration was not enabled.
The capability checks exercise all 16 metrics against the actual compiler;
separate tests cover population filtering, exact business date meanings,
conversion denominators, actual gestation rules and prompt size.

All 59 final live planning cases passed, covering all 16 metric variants,
group comparisons, monthly trends, source date bases, opt-in shares, ranking,
period comparisons, ambiguity, unsupported requests and follow-up preservation.
The final live planning benchmark is recorded in
`docs/bi-analyst-planner-context-evaluation.json`. It sends synthetic questions and
catalogue metadata only, with no CRM records or database queries. It validates
plan selection and scope, rather than certifying new business calculations.

These are local changes. Deploy the analyst backend to use prompt 1.2.0, then
submit a new run. A previously paused run retains its recorded workflow version
and should not be used to validate the updated planner. No database migration
is needed for this context change.

## Clarification-loop correction, 26 October 2026

The saved run for "What is our conversion rate?" recorded the same two-year
inclusive option twice. It made three planning calls and no tool calls, returning
the metric clarification each time. The resume requests and checkpointed replies
were intact; the planner continued treating the original question as ambiguous.
Previously it received a flat list of reply strings without their clarification
questions or offered options, and exact option selections were not resolved by
the server before replanning.

The graph now checkpoints each reply with its clarification. Exact metric-option
matches are checked against the current permitted catalogue and narrow the next
planning context to that selected metric. Free-text replies retain the full
permitted context. The planner prompt explicitly combines the original request
with these responses, derives a unique supported period from a selected metric,
and retains requested filters, breakdowns and comparisons.

Validation: 112 workflow, provider-contract and exploration tests passed against
an isolated local PostgreSQL fixture. Added cases cover all four conversion
choices, a filtered type breakdown, restart/resume, idempotent resubmission,
case/whitespace matching and permission/option checks. Two live planner replays
produced `conversion_two_year` with `two_year_cohort`, including the requested
New Build filter and type breakdown in the second replay. The live replays did
not execute analytical queries or modify saved workflow records.

The initial correction used graph `2.5.0` and planner prompt
`bi-conversation-1.3.0`. Before deployment, a new question that explicitly names
the two-year inclusive conversion rate avoids this metric clarification path.

## Wider clarification audit, 26 October 2026

The follow-up audit found and corrected these additional gaps:

- A draft patch was discarded when the planner requested clarification. A
  free-text entity reply also replanned from the last completed answer instead
  of the current plan. A separate checkpointed working scope now retains the
  metric, period, filters, grain, breakdown, comparison and explicit comparison
  clearing. The request for causal explanation is retained across replanning.
- Period options combined periods from unrelated metrics. They now use the
  resolved metric's supported periods, or the remaining candidates when the
  metric is unresolved.
- Exact period choices could be omitted or contradicted by a model patch.
  Offered, permitted metric and period selections now populate the effective
  plan. A repeated question about a resolved choice triggers one bounded model
  repair; another repetition fails without executing a query or asking the user
  the same question again.
- An earlier exact metric selection could constrain a later free-text correction
  to the old metric. Free text now releases that constraint while preserving the
  working scope and clarification history for the planner to interpret.
- The exploration handoff omitted structured clarification history and the
  working scope. Both are now supplied to SQL generation with instructions to
  retain the selected definition, period and filters.
- Entity labels differing only by case could not be selected unambiguously.
  Literal matches now precede case-insensitive matches. The resolver accepts the
  same 256-character maximum as metric filters and offered candidates. Missing
  entity matches ask for a full name rather than offering an empty exact-choice list.

Existing protections were also checked: whitespace-only replies are rejected
by the shared contract before admission and do not consume a resume receipt.
Names are already normalized by the shared contracts and governed views;
no frontend change is needed for label padding.

All 163 focused clarification, workflow, provider-contract, exploration and
metric API tests passed against isolated local PostgreSQL. These include
restart/resume, baseline-only filter replacement, explicit period enforcement,
comparison clearing, correction of earlier choices, bounded repair, long and
case-only entity labels, blank-reply rejection, permissions, stale IDs,
cancelled runs and idempotent resubmission.

Five live synthetic cases passed: exact conversion choice with a type filter,
period clarification retaining a monthly breakdown and baseline, a free-text
entity correction retaining comparison filters, a later change to the conversion
definition, and clarified revenue scope passed to daily SQL generation. The SQL
passed the governed query validator; no live analytical SQL was executed.

Current versions are graph `2.6.0`, planner prompt `bi-conversation-1.4.0` and
exploration prompt `bi-exploration-1.8.0`. Deploy the backend, then
start a new run. Previously paused runs remain fenced to their recorded versions.
No database migration is required.
