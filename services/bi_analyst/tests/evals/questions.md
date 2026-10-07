# Phase 1 proposed evaluation questions

Dataset: `bi_eval_20261007_v1`. Reporting date: 7 October 2026. Timezone: Europe/London (evaluation assumption). Questions refer to the frozen dataset, not live company totals. Business/source and Power BI certification remain pending.

All answers must preserve the named metric, grain, population, period, units and material coverage limitations. Monetary values retain source currency/tax semantics pending confirmation. Stored blanks are not zero. Conversion results are fractions rounded to three decimals. Table answers have deterministic ordering. A top-ten answer is limited, not a full-population total.

The answer key and holdout cases must not be included in agent prompts. The development/holdout split is recorded in `cases.py` and the restricted database question artifact. These scenarios are proposed for reviewer approval.

| ID | Type | Question / conversation | Expected behaviour and SQL reference |
|---|---|---|---|
| Q001 | metric | What is the stored, unweighted New Enquiry Value across the frozen reportable projects, including its known-value count? | Answer with provenance; `enquiry_total` |
| Q002 | metric | What was gross New Enquiry Value last month under the completed-month actuals definition? | Answer with provenance; `enquiry_last_month` |
| Q003 | metric | What was gross New Enquiry Value in the month before last? | Answer with provenance; `enquiry_previous_month` |
| Q004 | metric | Show gross New Enquiry Value for each of the last 12 completed months, including empty months. | Answer with provenance; `enquiry_monthly` |
| Q005 | metric | Break stored, unweighted New Enquiry Value down by the recorded project category. | Answer with provenance; `enquiry_category` |
| Q006 | metric | Break stored, unweighted New Enquiry Value down by the recorded project type. | Answer with provenance; `enquiry_type` |
| Q007 | metric | Which ten reportable project IDs have the highest positive New Enquiry Value? | Answer with provenance; `enquiry_top` |
| Q008 | metric | How many reportable projects have blank, zero or negative New Enquiry Value? | Answer with provenance; `enquiry_missing` |
| Q009 | metric | What is the stored project-parent Order Value total for reportable projects, and how many values are known? | Answer with provenance; `order_parent_total` |
| Q010 | metric | For all frozen hidden-board rows, give the order subtotal where both material and additional charges are numeric, and count incomplete rows separately. | Answer with provenance; `order_hidden_complete` |
| Q011 | metric | What were project bookings last month, using the existing positive-value and won-stage rules? | Answer with provenance; `order_last_month` |
| Q012 | metric | What were project bookings in the month before last under those reporting rules? | Answer with provenance; `order_previous_month` |
| Q013 | metric | Show project bookings for each of the last 12 completed months, including empty months. | Answer with provenance; `order_monthly` |
| Q014 | metric | Break stored project-parent Order Value down by recorded category for reportable projects. | Answer with provenance; `order_category` |
| Q015 | metric | Which ten reportable project IDs have the largest positive stored parent Order Value? | Answer with provenance; `order_top` |
| Q016 | metric | How many reportable projects have blank, zero or negative stored parent Order Value? | Answer with provenance; `order_missing` |
| Q017 | metric | What is the stored project invoice total across reportable projects, without monthly revenue filters? | Answer with provenance; `invoice_parent_total` |
| Q018 | metric | What is total recorded Amount Invoiced across all frozen hidden-board rows, retaining signed amounts? | Answer with provenance; `invoice_hidden_total` |
| Q019 | metric | Sum the recorded child invoice mirrors linked to reportable projects, without business-status filtering. | Answer with provenance; `invoice_children_total` |
| Q020 | metric | What was monthly revenue last month under the plan definition: positive dated child invoices on reportable closed-invoiced parents? | Answer with provenance; `invoice_last_month` |
| Q021 | metric | What was revenue in the month before last under that plan definition? | Answer with provenance; `invoice_previous_month` |
| Q022 | metric | Show revenue for the last 12 completed months under the plan definition, including empty months. | Answer with provenance; `invoice_monthly` |
| Q023 | metric | Break stored project invoice totals down by recorded category for reportable projects. | Answer with provenance; `invoice_category` |
| Q024 | metric | How many hidden-board invoices are negative, blank or zero, and what is the signed negative subtotal? | Answer with provenance; `invoice_signed` |
| Q025 | metric | What is inclusive conversion for the existing five-year cohort, with wins and eligible counts? | Answer with provenance; `conversion_five_year` |
| Q026 | metric | What is inclusive conversion for the existing two-year cohort, with wins and eligible counts? | Answer with provenance; `conversion_two_year` |
| Q027 | metric | What is closed-only conversion for the existing five-year cohort? | Answer with provenance; `conversion_closed_five_year` |
| Q028 | metric | What is closed-only conversion for the existing two-year cohort? | Answer with provenance; `conversion_closed_two_year` |
| Q029 | metric | Break five-year inclusive conversion down by recorded category, showing each denominator. | Answer with provenance; `conversion_category` |
| Q030 | metric | Break five-year inclusive conversion down by recorded project type, showing each denominator. | Answer with provenance; `conversion_type` |
| Q031 | metric | Show counts by pipeline stage for the existing five-year cohort, including any unknown stage. | Answer with provenance; `conversion_stage_counts` |
| Q032 | metric | How many reportable projects have no creation date and therefore cannot enter the conversion cohorts? | Answer with provenance; `conversion_undated` |
| Q033 | metric | What is average stored actual gestation in the existing five-year cohort, excluding nonpositive and missing values? | Answer with provenance; `gestation_five_year` |
| Q034 | metric | What is average stored actual gestation in the existing two-year cohort, excluding nonpositive and missing values? | Answer with provenance; `gestation_two_year` |
| Q035 | metric | What is median positive stored actual gestation in the existing five-year cohort? | Answer with provenance; `gestation_median` |
| Q036 | metric | What are the 25th and 75th percentiles of positive actual gestation in the five-year cohort? | Answer with provenance; `gestation_percentiles` |
| Q037 | metric | Break average positive actual gestation in the five-year cohort down by recorded category. | Answer with provenance; `gestation_category` |
| Q038 | metric | Break average positive actual gestation in the five-year cohort down by recorded project type. | Answer with provenance; `gestation_type` |
| Q039 | metric | How many five-year cohort records have blank, zero or negative actual gestation? | Answer with provenance; `gestation_exclusions` |
| Q040 | metric | Which ten project IDs in the five-year cohort have the longest positive actual gestation? | Answer with provenance; `gestation_longest` |
| F001 | follow_up | What was gross New Enquiry Value last month under the completed-month actuals definition? **Then:** Now show the month before last, keeping the same gross enquiry definition. | Preserve unchanged scope; `enquiry_last_month` → `enquiry_previous_month` |
| F002 | follow_up | What were project bookings last month, using the existing positive-value and won-stage rules? **Then:** And the month before last, retaining the bookings rules? | Preserve unchanged scope; `order_last_month` → `order_previous_month` |
| F003 | follow_up | What was monthly revenue last month under the plan definition: positive dated child invoices on reportable closed-invoiced parents? **Then:** Now the month before last with the same parent-status and population filters. | Preserve unchanged scope; `invoice_last_month` → `invoice_previous_month` |
| F004 | follow_up | What is inclusive conversion for the existing five-year cohort, with wins and eligible counts? **Then:** Use the two-year cohort instead, keeping inclusive conversion. | Preserve unchanged scope; `conversion_five_year` → `conversion_two_year` |
| F005 | follow_up | What is inclusive conversion for the existing five-year cohort, with wins and eligible counts? **Then:** Switch to closed-only conversion but keep the five-year cohort. | Preserve unchanged scope; `conversion_five_year` → `conversion_closed_five_year` |
| F006 | follow_up | What is average stored actual gestation in the existing five-year cohort, excluding nonpositive and missing values? **Then:** Use the two-year cohort instead; keep actual gestation and the exclusions. | Preserve unchanged scope; `gestation_five_year` → `gestation_two_year` |
| F007 | follow_up | What is average stored actual gestation in the existing five-year cohort, excluding nonpositive and missing values? **Then:** Show the median instead, for the same eligible cohort. | Preserve unchanged scope; `gestation_five_year` → `gestation_median` |
| F008 | follow_up | What is the stored, unweighted New Enquiry Value across the frozen reportable projects, including its known-value count? **Then:** Break that same total down by recorded category. | Preserve unchanged scope; `enquiry_total` → `enquiry_category` |
| F009 | follow_up | What is the stored project-parent Order Value total for reportable projects, and how many values are known? **Then:** Show the ten project IDs contributing the largest positive values to that measure. | Preserve unchanged scope; `order_parent_total` → `order_top` |
| F010 | follow_up | What is the stored project invoice total across reportable projects, without monthly revenue filters? **Then:** Break that same stored project measure down by recorded category. | Preserve unchanged scope; `invoice_parent_total` → `invoice_category` |
| A001 | ambiguous | What is our order value? | Clarify: metric_scope, period. No numerical answer before resolution. |
| A002 | ambiguous | How much have we invoiced? | Clarify: grain, period, reporting_rules. No numerical answer before resolution. |
| A003 | ambiguous | What is conversion? | Clarify: cohort. No numerical answer before resolution. |
| A004 | ambiguous | How long do projects take? | Clarify: observed_or_predicted, cohort. No numerical answer before resolution. |
| A005 | ambiguous | How much enquiry do we have? | Clarify: gross_or_weighted, period. No numerical answer before resolution. |
| A006 | ambiguous | Show this financial year versus last year. | Clarify: metric, fiscal_calendar. No numerical answer before resolution. |
| A007 | ambiguous | Show order value for Acme. | Clarify: entity, metric_scope, period. No numerical answer before resolution. |
| A008 | ambiguous | Show the latest company-wide total including only verified active projects. | Clarify: metric, coverage. No numerical answer before resolution. |
| A009 | ambiguous | Show the trend. | Clarify: metric, period, grain. No numerical answer before resolution. |
| A010 | ambiguous | What is the value this month? | Clarify: metric_scope, current_period_definition. No numerical answer before resolution. |
| E001 | synthetic_edge | Calculate the complete-input hidden order subtotal and identify incomplete inputs in the synthetic fixture. | Synthetic fixture only; `fixture_charges` |
| E002 | synthetic_edge | Distinguish all-blank from numeric-zero child invoices in synthetic projects E2 and E3. | Synthetic fixture only; `fixture_blank_zero` |
| E003 | synthetic_edge | Sum all signed synthetic child invoices; also show the positive-only subtotal as a separate measure. | Synthetic fixture only; `fixture_signed` |
| E004 | synthetic_edge | Apply the exact New Enquiry reason rule to the synthetic child quotes. | Synthetic fixture only; `fixture_exact_enquiry` |
| E005 | synthetic_edge | Calculate inclusive conversion over all synthetic projects from summed counts. | Synthetic fixture only; `fixture_conversion` |
| E006 | synthetic_edge | Average positive synthetic actual gestation, retaining the eligible count. | Synthetic fixture only; `fixture_gestation` |
| E007 | synthetic_edge | Sum synthetic project order values once per parent, even when a parent has multiple children. | Synthetic fixture only; `fixture_join` |
| E008 | synthetic_edge | For synthetic source H1, show its hidden total once and the explicitly repeated total across its two links separately. | Synthetic fixture only; `fixture_repeated_source` |
| E009 | synthetic_edge | Calculate positive synthetic enquiry actuals for the last completed month, excluding the current month. | Synthetic fixture only; `fixture_completed_month` |
| E010 | synthetic_edge | Calculate closed-only conversion among synthetic Open Enquiry projects with no closed cases. | Synthetic fixture only; `fixture_zero_denominator` |
