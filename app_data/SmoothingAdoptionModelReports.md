Below are the most important Power BI reports to build for management and stakeholders now that the Smoothing Adoption Model is live. The theme is: keep the existing pipeline forecast intact, then add smoothing as a timing/risk lens that explains how much revenue may move across months.

**1\. Executive Forecast Overview** Purpose: give leadership one page showing current forecast value, smoothed timing impact, and risk.

Core visuals:

- KPI cards:  
    
  - Current unsmoothed forecast value  
  - Smoothed monthly forecast value  
  - Difference versus standard pipeline forecast  
  - Open pipeline smoothed allocation  
  - Committed smoothed allocation  
  - Count of High / Very High smoothing-risk projects


- Line or clustered column chart:  
    
  - Month on X-axis  
  - Standard pipeline expected value  
  - Smoothed allocated expected value  
  - Optional committed/open split


- Stacked monthly bar:  
    
  - `unsmoothed_expected_value`  
  - `smoothed_expected_value`  
  - Shows how much of each month’s total is staying in base month versus being spread over time.

Primary sources:

- `mv_pipeline_forecast_monthly_12m_v1`  
- `mv_pipeline_smoothed_revenue_monthly_12m_v1`

Management question answered:

“What does our 12-month forecast look like after realistic invoice timing?”

**2\. Smoothing Impact Bridge** Purpose: explain movement from the original pipeline timing into smoothed revenue timing.

Core visuals:

- Waterfall:  
    
  - Starting point: standard monthly expected value  
  - Negative movement: value smoothed out of the base month  
  - Positive movement: value smoothed into later months  
  - Ending point: smoothed allocated forecast


- Matrix by month and stage bucket:  
    
  - Standard expected value  
  - Smoothed allocated value  
  - Difference  
  - Difference %


- Conditional formatting:  
    
  - Red for months where smoothing reduces near-term forecast materially  
  - Green for months gaining deferred value

Primary sources:

- `mv_pipeline_forecast_monthly_12m_v1`  
- `mv_pipeline_smoothed_revenue_monthly_12m_v1`  
- optionally `pipeline_smoothing_forecast_snapshot` for historical bridge views

Management question answered:

“Which months are most exposed to invoice timing slippage?”

**3\. High-Risk Smoothing Projects** Purpose: give commercial/ops teams an actionable list of projects likely to invoice over a longer spread.

Core visuals:

- Table of projects:  
    
  - `project_id`  
  - `project_name`  
  - `account`  
  - `stage_bucket`  
  - `expected_value`  
  - `combined_smoothed_probability`  
  - `expected_spread_days`  
  - `risk_band`  
  - `workbook_suggested_treatment`  
  - `default_smoothing_recommended`  
  - `adoption_recommendation`


- Filters:  
    
  - Risk band  
  - Stage bucket  
  - Account  
  - Category  
  - Type  
  - Product  
  - Forecast month


- Highlight cards:  
    
  - Very High risk project count  
  - High risk project count  
  - Total expected value in High / Very High  
  - Average expected spread days

Primary source:

- `vw_pipeline_smoothing_score_v1`

Management question answered:

“Which specific projects need review because their invoice timing is likely to spread?”

**4\. Smoothing Risk by Segment** Purpose: show management which parts of the business structurally drive invoice-spread risk.

Core visuals:

- Bar charts by:  
  - Account  
  - Product type  
  - Category  
  - Type  
  - Stage bucket

Metrics:

- Average `combined_smoothed_probability`  
    
- Average `expected_spread_days`  
    
- Count of projects  
    
- Expected value  
    
- High / Very High risk value  
    
- Scatter plot:  
    
  - X-axis: average expected spread days  
  - Y-axis: combined smoothing probability  
  - Bubble size: expected value  
  - Legend: segment type or risk band

Primary sources:

- `vw_pipeline_smoothing_score_v1`  
- optionally `mv_invoice_smoothing_signal_v1` for historic signal strength

Management question answered:

“Which accounts/products/categories are most associated with spread-out invoicing?”

**5\. Forecast Confidence and Signal Quality** Purpose: help stakeholders understand whether smoothing scores are based on strong historic evidence or global fallback.

Core visuals:

- Matrix by dimension:  
    
  - Category confidence  
  - Type confidence  
  - Product confidence  
  - Account confidence  
  - Mature project counts  
  - Signal fallback flags


- Cards:  
    
  - Projects using any global fallback  
  - Projects with Very low confidence in one or more dimensions  
  - Average mature project count by dimension


- Bar chart:  
    
  - Count of projects by confidence band: High, Medium, Low, Very low

Primary source:

- `vw_pipeline_smoothing_score_v1`  
- `mv_invoice_smoothing_signal_v1`

Management question answered:

“How reliable are these smoothing recommendations, and where are we relying on sparse history?”

**6\. Snapshot Trend Report** Purpose: track how smoothing exposure changes over time.

Core visuals:

- Line chart by snapshot date:  
    
  - Total allocated expected value  
  - Smoothed allocated value  
  - Unsmoothed allocated value  
  - High / Very High risk value


- Monthly snapshot heatmap:  
    
  - Snapshot date on rows  
  - Forecast month on columns  
  - Allocated expected value or delta from previous snapshot


- Stage trend:  
    
  - Open/Committed/Lost smoothed allocation over snapshot dates

Primary source:

- `pipeline_smoothing_forecast_snapshot`

Important modelling note:

Use `allocated_expected_value` for additive totals, not `expected_value`, because smoothing snapshot rows are project-month rows and `expected_value` repeats per project-month.

Management question answered:

“Is our invoice timing exposure improving or worsening over time?”

**7\. Actuals Versus Smoothed Forecast** Purpose: once enough history accumulates, compare forecasted monthly allocation against actual invoiced revenue.

Core visuals:

- Line chart:  
    
  - Actual revenue  
  - Standard forecast expected value  
  - Smoothed allocated expected value


- Variance cards:  
    
  - Actual minus standard forecast  
  - Actual minus smoothed forecast  
  - WAPE / forecast error  
  - Bias ratio


- Table by month:  
    
  - Forecast snapshot date  
  - Forecast month  
  - Actual invoiced amount  
  - Standard expected value  
  - Smoothed allocated value  
  - Variance

Primary sources:

- `pipeline_smoothing_forecast_snapshot`  
- `pipeline_forecast_snapshot`  
- actual revenue view/table, likely `vw_actual_revenue_monthly_v1` or subitem invoice actuals

Management question answered:

“Does smoothing improve forecast accuracy against actual invoicing?”

**8\. Adoption Recommendation Dashboard** Purpose: turn the model into a practical operating workflow.

Core visuals:

- Funnel or grouped cards:  
    
  - Adopt smoothing by default  
  - Model smoothing scenario  
  - Commercial review / light smoothing  
  - No default smoothing


- Table:  
    
  - Project  
  - Account  
  - Stage  
  - Expected value  
  - Risk band  
  - Expected spread days  
  - Recommendation  
  - Reason fields from dimension rates/spreads


- Slicers:  
    
  - Sales rep  
  - Account  
  - Product  
  - Stage  
  - Risk band  
  - Forecast month

Primary source:

- `vw_pipeline_smoothing_score_v1`

Management question answered:

“Which projects should we treat differently in forecast reviews?”

**Suggested Report Pack Structure** I’d package this into 5 core Power BI pages first:

1. Executive Overview  
2. Monthly Forecast Impact  
3. High-Risk Projects  
4. Segment and Signal Quality  
5. Snapshot Trends

Then add the Actuals Versus Smoothed Forecast page once enough snapshot history and invoice actuals have accumulated.

**Most Important Fields** For current monthly reporting:

- `mv_pipeline_smoothed_revenue_monthly_12m_v1.allocated_expected_value`  
- `mv_pipeline_smoothed_revenue_monthly_12m_v1.smoothed_expected_value`  
- `mv_pipeline_smoothed_revenue_monthly_12m_v1.unsmoothed_expected_value`

For project explanation:

- `vw_pipeline_smoothing_score_v1.expected_value`  
- `combined_smoothed_probability`  
- `expected_spread_days`  
- `risk_band`  
- `default_smoothing_recommended`  
- `adoption_recommendation`  
- dimension rates/spreads/confidence fields

For historical snapshot reporting:

- `pipeline_smoothing_forecast_snapshot.allocated_expected_value`  
- `smoothed_allocated_value`  
- `unsmoothed_allocated_value`  
- `snapshot_date`  
- `forecast_month`  
- `stage_bucket`  
- `risk_band`

Avoid summing `pipeline_smoothing_forecast_snapshot.expected_value` across snapshot rows. Use `allocated_expected_value` for totals.  