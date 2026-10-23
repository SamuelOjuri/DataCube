export function comparisonFixture() {
  const pairing = {current_month: 'current_period_month', baseline_month: 'prior_period_month',
    current_value: 'current_period_revenue', baseline_value: 'prior_period_revenue', kind: 'year_over_year', completed_months_only: true};
  const columns = [
    {name: 'fiscal_month_label', label: 'Month', role: 'dimension', type: 'text', unit: 'none'},
    {name: pairing.current_month, label: 'Current period month', role: 'dimension', type: 'date', unit: 'none'},
    {name: pairing.current_value, label: 'Current period revenue', role: 'measure', type: 'decimal', unit: 'source_currency'},
    {name: 'current_invoices', label: 'Invoices', role: 'measure', type: 'integer', unit: 'count'},
    {name: pairing.baseline_month, label: 'Prior period month', role: 'dimension', type: 'date', unit: 'none'},
    {name: pairing.baseline_value, label: 'Prior period revenue', role: 'measure', type: 'decimal', unit: 'source_currency'},
  ];
  const amounts = ['709552.98', '288516.49', '301628.84', '315210.96', '590618.99', '565749.51',
    '581927.47', '560408.56', '925610.59', '584472.45', '610000.00', '0.00'];
  const prior = ['737328.31', '692422.80', '744642.04', '597960.34', '767563.13', '372626.34',
    '443280.24', '380468.61', '656219.52', '536986.50', '580000.00', '640000.00'];
  const rows = amounts.map((value, i) => {
    const month = new Date(Date.UTC(2025, 10 + i, 1)), baseline = new Date(Date.UTC(2024, 10 + i, 1));
    return [month.toLocaleDateString('en-GB', {month: 'short', timeZone: 'UTC'}), month.toISOString().slice(0, 10), value,
      40 + i, baseline.toISOString().slice(0, 10), prior[i]];
  });
  return {result: {id: '20000000-0000-4000-8000-000000000001', run_id: '30000000-0000-4000-8000-000000000001',
    columns: columns.map(c => c.name), rows, provenance: {kind: 'exploratory', title: 'Monthly invoiced revenue comparison', columns,
      sql: 'SELECT 1', relations: ['invoice_reporting_facts_v1'], catalogue_version: '1.1.0', query_reference: 'synthetic-comparison',
      business_timezone: 'Europe/London', freshness: {status: 'unknown', queried_at: '2026-10-23T12:00:00Z', limitation: 'Source freshness is unknown.'},
      dataset_scope: 'complete', truncated: false, returned_rows: 12, caveats: [], limitations: []}},
    intent: {kind: 'line', x: pairing.current_month, series: [pairing.current_value, pairing.baseline_value], comparison: pairing},
    summary: "Monthly values for revenue were lower than a year earlier from November 2025 to March 2026, then higher than a year earlier from April 2026 to September 2026. In September 2026, it was £610,000.00, £30,000.00 higher than £580,000.00 in September 2025. The comparison covers November 2025 to September 2026, compared with November 2024 to September 2025."};
}
