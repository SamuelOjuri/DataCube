"""One-time authoring helper; generated catalogue and SQL comments are reviewed files."""
import ast
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
target = root / 'services/bi_analyst/bi_analyst/semantic/catalogue.json'
relations = []
def relation(id, sources, grain, key, columns, population='reportable', description=''):
    relations.append(dict(id=id, source_relations=sources, grain=grain, key=key,
        columns=dict(columns), population=population, description=description or grain))

project_cols = [('monday_id','text'),('pipeline_stage','text'),('category','text'),('type','text'),
 ('account','text'),('product_type','text'),('date_created','date'),('date_order_received','date'),
 ('first_date_designed','date'),('first_date_invoiced','date'),('new_enquiry_value','numeric'),
 ('total_order_value','numeric'),('total_amount_invoiced','numeric'),('gestation_period','int4')]
relation('projects_v1',['public.reportable_projects'],'one row per approved reportable project',['monday_id'],project_cols)
relation('children_v1',['public.subitems','analytics.projects_v1'],'one stored child of a reportable parent',['monday_id'],
 [(k,'text') for k in ['monday_id','parent_monday_id','hidden_item_id','reason_for_change']]+[(k,'numeric') for k in ['quote_amount','new_enquiry_value','amount_invoiced']]+[('invoice_date','date'),('formula_enquiry_value','numeric')])
relation('child_totals_v1',['analytics.children_v1'],'one pre-aggregated row per parent; diagnostic stored-child totals',['parent_monday_id'],
 [('parent_monday_id','text'),('child_count','int8'),('known_invoice_count','int8'),('child_invoice_amount','numeric'),('child_formula_enquiry_amount','numeric'),('missing_source_count','int8')])
relation('hidden_values_v1',['public.hidden_items'],'one hidden-board item; independent of project membership',['monday_id'],
 [('monday_id','text')]+[(k,'numeric') for k in ['cust_order_value_material','cust_additional_charges','amount_invoiced','complete_input_order_value']]+[('order_inputs_complete','bool')], 'hidden_inventory')
relation('latest_analysis_v1',['public.analysis_results','analytics.projects_v1'],'latest analysis per reportable project; timestamp DESC NULLS LAST, id DESC',['project_id'],
 [('project_id','text'),('analysis_id','uuid'),('analysis_timestamp','timestamptz'),('expected_conversion_rate','numeric'),('expected_gestation_days','int4')])
relation('enquiry_monthly_v1',['public.vw_actual_enquiry_monthly_v1'],'one completed calendar month',['enquiry_month'],
 [('enquiry_month','date'),('project_count','int4'),('actual_enquiry_value','numeric'),('actual_pipeline_value','numeric')])
relation('bookings_monthly_v1',['public.vw_actual_bookings_monthly_v1'],'one completed calendar month',['booking_month'],
 [('booking_month','date'),('project_count','int8'),('actual_bookings','numeric')])
relation('revenue_monthly_baseline_v1',['public.vw_actual_revenue_monthly_v1'],'one month of deployed revenue; diagnostic only, population mismatch unresolved',['revenue_month'],
 [('revenue_month','date'),('project_count','int8'),('invoice_count','int8'),('actual_revenue','numeric')],
 description='Diagnostic baseline only. Deployed TEST lacks reportable-parent and stage restrictions; never use as intended monthly invoice metric.')
relation('invoice_reporting_facts_v1',['analytics.children_v1','analytics.projects_v1'],'positive dated child invoice for a currently closed-invoiced parent in a completed month',['monday_id'],
 [('monday_id','text'),('parent_monday_id','text'),('invoice_date','date'),('amount_invoiced','numeric'),('category','text'),('type','text')])
relation('conversion_cohorts_v1',['analytics.projects_v1'],'one project per cohort; selecting exactly one cohort is mandatory',['monday_id','cohort_years'],
 [('monday_id','text'),('category','text'),('type','text'),('date_created','date'),('cohort_years','int4'),('eligible_count','int8'),('win_count','int8'),('closed_count','int8')])
relation('coverage_v1',['analytics.projects_v1','analytics.children_v1','analytics.child_totals_v1','analytics.hidden_values_v1','public.hidden_items'],
 'one aggregate coverage row',['reportable_projects'],[(k,'int8') for k in ['reportable_projects','projects_without_children','children_without_source','unresolved_source_links','repeated_source_ids','incomplete_order_inputs']])

metrics=[]
def metric(id, family, label, relation, columns, source, *, aliases=(), expression=None, aggregation='sum',
           population='reportable', date_basis=None, periods=('all_stored',), filters=(), dimensions=('category','type'),
           numerator=None, denominator=None, unit='source_currency', precision=2, limitations=(), nulls=None, zeroes=None, negatives=None, empty=None):
    metrics.append(dict(id=id,version='1.0.0',family=family,label=label,aliases=list(aliases or [label]),kind='observed',
        source=source,relation=relation,value_columns=columns,population=population,
        calculation=dict(expression=expression or f'sum({columns[0]})',aggregation=aggregation,numerator=numerator,denominator=denominator,
         nulls=nulls or 'Preserve unknown values; SUM ignores NULL but expose known-value counts. Stored NULL does not prove a typed Monday blank.',
         zeroes=zeroes or 'Numeric zero remains zero.',negatives=negatives or 'Signed values retained.',empty=empty or 'NULL when no known values.'),
        date_basis=date_basis,periods=list(periods),status_filters=list(filters),unit=unit,precision=precision,dimensions=list(dimensions),
        coverage=['Phase 1 source/business certification','Phase 2 reference parity','Explicit reviewed population and successful ingestion/rollup/refresh evidence'],
        examples=[f'Show {label.lower()} for its stated population.'],limitations=list(limitations)+['Candidate definition: Phase 1 owner review pending.'],certification='pending_phase1'))

def source(board, columns, database, priority):
    return dict(board_id=board,columns=columns,database=database,priority=priority)
enquiry=source('1825117125',['lookup_mkqanpbe','subitems.formula_mkqa31kh','subitems.mirror03__1','subitems.mirror77__1'],
 'public.reportable_projects.new_enquiry_value',
 ['Stored parent source-authoritative value; Open parents sum current API-active child formula values.',
  "Child formula: CASE WHEN reason_for_change='New Enquiry' THEN quote_amount ELSE 0 END.",
  'Won/Lost parents retain stored values; do not recalculate from every persisted child.'])
orders=source('1825117125',['mirror5__1'],'public.reportable_projects.total_order_value',
 ['Typed configured Monday parent mirror and verified multiplicity.', 'Never replace the parent mirror with the hidden-board total to force equality.'])
hidden_order=source('1825138260',['numbers98__1','numbers3__1'],'public.hidden_items',
 ['Total Customer Order Value is material plus customer additional charges.', 'Only both-numeric rows contribute to complete-input subtotal; expose missing-input counts.'])
invoice=source('1825138260',['numbers56__1','subitems.mirror5__1'],'public.hidden_items.amount_invoiced',
 ['Hidden Amount Invoiced -> child invoice mirror -> derived parent sum of complete current membership.',
  'Do not apply business-status filters to source/project totals. All-blank remains NULL.'])
conversion=source('1825117125',['status4__1','date9__1'],'public.reportable_projects',
 ['Closed-invoiced wins divided by all eligible projects; count before dividing.', 'Closed-only variant uses wins plus Lost.'])
gestation=source('1825117125',['formula_mkpp85yw','lookup_mkpptgsn','lookup_mkppq0gc'],'public.reportable_projects.gestation_period',
 ['Stored actual gestation: first design completion to first invoice, retaining source/fallback semantics.', 'Never substitute expected_gestation_days or recompute dates over the stored value.'])

metric('new_enquiry_value','enquiry','New Enquiry Value','projects_v1',['new_enquiry_value'],enquiry,
 aliases=['enquiry value','raw enquiry value'],limitations=['Unweighted; different from predicted/weighted pipeline.'])
metric('order_parent_value','order','Project Order Value','projects_v1',['total_order_value'],orders,aliases=['order value','parent order mirror'])
metric('order_hidden_complete_subtotal','order','Hidden Total Customer Order Value (complete-input subtotal)','hidden_values_v1',['complete_input_order_value','order_inputs_complete'],hidden_order,
 aliases=['order value','hidden order value'],population='hidden_inventory',dimensions=(),limitations=['Full hidden inventory, not a project-attributed total. Incomplete inputs must be displayed; this is not a company-wide certified total.'])
metric('invoice_project_value','invoice','Project Invoiced Value','projects_v1',['total_amount_invoiced'],invoice,
 aliases=['invoiced value','project invoice total'],limitations=['Stored parent total; differs from all persisted-child sums when membership/source precedence differs.'])
metric('invoice_hidden_value','invoice','Hidden Amount Invoiced','hidden_values_v1',['amount_invoiced'],invoice,
 aliases=['invoiced value','hidden invoices'],population='hidden_inventory',dimensions=())
metric('invoice_stored_child_value','invoice','Stored Child Invoiced Value','children_v1',['amount_invoiced'],invoice,
 aliases=['stored child invoices'],dimensions=(),limitations=['Reportable parents, persisted children; not automatically verified-active membership. Repeated mirrors retain their recorded contributions.'])
monthly=('last_month','previous_month','completed_12_months')
for id,family,label,rel,col,src,date_col,filters in [
 ('enquiry_monthly_actual','enquiry','Monthly New Enquiry Value','enquiry_monthly_v1','actual_enquiry_value',enquiry,'enquiry_month',[]),
 ('bookings_monthly_actual','order','Monthly Bookings','bookings_monthly_v1','actual_bookings',orders,'booking_month',
  ["pipeline_stage IN ('Won - Open (Order Received)','Won - Closed (Invoiced)','Won Via Other Ref')"])]:
    metric(id,family,label,rel,[col],src,date_basis=date_col,periods=monthly,filters=filters,dimensions=(),
      limitations=['Completed months only; positive source values only. Monthly aggregate cannot answer category/account/product questions.'],
      nulls='Published month spine fills missing months with zero.',negatives='Excluded before aggregation.',empty='Zero within the published month spine; absent months require an explicit date spine.')
metric('invoice_monthly_actual','invoice','Monthly Invoiced Revenue (intended definition)','invoice_reporting_facts_v1',['amount_invoiced'],invoice,
 date_basis='invoice_date',periods=monthly,filters=["pipeline_stage = 'Won - Closed (Invoiced)'",'amount_invoiced > 0'],
 expression='coalesce(sum(amount_invoiced),0)::numeric(14,2)',negatives='Excluded.',nulls='Undated and NULL invoices excluded.',empty='Zero; calendar spine required.',
 limitations=['Candidate intended contract. Restored deployed revenue differs; revenue_monthly_baseline_v1 is diagnostic only.'])
for years,name in [(5,'five'),(2,'two')]:
    for closed in [False,True]:
        denom='closed_count' if closed else 'eligible_count'
        metric(f'conversion_{"closed_" if closed else ""}{name}_year','conversion',f'{years}-year {"Closed-only" if closed else "Inclusive"} Conversion Rate',
          'conversion_cohorts_v1',['win_count',denom,'cohort_years'],conversion,unit='ratio',precision=3,
          expression=f'round(sum(win_count)::numeric/nullif(sum({denom}),0),3)',aggregation='ratio_of_counts',
          numerator='sum(win_count)',denominator=f'sum({denom})',date_basis='date_created',periods=[f'{name}_year_cohort'],filters=[f'cohort_years = {years}'],
          nulls='Undated projects excluded; NULL stage remains eligible but is not a win or closed case.',zeroes='Zero numerator gives zero; zero denominator gives NULL.',negatives='Not applicable to counts.',empty='NULL rate.',
          limitations=['Lower date bound only; future-dated projects remain eligible under the baseline. Never average segment rates.'])
    metric(f'gestation_{name}_year','gestation',f'{years}-year Actual Gestation Period','projects_v1',['gestation_period'],gestation,
      aggregation='mean',expression='avg(gestation_period) FILTER (WHERE gestation_period>0)',date_basis='date_created',periods=[f'{name}_year_cohort'],unit='days',precision=6,
      nulls='Excluded.',zeroes='Excluded.',negatives='Excluded.',limitations=['Filter the resolved lower-bound-only creation cohort; positive actual gestation only.'])
metric('gestation_median_five_year','gestation','5-year Median Actual Gestation','projects_v1',['gestation_period'],gestation,
 aggregation='percentile',expression='percentile_cont(0.5) WITHIN GROUP (ORDER BY gestation_period) FILTER (WHERE gestation_period>0)',
 date_basis='date_created',periods=['five_year_cohort'],unit='days',precision=6,nulls='Excluded.',zeroes='Excluded.',negatives='Excluded.')

catalogue=dict(version='1.0.0',populations=[
 dict(id='reportable',kind='reportable',available=True,description='Deployed approved reporting population, including historical cohorts; not auto-substituted with current_*.',requirements=['Preserve public.reportable_projects exclusions','Phase 1 population approval']),
 dict(id='hidden_inventory',kind='hidden_inventory',available=True,description='Independent hidden-board inventory; no project allocation is implied.',requirements=['Explicit hidden scope','Incomplete-input coverage']),
 dict(id='verified_active',kind='verified_active',available=False,description='Reserved; requires archive rollout and complete coverage before adding separate wrappers.',requirements=['MONDAY_ARCHIVE_ENABLED and MONDAY_ARCHIVE_REPORTING_ENABLED observed on all deployed writers','All five archive.coverage counters zero','Metric and population approval']),
 dict(id='historical_snapshot',kind='snapshot',available=False,description='Reserved dated snapshots; never joined to mutable current classifications.',requirements=['Certified snapshot date and stored dimensions','Separate Phase 8 contract'])],
 relations=relations,dimensions=[dict(id=k,column=k,semantics='scalar',description=f'Whole stored {k} classification; trim whitespace and map empty string to NULL; never explode memberships.') for k in ['category','type']],
 metrics=metrics,joins=[
 dict(id='project_child_totals',left='projects_v1',right='child_totals_v1',left_key=['monday_id'],right_key=['parent_monday_id'],cardinality='one_to_zero_or_one',purpose='project_measures',description='Pre-aggregate all children before joining project amounts; stored-child totals are diagnostic.'),
 dict(id='project_latest_analysis',left='projects_v1',right='latest_analysis_v1',left_key=['monday_id'],right_key=['project_id'],cardinality='one_to_zero_or_one',purpose='project_measures',description='At most one deterministic latest analysis. Expected values remain predictions.'),
 dict(id='child_parent_detail',left='children_v1',right='projects_v1',left_key=['parent_monday_id'],right_key=['monday_id'],cardinality='many_to_one',purpose='child_detail',description='Child amounts may be attributed to parent classifications. Parent amounts must never be summed at child grain.')],
 notices=[
 'All metrics are pending certification. A successful schema/parity check is not business approval.',
 'Currency and tax basis require owner review; source_currency never silently means GBP/net/gross.',
 'Business timezone must be supplied explicitly; Europe/London is only the frozen evaluation assumption. No fiscal calendar or MTD definition is registered.',
 'vw_enquiry_value_forecast_chart_v1.actual_enquiry_value is weighted actual_pipeline_value; gross_enquiry_value is the unweighted measure.',
 'Weighted enquiry and latest expected conversion/gestation are predictive and mutable; no observed metric aliases them.',
 'Account/product membership expansion is unsupported for monetary totals until attributable detail or labelled overlapping membership totals are certified. Distinct project counts cannot repair duplicated sums.',
 'Repeated hidden source links preserve verified mirror multiplicity; unresolved duplicates require evidence, never automatic DISTINCT.',
 'Latest source row is not successful ingestion, rollup or materialized refresh freshness.'
 ])
target.write_text(json.dumps(catalogue,indent=2)+'\n',encoding='utf-8')

comments=[]
for r in relations:
    comments.append(f"COMMENT ON VIEW analytics.{r['id']} IS '{r['description'].replace(chr(39), chr(39)*2)}';")
    for col in r['columns']:
        description=f"{col}: {r['grain']}; see semantic catalogue 1.0.0 for eligibility, NULL and unit rules."
        comments.append(f"COMMENT ON COLUMN analytics.{r['id']}.{col} IS '{description.replace(chr(39), chr(39)*2)}';")
migration=root/'src/database/migrations/20261007_001_analytics_contracts.sql'
with migration.open('a',encoding='utf-8') as f:
    f.write('\n-- Catalogue documentation.\n'+'\n'.join(comments)+'\n')

# A narrowly scoped, aggregate-only read interface to the unchanged archive gate.
tree=ast.parse((root/'src/services/monday_archive.py').read_text(encoding='utf-8'))
fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='coverage')
query=next(n.args[0].value for n in ast.walk(fn) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='execute')
for value in ['verified_archive_v1','1825117125','1825117144','1825138260']:
    query=query.replace('%s',"'"+value+"'",1)
coverage_sql='''-- Optional archive coverage interface; does not enable archive reporting.
-- Exact SQL from src.services.monday_archive.coverage, with reviewed board IDs.
-- No privileged Python imports, raw lifecycle payloads, or SECURITY DEFINER.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
CREATE OR REPLACE VIEW analytics.archive_coverage_v1 WITH (security_invoker=true, security_barrier=true) AS
'''+query.strip()+''';
COMMENT ON VIEW analytics.archive_coverage_v1 IS 'Five unchanged archive.coverage counters; zero is necessary but not sufficient for verified-active reporting.';
REVOKE ALL ON analytics.archive_coverage_v1 FROM PUBLIC;
DO $revoke$
DECLARE role_name text;
BEGIN
 FOR role_name IN SELECT rolname FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role') LOOP
   EXECUTE format('REVOKE ALL ON analytics.archive_coverage_v1 FROM %I',role_name);
 END LOOP;
END $revoke$;
'''
for col in ['unverified_projects','unverified_subitems','unverified_sources','unverified_current_values','unresolved_archive_jobs']:
    coverage_sql+=f"COMMENT ON COLUMN analytics.archive_coverage_v1.{col} IS 'Blocking archive coverage count; same definition as monday_archive.coverage.';\n"
(root/'src/database/migrations/20261007_002_analytics_archive_coverage.sql').write_text(coverage_sql,encoding='utf-8')
print(f'Authored {len(metrics)} metrics and {len(relations)} relations.')
