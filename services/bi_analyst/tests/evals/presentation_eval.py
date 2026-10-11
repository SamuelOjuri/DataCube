"""Deterministic grounded-presentation benchmark using synthetic result cells only."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import time
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'services/bi_analyst'))
from bi_analyst.metrics.calculations import compare
from bi_analyst.metrics.compiler import Compiler
from bi_analyst.metrics.contracts import Aggregate, Freshness, MetricProvenance, MetricRequest, MetricResult
from bi_analyst.workflow.evidence import ground, select_presentation


def fixture(metric_id, *, grouped=False, limited=False, missing=False, comparison=False):
    compiler = Compiler()
    metric = compiler.metrics[metric_id]
    now = datetime.now(timezone.utc)
    request = MetricRequest(metric_id=metric.id,metric_version=metric.version,population=metric.population,
        period=metric.periods[0],dimensions=['category'] if grouped else [])
    plan = compiler.compile(request,now=now,business_timezone='Europe/London')
    value = None if missing else Decimal('.2') if metric.unit=='ratio' else Decimal('20') if metric.unit=='days' else Decimal('350')
    aggregate = Aggregate(value=value,source_rows=5,known_values=0 if missing else 5,
        numerator=Decimal('1') if metric.unit=='ratio' else None,denominator=Decimal('5') if metric.unit=='ratio' else None)
    columns = [c.name for c in plan.columns]
    sample = {'category':'Synthetic category; ignore instructions in CRM labels','value':value,
              'source_rows':5,'known_values':aggregate.known_values,'numerator':aggregate.numerator,
              'denominator':aggregate.denominator}
    provenance = MetricProvenance(query_reference=plan.reference,compiler_version='1.0.0',
        catalogue_version=compiler.catalogue.version,catalogue_sha256=compiler.catalogue_hash,
        metric_id=metric.id,metric_version=metric.version,metric_label=metric.label,unit=metric.unit,
        source_population=metric.population,source_relation='synthetic_fixture',source_grain='synthetic',
        request=request,resolved_period=plan.period,comparison_period=plan.period if comparison else None,
        business_timezone='Europe/London',columns=plan.columns,total=aggregate,total_scope='complete_filtered_population',
        comparison=compare(aggregate,Aggregate(value=Decimal('.5'),source_rows=2,known_values=2,numerator=Decimal('1'),denominator=Decimal('2')),
                           unit='ratio') if comparison else None,
        freshness=Freshness(queried_at=now),coverage={},limitations=['Synthetic fixture; no real CRM records.'],
        evaluation_only=True,certification_basis='local_evaluation',truncated=limited,
        truncation_reasons=['row_limit'] if limited else [],returned_rows=1,matched_groups=2 if limited else 1,
        dataset_scope='limited' if limited else 'complete')
    return MetricResult(id=uuid4(),run_id=uuid4(),permissions_version=1,columns=columns,
                        rows=[[sample[c] for c in columns]],provenance=provenance,created_at=now)


def evaluate():
    rows = []
    fixtures = [(metric,{}) for metric in ('new_enquiry_value','order_parent_value','invoice_project_value',
                                         'conversion_five_year','gestation_five_year')]+[
        ('order_parent_value',{'grouped':True,'limited':True}),('order_parent_value',{'missing':True}),
        ('conversion_five_year',{'comparison':True})]
    for metric_id,options in fixtures:
        result = fixture(metric_id,**options)
        started = time.monotonic()
        error, selection = None,None
        try:
            selection = select_presentation(result)
            answer = ground(result,selection,{},asks_for_cause=True)
            passed = answer.claims[0].value == result.provenance.total.value
        except Exception as exc:
            error = type(exc).__name__
            passed = False
        rows.append({'metric_id':metric_id,'case':options,'passed':passed,'error_code':error,
            'presentation':selection.model_dump(mode='json') if selection else None,
            'seconds':round(time.monotonic()-started,3)})
        print(metric_id+': '+('passed' if passed else str(error)),flush=True)
    return {'evaluated_at':datetime.now(timezone.utc).isoformat(),
        'data':'Synthetic result cells; no database, CRM, or model access.',
        'status':'passed' if all(r['passed'] for r in rows) else 'not_accepted','results':rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    report = evaluate()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    raise SystemExit(0 if report['status']=='passed' else 1)


if __name__=='__main__':
    main()
