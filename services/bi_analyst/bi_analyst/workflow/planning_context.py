"""Business definitions and compiler capabilities for the authorised planning catalogue."""
from datetime import datetime
from zoneinfo import ZoneInfo

from ..exploration.context import CLASSIFICATION, COLUMN_NOTES
from ..metrics.compiler import MONTHLY, Compiler
from ..semantic.catalogue import Metric
from ..semantic.periods import resolve_period


PERIOD_MEANINGS = {
    'all_stored': 'All retained stored records in the selected population; no date filter.',
    'last_month': 'The last completed calendar month.',
    'previous_month': 'The completed calendar month before last.',
    'completed_12_months': 'The last twelve completed calendar months, excluding the current month.',
    'five_year_cohort': 'Projects created from the local as-of date minus five years; lower bound only, including future dates.',
    'two_year_cohort': 'Projects created from the local as-of date minus two years; lower bound only, including future dates.',
}
RESULT_FIELDS = {
    'value': 'The defined aggregate, with the metric unit and precision.',
    'source_rows': 'Number of matching source rows at the stated source grain; not always a project count.',
    'known_values': 'Rows with a usable measured value; gestation counts positive stored actual values only.',
    'numerator': 'Aggregated invoiced-win count for conversion.',
    'denominator': 'Aggregated eligible count for inclusive conversion, or wins plus losses for closed-only conversion.',
    'share': 'Group value divided by the complete filtered total; additive monetary groups only.',
}


def metric_context(compiler: Compiler, metric: Metric) -> dict:
    entry = {key:getattr(metric, key) for key in
             ('id', 'version', 'family', 'label', 'aliases', 'population', 'periods', 'unit',
              'precision', 'dimensions', 'kind', 'examples', 'date_basis', 'status_filters')}
    relation = compiler.relations[metric.relation]
    monthly = metric.id in MONTHLY
    ordering = {'total':['value', *metric.dimensions]}
    if monthly:
        ordering['month'] = ['value', *metric.dimensions, 'month']
    entry.update(
        meaning=metric.source.priority,
        calculation=metric.calculation.model_dump(mode='json'),
        source={'relation':metric.relation, 'grain':relation.grain, 'key':relation.key,
                'description':relation.description},
        column_meanings={name:COLUMN_NOTES[metric.relation][name]
                         for name in dict.fromkeys([*metric.value_columns, metric.date_basis])
                         if name in COLUMN_NOTES.get(metric.relation, {})},
        capabilities={'grains':['total', 'month'] if monthly else ['total'],
                      'filter_dimensions':metric.dimensions,
                      'ordering_columns':ordering,
                      'include_share':metric.calculation.aggregation == 'sum' and bool(metric.dimensions or monthly)},
        result_fields=['value', 'source_rows', 'known_values'],
        limitations=compiler.catalogue.runtime_limitations(metric))
    if metric.family == 'conversion':
        entry['supporting_counts'] = {'numerator':metric.calculation.numerator,
                                      'denominator':metric.calculation.denominator}
        entry['result_fields'] += ['numerator', 'denominator']
    return entry


def build_planning_context(compiler: Compiler, metrics: list[Metric], *, now: datetime,
                           business_timezone: str, max_rows: int) -> dict:
    """Use only permitted metrics; keep the sealed catalogue and its acceptance unchanged."""
    dimensions = {d for metric in metrics for d in metric.dimensions}
    populations = {metric.population for metric in metrics}
    periods = {period for metric in metrics for period in metric.periods}
    resolved = []
    for name in sorted(periods):
        period = None if name == 'all_stored' else resolve_period(
            name, now=now, business_timezone=business_timezone)
        resolved.append({'name':name, 'meaning':PERIOD_MEANINGS[name],
                         'start_date':period.start_date.isoformat() if period else None,
                         'end_date_exclusive':period.end_date_exclusive.isoformat()
                         if period and period.end_date_exclusive else None})
    return {
        'catalogue':[metric_context(compiler, metric) for metric in metrics],
        'dimensions':[{'id':d.id, 'description':d.description, 'semantics':d.semantics,
                       **CLASSIFICATION.get(d.column, {})}
                      for d in compiler.catalogue.dimensions if d.id in dimensions],
        'populations':[{'id':p.id, 'description':p.description}
                       for p in compiler.catalogue.populations if p.id in populations],
        'today':now.astimezone(ZoneInfo(business_timezone)).date().isoformat(),
        'business_timezone':business_timezone,
        'periods':resolved,
        'period_resolution':'Boundaries are planning context; execution resolves them again at query time.',
        'result_fields':RESULT_FIELDS,
        'max_rows':max_rows,
        'notices':compiler.catalogue.runtime_notices,
    }
