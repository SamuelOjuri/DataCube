"""Inspect the screenshot's stored result without executing or changing its SQL."""
from datetime import date
import json
from pathlib import Path
import sys

from dotenv import dotenv_values
import psycopg
from psycopg.rows import dict_row
import sqlglot

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'services/bi_analyst'))

from bi_analyst.answer_presentation.calendar import dates_for
from bi_analyst.answer_presentation.service import present_result
from bi_analyst.exploration.contracts import ExploratoryResult
from bi_analyst.exploration.semantics import describe_columns, projections

REFERENCE = '14ad20a130645772e66257f537d680644f07f55bbc73dce04d2592a50a0b83c8'


def show(probe, data):
    print(json.dumps({'probe': probe, 'data': data}, default=str), flush=True)


def main():
    env = dotenv_values(ROOT / 'services/bi_analyst/.env')
    with psycopg.connect(
        env['SUPABASE_DB_URL'], connect_timeout=10, row_factory=dict_row,
        application_name='monthly_chart_diagnosis_readonly',
        options='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=1000',
    ) as conn:
        conn.execute('SET TRANSACTION READ ONLY')
        read_only = conn.execute('SHOW transaction_read_only').fetchone()
        show('transaction_read_only', read_only)
        if read_only['transaction_read_only'] != 'on':
            raise RuntimeError('read_only_transaction_required')
        records = conn.execute('''
            SELECT r.id,r.run_id,r.permissions_version,r.columns,r.rows,r.provenance,r.created_at,
                   j.answer AS saved_answer
            FROM analyst_state.results r
            LEFT JOIN analyst_state.workflow_jobs j ON j.run_id=r.run_id
            WHERE r.provenance->>'query_reference'=%s
            ORDER BY r.created_at DESC LIMIT 5
        ''', (REFERENCE,)).fetchall()
    show('matching_saved_results', len(records))
    if not records:
        raise RuntimeError('saved_result_not_found')
    for record in records:
        saved_answer = record.pop('saved_answer') or {}
        result = ExploratoryResult.model_validate(record)
        p = result.provenance
        show('saved_result', {
            'id': result.id, 'created_at': result.created_at,
            'row_count': len(result.rows), 'columns': [c.model_dump() for c in p.columns],
            'analysis_intent': p.analysis_intent.model_dump() if p.analysis_intent else None,
            'truncated': p.truncated, 'query_limited': p.query_limited, 'dataset_scope': p.dataset_scope,
            'saved_answer': {k: saved_answer.get(k) for k in ('summary_kind', 'chart', 'exploratory_chart', 'chart_reason')},
        })
        described, limited = describe_columns(p.sql, p.columns)
        nodes, grouped, raw_rows = projections(sqlglot.parse_one(p.sql, read='postgres'))
        show('sql_metadata_reproduction', {
            'same_as_saved': described == p.columns, 'query_limited': limited,
            'projection_nodes': {name: {'type': type(node).__name__, 'expression': node.sql(dialect='postgres')} for name, node in nodes.items()},
            'grouped': grouped, 'raw_rows': raw_rows,
        })
        rendered = present_result(result)
        axis = result.columns.index('invoice_month')
        parsed = [date.fromisoformat(str(row[axis])) for row in result.rows]
        show('actual_presentation', {
            'kind': rendered.kind, 'chart': rendered.chart, 'reason': rendered.chart_reason,
            'distinct_dates': len(set(parsed)), 'first_date': min(parsed), 'last_date': max(parsed),
            'all_month_starts': all(day.day == 1 for day in parsed),
            'current_grain_dates_valid': dates_for(result, axis, p.columns[axis].time_grain) is not None,
            'month_grain_dates_valid': dates_for(result, axis, 'month') is not None,
        })
        # Diagnostic only: alter an isolated in-memory copy, with the same owned rows.
        hypothetical = result.model_copy(deep=True)
        hypothetical.provenance.columns[axis].time_grain = 'month'
        after = present_result(hypothetical)
        show('in_memory_month_metadata_only', {
            'kind': after.kind, 'chart': after.chart.model_dump() if after.chart else None,
            'reason': after.chart_reason,
        })
        calendar_sql = """SELECT CAST(GENERATE_SERIES(CAST('2025-11-01' AS DATE),
            CAST('2026-09-01' AS DATE), CAST('1 month' AS INTERVAL)) AS DATE) AS invoice_month"""
        calendar_columns, _ = describe_columns(calendar_sql, [p.columns[axis]])
        show('generate_series_without_join', {'time_grain': calendar_columns[0].time_grain})


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        show('error', {'type': type(error).__name__, 'sqlstate': getattr(error, 'sqlstate', None)})
        sys.exit(1)
