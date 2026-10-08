"""Owned result presentation: bounded project evidence and idempotent feedback."""
import asyncio
from typing import Literal
from uuid import UUID

from fastapi import HTTPException
from pydantic import Field

from .contracts import Contract
from .metrics.contracts import MetricProvenance
from .metrics.service import wire_size


class Feedback(Contract):
    rating: Literal['helpful', 'not_helpful']
    comment: str = Field(default='', max_length=2000)


async def save_feedback(store, actor, result_id: UUID, body: Feedback):
    if store.db.schema_version != 7:
        raise HTTPException(503, 'feedback_requires_migration_007')
    await store.result(actor, result_id)
    async with store.scoped(actor) as conn:
        # One mutable feedback record per owned result; repeat submissions are safe.
        row = await (await conn.execute('''INSERT INTO analyst_state.result_feedback
            (result_id,owner_id,permissions_version,rating,comment)
            SELECT id,owner_id,permissions_version,%s,%s FROM analyst_state.results
            WHERE id=%s AND owner_id=%s AND permissions_version=%s
            ON CONFLICT(result_id) DO UPDATE SET rating=EXCLUDED.rating,comment=EXCLUDED.comment
            RETURNING result_id,rating''',
            (body.rating, body.comment, result_id, actor.subject, actor.permissions_version))).fetchone()
    if not row:
        raise HTTPException(404, 'not_found')
    return row


def project_statement(compiler, provenance: MetricProvenance, limit: int, offset: int):
    """Re-use the compiled filtered CTE at the original resolution time.

    Monthly enquiry/bookings gateways are already aggregated, so explicitly map
    their certified positive-value and stage contracts onto project evidence.
    Never join independent hidden inventory to projects or allocate its values.
    """
    request = provenance.request
    metric = compiler.validate(request)
    if metric.population != 'reportable':
        raise HTTPException(422, 'project_drilldown_unavailable')
    if provenance.catalogue_sha256 != compiler.catalogue_hash:
        raise HTTPException(409, 'result_catalogue_changed')
    plan = compiler.compile(request, now=provenance.freshness.queried_at,
                            business_timezone=provenance.business_timezone)
    # Both totals and detail derive from this same trusted compiler statement.
    base = plan.filtered.sql
    params = list(plan.filtered.params)
    if metric.id in {'enquiry_monthly_actual', 'bookings_monthly_actual'}:
        column = 'new_enquiry_value' if metric.id == 'enquiry_monthly_actual' else 'total_order_value'
        date = 'date_created' if metric.id == 'enquiry_monthly_actual' else 'date_order_received'
        period = provenance.resolved_period
        if not period or not period.end_date_exclusive:
            raise HTTPException(409, 'result_period_unavailable')
        clauses = [f'"{date}">=%s', f'"{date}"<%s', f'"{column}">0']
        params = [period.start_date, period.end_date_exclusive]
        if metric.id == 'bookings_monthly_actual':
            clauses.append("pipeline_stage=ANY(%s)")
            params.append(['Won - Open (Order Received)', 'Won - Closed (Invoiced)', 'Won Via Other Ref'])
        base = 'WITH filtered AS (SELECT * FROM analyst_query.projects_v1 WHERE ' + ' AND '.join(clauses) + ')'
    else:
        column = metric.value_columns[0]
    if metric.relation in {'children_v1', 'invoice_reporting_facts_v1'}:
        project = 'parent_monday_id'
    else:
        project = 'monday_id'
    fields = [f'{project} AS project_id', 'monday_id AS source_id', f'"{column}" AS source_value']
    columns = ['project_id', 'source_id', 'source_value']
    if metric.family == 'conversion':
        denominator = 'closed_count' if '_closed_' in metric.id else 'eligible_count'
        fields.append(f'{denominator} AS denominator')
        columns.append('denominator')
    if 'category' in compiler.relations[metric.relation].columns or metric.id in {'enquiry_monthly_actual', 'bookings_monthly_actual'}:
        fields.extend(['left(category,256) AS category', 'left(type,256) AS type'])
        columns.extend(['category', 'type'])
    # Gestation statistics exclude non-positive values; show only contributors.
    eligible = ' WHERE gestation_period>0' if metric.family == 'gestation' else ''
    statement = base + ' SELECT ' + ','.join(fields) + ' FROM filtered' + eligible + ' ORDER BY project_id,source_id LIMIT %s OFFSET %s'
    return statement, tuple(params + [limit + 1, offset]), columns


async def projects(metrics, actor, result_id: UUID, limit: int, offset: int, http_request):
    result = await metrics.store.result(actor, result_id)
    p = MetricProvenance.model_validate(result['provenance'])
    metrics.require_access(p.metric_id, p.metric_version, p.source_population)
    statement, params, columns = project_statement(metrics.compiler, p, limit, offset)
    async with metrics.admitted(), asyncio.timeout(metrics.settings.metric_timeout_seconds):
        async with metrics.db.transaction(analytical=True) as conn:
            await conn.execute("SELECT set_config('TimeZone',%s,true)", (p.business_timezone,))
            queried_at = (await (await conn.execute('SELECT transaction_timestamp() AS now')).fetchone())['now']
            rows, size, more = [], 0, False
            async with conn.cursor(name='project_evidence') as cursor:
                await cursor.execute(statement, params)
                while row := await cursor.fetchone():
                    if await http_request.is_disconnected():
                        raise HTTPException(409, 'request_disconnected')
                    cells = [row[name] for name in columns]
                    row_size = wire_size(cells)
                    if len(rows) >= limit or size + row_size > metrics.settings.metric_max_bytes - 2048:
                        more = True
                        break
                    rows.append(cells)
                    size += row_size
        # Recheck after the reader is released and before disclosing any values.
        await metrics.store.result(actor, result_id)
    # Decimal values remain strings, consistent with the metric result contract.
    from fastapi.encoders import jsonable_encoder
    from decimal import Decimal
    return jsonable_encoder({'result_id': result_id, 'columns': columns, 'rows': rows,
        'offset': offset, 'has_more': more, 'queried_at': queried_at,
        'limitation': 'Live project evidence for the saved plan and resolved period; data may have changed since the answer. '
                      'This is a page of source rows, not a total or historical snapshot. Child invoice rows may repeat a project. '
                      'Source values and denominators must not be interpreted as project-level ratios. Classification text is limited to 256 characters.'},
        custom_encoder={Decimal: str})
