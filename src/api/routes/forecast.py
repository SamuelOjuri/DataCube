from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from ...database.supabase_client import SupabaseClient

router = APIRouter(prefix="/forecast", tags=["forecast"])
logger = logging.getLogger(__name__)

_ALLOWED_STAGE_BUCKETS = {"Committed", "Open", "Lost"}
_DEFAULT_SNAPSHOT_LIMIT = 1000
_MAX_SNAPSHOT_LIMIT = 5000


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _add_months(month_start: date, months: int) -> date:
    month_index = (month_start.month - 1) + months
    year = month_start.year + (month_index // 12)
    month = (month_index % 12) + 1
    return date(year, month, 1)


def _normalize_stage_bucket(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    normalized = value.strip().title()
    if not normalized:
        return None

    if normalized not in _ALLOWED_STAGE_BUCKETS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid stage_bucket '{value}'. "
                f"Use one of: {sorted(_ALLOWED_STAGE_BUCKETS)}"
            ),
        )
    return normalized


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sum_numeric(rows: list[Dict[str, Any]], key: str) -> float:
    return round(sum(_to_float(row.get(key)) for row in rows), 2)


@router.get("/pipeline")
def get_pipeline_forecast(
    months: int = Query(
        12,
        ge=1,
        le=12,
        description="Number of months to return from the start month (1-12).",
    ),
    as_of_month: Optional[date] = Query(
        None,
        description="Optional month anchor date (YYYY-MM-DD). Day is ignored.",
    ),
    stage_bucket: Optional[str] = Query(
        None,
        description="Optional filter: Committed, Open, or Lost.",
    ),
) -> Dict[str, Any]:
    """
    Read-only monthly forecast endpoint backed by mv_pipeline_forecast_monthly_12m_v1.
    """
    try:
        stage_filter = _normalize_stage_bucket(stage_bucket)
        window_start = _month_start(as_of_month or date.today())
        window_end = _add_months(window_start, months)

        supabase = SupabaseClient()
        query = (
            supabase.client.table("mv_pipeline_forecast_monthly_12m_v1")
            .select(
                "forecast_month,stage_bucket,project_count,contract_value,"
                "committed_value,expected_value,best_case_value,worst_case_value"
            )
            .gte("forecast_month", window_start.isoformat())
            .lt("forecast_month", window_end.isoformat())
            .order("forecast_month")
            .order("stage_bucket")
        )

        if stage_filter:
            query = query.eq("stage_bucket", stage_filter)

        result = query.execute()
        rows = result.data or []

        project_count = 0
        for row in rows:
            try:
                project_count += int(row.get("project_count") or 0)
            except (TypeError, ValueError):
                continue

        return {
            "source": "mv_pipeline_forecast_monthly_12m_v1",
            "window_start": window_start.isoformat(),
            "window_end_exclusive": window_end.isoformat(),
            "months_requested": months,
            "stage_bucket": stage_filter,
            "row_count": len(rows),
            "totals": {
                "project_count": project_count,
                "contract_value": _sum_numeric(rows, "contract_value"),
                "committed_value": _sum_numeric(rows, "committed_value"),
                "expected_value": _sum_numeric(rows, "expected_value"),
                "best_case_value": _sum_numeric(rows, "best_case_value"),
                "worst_case_value": _sum_numeric(rows, "worst_case_value"),
            },
            "rows": rows,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch forecast monthly pipeline")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch forecast monthly pipeline data",
        ) from exc


@router.get("/snapshot")
def get_pipeline_forecast_snapshot(
    snapshot_date: Optional[date] = Query(
        None,
        description="Snapshot date (YYYY-MM-DD). Defaults to latest snapshot date.",
    ),
    project_id: Optional[str] = Query(
        None,
        description="Optional project_id filter.",
    ),
    stage_bucket: Optional[str] = Query(
        None,
        description="Optional filter: Committed, Open, or Lost.",
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(_DEFAULT_SNAPSHOT_LIMIT, ge=1, le=_MAX_SNAPSHOT_LIMIT),
) -> Dict[str, Any]:
    """
    Read-only snapshot endpoint backed by pipeline_forecast_snapshot.
    """
    try:
        stage_filter = _normalize_stage_bucket(stage_bucket)
        supabase = SupabaseClient()

        resolved_snapshot_date = snapshot_date
        if resolved_snapshot_date is None:
            latest_result = (
                supabase.client.table("pipeline_forecast_snapshot")
                .select("snapshot_date")
                .order("snapshot_date", desc=True)
                .limit(1)
                .execute()
            )
            latest_rows = latest_result.data or []
            if not latest_rows:
                return {
                    "source": "pipeline_forecast_snapshot",
                    "snapshot_date": None,
                    "row_count": 0,
                    "total_rows": 0,
                    "offset": offset,
                    "limit": limit,
                    "rows": [],
                }
            resolved_snapshot_date = date.fromisoformat(str(latest_rows[0]["snapshot_date"]))

        query = (
            supabase.client.table("pipeline_forecast_snapshot")
            .select(
                "snapshot_date,project_id,forecast_month,stage_bucket,"
                "contract_value,probability,committed_value,expected_value,"
                "best_case_value,worst_case_value,analysis_timestamp,created_at",
                count="exact",
            )
            .eq("snapshot_date", resolved_snapshot_date.isoformat())
            .order("forecast_month")
            .order("project_id")
        )

        if project_id:
            query = query.eq("project_id", project_id)
        if stage_filter:
            query = query.eq("stage_bucket", stage_filter)

        query = query.range(offset, offset + limit - 1)
        result = query.execute()
        rows = result.data or []
        total_rows = result.count if result.count is not None else len(rows)

        return {
            "source": "pipeline_forecast_snapshot",
            "snapshot_date": resolved_snapshot_date.isoformat(),
            "project_id": project_id,
            "stage_bucket": stage_filter,
            "offset": offset,
            "limit": limit,
            "row_count": len(rows),
            "total_rows": total_rows,
            "rows": rows,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch forecast snapshot")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch forecast snapshot data",
        ) from exc