from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from ...database.supabase_client import SupabaseClient

router = APIRouter(prefix="/forecast", tags=["forecast"])
logger = logging.getLogger(__name__)

_ALLOWED_STAGE_BUCKETS = {"Committed", "Open", "Lost"}
_ALLOWED_SMOOTHING_RISK_BANDS = {"Very High", "High", "Moderate", "Low"}
_DEFAULT_SNAPSHOT_LIMIT = 1000
_MAX_SNAPSHOT_LIMIT = 5000
_TOTALS_PAGE_SIZE = 1000


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


def _normalize_risk_band(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None

    normalized = " ".join(part.capitalize() for part in value.strip().split())
    if not normalized:
        return None

    if normalized not in _ALLOWED_SMOOTHING_RISK_BANDS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid risk_band '{value}'. "
                f"Use one of: {sorted(_ALLOWED_SMOOTHING_RISK_BANDS)}"
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


def _sum_int(rows: list[Dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        try:
            total += int(row.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return total


def _smoothing_totals(
    rows: list[Dict[str, Any]],
    *,
    project_count_key: Optional[str] = "project_count",
    expected_key: str = "expected_value",
    smoothed_key: str = "smoothed_expected_value",
    unsmoothed_key: str = "unsmoothed_expected_value",
    allocated_key: str = "allocated_expected_value",
) -> Dict[str, Any]:
    if project_count_key:
        project_count = _sum_int(rows, project_count_key)
    else:
        project_count = len(rows)

    return {
        "project_count": project_count,
        "expected_value": _sum_numeric(rows, expected_key),
        "smoothed_expected_value": _sum_numeric(rows, smoothed_key),
        "unsmoothed_expected_value": _sum_numeric(rows, unsmoothed_key),
        "allocated_monthly_value": _sum_numeric(rows, allocated_key),
    }


def _resolve_latest_smoothing_snapshot_date(supabase: SupabaseClient) -> Optional[date]:
    latest_result = (
        supabase.client.table("pipeline_smoothing_forecast_snapshot")
        .select("snapshot_date")
        .order("snapshot_date", desc=True)
        .limit(1)
        .execute()
    )
    latest_rows = latest_result.data or []
    if not latest_rows:
        return None
    return date.fromisoformat(str(latest_rows[0]["snapshot_date"]))


def _build_smoothing_snapshot_query(
    supabase: SupabaseClient,
    *,
    snapshot_date_value: date,
    project_id: Optional[str] = None,
    stage_bucket: Optional[str] = None,
    risk_band: Optional[str] = None,
    select_clause: str = "*",
    count: Optional[str] = None,
):
    query = (
        supabase.client.table("pipeline_smoothing_forecast_snapshot")
        .select(select_clause, count=count)
        .eq("snapshot_date", snapshot_date_value.isoformat())
    )

    if project_id:
        query = query.eq("project_id", project_id)
    if stage_bucket:
        query = query.eq("stage_bucket", stage_bucket)
    if risk_band:
        query = query.eq("risk_band", risk_band)

    return query


def _fetch_all_smoothing_snapshot_total_rows(
    supabase: SupabaseClient,
    *,
    snapshot_date_value: date,
    project_id: Optional[str] = None,
    stage_bucket: Optional[str] = None,
    risk_band: Optional[str] = None,
) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    offset = 0
    select_clause = (
        "project_id,expected_value,unsmoothed_allocated_value,"
        "smoothed_allocated_value,allocated_expected_value"
    )

    while True:
        result = (
            _build_smoothing_snapshot_query(
                supabase,
                snapshot_date_value=snapshot_date_value,
                project_id=project_id,
                stage_bucket=stage_bucket,
                risk_band=risk_band,
                select_clause=select_clause,
            )
            .order("forecast_month")
            .order("project_id")
            .range(offset, offset + _TOTALS_PAGE_SIZE - 1)
            .execute()
        )
        page_rows = result.data or []
        rows.extend(page_rows)
        if len(page_rows) < _TOTALS_PAGE_SIZE:
            break
        offset += _TOTALS_PAGE_SIZE

    return rows


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


@router.get("/smoothing/projects")
def get_pipeline_smoothing_projects(
    risk_band: Optional[str] = Query(
        None,
        description="Optional smoothing risk band: Very High, High, Moderate, or Low.",
    ),
    stage_bucket: Optional[str] = Query(
        None,
        description="Optional filter: Committed, Open, or Lost.",
    ),
    project_id: Optional[str] = Query(
        None,
        description="Optional project_id filter.",
    ),
    category: Optional[str] = Query(
        None,
        description="Optional category contains filter.",
    ),
    project_type: Optional[str] = Query(
        None,
        alias="type",
        description="Optional type contains filter.",
    ),
    product: Optional[str] = Query(
        None,
        description="Optional product/product_type contains filter.",
    ),
    account: Optional[str] = Query(
        None,
        description="Optional account contains filter.",
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(_DEFAULT_SNAPSHOT_LIMIT, ge=1, le=_MAX_SNAPSHOT_LIMIT),
) -> Dict[str, Any]:
    """
    Read-only project-level smoothing scores backed by vw_pipeline_smoothing_score_v1.
    """
    try:
        stage_filter = _normalize_stage_bucket(stage_bucket)
        risk_filter = _normalize_risk_band(risk_band)
        supabase = SupabaseClient()

        query = (
            supabase.client.table("vw_pipeline_smoothing_score_v1")
            .select(
                "project_id,project_name,account,type,category,product_type,product_key,"
                "pipeline_stage,stage_bucket,forecast_date,forecast_month,forecast_date_source,"
                "conversion_probability,expected_conversion_rate,expected_value,"
                "smoothed_expected_value,unsmoothed_expected_value,"
                "combined_smoothed_probability,expected_spread_days,risk_band,"
                "workbook_suggested_treatment,default_smoothing_recommended,adoption_recommendation,"
                "category_rate,type_rate,product_rate,account_rate,"
                "category_spread_days,type_spread_days,product_spread_days,account_spread_days,"
                "category_mature_project_count,type_mature_project_count,"
                "product_mature_project_count,account_mature_project_count,"
                "category_confidence,type_confidence,product_confidence,account_confidence,"
                "smoothing_as_of_date",
                count="exact",
            )
            .order("combined_smoothed_probability", desc=True)
            .order("expected_spread_days", desc=True)
            .order("project_id")
        )

        if risk_filter:
            query = query.eq("risk_band", risk_filter)
        if stage_filter:
            query = query.eq("stage_bucket", stage_filter)
        if project_id:
            query = query.eq("project_id", project_id)
        if category:
            query = query.ilike("category", f"%{category.strip()}%")
        if project_type:
            query = query.ilike("type", f"%{project_type.strip()}%")
        if product:
            query = query.ilike("product_type", f"%{product.strip()}%")
        if account:
            query = query.ilike("account", f"%{account.strip()}%")

        result = query.range(offset, offset + limit - 1).execute()
        rows = result.data or []
        total_rows = result.count if result.count is not None else len(rows)

        return {
            "source": "vw_pipeline_smoothing_score_v1",
            "risk_band": risk_filter,
            "stage_bucket": stage_filter,
            "project_id": project_id,
            "category": category,
            "type": project_type,
            "product": product,
            "account": account,
            "offset": offset,
            "limit": limit,
            "row_count": len(rows),
            "total_rows": total_rows,
            "totals": _smoothing_totals(
                rows,
                project_count_key=None,
                allocated_key="expected_value",
            ),
            "rows": rows,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch smoothing project scores")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch smoothing project score data",
        ) from exc


@router.get("/smoothing/monthly")
def get_pipeline_smoothing_monthly(
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
    Read-only smoothed monthly revenue endpoint backed by mv_pipeline_smoothed_revenue_monthly_12m_v1.
    """
    try:
        stage_filter = _normalize_stage_bucket(stage_bucket)
        window_start = _month_start(as_of_month or date.today())
        window_end = _add_months(window_start, months)

        supabase = SupabaseClient()
        query = (
            supabase.client.table("mv_pipeline_smoothed_revenue_monthly_12m_v1")
            .select(
                "forecast_month,stage_bucket,project_count,unsmoothed_project_count,"
                "smoothed_project_count,unsmoothed_expected_value,"
                "smoothed_expected_value,allocated_expected_value"
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

        return {
            "source": "mv_pipeline_smoothed_revenue_monthly_12m_v1",
            "window_start": window_start.isoformat(),
            "window_end_exclusive": window_end.isoformat(),
            "months_requested": months,
            "stage_bucket": stage_filter,
            "row_count": len(rows),
            "totals": _smoothing_totals(
                rows,
                expected_key="allocated_expected_value",
            ),
            "rows": rows,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch smoothing monthly forecast")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch smoothing monthly forecast data",
        ) from exc


@router.get("/smoothing/snapshot")
def get_pipeline_smoothing_snapshot(
    snapshot_date: Optional[date] = Query(
        None,
        description="Snapshot date (YYYY-MM-DD). Defaults to latest smoothing snapshot date.",
    ),
    project_id: Optional[str] = Query(
        None,
        description="Optional project_id filter.",
    ),
    stage_bucket: Optional[str] = Query(
        None,
        description="Optional filter: Committed, Open, or Lost.",
    ),
    risk_band: Optional[str] = Query(
        None,
        description="Optional smoothing risk band: Very High, High, Moderate, or Low.",
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(_DEFAULT_SNAPSHOT_LIMIT, ge=1, le=_MAX_SNAPSHOT_LIMIT),
) -> Dict[str, Any]:
    """
    Read-only smoothing snapshot endpoint backed by pipeline_smoothing_forecast_snapshot.
    """
    try:
        stage_filter = _normalize_stage_bucket(stage_bucket)
        risk_filter = _normalize_risk_band(risk_band)
        supabase = SupabaseClient()

        resolved_snapshot_date = snapshot_date
        if resolved_snapshot_date is None:
            resolved_snapshot_date = _resolve_latest_smoothing_snapshot_date(supabase)
            if resolved_snapshot_date is None:
                return {
                    "source": "pipeline_smoothing_forecast_snapshot",
                    "snapshot_date": None,
                    "row_count": 0,
                    "total_rows": 0,
                    "offset": offset,
                    "limit": limit,
                    "totals": _smoothing_totals([], project_count_key=None),
                    "rows": [],
                }

        query = (
            _build_smoothing_snapshot_query(
                supabase,
                snapshot_date_value=resolved_snapshot_date,
                project_id=project_id,
                stage_bucket=stage_filter,
                risk_band=risk_filter,
                select_clause=(
                "snapshot_date,project_id,forecast_month,stage_bucket,base_forecast_month,"
                "forecast_date,smoothing_as_of_date,expected_value,combined_smoothed_probability,"
                "expected_spread_days,risk_band,workbook_suggested_treatment,"
                "default_smoothing_recommended,unsmoothed_allocated_value,"
                    "smoothed_allocated_value,allocated_expected_value,created_at"
                ),
                count="exact",
            )
            .order("forecast_month")
            .order("project_id")
        )

        result = query.range(offset, offset + limit - 1).execute()
        rows = result.data or []
        total_rows = result.count if result.count is not None else len(rows)

        return {
            "source": "pipeline_smoothing_forecast_snapshot",
            "snapshot_date": resolved_snapshot_date.isoformat(),
            "project_id": project_id,
            "stage_bucket": stage_filter,
            "risk_band": risk_filter,
            "offset": offset,
            "limit": limit,
            "row_count": len(rows),
            "total_rows": total_rows,
            "totals": _smoothing_totals(
                rows,
                project_count_key=None,
                smoothed_key="smoothed_allocated_value",
                unsmoothed_key="unsmoothed_allocated_value",
            ),
            "rows": rows,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch smoothing snapshot")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch smoothing snapshot data",
        ) from exc


@router.get("/smoothing/snapshot/totals")
def get_pipeline_smoothing_snapshot_totals(
    snapshot_date: Optional[date] = Query(
        None,
        description="Snapshot date (YYYY-MM-DD). Defaults to latest smoothing snapshot date.",
    ),
    project_id: Optional[str] = Query(
        None,
        description="Optional project_id filter.",
    ),
    stage_bucket: Optional[str] = Query(
        None,
        description="Optional filter: Committed, Open, or Lost.",
    ),
    risk_band: Optional[str] = Query(
        None,
        description="Optional smoothing risk band: Very High, High, Moderate, or Low.",
    ),
) -> Dict[str, Any]:
    """
    Full filtered totals for pipeline_smoothing_forecast_snapshot, independent of pagination.

    Snapshot rows are project-month allocations: expected_value repeats the original
    project value for context, while allocated_monthly_value is the additive
    forecast total from allocated_expected_value.
    """
    try:
        stage_filter = _normalize_stage_bucket(stage_bucket)
        risk_filter = _normalize_risk_band(risk_band)
        supabase = SupabaseClient()

        resolved_snapshot_date = snapshot_date or _resolve_latest_smoothing_snapshot_date(supabase)
        if resolved_snapshot_date is None:
            return {
                "source": "pipeline_smoothing_forecast_snapshot",
                "snapshot_date": None,
                "project_id": project_id,
                "stage_bucket": stage_filter,
                "risk_band": risk_filter,
                "row_count": 0,
                "totals": _smoothing_totals([], project_count_key=None),
            }

        rows = _fetch_all_smoothing_snapshot_total_rows(
            supabase,
            snapshot_date_value=resolved_snapshot_date,
            project_id=project_id,
            stage_bucket=stage_filter,
            risk_band=risk_filter,
        )

        return {
            "source": "pipeline_smoothing_forecast_snapshot",
            "snapshot_date": resolved_snapshot_date.isoformat(),
            "project_id": project_id,
            "stage_bucket": stage_filter,
            "risk_band": risk_filter,
            "row_count": len(rows),
            "totals": _smoothing_totals(
                rows,
                project_count_key=None,
                smoothed_key="smoothed_allocated_value",
                unsmoothed_key="unsmoothed_allocated_value",
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to fetch smoothing snapshot totals")
        raise HTTPException(
            status_code=500,
            detail="Failed to fetch smoothing snapshot totals",
        ) from exc