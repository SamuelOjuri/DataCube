import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient  # noqa: E402
from src.database.sync_service import DataSyncService  # noqa: E402
from src.tasks.pipeline import rehydrate_projects_by_ids  # noqa: E402

logger = logging.getLogger("trace_rehydrate_rollups")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace persisted rollups for a small set of project IDs"
    )
    parser.add_argument(
        "--project-id",
        action="append",
        dest="project_ids",
        required=True,
        help="Project monday_id to trace. Pass multiple times.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10,
        help="Chunk size passed into rehydrate_projects_by_ids",
    )
    parser.add_argument(
        "--verbose-subitems",
        action="store_true",
        help="Log each subitem contributing to the rollups",
    )
    return parser.parse_args()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            cleaned = (
                value.replace("£", "")
                .replace("$", "")
                .replace("€", "")
                .replace(",", "")
                .strip()
            )
            if not cleaned:
                return None
            return float(cleaned)
        return float(value)
    except Exception:
        return None


def _to_date(value: Any) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            try:
                return datetime.strptime(value[:10], "%Y-%m-%d").date().isoformat()
            except Exception:
                return None
    try:
        return str(value)[:10]
    except Exception:
        return None


def _compute_expected_rollups(
    subitems: List[Dict[str, Any]]
) -> Tuple[Dict[str, float], Dict[str, str], Dict[str, float], Dict[str, float], Dict[str, int]]:
    order_totals: Dict[str, float] = defaultdict(float)
    order_dates: Dict[str, str] = {}
    invoice_totals: Dict[str, float] = defaultdict(float)
    new_enquiry_totals: Dict[str, float] = defaultdict(float)
    earliest_design_dates: Dict[str, str] = {}
    earliest_invoice_dates: Dict[str, str] = {}

    for row in subitems:
        pid = str(row.get("parent_monday_id") or "").strip()
        if not pid:
            continue

        order_value = _to_float(row.get("cust_order_value_material"))
        if order_value is not None:
            order_totals[pid] = round(order_totals[pid] + order_value, 2)

        invoice_value = _to_float(row.get("amount_invoiced"))
        if invoice_value is not None:
            invoice_totals[pid] = round(invoice_totals[pid] + invoice_value, 2)

        order_date = _to_date(row.get("date_order_received"))
        if order_date and (pid not in order_dates or order_date < order_dates[pid]):
            order_dates[pid] = order_date

        new_enquiry_value = _to_float(row.get("new_enquiry_value"))
        if new_enquiry_value is None:
            reason = str(row.get("reason_for_change") or "").strip()
            if reason == "New Enquiry":
                new_enquiry_value = _to_float(row.get("quote_amount")) or 0.0
            else:
                new_enquiry_value = 0.0

        new_enquiry_totals[pid] = round(
            new_enquiry_totals[pid] + float(new_enquiry_value), 2
        )

        design_date = _to_date(row.get("date_design_completed"))
        if design_date and (
            pid not in earliest_design_dates or design_date < earliest_design_dates[pid]
        ):
            earliest_design_dates[pid] = design_date

        invoice_date = _to_date(row.get("invoice_date"))
        if invoice_date and (
            pid not in earliest_invoice_dates or invoice_date < earliest_invoice_dates[pid]
        ):
            earliest_invoice_dates[pid] = invoice_date

    gestation_totals: Dict[str, int] = {}
    for pid, design_date in earliest_design_dates.items():
        invoice_date = earliest_invoice_dates.get(pid)
        if not invoice_date:
            continue
        try:
            design_dt = datetime.strptime(design_date, "%Y-%m-%d")
            invoice_dt = datetime.strptime(invoice_date, "%Y-%m-%d")
        except Exception:
            continue

        days = (invoice_dt - design_dt).days
        if 0 < days <= 500000:
            gestation_totals[pid] = int(days)

    return (
        dict(order_totals),
        dict(order_dates),
        dict(invoice_totals),
        dict(new_enquiry_totals),
        dict(gestation_totals),
    )


def _fetch_projects(db: SupabaseClient, project_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    rows = (
        db.client.table("projects")
        .select(
            "monday_id, item_name, total_order_value, date_order_received, "
            "total_amount_invoiced, new_enquiry_value, gestation_period"
        )
        .in_("monday_id", project_ids)
        .execute()
        .data
        or []
    )
    return {str(row["monday_id"]): row for row in rows if row.get("monday_id")}


def _fetch_subitems(db: SupabaseClient, project_ids: List[str]) -> List[Dict[str, Any]]:
    return (
        db.client.table("subitems")
        .select(
            "monday_id, parent_monday_id, item_name, "
            "cust_order_value_material, date_order_received, amount_invoiced, "
            "new_enquiry_value, reason_for_change, quote_amount, "
            "date_design_completed, invoice_date"
        )
        .in_("parent_monday_id", project_ids)
        .execute()
        .data
        or []
    )


def _log_snapshot(
    label: str,
    project_ids: List[str],
    projects: Dict[str, Dict[str, Any]],
    subitems: List[Dict[str, Any]],
    verbose_subitems: bool,
) -> None:
    order_totals, order_dates, invoice_totals, new_enquiry_totals, gestation_totals = _compute_expected_rollups(
        subitems
    )
    subitems_by_parent: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in subitems:
        pid = str(row.get("parent_monday_id") or "").strip()
        if pid:
            subitems_by_parent[pid].append(row)

    logger.info("========== %s ==========", label)

    for pid in project_ids:
        project = projects.get(pid) or {}
        rows = subitems_by_parent.get(pid, [])

        logger.info(
            (
                "Project %s | name=%s "
                "| project.total_order_value=%s | expected.total_order_value=%s "
                "| project.date_order_received=%s | expected.date_order_received=%s "
                "| project.total_amount_invoiced=%s | expected.total_amount_invoiced=%s "
                "| project.new_enquiry_value=%s | expected.new_enquiry_value=%s "
                "| project.gestation_period=%s | expected.gestation_period=%s "
                "| subitems=%s"
            ),
            pid,
            project.get("item_name"),
            project.get("total_order_value"),
            order_totals.get(pid),
            project.get("date_order_received"),
            order_dates.get(pid),
            project.get("total_amount_invoiced"),
            invoice_totals.get(pid),
            project.get("new_enquiry_value"),
            new_enquiry_totals.get(pid),
            project.get("gestation_period"),
            gestation_totals.get(pid),
            len(rows),
        )

        if verbose_subitems:
            for row in rows:
                logger.info(
                    (
                        "  subitem=%s | name=%s | new_enquiry_value=%s | reason_for_change=%s "
                        "| quote_amount=%s | cust_order_value_material=%s "
                        "| date_order_received=%s | amount_invoiced=%s "
                        "| date_design_completed=%s | invoice_date=%s"
                    ),
                    row.get("monday_id"),
                    row.get("item_name"),
                    row.get("new_enquiry_value"),
                    row.get("reason_for_change"),
                    row.get("quote_amount"),
                    row.get("cust_order_value_material"),
                    row.get("date_order_received"),
                    row.get("amount_invoiced"),
                    row.get("date_design_completed"),
                    row.get("invoice_date"),
                )


def _install_trace_hooks() -> None:
    original_order_rollup = DataSyncService._rollup_order_values_from_subitems
    original_invoice_rollup = DataSyncService._rollup_invoice_totals_from_subitems
    original_new_enquiry_rollup = DataSyncService._rollup_new_enquiry_from_subitems
    original_persisted_gestation_rollup = DataSyncService._compute_persisted_gestation_rollup_from_subitems
    original_batch_update = DataSyncService._batch_update_rollup_column
    original_batch_fill = DataSyncService._batch_fill_missing_numeric_rollup_column
    original_batch_fill_gestation = DataSyncService._batch_fill_missing_gestation_rollup_column
    original_compute_persisted = DataSyncService._compute_project_rollups_from_persisted_subitems

    def traced_order_rollup(self, subitems_data):
        totals, dates = original_order_rollup(self, subitems_data)
        logger.info("TRACE order rollup totals: %s", totals)
        logger.info("TRACE order rollup dates: %s", dates)
        return totals, dates

    def traced_invoice_rollup(self, subitems_data):
        totals = original_invoice_rollup(self, subitems_data)
        logger.info("TRACE invoice rollup totals: %s", totals)
        return totals

    def traced_new_enquiry_rollup(self, subitems_data):
        totals = original_new_enquiry_rollup(self, subitems_data)
        logger.info("TRACE new enquiry rollup totals: %s", totals)
        return totals

    def traced_persisted_gestation_rollup(self, subitems_data):
        totals = original_persisted_gestation_rollup(self, subitems_data)
        logger.info("TRACE persisted gestation totals: %s", totals)
        return totals

    def traced_compute_persisted(self, parent_ids):
        result = original_compute_persisted(self, parent_ids)
        (
            order_totals,
            order_dates,
            invoice_totals,
            new_enquiry_totals,
            gestation_totals,
            first_invoice_dates,
            last_invoice_dates,
            invoice_spreads,
        ) = result
        logger.info("TRACE persisted parent_ids: %s", parent_ids)
        logger.info("TRACE persisted order totals: %s", order_totals)
        logger.info("TRACE persisted order dates: %s", order_dates)
        logger.info("TRACE persisted invoice totals: %s", invoice_totals)
        logger.info("TRACE persisted first invoice dates: %s", first_invoice_dates)
        logger.info("TRACE persisted last invoice dates: %s", last_invoice_dates)
        logger.info("TRACE persisted invoice spreads: %s", invoice_spreads)
        logger.info("TRACE persisted new enquiry totals: %s", new_enquiry_totals)
        logger.info("TRACE persisted gestation totals: %s", gestation_totals)
        return result

    async def traced_batch_update(self, column, values):
        logger.info(
            "TRACE batch update request | column=%s | parent_count=%s | values=%s",
            column,
            len(values or {}),
            values,
        )
        result = await original_batch_update(self, column, values)
        logger.info(
            "TRACE batch update result | column=%s | updated=%s",
            column,
            result,
        )
        return result

    async def traced_batch_fill(self, column, values):
        logger.info(
            "TRACE batch fill request | column=%s | parent_count=%s | values=%s",
            column,
            len(values or {}),
            values,
        )
        result = await original_batch_fill(self, column, values)
        logger.info(
            "TRACE batch fill result | column=%s | updated=%s",
            column,
            result,
        )
        return result

    async def traced_batch_fill_gestation(self, values):
        logger.info(
            "TRACE batch fill request | column=gestation_period | parent_count=%s | values=%s",
            len(values or {}),
            values,
        )
        result = await original_batch_fill_gestation(self, values)
        logger.info(
            "TRACE batch fill result | column=gestation_period | updated=%s",
            result,
        )
        return result

    DataSyncService._rollup_order_values_from_subitems = traced_order_rollup
    DataSyncService._rollup_invoice_totals_from_subitems = traced_invoice_rollup
    DataSyncService._rollup_new_enquiry_from_subitems = traced_new_enquiry_rollup
    DataSyncService._compute_persisted_gestation_rollup_from_subitems = traced_persisted_gestation_rollup
    DataSyncService._compute_project_rollups_from_persisted_subitems = traced_compute_persisted
    DataSyncService._batch_update_rollup_column = traced_batch_update
    DataSyncService._batch_fill_missing_numeric_rollup_column = traced_batch_fill
    DataSyncService._batch_fill_missing_gestation_rollup_column = traced_batch_fill_gestation


async def main() -> None:
    args = _parse_args()
    project_ids = [str(pid).strip() for pid in args.project_ids if str(pid).strip()]

    if not project_ids:
        raise SystemExit("No project IDs supplied")

    logger.info("Tracing rollups for %s project(s): %s", len(project_ids), ", ".join(project_ids))

    db = SupabaseClient()

    before_projects = _fetch_projects(db, project_ids)
    before_subitems = _fetch_subitems(db, project_ids)
    _log_snapshot("BEFORE", project_ids, before_projects, before_subitems, args.verbose_subitems)

    _install_trace_hooks()

    await rehydrate_projects_by_ids(
        project_ids,
        chunk_size=max(1, args.chunk_size),
        logger=logger,
    )

    after_projects = _fetch_projects(db, project_ids)
    after_subitems = _fetch_subitems(db, project_ids)
    _log_snapshot("AFTER", project_ids, after_projects, after_subitems, args.verbose_subitems)

    logger.info("Trace run complete")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main())