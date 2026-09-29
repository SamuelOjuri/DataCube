"""Stage, review and transactionally apply an order-only Monday backfill."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from dotenv import load_dotenv
from psycopg import sql
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.sync_service import DataSyncService
from src.config import HIDDEN_ITEMS_BOARD_ID, HIDDEN_ITEMS_COLUMNS, PARENT_BOARD_ID, SUBITEM_BOARD_ID, SUBITEM_COLUMNS
from src.core.monday_client import MondayClient

ORDER_FIELDS = ("cust_order_value_material", "cust_additional_charges")
SCRIPT_VERSION = 1
TOTAL_COLUMN = "formula_mkncjq9"
BASELINE_COLUMNS = {
    "projects": ("monday_id", "item_name", "total_order_value", "new_enquiry_value",
                 "total_amount_invoiced", "date_order_received"),
    "subitems": ("monday_id", "parent_monday_id", "hidden_item_id", *ORDER_FIELDS,
                 "amount_invoiced", "invoice_date", "date_order_received"),
    "hidden_items": ("monday_id", *ORDER_FIELDS, "amount_invoiced", "invoice_date", "date_order_received"),
}
LOG = logging.getLogger(__name__)


def money(value: Any) -> str | None:
    if value is None:
        return None
    service = DataSyncService.__new__(DataSyncService)
    return format(service._parse_order_amount(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def indexed(rows: list[dict], key: str = "monday_id") -> dict[str, dict]:
    result = {}
    for row in rows:
        item_id = str(row.get(key) or "").strip()
        if not item_id or item_id in result:
            raise ValueError(f"Empty or duplicate {key} in input")
        result[item_id] = row
    return result


def build_plan(baseline: dict, source: dict, approved_empty: set[str] | None = None) -> dict:
    service = DataSyncService.__new__(DataSyncService)
    approved_empty = approved_empty or set()
    projects = indexed(baseline["projects"])
    if approved_empty - projects.keys():
        raise ValueError("An approved empty-project ID is absent from the database baseline")
    stored_children = indexed(baseline["subitems"])
    stored_hidden = indexed(baseline["hidden_items"])
    live_children = indexed(source["subitems"])
    live_hidden = indexed(source["hidden_items"])
    live_projects = set(source["project_ids"])
    issues: dict[str, set[str]] = defaultdict(set)
    children: dict[str, list[dict]] = defaultdict(list)
    hidden_parents: dict[str, list[str]] = defaultdict(list)
    stored_hidden_parents: dict[str, list[str]] = defaultdict(list)
    for child in stored_children.values():
        if child.get("hidden_item_id"):
            stored_hidden_parents[child["hidden_item_id"]].append(str(child.get("parent_monday_id") or ""))
    diagnostics = []
    for child_id in sorted(set(stored_children) | set(live_children)):
        stored = stored_children.get(child_id)
        live = live_children.get(child_id)
        parents = {
            str(row["parent_monday_id"]) for row in (stored, live)
            if row and row.get("parent_monday_id")
        }
        reason = None
        if stored is None or live is None:
            reason = "subitem_missing_in_database_or_monday"
        elif stored.get("parent_monday_id") != live.get("parent_monday_id"):
            reason = "subitem_parent_mismatch"
        if reason:
            for parent_id in parents:
                issues[parent_id].add(reason)
            diagnostics.append({"subitem_id": child_id, "issue": reason})
        if live is None:
            continue
        parent_id = str(live.get("parent_monday_id") or "")
        if parent_id not in projects:
            diagnostics.append({"subitem_id": child_id, "issue": "parent_missing_in_database"})
        linked_ids = live.get("hidden_ids", [])
        for hidden_id in linked_ids:
            hidden_parents[hidden_id].append(parent_id)
        if live.get("link_error") or len(linked_ids) != 1:
            issues[parent_id].add("missing_invalid_or_multiple_hidden_links")
            continue
        hidden_id = linked_ids[0]
        hidden = live_hidden.get(hidden_id)
        if stored and stored.get("hidden_item_id") != hidden_id:
            issues[parent_id].add("stored_hidden_link_mismatch")
        if hidden_id not in stored_hidden or hidden is None:
            issues[parent_id].add("hidden_item_missing_in_database_or_monday")
            continue
        if hidden.get("issues"):
            issues[parent_id].update(hidden["issues"])
        if any(hidden.get(field) is None for field in ORDER_FIELDS):
            issues[parent_id].add("unknown_order_component")
        children[parent_id].append({
            "monday_id": child_id, "parent_monday_id": parent_id,
            "hidden_item_id": hidden_id,
            **{field: hidden.get(field) for field in ORDER_FIELDS},
        })

    for hidden_id, parents in hidden_parents.items():
        stored_parents = stored_hidden_parents[hidden_id]
        if len(parents) > 1 or len(stored_parents) > 1:
            for parent_id in set(parents + stored_parents):
                issues[parent_id].add("duplicate_hidden_source_link")

    for hidden_id, hidden in live_hidden.items():
        if hidden_id not in hidden_parents and any(
            hidden.get(field) is not None and Decimal(hidden[field]) != 0 for field in ORDER_FIELDS
        ):
            diagnostics.append({"hidden_item_id": hidden_id, "issue": "unlinked_hidden_order"})

    report = []
    updates = {"hidden_items": [], "subitems": [], "projects": []}
    for project_id, project in sorted(projects.items()):
        if project_id not in live_projects:
            issues[project_id].add("project_missing_in_monday")
        project_children = children[project_id]
        if not project_children and project_id not in issues and project_id not in approved_empty:
            issues[project_id].add("empty_project_requires_explicit_approval")
        totals, _ = service._rollup_order_values_from_subitems(project_children)
        new_total = money(totals.get(project_id)) if project_children else "0.00"
        if project_children and new_total is None:
            issues[project_id].add("invalid_order_rollup")
        blocked = bool(issues[project_id])
        material = sum((Decimal(row[ORDER_FIELDS[0]]) for row in project_children
                        if row[ORDER_FIELDS[0]] is not None), Decimal("0"))
        charges = sum((Decimal(row[ORDER_FIELDS[1]]) for row in project_children
                       if row[ORDER_FIELDS[1]] is not None), Decimal("0"))
        old_total = money(project.get("total_order_value"))
        report.append({
            "project_id": project_id, "item_name": project.get("item_name", ""),
            "old_total": old_total, "material_sum": str(material) if not blocked else None,
            "charge_sum": str(charges) if not blocked else None,
            "new_total": new_total if not blocked else None,
            "difference": str(Decimal(new_total) - Decimal(old_total))
            if not blocked and old_total is not None else None,
            "subitem_count": len(project_children),
            "status": "blocked" if blocked else "verified",
            "issues": sorted(issues[project_id]),
        })
        if blocked:
            continue
        if old_total != new_total:
            updates["projects"].append({"monday_id": project_id, "total_order_value": new_total})
        for child in project_children:
            values = {field: money(child[field]) for field in ORDER_FIELDS}
            for table, item_id, existing in (
                ("subitems", child["monday_id"], stored_children[child["monday_id"]]),
                ("hidden_items", child["hidden_item_id"], stored_hidden[child["hidden_item_id"]]),
            ):
                if any(money(existing.get(field)) != values[field] for field in ORDER_FIELDS):
                    updates[table].append({"monday_id": item_id, **values})
    return {"projects": report, "updates": updates, "diagnostics": diagnostics}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True).encode("utf-8")


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def code_fingerprint() -> str:
    return hashlib.sha256(
        Path(__file__).read_bytes()
        + (Path(__file__).resolve().parent.parent / "src/database/sync_service.py").read_bytes()
    ).hexdigest()


def source_contract() -> dict:
    return {"boards": [PARENT_BOARD_ID, SUBITEM_BOARD_ID, HIDDEN_ITEMS_BOARD_ID],
            "order_fields": {field: HIDDEN_ITEMS_COLUMNS[field] for field in ORDER_FIELDS},
            "link_column": SUBITEM_COLUMNS["hidden_item_id"], "total_column": TOTAL_COLUMN}


def target_fingerprint(connection) -> str:
    return fingerprint({key: str(getattr(connection.info, key)) for key in ("host", "port", "dbname", "user")})


def write_json(path: Path, value: Any) -> None:
    with path.open("xb") as stream:
        stream.write(canonical_bytes(value))


def read_baseline(connection) -> dict:
    baseline = {}
    with connection.cursor(row_factory=dict_row) as cursor:
        for table, columns in BASELINE_COLUMNS.items():
            rows = []
            last_id = None
            while True:
                cursor.execute(
                    sql.SQL("SELECT {} FROM public.{} WHERE (%s::text IS NULL OR monday_id > %s) ORDER BY monday_id LIMIT %s").format(
                        sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(table)
                    ), (last_id, last_id, 1000),
                )
                page = cursor.fetchall()
                if not page:
                    break
                for row in page:
                    rows.append({key: str(value) if isinstance(value, Decimal)
                                 else value.isoformat() if isinstance(value, (date, datetime)) else value
                                 for key, value in row.items()})
                last_id = page[-1]["monday_id"]
            baseline[table] = rows
    return baseline


def fetch_board(monday, board_id: str, columns: list[str], include_parent: bool = False) -> list[dict]:
    before_count = monday.get_board_info(board_id)["items_count"]
    cursor = None
    cursors = set()
    items = {}
    while True:
        page = (monday.get_next_item_ids_page(cursor, 100) if cursor
                else monday.get_item_ids_page(board_id, 100, None))
        page_ids = [str(row["id"]) for row in page["items"]]
        if len(set(page_ids)) != len(page_ids) or set(page_ids) & items.keys():
            raise ValueError("Duplicate Monday IDs during traversal; prepare a fresh run")
        if page_ids and not columns:
            items.update({item_id: {"id": item_id} for item_id in page_ids})
        elif page_ids:
            query = """
                query BackfillOrders($ids: [ID!]!, $columns: [String!]!) {
                    items(ids: $ids, limit: 100) {
                        id name PARENT_FRAGMENT
                        column_values(ids: $columns) {
                            id type text value
                            ... on FormulaValue { display_value }
                            ... on BoardRelationValue { linked_item_ids }
                        }
                    }
                }
            """.replace("PARENT_FRAGMENT", "parent_item { id }" if include_parent else "")
            details = monday.execute_query(query, {"ids": page_ids, "columns": columns})["data"]["items"]
            detail_map = indexed(details, "id")
            if set(detail_map) != set(page_ids):
                raise ValueError("Incomplete Monday detail response; prepare a fresh run")
            items.update(detail_map)
        cursor = page.get("next_cursor")
        if not cursor:
            break
        if not page_ids or cursor in cursors:
            raise ValueError("Monday pagination did not advance")
        cursors.add(cursor)
        LOG.info("Monday board %s: %d items captured", board_id, len(items))
    after_count = monday.get_board_info(board_id)["items_count"]
    if before_count != after_count or len(items) != before_count:
        raise ValueError(
            "Monday board count changed or traversal was incomplete "
            f"(board_id={board_id}, before_count={before_count}, "
            f"after_count={after_count}, len(items)={len(items)}); prepare a fresh run"
        )
    return [items[item_id] for item_id in sorted(items)]


def normalize_hidden(item: dict) -> dict:
    service = DataSyncService.__new__(DataSyncService)
    amounts = {field: money(value) for field, value in service._extract_hidden_order_amounts(item).items()}
    issues = []
    columns = {column["id"]: column for column in item["column_values"]}
    total_column = columns.get(TOTAL_COLUMN)
    total = None
    try:
        if total_column is None or not ({"display_value", "text"} & total_column.keys()):
            raise ValueError("Total formula missing")
        raw_total = total_column.get("display_value")
        if raw_total is None:
            raw_total = total_column.get("text")
        total = money(service._parse_order_amount(raw_total))
        if all(value is not None for value in amounts.values()):
            if sum((Decimal(value) for value in amounts.values()), Decimal("0")) != Decimal(total):
                issues.append("monday_formula_mismatch")
    except ValueError:
        issues.append("monday_total_unavailable_or_invalid")
    return {"monday_id": str(item["id"]), "item_name": item.get("name", ""),
            **amounts, "monday_total": total, "issues": issues}


def normalize_subitem(item: dict) -> dict:
    column = next((value for value in item["column_values"]
                   if value["id"] == SUBITEM_COLUMNS["hidden_item_id"]), None)
    linked_ids = []
    link_error = False
    try:
        if column is None:
            raise ValueError("Link column missing")
        if "linked_item_ids" in column:
            linked_ids = column["linked_item_ids"]
        else:
            value = column.get("value")
            value = json.loads(value) if isinstance(value, str) else value
            if not isinstance(value, dict):
                raise ValueError("Link value unavailable")
            if "linkedPulseIds" in value:
                linked_ids = [row["linkedPulseId"] for row in value["linkedPulseIds"]]
            else:
                linked_ids = value["linkedItemIds"]
        if not isinstance(linked_ids, list):
            raise ValueError("Invalid links")
        linked_ids = sorted({str(item_id) for item_id in linked_ids})
        if any(not item_id.isdigit() for item_id in linked_ids):
            raise ValueError("Invalid link ID")
    except (ValueError, TypeError, KeyError):
        linked_ids = []
        link_error = True
    return {"monday_id": str(item["id"]), "item_name": item.get("name", ""),
            "parent_monday_id": str((item.get("parent_item") or {}).get("id") or ""),
            "hidden_ids": linked_ids, "link_error": link_error}


def capture_source(monday) -> dict:
    projects = fetch_board(monday, PARENT_BOARD_ID, [])
    hidden = fetch_board(monday, HIDDEN_ITEMS_BOARD_ID,
                         [HIDDEN_ITEMS_COLUMNS[field] for field in ORDER_FIELDS] + [TOTAL_COLUMN])
    subitems = fetch_board(monday, SUBITEM_BOARD_ID, [SUBITEM_COLUMNS["hidden_item_id"]], True)
    return {"project_ids": [str(row["id"]) for row in projects],
            "hidden_items": [normalize_hidden(row) for row in hidden],
            "subitems": [normalize_subitem(row) for row in subitems]}


def write_review(run_dir: Path, plan: dict) -> None:
    with (run_dir / "review.csv").open("x", encoding="utf-8", newline="") as stream:
        fields = ["project_id", "item_name", "old_total", "material_sum", "charge_sum", "new_total",
                  "difference", "subitem_count", "status", "issues"]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in plan["projects"]:
            output = {**row, "issues": ";".join(row["issues"])}
            name = str(output["item_name"])
            if name.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")):
                output["item_name"] = "'" + name
            writer.writerow(output)


def prepare_run(connection, monday, run_dir: Path, approved_empty: set[str]) -> dict:
    run_dir.mkdir(parents=True, exist_ok=False)
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        baseline = read_baseline(connection)
    write_json(run_dir / "baseline.json", baseline)
    source = capture_source(monday)
    write_json(run_dir / "source.json", source)
    plan = build_plan(baseline, source, approved_empty)
    write_json(run_dir / "plan.json", plan)
    write_review(run_dir, plan)
    manifest = {
        "version": SCRIPT_VERSION, "run_id": str(uuid4()),
        "prepared_at": datetime.now(timezone.utc).isoformat(),
        "target": target_fingerprint(connection), "code": code_fingerprint(),
        "source_contract": source_contract(),
        "approved_empty": sorted(approved_empty),
        "review_sha256": hashlib.sha256((run_dir / "review.csv").read_bytes()).hexdigest(),
        "hashes": {name: fingerprint(value) for name, value in
                   (("baseline", baseline), ("source", source), ("plan", plan))},
        "summary": {"verified_projects": sum(row["status"] == "verified" for row in plan["projects"]),
                    "blocked_projects": sum(row["status"] == "blocked" for row in plan["projects"]),
                    "diagnostics": len(plan["diagnostics"]),
                    "updates": {table: len(rows) for table, rows in plan["updates"].items()}},
    }
    write_json(run_dir / "manifest.json", manifest)
    return manifest


def load_run(run_dir: Path) -> tuple[dict, dict, dict, dict]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest["version"] != SCRIPT_VERSION or manifest["code"] != code_fingerprint():
        raise ValueError("Backfill code changed since preparation; prepare a fresh run")
    if manifest["source_contract"] != source_contract():
        raise ValueError("Source mapping changed since preparation; prepare a fresh run")
    artifacts = {}
    for name in ("baseline", "source", "plan"):
        value = json.loads((run_dir / f"{name}.json").read_text(encoding="utf-8"))
        if fingerprint(value) != manifest["hashes"][name]:
            raise ValueError(f"The {name} artifact changed; prepare a fresh run")
        artifacts[name] = value
    if hashlib.sha256((run_dir / "review.csv").read_bytes()).hexdigest() != manifest["review_sha256"]:
        raise ValueError("The review CSV changed; prepare a fresh run")
    rebuilt = build_plan(artifacts["baseline"], artifacts["source"], set(manifest["approved_empty"]))
    if rebuilt != artifacts["plan"]:
        raise ValueError("Stored plan does not match its source evidence")
    return manifest, artifacts["baseline"], artifacts["source"], rebuilt


def expected_baseline(baseline: dict, plan: dict) -> dict:
    expected = json.loads(json.dumps(baseline))
    for table, rows in plan["updates"].items():
        by_id = indexed(expected[table])
        for row in rows:
            by_id[row["monday_id"]].update(row)
    return expected


def apply_updates(connection, updates: dict) -> dict:
    counts = {}
    with connection.cursor(row_factory=dict_row) as cursor:
        for table in ("hidden_items", "subitems", "projects"):
            fields = ("total_order_value",) if table == "projects" else ORDER_FIELDS
            statement = sql.SQL("UPDATE public.{} SET {} WHERE monday_id = %s RETURNING monday_id").format(
                sql.Identifier(table),
                sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(field)) for field in fields),
            )
            counts[table] = 0
            for row in updates[table]:
                values = [Decimal(row[field]) for field in fields]
                cursor.execute(statement, (*values, row["monday_id"]))
                result = cursor.fetchone()
                if result is None or result["monday_id"] != row["monday_id"]:
                    raise ValueError("An expected row was not updated; rolling back")
                counts[table] += 1
    return counts


def apply_run(connection, monday, run_dir: Path, *, confirm_run_id: str,
              writers_paused: bool, allow_blocked: bool = False) -> dict:
    if not writers_paused:
        raise ValueError("Pause sync writers, queue consumers and snapshots before applying")
    manifest, baseline, source, plan = load_run(run_dir)
    if manifest["run_id"] != confirm_run_id:
        raise ValueError("Confirmation does not match the reviewed run ID")
    if manifest["target"] != target_fingerprint(connection):
        raise ValueError("Database connection target differs from the prepared run")
    blocked = sum(row["status"] == "blocked" for row in plan["projects"])
    if (blocked or plan["diagnostics"]) and not allow_blocked:
        raise ValueError("Unresolved projects or source diagnostics exist; resolve them or explicitly use --allow-blocked")
    LOG.info("Rechecking fresh Monday values against the reviewed source")
    if capture_source(monday) != source:
        raise ValueError("Monday sources changed since preparation; prepare a fresh run")
    expected = expected_baseline(baseline, plan)
    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout = '5s'")
        connection.execute("SET LOCAL statement_timeout = '60s'")
        connection.execute("LOCK TABLE public.hidden_items, public.subitems, public.projects IN SHARE ROW EXCLUSIVE MODE")
        current = read_baseline(connection)
        if current == expected:
            counts = {table: 0 for table in plan["updates"]}
            status = "already_applied"
        else:
            if current != baseline:
                raise ValueError("Database values or relationships changed since preparation; rolling back")
            counts = apply_updates(connection, plan["updates"])
            if read_baseline(connection) != expected:
                raise ValueError("Post-write reconciliation failed; rolling back all changes")
            status = "applied"
    receipt = {"run_id": manifest["run_id"], "status": status,
               "completed_at": datetime.now(timezone.utc).isoformat(),
               "updated_rows": counts, "blocked_projects": blocked,
               "source_diagnostics": len(plan["diagnostics"]),
               "expected_sha256": fingerprint(expected)}
    write_json(run_dir / f"apply-{uuid4()}.json", receipt)
    return receipt


def verify_run(connection, run_dir: Path) -> dict:
    manifest, baseline, _, plan = load_run(run_dir)
    if manifest["target"] != target_fingerprint(connection):
        raise ValueError("Database connection target differs from the prepared run")
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        current = read_baseline(connection)
    expected = expected_baseline(baseline, plan)
    result = {"run_id": manifest["run_id"], "verified_at": datetime.now(timezone.utc).isoformat(),
              "matches_reviewed_result": current == expected,
              "expected_sha256": fingerprint(expected), "actual_sha256": fingerprint(current),
              "blocked_projects": sum(row["status"] == "blocked" for row in plan["projects"]),
              "source_diagnostics": len(plan["diagnostics"])}
    write_json(run_dir / f"verify-{uuid4()}.json", result)
    return result


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="Read-only dry run: capture sources and write an audit plan")
    prepare.add_argument("--run-dir", type=Path, required=True, help="New directory; existing directories are never overwritten")
    prepare.add_argument("--approve-empty-project", action="append", default=[], metavar="PROJECT_ID",
                         help="Explicitly approve clearing a project with no stored or live subitems; repeat as needed")
    apply = commands.add_parser("apply", help="Apply the reviewed run in one transaction")
    apply.add_argument("--run-dir", type=Path, required=True)
    apply.add_argument("--confirm-run-id", required=True, help="Exact run ID printed by prepare")
    apply.add_argument("--writers-paused", action="store_true", required=True,
                       help="Confirm all writers and snapshot jobs are paused and webhook events retained for replay")
    apply.add_argument("--allow-blocked", action="store_true",
                       help="Explicitly accept a PARTIAL backfill; unresolved projects and their order components stay untouched")
    verify = commands.add_parser("verify", help="Read-only comparison against the reviewed post-apply state")
    verify.add_argument("--run-dir", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = argument_parser().parse_args(argv)
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("src.database.sync_service").setLevel(logging.WARNING)
    dsn = os.getenv("SUPABASE_DB_URL")
    if not dsn:
        LOG.error("SUPABASE_DB_URL is required; configure it in the environment, not on the command line")
        return 1
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            if args.command == "prepare":
                result = prepare_run(connection, MondayClient(), args.run_dir, set(args.approve_empty_project))
            elif args.command == "apply":
                result = apply_run(connection, MondayClient(), args.run_dir,
                                   confirm_run_id=args.confirm_run_id, writers_paused=args.writers_paused,
                                   allow_blocked=args.allow_blocked)
            else:
                result = verify_run(connection, args.run_dir)
        print(json.dumps(result, indent=2))
        return 2 if result.get("matches_reviewed_result") is False else 0
    except ValueError as exc:
        LOG.error("Backfill stopped: %s", exc)
    except Exception as exc:
        LOG.error("Backfill stopped (%s). Check connectivity, permissions and run artifacts. "
                  "No successful completion was confirmed; use verify before retrying an uncertain apply.", type(exc).__name__)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())