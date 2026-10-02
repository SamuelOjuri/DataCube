"""Diagnose Monday inventory, or stage and apply an order-only backfill."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import psycopg
import requests
from dotenv import load_dotenv
from psycopg import sql
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.sync_service import DataSyncService
from src.config import HIDDEN_ITEMS_BOARD_ID, HIDDEN_ITEMS_COLUMNS, PARENT_BOARD_ID, SUBITEM_BOARD_ID, SUBITEM_COLUMNS
from src.core.monday_client import MondayClient

ORDER_FIELDS = ("cust_order_value_material", "cust_additional_charges")
SCRIPT_VERSION = 2
INVENTORY_READ_DELAYS = (5, 10, 20, 40, 60)
INVENTORY_READ_ATTEMPTS = len(INVENTORY_READ_DELAYS) + 1
TOTAL_COLUMN = "formula_mkncjq9"
REVIEWED_PARENTLESS_DUPLICATES = {
    "2121791235": {"subitem_id": "2119952710", "parent_id": "2118634736", "hidden_id": "2119952498"},
    "2121791391": {"subitem_id": "2120432332", "parent_id": "2120277774", "hidden_id": "2120432144"},
    "2121791467": {"subitem_id": "2120454800", "parent_id": "2120150600", "hidden_id": "2120454642"},
    "2121791522": {"subitem_id": "2120852454", "parent_id": "2120673411", "hidden_id": "2120852318"},
}
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


def validate_reviewed_duplicates(baseline: dict, source: dict) -> list[dict]:
    if "exclusion_evidence" not in source:
        return []
    evidence = source["exclusion_evidence"]
    if not isinstance(evidence, dict) or evidence.get("approved_mappings") != REVIEWED_PARENTLESS_DUPLICATES:
        raise ValueError("Duplicate exclusion mappings differ from the reviewed four-item contract")
    excluded = indexed(evidence["excluded_subitems"])
    if set(excluded) != set(REVIEWED_PARENTLESS_DUPLICATES):
        raise ValueError("Duplicate exclusion IDs differ from the reviewed four-item contract")
    live_children = indexed(source["subitems"])
    live_hidden = indexed(source["hidden_items"])
    stored_children = indexed(baseline["subitems"])
    stored_hidden = indexed(baseline["hidden_items"])
    projects = indexed(baseline["projects"])
    board_inventory = evidence["subitem_inventory"]
    board_ids = set(board_inventory["item_ids"])
    if (len(board_ids) != len(board_inventory["item_ids"])
            or board_ids != set(live_children) | set(excluded)
            or board_inventory["before_count"] != board_inventory["after_count"]
            or board_inventory["before_count"] != len(live_children)):
        raise ValueError("Reviewed exclusions do not exactly reconcile the subitem board inventory and counts")
    parent_inventory = evidence["parent_inventory"]
    parent_ids = set(source["project_ids"])
    if (len(parent_ids) != len(source["project_ids"])
            or parent_inventory["item_ids"] != sorted(parent_ids)
            or parent_inventory["before_count"] != parent_inventory["after_count"]
            or parent_inventory["before_count"] != len(parent_ids)
            or set(indexed(evidence["parent_details"]["items"], "id")) != parent_ids):
        raise ValueError("Parent inventory or detail coverage changed during exclusion capture")
    hidden_inventory = evidence["hidden_inventory"]
    if (hidden_inventory["item_ids"] != sorted(live_hidden)
            or hidden_inventory["before_count"] != hidden_inventory["after_count"]
            or hidden_inventory["before_count"] != len(live_hidden)):
        raise ValueError("Hidden inventory changed during exclusion capture")
    parent_check = compare_parent_inventory(set(live_children), {"counts_match": True}, evidence["parent_details"])
    if not parent_check["consistent"]:
        raise ValueError("Retained subitems do not exactly match the active parent-derived inventory")
    for child_id, child in live_children.items():
        if (parent_check["child_parent_ids"][child_id] != [child.get("parent_monday_id")]
                or child.get("board_id") != SUBITEM_BOARD_ID or child.get("state") != "active"
                or child.get("parent_board_id") != PARENT_BOARD_ID or child.get("parent_state") != "active"):
            raise ValueError("Retained subitem metadata disagrees with the parent-derived inventory")
    audit = []
    for excluded_id, expected in REVIEWED_PARENTLESS_DUPLICATES.items():
        failure = f"Reviewed duplicate {excluded_id} no longer meets exclusion conditions"
        if excluded_id in live_children or any(excluded_id in rows for rows in (stored_children, stored_hidden, projects)):
            raise ValueError(failure + ": duplicate exists in retained inventory or database")
        duplicate = excluded[excluded_id]
        child_id, parent_id, hidden_id = expected["subitem_id"], expected["parent_id"], expected["hidden_id"]
        child, hidden = live_children.get(child_id), live_hidden.get(hidden_id)
        if (child is None or hidden is None or parent_id not in projects
                or parent_id not in source["project_ids"]):
            raise ValueError(failure + ": counterpart, parent or source missing")
        for item, expected_parent in ((duplicate, ""), (child, parent_id)):
            if (item.get("board_id") != SUBITEM_BOARD_ID or item.get("state") != "active"
                    or item.get("parent_monday_id") != expected_parent or item.get("link_error")
                    or item.get("hidden_ids") != [hidden_id]):
                raise ValueError(failure + ": item state, board, parent or hidden link changed")
        if child.get("parent_board_id") != PARENT_BOARD_ID or child.get("parent_state") != "active":
            raise ValueError(failure + ": counterpart parent is not active on the expected board")
        financial_fields = (*ORDER_FIELDS, "monday_total", "amount_invoiced")
        if (hidden.get("board_id") != HIDDEN_ITEMS_BOARD_ID or hidden.get("state") != "active"
                or hidden.get("issues") or any(hidden.get(field) != "0.00" for field in financial_fields)
                or any(field not in hidden or hidden[field] is not None
                       for field in ("date_order_received", "invoice_date"))):
            raise ValueError(failure + ": source amounts, dates or metadata changed or are unknown")
        stored = stored_children.get(child_id)
        stored_source = stored_hidden.get(hidden_id)
        if (stored is None or stored_source is None or stored.get("parent_monday_id") != parent_id
                or stored.get("hidden_item_id") != hidden_id):
            raise ValueError(failure + ": database counterpart relationships changed")
        stored_links = {item_id for item_id, row in stored_children.items() if row.get("hidden_item_id") == hidden_id}
        live_links = {item_id for item_id, row in live_children.items() if hidden_id in row.get("hidden_ids", [])}
        if stored_links != {child_id} or live_links != {child_id}:
            raise ValueError(failure + ": source is not represented by exactly the reviewed counterpart")
        for row in (stored, stored_source):
            if (any(money(row.get(field)) not in (None, "0.00") for field in (*ORDER_FIELDS, "amount_invoiced"))
                    or any(row.get(field) is not None for field in ("date_order_received", "invoice_date"))):
                raise ValueError(failure + ": stored financial values or dates require reconciliation")
        audit.append({"excluded_subitem_id": excluded_id, **expected,
                      "reason": "Reviewed active parentless duplicate; source represented by active parented counterpart",
                      "source_order_total": hidden["monday_total"]})
    return audit


def build_plan(baseline: dict, source: dict, approved_empty: set[str] | None = None) -> dict:
    exclusions = validate_reviewed_duplicates(baseline, source)
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
    plan = {"projects": report, "updates": updates, "diagnostics": diagnostics}
    if exclusions:
        plan["excluded_subitems"] = exclusions
    return plan


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


def _inventory_read(stage: str, request: Callable[[], dict], *, monday=None) -> dict:
    attempt = 1
    while True:
        try:
            return request()
        except requests.exceptions.SSLError:
            raise
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt >= INVENTORY_READ_ATTEMPTS:
                raise ValueError(
                    f"Monday inventory read failed during {stage} after {attempt} attempts "
                    f"({type(exc).__name__}); check Monday availability and your network/proxy. "
                    "The read request did not complete."
                ) from None
            session = getattr(monday, "session", None)
            if session is not None:
                session.close()
            delay = INVENTORY_READ_DELAYS[attempt - 1]
            LOG.warning(
                "Inventory read %s failed (%s), attempt %d/%d; retrying the same request in %ds",
                stage, type(exc).__name__, attempt, INVENTORY_READ_ATTEMPTS, delay,
            )
            time.sleep(delay)
            attempt += 1


def scan_board_inventory(monday, board_id: str) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    before_count = _inventory_read(
        f"board {board_id} before-count", lambda: monday.get_board_info(board_id), monday=monday
    )["items_count"]
    cursor = None
    cursors = set()
    item_ids = set()
    page_count = 0
    while True:
        if cursor is None:
            query = """
                query InventoryFirstPage($board_id: ID!) {
                    boards(ids: [$board_id]) {
                        id items_page(limit: 100) { cursor items { id } }
                    }
                }
            """
            variables = {"board_id": board_id}
        else:
            query = """
                query InventoryNextPage($cursor: String!) {
                    next_items_page(cursor: $cursor, limit: 100) { cursor items { id } }
                }
            """
            variables = {"cursor": cursor}
        response = _inventory_read(
            f"board {board_id} page {page_count + 1}", lambda: monday.execute_query(query, variables), monday=monday
        )
        if response.get("errors") or not isinstance(response.get("data"), dict):
            raise ValueError(f"Invalid Monday inventory response for board {board_id}")
        data = response["data"]
        if cursor is None:
            boards = data.get("boards")
            if not isinstance(boards, list) or len(boards) != 1 or str(boards[0]["id"]) != board_id:
                raise ValueError(f"Missing or unexpected inventory board {board_id}")
            page = boards[0].get("items_page")
        else:
            page = data.get("next_items_page")
        if (not isinstance(page, dict) or not isinstance(page.get("items"), list)
                or "cursor" not in page):
            raise ValueError(f"Incomplete Monday inventory page for board {board_id}")
        page_ids = set(indexed(page["items"], "id"))
        if page_ids & item_ids:
            raise ValueError(f"Duplicate Monday inventory IDs for board {board_id}")
        item_ids.update(page_ids)
        page_count += 1
        LOG.info("Inventory board %s: %d unique IDs captured", board_id, len(item_ids))
        cursor = page["cursor"]
        if cursor is None:
            break
        if not isinstance(cursor, str) or not cursor or not page_ids or cursor in cursors:
            raise ValueError(f"Monday inventory pagination did not advance for board {board_id}")
        cursors.add(cursor)
    after_count = _inventory_read(
        f"board {board_id} after-count", lambda: monday.get_board_info(board_id), monday=monday
    )["items_count"]
    if any(type(count) is not int or count < 0 for count in (before_count, after_count)):
        raise ValueError(f"Invalid Monday inventory count for board {board_id}")
    return {"board_id": board_id, "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(), "page_count": page_count,
            "before_count": before_count, "after_count": after_count,
            "captured_count": len(item_ids), "count_delta": len(item_ids) - before_count,
            "count_stable": before_count == after_count,
            "counts_match": before_count == after_count == len(item_ids),
            "item_ids": sorted(item_ids)}


def compare_inventory_scans(first: dict, second: dict) -> dict:
    first_ids, second_ids = set(first["item_ids"]), set(second["item_ids"])
    reported_counts = [scan[key] for scan in (first, second) for key in ("before_count", "after_count")]
    if first_ids != second_ids:
        classification = "inventory_changed"
    elif len(set(reported_counts)) != 1:
        classification = "reported_count_changed"
    elif not first["counts_match"] or not second["counts_match"]:
        classification = "stable_inventory_count_mismatch"
    else:
        classification = "counts_and_ids_match"
    return {"classification": classification, "sets_match": first_ids == second_ids,
            "reported_counts": reported_counts,
            "only_first_ids": sorted(first_ids - second_ids),
            "only_second_ids": sorted(second_ids - first_ids)}


def fetch_inventory_details(monday, item_ids: list[str], *, include_subitems: bool = False,
                            column_ids: list[str] | None = None, checkpoint_dir: Path | None = None) -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    metadata = "id state board { id } parent_item { id state board { id } }"
    query = """
        query InventoryDetails($ids: [ID!]! COLUMN_VARIABLE) {
            items(ids: $ids, limit: 100, exclude_nonactive: false) {
                ITEM_FIELDS
                CHILD_FIELDS
                COLUMN_FIELDS
            }
        }
    """.replace("ITEM_FIELDS", metadata).replace(
        "CHILD_FIELDS", "subitems { " + metadata + " }" if include_subitems else ""
    )
    query = query.replace("COLUMN_VARIABLE", ", $columns: [String!]!" if column_ids is not None else "")
    query = query.replace("COLUMN_FIELDS", """
        name column_values(ids: $columns) {
            id type text value
            ... on FormulaValue { display_value }
            ... on BoardRelationValue { linked_item_ids }
        }
    """ if column_ids is not None else "")
    requested_ids = sorted(set(item_ids))
    items = {}
    for offset in range(0, len(requested_ids), 100):
        batch = requested_ids[offset:offset + 100]
        variables = {"ids": batch}
        if column_ids is not None:
            variables["columns"] = column_ids
        request = {"query": query, "variables": variables}
        checkpoint = checkpoint_dir / f"{fingerprint(request)}.json" if checkpoint_dir is not None else None
        reused = checkpoint is not None and checkpoint.exists()
        if reused:
            record = json.loads(checkpoint.read_text(encoding="utf-8"))
            if (not isinstance(record, dict) or record.get("request") != request
                    or record.get("response_sha256") != fingerprint(record.get("response"))):
                raise ValueError("Capture checkpoint integrity failed; prepare a fresh run")
            response = record["response"]
        else:
            response = _inventory_read(
                f"{'parent' if include_subitems else 'item'} metadata batch {offset // 100 + 1}",
                lambda: monday.execute_query(query, variables), monday=monday,
            )
        data = response.get("data")
        if (response.get("errors") or not isinstance(data, dict)
                or not isinstance(data.get("items"), list)):
            raise ValueError("Incomplete Monday inventory detail response")
        batch_items = indexed(data["items"], "id")
        if set(batch_items) - set(batch):
            raise ValueError("Unexpected Monday inventory detail IDs")
        for item in batch_items.values():
            if not {"state", "board", "parent_item"} <= item.keys():
                raise ValueError("Missing Monday inventory metadata fields")
            if include_subitems and not isinstance(item.get("subitems"), list):
                raise ValueError("Missing Monday parent subitem list")
        if checkpoint is not None and not reused and set(batch_items) == set(batch):
            write_json(checkpoint, {"request": request, "response": response,
                                    "response_sha256": fingerprint(response),
                                    "captured_at": datetime.now(timezone.utc).isoformat()})
        items.update(batch_items)
        LOG.info("Inventory details: %d/%d requested IDs checked%s", offset + len(batch), len(requested_ids),
                 " (saved batch reused; apply will recheck live)" if reused else "")
    return {"started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
            "items": [items[item_id] for item_id in sorted(items)],
            "not_returned_ids": sorted(set(requested_ids) - items.keys())}


def compare_parent_inventory(board_ids: set[str], parent_scan: dict, details: dict) -> dict:
    child_parents = defaultdict(set)
    issues = []
    for parent in details["items"]:
        parent_id = str(parent["id"])
        parent_issues = []
        if str((parent.get("board") or {}).get("id")) != PARENT_BOARD_ID:
            parent_issues.append("unexpected_parent_board")
        if parent.get("state") != "active":
            parent_issues.append("parent_not_active_or_state_unknown")
        if parent.get("parent_item") is not None:
            parent_issues.append("parent_is_itself_a_subitem")
        if parent_issues:
            issues.append({"item_id": parent_id, "issues": parent_issues})
        for child in parent["subitems"]:
            child_id = str(child.get("id") or "")
            if not child_id:
                raise ValueError("Missing ID in Monday parent subitem list")
            child_issues = []
            if child_id in child_parents:
                child_issues.append("subitem_listed_more_than_once")
            child_parents[child_id].add(parent_id)
            if str((child.get("board") or {}).get("id")) != SUBITEM_BOARD_ID:
                child_issues.append("unexpected_subitem_board")
            if child.get("state") != "active":
                child_issues.append("subitem_not_active_or_state_unknown")
            reported_parent = child.get("parent_item") or {}
            if str(reported_parent.get("id")) != parent_id:
                child_issues.append("parent_relationship_mismatch")
            if str((reported_parent.get("board") or {}).get("id")) != PARENT_BOARD_ID:
                child_issues.append("unexpected_reported_parent_board")
            if reported_parent.get("state") != "active":
                child_issues.append("reported_parent_not_active_or_state_unknown")
            if child_issues:
                issues.append({"item_id": child_id, "listed_parent_id": parent_id, "issues": child_issues})
    child_ids = set(child_parents)
    only_board = sorted(board_ids - child_ids)
    only_parents = sorted(child_ids - board_ids)
    return {"board_inventory_count": len(board_ids), "parent_subitem_count": len(child_ids),
            "parent_counts_match": parent_scan["counts_match"],
            "missing_parent_ids": details["not_returned_ids"],
            "only_in_board_scans": only_board, "only_under_parents": only_parents,
            "child_parent_ids": {child_id: sorted(child_parents[child_id]) for child_id in sorted(child_ids)},
            "metadata_issues": issues,
            "consistent": not (only_board or only_parents or issues or details["not_returned_ids"])
                          and parent_scan["counts_match"]}


def diagnose_inventory(monday, run_dir: Path, *, ids_only: bool = False) -> dict:
    try:
        run_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        raise ValueError("Diagnostic directory already exists; choose a new --run-dir") from None
    scans = []
    for scan_number in (1, 2):
        LOG.info("Starting fresh subitem inventory scan %d of 2", scan_number)
        scan = scan_board_inventory(monday, SUBITEM_BOARD_ID)
        write_json(run_dir / f"inventory-{scan_number}.json", scan)
        scans.append(scan)
    comparison = compare_inventory_scans(*scans)
    write_json(run_dir / "comparison.json", comparison)
    LOG.info("Inventory comparison: %s", comparison["classification"])
    suspect_ids = set(comparison["only_first_ids"]) | set(comparison["only_second_ids"])
    summary = {"diagnostic_only": True, "board_id": SUBITEM_BOARD_ID,
               "classification": comparison["classification"], "sets_match": comparison["sets_match"],
               "scans": [{key: value for key, value in scan.items() if key != "item_ids"} for scan in scans],
               "parent_check_performed": not ids_only,
               "needs_investigation": comparison["classification"] != "counts_and_ids_match"}
    if not ids_only:
        parent_scan = scan_board_inventory(monday, PARENT_BOARD_ID)
        write_json(run_dir / "parent-inventory.json", parent_scan)
        parent_details = fetch_inventory_details(monday, parent_scan["item_ids"], include_subitems=True)
        write_json(run_dir / "parent-details.json", parent_details)
        board_ids = set(scans[0]["item_ids"]) | set(scans[1]["item_ids"])
        parent_check = compare_parent_inventory(board_ids, parent_scan, parent_details)
        write_json(run_dir / "parent-comparison.json", parent_check)
        suspect_ids.update(parent_check["only_in_board_scans"])
        suspect_ids.update(parent_check["only_under_parents"])
        suspect_ids.update(parent_check["missing_parent_ids"])
        suspect_ids.update(issue["item_id"] for issue in parent_check["metadata_issues"])
        inspection = fetch_inventory_details(monday, sorted(suspect_ids))
        write_json(run_dir / "discrepancy-details.json", inspection)
        summary["parent_check"] = {key: value for key, value in parent_check.items()
                                   if key not in {"child_parent_ids", "metadata_issues"}}
        summary["parent_check"]["metadata_issue_count"] = len(parent_check["metadata_issues"])
        summary["unavailable_discrepancy_ids"] = inspection["not_returned_ids"]
        summary["needs_investigation"] |= not parent_check["consistent"] or bool(inspection["not_returned_ids"])
    summary["completed_at"] = datetime.now(timezone.utc).isoformat()
    write_json(run_dir / "summary.json", summary)
    return summary


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


def capture_source_with_reviewed_duplicates(monday, *, checkpoint_dir: Path | None = None) -> dict:
    parent_scan = scan_board_inventory(monday, PARENT_BOARD_ID)
    if not parent_scan["counts_match"]:
        raise ValueError("Parent board inventory count mismatch; reviewed exclusions cannot bypass it")
    parent_details = fetch_inventory_details(monday, parent_scan["item_ids"], include_subitems=True,
                                              checkpoint_dir=checkpoint_dir)
    if parent_details["not_returned_ids"]:
        raise ValueError("Missing parent details during reviewed exclusion capture")
    for parent in parent_details["items"]:
        parent["subitems"] = sorted(parent["subitems"], key=lambda child: str(child["id"]))
    subitem_scan = scan_board_inventory(monday, SUBITEM_BOARD_ID)
    excluded_ids = set(REVIEWED_PARENTLESS_DUPLICATES)
    if (not subitem_scan["count_stable"] or not excluded_ids <= set(subitem_scan["item_ids"])
            or subitem_scan["captured_count"] - len(excluded_ids) != subitem_scan["before_count"]):
        raise ValueError("Subitem count difference is not exactly the four reviewed duplicates")
    subitem_details = fetch_inventory_details(monday, subitem_scan["item_ids"],
                                               column_ids=[SUBITEM_COLUMNS["hidden_item_id"]], checkpoint_dir=checkpoint_dir)
    if subitem_details["not_returned_ids"]:
        raise ValueError("Missing subitem details during reviewed exclusion capture")
    retained, excluded = [], []
    for item in subitem_details["items"]:
        parent = item["parent_item"] or {}
        normalized = {**normalize_subitem(item), "board_id": (item["board"] or {}).get("id"),
                      "state": item["state"], "parent_board_id": (parent.get("board") or {}).get("id"),
                      "parent_state": parent.get("state")}
        (excluded if str(item["id"]) in excluded_ids else retained).append(normalized)
    parent_check = compare_parent_inventory({row["monday_id"] for row in retained}, parent_scan, parent_details)
    if not parent_check["consistent"]:
        raise ValueError("Retained subitems do not exactly match the active parent-derived inventory")
    hidden_scan = scan_board_inventory(monday, HIDDEN_ITEMS_BOARD_ID)
    if not hidden_scan["counts_match"]:
        raise ValueError("Hidden board inventory count mismatch; reviewed exclusions cannot bypass it")
    financial_columns = [HIDDEN_ITEMS_COLUMNS[field] for field in (*ORDER_FIELDS, "amount_invoiced",
                                                                 "date_order_received", "invoice_date")]
    hidden_details = fetch_inventory_details(monday, hidden_scan["item_ids"],
                                              column_ids=financial_columns + [TOTAL_COLUMN], checkpoint_dir=checkpoint_dir)
    if hidden_details["not_returned_ids"]:
        raise ValueError("Missing hidden-source details during reviewed exclusion capture")
    reviewed_sources = {row["hidden_id"] for row in REVIEWED_PARENTLESS_DUPLICATES.values()}
    hidden = []
    for item in hidden_details["items"]:
        normalized = normalize_hidden(item)
        if str(item["id"]) in reviewed_sources:
            normalized.update({"board_id": (item["board"] or {}).get("id"), "state": item["state"]})
            columns = indexed(item["column_values"], "id")
            for field in ("amount_invoiced", "date_order_received", "invoice_date"):
                column = columns.get(HIDDEN_ITEMS_COLUMNS[field])
                if column is None or "value" not in column:
                    raise ValueError("Missing reviewed-source financial input")
                raw_value = column["value"]
                value = json.loads(raw_value) if isinstance(raw_value, str) and raw_value else raw_value
                if field == "amount_invoiced":
                    normalized[field] = money("0" if value is None or value == "" else value)
                elif value is None or value == "":
                    normalized[field] = None
                else:
                    raise ValueError("Reviewed duplicate source now has an order or invoice date")
        hidden.append(normalized)
    inventory_keys = ("before_count", "after_count", "item_ids")
    return {"project_ids": parent_scan["item_ids"], "subitems": retained, "hidden_items": hidden,
            "exclusion_evidence": {
                "approved_mappings": json.loads(json.dumps(REVIEWED_PARENTLESS_DUPLICATES)),
                "excluded_subitems": excluded,
                "subitem_inventory": {key: subitem_scan[key] for key in inventory_keys},
                "parent_inventory": {key: parent_scan[key] for key in inventory_keys},
                "parent_details": {"items": parent_details["items"], "not_returned_ids": []},
                "hidden_inventory": {key: hidden_scan[key] for key in inventory_keys},
            }}


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


def prepare_run(connection, monday, run_dir: Path, approved_empty: set[str], *,
                approve_reviewed_parentless_duplicates: bool = False, resume: bool = False) -> dict:
    if resume:
        if not approve_reviewed_parentless_duplicates:
            raise ValueError("Resume requires --approve-reviewed-parentless-duplicates and its original approvals")
        if not (run_dir / "capture-context.json").is_file():
            raise ValueError("No resumable capture context exists; prepare a fresh run in a new directory")
        if any((run_dir / name).exists() for name in ("source.json", "plan.json", "review.csv", "manifest.json")):
            raise ValueError("Source capture already finished; do not resume or overwrite its review artifacts")
    else:
        run_dir.mkdir(parents=True, exist_ok=False)
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        baseline = read_baseline(connection)
    if resume:
        saved_baseline = json.loads((run_dir / "baseline.json").read_text(encoding="utf-8"))
        if saved_baseline != baseline:
            raise ValueError("Database baseline changed; prepare a fresh run instead of resuming")
    else:
        write_json(run_dir / "baseline.json", baseline)
    if approve_reviewed_parentless_duplicates:
        context = {"version": SCRIPT_VERSION, "code": code_fingerprint(), "target": target_fingerprint(connection),
                   "source_contract": source_contract(), "baseline_sha256": fingerprint(baseline),
                   "approved_empty": sorted(approved_empty), "approved_mappings": REVIEWED_PARENTLESS_DUPLICATES}
        checkpoint_dir = run_dir / "capture-batches"
        if resume:
            saved_context = json.loads((run_dir / "capture-context.json").read_text(encoding="utf-8"))
            if saved_context != context or not checkpoint_dir.is_dir():
                raise ValueError("Capture context changed or is incomplete; prepare a fresh run")
        else:
            checkpoint_dir.mkdir()
            write_json(run_dir / "capture-context.json", context)
        LOG.info("Preparation checkpoints enabled; ID scans are fresh and apply never reuses saved details")
        try:
            source = capture_source_with_reviewed_duplicates(monday, checkpoint_dir=checkpoint_dir)
        except Exception:
            LOG.error("Preparation incomplete; completed detail batches remain. After a transport interruption, "
                      "use prepare --resume with this run directory and the same approvals. "
                      "Validation failures require investigation, not a bypass. No backfill updates were written.")
            raise
    else:
        source = capture_source(monday)
    if approve_reviewed_parentless_duplicates != ("exclusion_evidence" in source):
        raise ValueError("Source exclusion evidence does not match explicit preparation approval")
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
        "preparation_resumed": resume,
        "approve_reviewed_parentless_duplicates": approve_reviewed_parentless_duplicates,
        "excluded_subitems": plan.get("excluded_subitems", []),
        "review_sha256": hashlib.sha256((run_dir / "review.csv").read_bytes()).hexdigest(),
        "hashes": {name: fingerprint(value) for name, value in
                   (("baseline", baseline), ("source", source), ("plan", plan))},
        "summary": {"verified_projects": sum(row["status"] == "verified" for row in plan["projects"]),
                    "blocked_projects": sum(row["status"] == "blocked" for row in plan["projects"]),
                    "excluded_subitems": len(plan.get("excluded_subitems", [])),
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
    approved = manifest.get("approve_reviewed_parentless_duplicates")
    if type(approved) is not bool or approved != ("exclusion_evidence" in artifacts["source"]):
        raise ValueError("Manifest approval does not match the stored exclusion evidence")
    rebuilt = build_plan(artifacts["baseline"], artifacts["source"], set(manifest["approved_empty"]))
    if rebuilt != artifacts["plan"]:
        raise ValueError("Stored plan does not match its source evidence")
    if manifest.get("excluded_subitems") != rebuilt.get("excluded_subitems", []):
        raise ValueError("Manifest exclusions do not match the validated audit plan")
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
    capture = capture_source_with_reviewed_duplicates if manifest["approve_reviewed_parentless_duplicates"] else capture_source
    fresh_source = capture(monday)
    validate_reviewed_duplicates(baseline, fresh_source)
    if fresh_source != source:
        raise ValueError("Monday sources changed since preparation; prepare a fresh run")
    expected = expected_baseline(baseline, plan)
    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout = '5s'")
        connection.execute("SET LOCAL statement_timeout = '60s'")
        connection.execute("LOCK TABLE public.hidden_items, public.subitems, public.projects IN SHARE ROW EXCLUSIVE MODE")
        current = read_baseline(connection)
        validate_reviewed_duplicates(current, fresh_source)
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
               "excluded_subitems": plan.get("excluded_subitems", []),
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
              "excluded_subitems": plan.get("excluded_subitems", []),
              "source_diagnostics": len(plan["diagnostics"])}
    write_json(run_dir / f"verify-{uuid4()}.json", result)
    return result


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check-monday", help="Read one subitem-board count to check Monday connectivity; no database access")
    diagnose = commands.add_parser("diagnose-inventory", help="Read-only Monday inventory diagnostic; no database access")
    diagnose.add_argument("--run-dir", type=Path, required=True, help="New diagnostic directory; never overwritten")
    diagnose.add_argument("--ids-only", action="store_true", help="Only compare two ID scans; skip parent and metadata checks")
    prepare = commands.add_parser("prepare", help="Read-only dry run: capture sources and write an audit plan")
    prepare.add_argument("--run-dir", type=Path, required=True, help="New directory, or interrupted checkpointed run with --resume")
    prepare.add_argument("--resume", action="store_true",
                         help="Resume an interrupted approved-duplicate preparation after rechecking its baseline and context")
    prepare.add_argument("--approve-empty-project", action="append", default=[], metavar="PROJECT_ID",
                         help="Explicitly approve clearing a project with no stored or live subitems; repeat as needed")
    prepare.add_argument("--approve-reviewed-parentless-duplicates", action="store_true",
                         help="Approve only the four reviewed duplicate IDs, subject to live relationship and zero-value checks")
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
    label = {"check-monday": "Monday connectivity check", "diagnose-inventory": "Inventory diagnostic"}.get(args.command, "Backfill")
    try:
        if args.command == "check-monday":
            monday = MondayClient()
            info = _inventory_read(f"connectivity board {SUBITEM_BOARD_ID}",
                                   lambda: monday.get_board_info(SUBITEM_BOARD_ID), monday=monday)
            count = info.get("items_count")
            if type(count) is not int or count < 0:
                raise ValueError("Monday returned an invalid board count")
            print(json.dumps({"board_id": SUBITEM_BOARD_ID, "items_count": count,
                              "checked_at": datetime.now(timezone.utc).isoformat()}, indent=2))
            return 0
        if args.command == "diagnose-inventory":
            result = diagnose_inventory(MondayClient(), args.run_dir, ids_only=args.ids_only)
            print(json.dumps(result, indent=2))
            return 2 if result["needs_investigation"] else 0
        dsn = os.getenv("SUPABASE_DB_URL")
        if not dsn:
            LOG.error("SUPABASE_DB_URL is required; configure it in the environment, not on the command line")
            return 1
        with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
            if args.command == "prepare":
                result = prepare_run(connection, MondayClient(), args.run_dir, set(args.approve_empty_project),
                                     approve_reviewed_parentless_duplicates=args.approve_reviewed_parentless_duplicates,
                                     resume=args.resume)
            elif args.command == "apply":
                result = apply_run(connection, MondayClient(), args.run_dir,
                                   confirm_run_id=args.confirm_run_id, writers_paused=args.writers_paused,
                                   allow_blocked=args.allow_blocked)
            else:
                result = verify_run(connection, args.run_dir)
        print(json.dumps(result, indent=2))
        return 2 if result.get("matches_reviewed_result") is False else 0
    except ValueError as exc:
        LOG.error("%s stopped: %s", label, exc)
    except Exception as exc:
        if args.command in ("diagnose-inventory", "check-monday"):
            LOG.error("%s stopped (%s). No database connection or writes were attempted. "
                      "Diagnostic artifacts, if any, remain; a diagnostic retry needs a new directory.", label, type(exc).__name__)
        else:
            LOG.error("Backfill stopped (%s). Check connectivity, permissions and run artifacts. "
                      "No successful completion was confirmed; use verify before retrying an uncertain apply.", type(exc).__name__)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())