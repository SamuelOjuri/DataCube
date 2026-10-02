"""Review order-backfill discrepancies and target authoritative parent rehydration."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts import backfill_order_values as backfill
from src.config import get_hidden_items_extraction_columns, get_subitems_extraction_columns
from src.core.data_processor import EnhancedMirrorResolver, LabelNormalizer

LOG = logging.getLogger(__name__)

RESOLUTION_STEPS = {
    "parent_missing": "Confirm the parent's lifecycle state; retain historical data pending an explicit removal decision.",
    "empty_project_requires_review": "Confirm the empty live inventory and approve a totals policy; do not infer zero.",
    "stored_children_not_in_live_parent": "Review missing or moved child IDs before removing, moving or excluding stored rows.",
    "inactive_or_unknown_metadata": "Read exact IDs again and verify item, parent and board metadata.",
    "parent_mismatch": "Confirm the correct parent and review both parents' rollups before approving a move.",
    "invalid_live_hidden_link": "Review source IDs using corroborating fields; duplicate names alone cannot establish a link.",
    "shared_live_hidden_source": "Agree ownership or allocation of the shared source before counting its order value.",
    "missing_or_invalid_hidden_source": "Verify the hidden source, both order inputs and formula; missing values are not certified zeros.",
    "stored_owner_requires_review": "Review the blocking stored owners with this group; they cannot be rehydrated automatically.",
    "dependency_group_exceeds_limit": "Review the dependency group as a whole; it exceeds the 25-project transaction limit.",
}


def repair_code() -> str:
    return hashlib.sha256(Path(__file__).read_bytes() + backfill.code_fingerprint().encode()).hexdigest()


def json_value(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_value(item) for item in value]
    return value


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            output = {}
            for field in fields:
                value = row.get(field)
                text = json.dumps(value) if isinstance(value, (list, dict)) else str(value) if value is not None else ""
                output[field] = "'" + text if text.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")) else text
            writer.writerow(output)


def build_report(baseline: dict, source: dict, plan: dict) -> dict:
    stored = backfill.indexed(baseline["subitems"])
    live = backfill.indexed(source["subitems"])
    hidden = backfill.indexed(source["hidden_items"])
    projects = backfill.indexed(baseline["projects"])
    source_projects = set(source["project_ids"])
    live_children = defaultdict(set)
    stored_children = defaultdict(set)
    live_users = defaultdict(set)
    stored_users = defaultdict(set)
    for child_id, child in live.items():
        live_children[child.get("parent_monday_id")].add(child_id)
        for hidden_id in child.get("hidden_ids", []):
            live_users[hidden_id].add(child_id)
    for child_id, child in stored.items():
        stored_children[child.get("parent_monday_id")].add(child_id)
        if child.get("hidden_item_id"):
            stored_users[child["hidden_item_id"]].add(child_id)
    discrepancies = []
    for child_id in sorted(set(stored) | set(live)):
        before, current = stored.get(child_id), live.get(child_id)
        reasons = []
        if before is None:
            reasons.append("missing_in_database")
        elif current is None:
            reasons.append("missing_in_monday")
        elif before.get("parent_monday_id") != current.get("parent_monday_id"):
            reasons.append("parent_mismatch")
        if current is not None:
            linked = current.get("hidden_ids", [])
            if current.get("link_error") or len(linked) != 1:
                reasons.append("invalid_or_multiple_live_hidden_links")
            elif before is not None and before.get("hidden_item_id") != linked[0]:
                reasons.append("stored_hidden_link_mismatch")
            if any(len(live_users[hidden_id]) > 1 for hidden_id in linked):
                reasons.append("shared_live_hidden_source")
        if reasons:
            discrepancies.append({"subitem_id": child_id, "issues": reasons,
                                  "stored_parent_id": (before or {}).get("parent_monday_id"),
                                  "live_parent_id": (current or {}).get("parent_monday_id"),
                                  "stored_hidden_id": (before or {}).get("hidden_item_id"),
                                  "live_hidden_ids": (current or {}).get("hidden_ids", [])})
    project_reviews = []
    for project in plan["projects"]:
        if project["status"] != "blocked":
            continue
        parent_id = project["project_id"]
        blockers = set()
        children = live_children[parent_id]
        if parent_id not in projects or parent_id not in source_projects:
            blockers.add("parent_missing")
        if not children:
            blockers.add("empty_project_requires_review")
        if stored_children[parent_id] - children:
            blockers.add("stored_children_not_in_live_parent")
        for child_id in children:
            child = live[child_id]
            linked = child.get("hidden_ids", [])
            if (child.get("state") != "active" or child.get("board_id") != backfill.SUBITEM_BOARD_ID
                    or child.get("parent_state") != "active" or child.get("parent_board_id") != backfill.PARENT_BOARD_ID):
                blockers.add("inactive_or_unknown_metadata")
            if child_id in stored and stored[child_id].get("parent_monday_id") != parent_id:
                blockers.add("parent_mismatch")
            if child.get("link_error") or len(linked) != 1:
                blockers.add("invalid_live_hidden_link")
                continue
            hidden_id = linked[0]
            if len(live_users[hidden_id]) != 1:
                blockers.add("shared_live_hidden_source")
            item = hidden.get(hidden_id)
            if item is None or item.get("issues") or any(item.get(field) is None for field in backfill.ORDER_FIELDS):
                blockers.add("missing_or_invalid_hidden_source")
        project_reviews.append({"project_id": parent_id, "item_name": project.get("item_name", ""),
                                "old_total": project.get("old_total"), "issues": project["issues"],
                                "action": "manual_review" if blockers else "rehydrate_candidate",
                                "blockers": sorted(blockers), "live_subitem_ids": sorted(children)})
    duplicate_sources = [{"hidden_id": hidden_id, "live_subitem_ids": sorted(live_users[hidden_id]),
                          "stored_subitem_ids": sorted(stored_users[hidden_id]),
                          "classification": "shared_live_source" if len(live_users[hidden_id]) > 1 else "stored_links_only"}
                         for hidden_id in sorted(set(live_users) | set(stored_users))
                         if len(live_users[hidden_id]) > 1 or len(stored_users[hidden_id]) > 1]
    groups = build_repair_groups(baseline, source, project_reviews)
    membership = {parent_id: group for group in groups for parent_id in group["project_ids"]}
    for project in project_reviews:
        group = membership.get(project["project_id"], {})
        project["repair_group"] = group.get("group_id")
        project["repair_readiness"] = group.get("action", "manual_review")
        blockers = sorted(set(project["blockers"]) | set(group.get("blockers", [])))
        project["resolution_steps"] = [RESOLUTION_STEPS[reason] for reason in blockers] or [
            "Prepare the complete exact-ID repair group; review staged values and pause writers before applying."]
    return {"projects": project_reviews, "subitems": discrepancies, "duplicate_sources": duplicate_sources,
            "unlinked_hidden_orders": [row for row in plan["diagnostics"] if row["issue"] == "unlinked_hidden_order"],
            "diagnostics": plan["diagnostics"], "excluded_subitems": plan.get("excluded_subitems", []),
            "repair_groups": groups}


def build_repair_groups(baseline: dict, source: dict, project_reviews: list[dict]) -> list[dict]:
    candidates = {row["project_id"] for row in project_reviews if row["action"] == "rehydrate_candidate"}
    neighbors = {parent_id: set() for parent_id in candidates}
    blocked_owners = defaultdict(set)
    stored_users = defaultdict(list)
    for child in baseline["subitems"]:
        if child.get("hidden_item_id"):
            stored_users[child["hidden_item_id"]].append(child)
    for child in source["subitems"]:
        parent_id = child.get("parent_monday_id")
        if parent_id not in candidates:
            continue
        for owner in stored_users[child["hidden_ids"][0]]:
            if owner["monday_id"] == child["monday_id"]:
                continue
            owner_parent = str(owner.get("parent_monday_id") or "")
            if owner_parent in candidates:
                neighbors[parent_id].add(owner_parent)
                neighbors[owner_parent].add(parent_id)
            else:
                blocked_owners[parent_id].add((owner["monday_id"], owner_parent))
    remaining = set(candidates)
    groups = []
    while remaining:
        pending = [min(remaining)]
        members = set()
        while pending:
            parent_id = pending.pop()
            if parent_id in members:
                continue
            members.add(parent_id)
            pending.extend(neighbors[parent_id] - members)
        remaining.difference_update(members)
        owners = set().union(*(blocked_owners[parent_id] for parent_id in members))
        blockers = []
        if owners:
            blockers.append("stored_owner_requires_review")
        if len(members) > 25:
            blockers.append("dependency_group_exceeds_limit")
        groups.append({"group_id": f"repair-{len(groups) + 1:03d}", "project_ids": sorted(members),
                       "action": "manual_review" if blockers else "prepare_candidate", "blockers": blockers,
                       "blocking_subitem_ids": sorted(child_id for child_id, _ in owners),
                       "blocking_project_ids": sorted({parent_id for _, parent_id in owners}),
                       "resolution_steps": [RESOLUTION_STEPS[reason] for reason in blockers]})
    return groups


def report_run(run_dir: Path, output_dir: Path) -> dict:
    manifest, baseline, source, plan = backfill.load_run(run_dir)
    report = build_report(baseline, source, plan)
    output_dir.mkdir(parents=True, exist_ok=False)
    backfill.write_json(output_dir / "report.json", report)
    write_csv(output_dir / "projects.csv", report["projects"],
              ["project_id", "item_name", "old_total", "action", "issues", "blockers", "live_subitem_ids",
               "repair_group", "repair_readiness", "resolution_steps"])
    backfill.write_json(output_dir / "repair-groups.json", report["repair_groups"])
    write_csv(output_dir / "repair-groups.csv", report["repair_groups"],
              ["group_id", "project_ids", "action", "blockers", "blocking_subitem_ids", "blocking_project_ids",
               "resolution_steps"])
    write_csv(output_dir / "subitem-links.csv", report["subitems"],
              ["subitem_id", "issues", "stored_parent_id", "live_parent_id", "stored_hidden_id", "live_hidden_ids"])
    write_csv(output_dir / "duplicate-sources.csv", report["duplicate_sources"],
              ["hidden_id", "classification", "live_subitem_ids", "stored_subitem_ids"])
    hidden = backfill.indexed(source["hidden_items"])
    unlinked = [{**row, **hidden[row["hidden_item_id"]]} for row in report["unlinked_hidden_orders"]]
    write_csv(output_dir / "unlinked-orders.csv", unlinked,
              ["hidden_item_id", "item_name", *backfill.ORDER_FIELDS, "monday_total", "issues"])
    candidates = [row["project_id"] for row in report["projects"] if row["action"] == "rehydrate_candidate"]
    backfill.write_json(output_dir / "candidate-project-ids.json", candidates)
    ready_groups = [group for group in report["repair_groups"] if group["action"] == "prepare_candidate"]
    ready_projects = sum(len(group["project_ids"]) for group in ready_groups)
    summary = {"backfill_run_id": manifest["run_id"], "candidate_projects": len(candidates),
               "manual_projects": len(report["projects"]) - len(candidates),
               "subitem_discrepancies": len(report["subitems"]), "duplicate_sources": len(report["duplicate_sources"]),
               "unlinked_orders": len(unlinked), "report_sha256": backfill.fingerprint(report),
               "ready_repair_groups": len(ready_groups), "ready_candidate_projects": ready_projects,
               "dependency_blocked_candidate_projects": len(candidates) - ready_projects,
               "manual_blocker_counts": dict(sorted(Counter(reason for row in report["projects"]
                                                            for reason in row["blockers"]).items()))}
    backfill.write_json(output_dir / "summary.json", summary)
    return summary


def transform_exact_rows(raw_hidden: list[dict], raw_children: list[dict], parent_ids: set[str]) -> dict:
    service = backfill.DataSyncService.__new__(backfill.DataSyncService)
    service.label_normalizer = LabelNormalizer()
    service.mirror_resolver = EnhancedMirrorResolver()
    service._hidden_lookup_by_id = {}
    service._hidden_lookup_by_name = {}
    service._hidden_lookup_by_normalized_name = {}
    service._hidden_lookup_by_prefix = {}
    hidden_ids = set(backfill.indexed(raw_hidden, "id"))
    for child in raw_children:
        normalized = backfill.normalize_subitem(child)
        if (normalized["link_error"] or len(normalized["hidden_ids"]) != 1
                or normalized["hidden_ids"][0] not in hidden_ids
                or normalized["parent_monday_id"] not in parent_ids):
            raise ValueError("Exact parent and hidden links are required before transformation")
    hidden_rows = service._transform_for_hidden_table(raw_hidden)
    if set(backfill.indexed(hidden_rows)) != hidden_ids:
        raise ValueError("Hidden transformation skipped or duplicated a requested source")
    for row in hidden_rows:
        cached = service._hidden_lookup_by_id[row["monday_id"]]
        for field in ("amount_invoiced", "invoice_date", "date_order_received", "date_design_completed", "quote_amount"):
            row[field] = cached.get(field)
    child_rows = service._transform_for_subitems_table(raw_children)
    if set(backfill.indexed(child_rows)) != set(backfill.indexed(raw_children, "id")):
        raise ValueError("Subitem transformation skipped or duplicated a requested child")
    orders, order_dates = service._rollup_order_values_from_subitems(child_rows)
    invoices = service._rollup_invoice_totals_from_subitems(child_rows)
    enquiries = service._rollup_new_enquiry_from_subitems(child_rows)
    invoice_dates = service._rollup_invoice_date_ranges_from_subitems(child_rows)
    if set(orders) != parent_ids:
        raise ValueError("Order rollup withheld; no repair can be staged")
    projects = [{"monday_id": parent_id, "total_order_value": backfill.money(orders[parent_id]),
                 "date_order_received": order_dates.get(parent_id),
                 "total_amount_invoiced": backfill.money(invoices.get(parent_id, 0)),
                 "new_enquiry_value": backfill.money(enquiries.get(parent_id, 0)),
                 "first_date_invoiced": invoice_dates.get(parent_id, {}).get("first_date_invoiced"),
                 "last_date_invoiced": invoice_dates.get(parent_id, {}).get("last_date_invoiced")}
                for parent_id in sorted(parent_ids)]
    result = {"hidden_items": hidden_rows, "subitems": child_rows, "projects": projects}
    for rows in result.values():
        for row in rows:
            row.pop("last_synced_at", None)
    return json_value(result)


def select_scope(baseline: dict, source: dict, plan: dict, project_ids: set[str]) -> dict:
    if not project_ids or len(project_ids) > 25:
        raise ValueError("Select between 1 and 25 reviewed project IDs per repair")
    report = build_report(baseline, source, plan)
    candidates = {row["project_id"] for row in report["projects"] if row["action"] == "rehydrate_candidate"}
    if project_ids - candidates:
        raise ValueError("Selected projects include non-candidates; inspect the manual-review report")
    children = [row for row in source["subitems"] if row["parent_monday_id"] in project_ids]
    child_ids = {row["monday_id"] for row in children}
    hidden_ids = {row["hidden_ids"][0] for row in children}
    resulting_links = {row["monday_id"]: row.get("hidden_item_id") for row in baseline["subitems"]}
    resulting_links.update({row["monday_id"]: row["hidden_ids"][0] for row in children})
    users = defaultdict(set)
    for child_id, hidden_id in resulting_links.items():
        users[hidden_id].add(child_id)
    if any(len(users[hidden_id]) != 1 for hidden_id in hidden_ids):
        raise ValueError("Repair would leave a shared stored source; include its other eligible owner or reconcile manually")
    if child_ids & set(backfill.REVIEWED_PARENTLESS_DUPLICATES):
        raise ValueError("Reviewed excluded IDs cannot be repaired or inserted")
    return {"projects": sorted(project_ids), "subitems": sorted(child_ids), "hidden_items": sorted(hidden_ids)}


def fetch_columns(monday, item_ids: list[str], column_ids: list[str], board_id: str) -> list[dict]:
    query = """
        query ReconciliationItems($ids: [ID!]!, $columns: [String!]!) {
            items(ids: $ids, limit: 100, exclude_nonactive: false) {
                id name state board { id } parent_item { id state board { id } }
                column_values(ids: $columns) {
                    id type text value
                    ... on FormulaValue { display_value }
                    ... on MirrorValue { display_value }
                    ... on BoardRelationValue { linked_item_ids }
                }
            }
        }
    """
    items = []
    columns = sorted(set(column_ids) - {"name"})
    for offset in range(0, len(item_ids), 100):
        batch = item_ids[offset:offset + 100]
        response = backfill._inventory_read("reconciliation exact-ID details",
                                             lambda: monday.execute_query(query, {"ids": batch, "columns": columns}),
                                             monday=monday)
        if response.get("errors") or not isinstance(response.get("data", {}).get("items"), list):
            raise ValueError("Invalid targeted detail response")
        found = backfill.indexed(response["data"]["items"], "id")
        if set(found) != set(batch):
            raise ValueError("Incomplete targeted detail response")
        for item in found.values():
            if item.get("state") != "active" or (item.get("board") or {}).get("id") != board_id:
                raise ValueError("Targeted item is inactive or on an unexpected board")
            if not isinstance(item.get("name"), str):
                raise ValueError("Missing targeted item name")
            values = backfill.indexed(item.get("column_values", []), "id")
            if set(values) != set(columns) or any("value" not in value for value in values.values()):
                raise ValueError("Missing targeted columns; do not overwrite fields from incomplete data")
            item["column_values"] = [values[column_id] for column_id in sorted(values)]
        items.extend(found.values())
    return sorted(items, key=lambda row: str(row["id"]))


def capture_targeted(monday, source: dict, scope: dict) -> dict:
    parents = backfill.fetch_inventory_details(monday, scope["projects"], include_subitems=True)
    if parents["not_returned_ids"]:
        raise ValueError("Missing selected parent details")
    check = backfill.compare_parent_inventory(set(scope["subitems"]), {"counts_match": True}, parents)
    if not check["consistent"]:
        raise ValueError("Selected parent child inventory changed")
    children = fetch_columns(monday, scope["subitems"], get_subitems_extraction_columns(), backfill.SUBITEM_BOARD_ID)
    hidden = fetch_columns(monday, scope["hidden_items"],
                           [*get_hidden_items_extraction_columns(), backfill.TOTAL_COLUMN], backfill.HIDDEN_ITEMS_BOARD_ID)
    expected_children = backfill.indexed(source["subitems"])
    expected_hidden = backfill.indexed(source["hidden_items"])
    for child in children:
        normalized = backfill.normalize_subitem(child)
        expected = expected_children[str(child["id"])]
        if (normalized["parent_monday_id"] != expected["parent_monday_id"]
                or normalized["hidden_ids"] != expected["hidden_ids"] or normalized["link_error"]
                or (child.get("parent_item") or {}).get("state") != "active"
                or ((child.get("parent_item") or {}).get("board") or {}).get("id") != backfill.PARENT_BOARD_ID):
            raise ValueError("Selected child relationship changed")
    for item in hidden:
        normalized = backfill.normalize_hidden(item)
        expected = expected_hidden[str(item["id"])]
        if normalized["issues"] or any(normalized[field] != expected[field] for field in backfill.ORDER_FIELDS):
            raise ValueError("Selected source amounts changed or cannot be reconciled")
    return {"hidden_items": hidden, "subitems": children}


def read_scope(connection, scope: dict) -> dict:
    result = {}
    with connection.cursor(row_factory=dict_row) as cursor:
        for table, item_ids in scope.items():
            cursor.execute(sql.SQL("SELECT * FROM public.{} WHERE monday_id = ANY(%s) ORDER BY monday_id").format(
                sql.Identifier(table)), (item_ids,))
            result[table] = json_value(cursor.fetchall())
    return result


def read_contract(connection) -> dict:
    result = {table: {} for table in backfill.BASELINE_COLUMNS}
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SELECT table_name, column_name, data_type, numeric_scale, is_generated FROM information_schema.columns "
                       "WHERE table_schema = 'public' AND table_name = ANY(%s)", (list(result),))
        for row in cursor.fetchall():
            result[row["table_name"]][row["column_name"]] = {
                "type": row["data_type"], "scale": row["numeric_scale"], "generated": row["is_generated"]}
    if any(not columns for columns in result.values()):
        raise ValueError("Missing table schema for reconciliation")
    return result


def normalize_updates(updates: dict, contract: dict) -> dict:
    result = json_value(updates)
    for table, rows in result.items():
        for row in rows:
            if set(row) - set(contract[table]):
                raise ValueError(f"Transform fields do not match the {table} database schema")
            for field, value in row.items():
                column = contract[table][field]
                if column.get("generated", "NEVER") != "NEVER":
                    raise ValueError(f"Cannot stage generated database column {table}.{field}")
                if value is not None and column["type"] == "numeric":
                    amount = Decimal(str(value))
                    if not amount.is_finite():
                        raise ValueError("Non-finite transformed amount")
                    row[field] = format(amount.quantize(Decimal(1).scaleb(-column["scale"]), rounding=ROUND_HALF_UP), "f")
                elif value is not None and column["type"] == "date":
                    row[field] = date.fromisoformat(str(value)).isoformat()
    return result


def prepare_repair(connection, monday, run_dir: Path, repair_dir: Path, project_ids: set[str]) -> dict:
    manifest, baseline, source, plan = backfill.load_run(run_dir)
    if manifest["target"] != backfill.target_fingerprint(connection):
        raise ValueError("Database target differs from the backfill run")
    scope = select_scope(baseline, source, plan, project_ids)
    if not manifest["approve_reviewed_parentless_duplicates"]:
        raise ValueError("This reconciliation requires the metadata-complete approved inventory capture")
    repair_dir.mkdir(parents=True, exist_ok=False)
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        if backfill.read_baseline(connection) != baseline:
            raise ValueError("Database changed; prepare a fresh backfill before reconciling")
        before = read_scope(connection, scope)
        contract = read_contract(connection)
    if backfill.capture_source_with_reviewed_duplicates(monday) != source:
        raise ValueError("Monday changed; prepare a fresh backfill before reconciling")
    raw = capture_targeted(monday, source, scope)
    updates = normalize_updates(transform_exact_rows(raw["hidden_items"], raw["subitems"], project_ids), contract)
    for table in scope:
        if set(backfill.indexed(updates[table])) != set(scope[table]):
            raise ValueError("Transformed rows exceed or omit the approved scope")
    repair = {"backfill_run_dir": str(run_dir.resolve()), "backfill_run_id": manifest["run_id"],
              "scope": scope, "before": before, "contract": contract, "raw": raw, "updates": updates}
    backfill.write_json(repair_dir / "repair.json", repair)
    changes = []
    for table, rows in updates.items():
        original = backfill.indexed(before[table])
        for row in rows:
            for field, value in row.items():
                previous = original.get(row["monday_id"], {}).get(field)
                if previous != value:
                    changes.append({"table": table, "monday_id": row["monday_id"], "field": field,
                                    "before": previous, "after": value})
    write_csv(repair_dir / "changes.csv", changes, ["table", "monday_id", "field", "before", "after"])
    receipt = {"repair_id": str(uuid4()), "prepared_at": datetime.now(timezone.utc).isoformat(),
               "code": repair_code(), "target": manifest["target"], "sha256": backfill.fingerprint(repair),
               "review_sha256": hashlib.sha256((repair_dir / "changes.csv").read_bytes()).hexdigest(),
               "rows": {table: len(rows) for table, rows in updates.items()}, "changes": len(changes)}
    backfill.write_json(repair_dir / "manifest.json", receipt)
    return receipt


def load_repair(repair_dir: Path):
    manifest = json.loads((repair_dir / "manifest.json").read_text(encoding="utf-8"))
    repair = json.loads((repair_dir / "repair.json").read_text(encoding="utf-8"))
    if (manifest["code"] != repair_code() or manifest["sha256"] != backfill.fingerprint(repair)
            or manifest["review_sha256"] != hashlib.sha256((repair_dir / "changes.csv").read_bytes()).hexdigest()):
        raise ValueError("Repair code or audit artifacts changed; prepare a new repair")
    original, baseline, source, plan = backfill.load_run(Path(repair["backfill_run_dir"]))
    if original["run_id"] != repair["backfill_run_id"] or original["target"] != manifest["target"]:
        raise ValueError("Repair references a different backfill run")
    if select_scope(baseline, source, plan, set(repair["scope"]["projects"])) != repair["scope"]:
        raise ValueError("Repair scope does not match reviewed candidates")
    rebuilt = normalize_updates(transform_exact_rows(repair["raw"]["hidden_items"], repair["raw"]["subitems"],
                                                      set(repair["scope"]["projects"])), repair["contract"])
    if rebuilt != repair["updates"]:
        raise ValueError("Repair does not match its source evidence")
    return manifest, repair, baseline, source


def repaired_baseline(baseline: dict, updates: dict) -> dict:
    result = {}
    for table, columns in backfill.BASELINE_COLUMNS.items():
        rows = {item_id: dict(row) for item_id, row in backfill.indexed(baseline[table]).items()}
        for update in updates[table]:
            row = rows.setdefault(update["monday_id"], dict.fromkeys(columns))
            row.update({field: value for field, value in update.items() if field in columns})
        result[table] = [rows[item_id] for item_id in sorted(rows)]
    return result


def check_rows(before: dict, current: dict, updates: dict) -> bool:
    for table, rows in updates.items():
        actual = backfill.indexed(current[table])
        original = backfill.indexed(before[table])
        if set(actual) != {row["monday_id"] for row in rows}:
            return False
        for update in rows:
            item_id = update["monday_id"]
            expected = {field: value for field, value in original.get(item_id, {}).items()
                        if field not in ("last_synced_at", "updated_at")}
            expected.update(update)
            if table == "projects" and "invoicing_spread_days" in expected:
                first_date = expected.get("first_date_invoiced")
                last_date = expected.get("last_date_invoiced")
                expected["invoicing_spread_days"] = (
                    max((date.fromisoformat(last_date) - date.fromisoformat(first_date)).days, 0)
                    if first_date and last_date else None)
            if any(actual[item_id].get(field) != value for field, value in expected.items()):
                return False
    return True


def write_updates(connection, updates: dict) -> None:
    with connection.cursor() as cursor:
        for table in ("hidden_items", "subitems", "projects"):
            for row in updates[table]:
                fields = list(row)
                values = [Jsonb(row[field]) if isinstance(row[field], (dict, list)) else row[field] for field in fields]
                if table == "projects":
                    writable = [field for field in fields if field != "monday_id"]
                    statement = sql.SQL("UPDATE public.{} SET {} WHERE monday_id = %s").format(
                        sql.Identifier(table), sql.SQL(", ").join(
                            sql.SQL("{} = %s").format(sql.Identifier(field)) for field in writable))
                    cursor.execute(statement, [row[field] for field in writable] + [row["monday_id"]])
                else:
                    statement = sql.SQL("INSERT INTO public.{} ({}) VALUES ({}) ON CONFLICT (monday_id) DO UPDATE SET {}").format(
                        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, fields)),
                        sql.SQL(", ").join(sql.Placeholder() for field in fields),
                        sql.SQL(", ").join(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(field), sql.Identifier(field))
                                          for field in fields if field != "monday_id"))
                    cursor.execute(statement, values)
                if cursor.rowcount != 1:
                    raise ValueError("Repair did not write exactly one reviewed row")


def apply_repair(connection, monday, repair_dir: Path, confirm_repair_id: str, writers_paused: bool) -> dict:
    if not writers_paused:
        raise ValueError("Pause writers and retain webhook events before repair")
    manifest, repair, baseline, source = load_repair(repair_dir)
    if manifest["repair_id"] != confirm_repair_id or manifest["target"] != backfill.target_fingerprint(connection):
        raise ValueError("Repair confirmation or database target differs")
    if backfill.capture_source_with_reviewed_duplicates(monday) != source:
        raise ValueError("Monday inventory or source changed since the reviewed backfill")
    if capture_targeted(monday, source, repair["scope"]) != repair["raw"]:
        raise ValueError("Targeted source values changed since repair preparation")
    expected = repaired_baseline(baseline, repair["updates"])
    with connection.transaction():
        connection.execute("SET LOCAL lock_timeout = '5s'")
        connection.execute("SET LOCAL statement_timeout = '60s'")
        connection.execute("LOCK TABLE public.hidden_items, public.subitems, public.projects IN SHARE ROW EXCLUSIVE MODE")
        current = backfill.read_baseline(connection)
        scoped = read_scope(connection, repair["scope"])
        if read_contract(connection) != repair["contract"]:
            raise ValueError("Database schema changed since preparation")
        if current == expected and check_rows(repair["before"], scoped, repair["updates"]):
            status = "already_applied"
        else:
            if current != baseline or scoped != repair["before"]:
                raise ValueError("Database changed since preparation; rolling back")
            write_updates(connection, repair["updates"])
            if (backfill.read_baseline(connection) != expected
                    or not check_rows(repair["before"], read_scope(connection, repair["scope"]), repair["updates"])):
                raise ValueError("Repair reconciliation failed; rolling back all changes")
            status = "applied"
    receipt = {"repair_id": manifest["repair_id"], "status": status,
               "completed_at": datetime.now(timezone.utc).isoformat(), "rows": manifest["rows"],
               "project_ids": repair["scope"]["projects"], "requires_fresh_backfill": True}
    backfill.write_json(repair_dir / f"apply-{uuid4()}.json", receipt)
    return receipt


def verify_repair(connection, repair_dir: Path) -> dict:
    manifest, repair, baseline, _ = load_repair(repair_dir)
    if manifest["target"] != backfill.target_fingerprint(connection):
        raise ValueError("Database target differs")
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        matches = (backfill.read_baseline(connection) == repaired_baseline(baseline, repair["updates"])
                   and check_rows(repair["before"], read_scope(connection, repair["scope"]), repair["updates"]))
    receipt = {"repair_id": manifest["repair_id"], "matches_reviewed_result": matches,
               "verified_at": datetime.now(timezone.utc).isoformat()}
    backfill.write_json(repair_dir / f"verify-{uuid4()}.json", receipt)
    return receipt


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    report = commands.add_parser("report", help="Offline discrepancy reports; no database or Monday access")
    report.add_argument("--run-dir", required=True, type=Path)
    report.add_argument("--output-dir", required=True, type=Path)
    prepare = commands.add_parser("prepare", help="Stage an exact-ID repair; read-only database and Monday access")
    prepare.add_argument("--run-dir", required=True, type=Path)
    prepare.add_argument("--repair-dir", required=True, type=Path)
    selection = prepare.add_mutually_exclusive_group(required=True)
    selection.add_argument("--project-id", action="append")
    selection.add_argument("--project-ids-file", type=Path, help="JSON array containing 1-25 reviewed project IDs")
    selection.add_argument("--repair-group", action="append", help="Reviewed dependency group ID; repeat within the 25-project limit")
    apply = commands.add_parser("apply", help="Apply only the staged repair in one database transaction")
    apply.add_argument("--repair-dir", required=True, type=Path)
    apply.add_argument("--confirm-repair-id", required=True)
    apply.add_argument("--writers-paused", required=True, action="store_true")
    verify = commands.add_parser("verify", help="Read-only repair verification")
    verify.add_argument("--repair-dir", required=True, type=Path)
    return parser


def main(argv=None) -> int:
    args = argument_parser().parse_args(argv)
    backfill.load_dotenv()
    logging.getLogger("src.database.sync_service").setLevel(logging.WARNING)
    try:
        if args.command == "report":
            result = report_run(args.run_dir, args.output_dir)
        else:
            if args.command == "prepare":
                if args.repair_group:
                    _, baseline, source, plan = backfill.load_run(args.run_dir)
                    groups = {row["group_id"]: row for row in build_report(baseline, source, plan)["repair_groups"]}
                    if (len(set(args.repair_group)) != len(args.repair_group)
                            or any(group_id not in groups or groups[group_id]["action"] != "prepare_candidate"
                                   for group_id in args.repair_group)):
                        raise ValueError("Unknown or blocked repair group; inspect repair-groups.csv")
                    selected = [parent_id for group_id in args.repair_group for parent_id in groups[group_id]["project_ids"]]
                else:
                    selected = json.loads(args.project_ids_file.read_text(encoding="utf-8")) if args.project_ids_file else args.project_id
                if (not isinstance(selected, list) or not 1 <= len(selected) <= 25
                        or any(not isinstance(item_id, str) or not item_id.isdigit() for item_id in selected)
                        or len(selected) != len(set(selected))):
                    raise ValueError("Provide between 1 and 25 unique numeric project IDs as strings")
            dsn = os.getenv("SUPABASE_DB_URL")
            if not dsn:
                raise ValueError("SUPABASE_DB_URL is required; never put credentials on the command line")
            with psycopg.connect(dsn, autocommit=True, connect_timeout=15) as connection:
                if args.command == "prepare":
                    result = prepare_repair(connection, backfill.MondayClient(), args.run_dir, args.repair_dir, set(selected))
                elif args.command == "apply":
                    result = apply_repair(connection, backfill.MondayClient(), args.repair_dir,
                                          args.confirm_repair_id, args.writers_paused)
                else:
                    result = verify_repair(connection, args.repair_dir)
        print(json.dumps(result, indent=2))
        return 2 if result.get("matches_reviewed_result") is False else 0
    except Exception as exc:
        LOG.error("Reconciliation stopped (%s): %s", type(exc).__name__,
                  str(exc) if isinstance(exc, ValueError) else "Inspect connectivity, schema and private audit artifacts")
        if args.command == "apply":
            LOG.error("If commit or receipt status is uncertain, run verify before retrying")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())