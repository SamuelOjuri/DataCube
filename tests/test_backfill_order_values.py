from copy import deepcopy
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import pytest
import requests

from scripts import backfill_order_values as backfill
from scripts.backfill_order_values import build_plan, fetch_board, normalize_hidden, normalize_subitem, ORDER_FIELDS, TOTAL_COLUMN
from src.config import HIDDEN_ITEMS_COLUMNS, SUBITEM_COLUMNS


def sample_inputs():
    baseline = {
        "projects": [{"monday_id": "p1", "total_order_value": "100.00"}],
        "subitems": [{"monday_id": "s1", "parent_monday_id": "p1", "hidden_item_id": "h1",
                      "cust_order_value_material": "100.00", "cust_additional_charges": None}],
        "hidden_items": [{"monday_id": "h1", "cust_order_value_material": "100.00",
                          "cust_additional_charges": None}],
    }
    source = {
        "project_ids": ["p1"],
        "subitems": [{"monday_id": "s1", "parent_monday_id": "p1", "hidden_ids": ["h1"]}],
        "hidden_items": [{"monday_id": "h1", "cust_order_value_material": "100.00",
                          "cust_additional_charges": "5.00", "issues": []}],
    }
    return baseline, source


def reviewed_duplicate_inputs():
    baseline = {"projects": [], "subitems": [], "hidden_items": []}
    source = {"project_ids": [], "subitems": [], "hidden_items": [],
              "exclusion_evidence": {"approved_mappings": deepcopy(backfill.REVIEWED_PARENTLESS_DUPLICATES),
                                     "excluded_subitems": []}}
    for excluded_id, expected in backfill.REVIEWED_PARENTLESS_DUPLICATES.items():
        child_id, parent_id, hidden_id = expected["subitem_id"], expected["parent_id"], expected["hidden_id"]
        baseline["projects"].append({"monday_id": parent_id, "total_order_value": "0.00"})
        baseline["subitems"].append({"monday_id": child_id, "parent_monday_id": parent_id,
                                     "hidden_item_id": hidden_id, **dict.fromkeys(ORDER_FIELDS)})
        baseline["hidden_items"].append({"monday_id": hidden_id, **dict.fromkeys(ORDER_FIELDS)})
        source["project_ids"].append(parent_id)
        child = {"monday_id": child_id, "item_name": child_id, "parent_monday_id": parent_id,
                 "parent_board_id": backfill.PARENT_BOARD_ID, "parent_state": "active",
                 "board_id": backfill.SUBITEM_BOARD_ID, "state": "active",
                 "hidden_ids": [hidden_id], "link_error": False}
        source["subitems"].append(child)
        source["exclusion_evidence"]["excluded_subitems"].append(
            {**child, "monday_id": excluded_id, "parent_monday_id": "", "parent_board_id": None, "parent_state": None})
        source["hidden_items"].append({"monday_id": hidden_id, "state": "active", "board_id": backfill.HIDDEN_ITEMS_BOARD_ID,
                                       **{field: "0.00" for field in ORDER_FIELDS}, "monday_total": "0.00", "issues": [],
                                       "amount_invoiced": "0.00", "date_order_received": None, "invoice_date": None})
    parent_items = []
    for child in source["subitems"]:
        parent = {"id": child["parent_monday_id"], "state": "active", "board": {"id": backfill.PARENT_BOARD_ID},
                  "parent_item": None}
        parent_items.append({**parent, "subitems": [{"id": child["monday_id"], "state": "active",
                                                     "board": {"id": backfill.SUBITEM_BOARD_ID},
                                                     "parent_item": {key: parent[key] for key in ("id", "state", "board")}}]})
    source["exclusion_evidence"].update({
        "subitem_inventory": {"before_count": 4, "after_count": 4, "item_ids": sorted(
            [row["monday_id"] for row in source["subitems"]] + list(backfill.REVIEWED_PARENTLESS_DUPLICATES))},
        "parent_inventory": {"before_count": 4, "after_count": 4, "item_ids": sorted(source["project_ids"])},
        "parent_details": {"items": parent_items, "not_returned_ids": []},
        "hidden_inventory": {"before_count": 4, "after_count": 4,
                             "item_ids": sorted(row["monday_id"] for row in source["hidden_items"])},
    })
    return baseline, source


class ReviewedDuplicateMonday:
    def __init__(self):
        _, source = reviewed_duplicate_inputs()
        parents = deepcopy(source["exclusion_evidence"]["parent_details"]["items"])
        self.rows = {str(parent["id"]): parent for parent in parents}
        for child in source["subitems"] + source["exclusion_evidence"]["excluded_subitems"]:
            parent = self.rows.get(child["parent_monday_id"])
            self.rows[child["monday_id"]] = {
                "id": child["monday_id"], "name": child["item_name"], "state": child["state"],
                "board": {"id": backfill.SUBITEM_BOARD_ID},
                "parent_item": {key: parent[key] for key in ("id", "state", "board")} if parent else None,
                "column_values": [{"id": SUBITEM_COLUMNS["hidden_item_id"], "linked_item_ids": child["hidden_ids"]}],
            }
        for hidden in source["hidden_items"]:
            self.rows[hidden["monday_id"]] = {
                "id": hidden["monday_id"], "name": hidden["monday_id"], "state": "active",
                "board": {"id": backfill.HIDDEN_ITEMS_BOARD_ID}, "parent_item": None,
                "column_values": [{"id": HIDDEN_ITEMS_COLUMNS[field], "text": "", "value": None}
                                  for field in (*ORDER_FIELDS, "amount_invoiced", "date_order_received", "invoice_date")]
                                 + [{"id": TOTAL_COLUMN, "display_value": "0"}],
            }
        self.board_ids = {board_id: sorted(item_id for item_id, row in self.rows.items() if row["board"]["id"] == board_id)
                          for board_id in (backfill.PARENT_BOARD_ID, backfill.SUBITEM_BOARD_ID, backfill.HIDDEN_ITEMS_BOARD_ID)}
        self.counts = {board_id: len(item_ids) for board_id, item_ids in self.board_ids.items()}
        self.counts[backfill.SUBITEM_BOARD_ID] -= 4

    def get_board_info(self, board_id):
        return {"items_count": self.counts[board_id]}

    def execute_query(self, query, variables):
        assert "mutation" not in query
        if "board_id" in variables:
            board_id = variables["board_id"]
            return {"data": {"boards": [{"id": board_id, "items_page": {
                "items": [{"id": item_id} for item_id in self.board_ids[board_id]], "cursor": None}}]}}
        assert "exclude_nonactive: false" in query
        return {"data": {"items": [deepcopy(self.rows[item_id]) for item_id in variables["ids"] if item_id in self.rows]}}


def test_reviewed_duplicate_capture_is_repeatable_and_keeps_normal_sources():
    monday = ReviewedDuplicateMonday()
    baseline, _ = reviewed_duplicate_inputs()
    first = backfill.capture_source_with_reviewed_duplicates(monday)
    second = backfill.capture_source_with_reviewed_duplicates(monday)
    assert first == second
    plan = build_plan(baseline, first)
    assert len(plan["excluded_subitems"]) == 4
    assert len(plan["updates"]["subitems"]) == len(plan["updates"]["hidden_items"]) == 4
    assert not plan["diagnostics"]


@pytest.mark.parametrize("change", ["extra_item", "missing_duplicate", "wrong_count", "parent_child_missing",
                                    "hidden_count", "missing_source_detail", "missing_financial_column", "new_date"])
def test_reviewed_duplicate_capture_rejects_unexplained_changes(change):
    monday = ReviewedDuplicateMonday()
    excluded_id, expected = next(iter(backfill.REVIEWED_PARENTLESS_DUPLICATES.items()))
    if change == "extra_item":
        monday.board_ids[backfill.SUBITEM_BOARD_ID].append("extra")
    elif change == "missing_duplicate":
        monday.board_ids[backfill.SUBITEM_BOARD_ID].remove(excluded_id)
    elif change == "wrong_count":
        monday.counts[backfill.SUBITEM_BOARD_ID] += 1
    elif change == "parent_child_missing":
        monday.rows[expected["parent_id"]]["subitems"] = []
    elif change == "hidden_count":
        monday.counts[backfill.HIDDEN_ITEMS_BOARD_ID] += 1
    elif change == "missing_source_detail":
        del monday.rows[expected["hidden_id"]]
    elif change == "missing_financial_column":
        monday.rows[expected["hidden_id"]]["column_values"] = [
            row for row in monday.rows[expected["hidden_id"]]["column_values"]
            if row["id"] != HIDDEN_ITEMS_COLUMNS["amount_invoiced"]]
    else:
        column = next(row for row in monday.rows[expected["hidden_id"]]["column_values"]
                      if row["id"] == HIDDEN_ITEMS_COLUMNS["date_order_received"])
        column["value"] = '{"date": "2026-09-30"}'
    with pytest.raises(ValueError):
        backfill.capture_source_with_reviewed_duplicates(monday)


def test_reviewed_duplicates_preserve_counterparts_in_plan():
    baseline, source = reviewed_duplicate_inputs()
    plan = build_plan(baseline, source)
    assert not plan["diagnostics"]
    assert all(row["status"] == "verified" for row in plan["projects"])
    assert len(plan["excluded_subitems"]) == 4
    assert {row["monday_id"] for row in plan["updates"]["subitems"]} == {
        row["subitem_id"] for row in backfill.REVIEWED_PARENTLESS_DUPLICATES.values()}
    assert len(plan["updates"]["hidden_items"]) == 4


def test_reviewed_duplicates_preserve_other_nonzero_orders_on_same_parent():
    baseline, source = reviewed_duplicate_inputs()
    child_id, hidden_id = "9000001", "9000002"
    baseline["subitems"].append({**baseline["subitems"][0], "monday_id": child_id, "hidden_item_id": hidden_id})
    baseline["hidden_items"].append({**baseline["hidden_items"][0], "monday_id": hidden_id})
    source["subitems"].append({**source["subitems"][0], "monday_id": child_id, "hidden_ids": [hidden_id]})
    source["hidden_items"].append({**source["hidden_items"][0], "monday_id": hidden_id,
                                   "cust_order_value_material": "100.00", "cust_additional_charges": "5.00",
                                   "monday_total": "105.00"})
    evidence = source["exclusion_evidence"]
    parent = evidence["parent_details"]["items"][0]
    parent["subitems"].append({**parent["subitems"][0], "id": child_id})
    for inventory_name, item_id in (("subitem_inventory", child_id), ("hidden_inventory", hidden_id)):
        inventory = evidence[inventory_name]
        inventory["item_ids"] = sorted([*inventory["item_ids"], item_id])
        inventory["before_count"] += 1
        inventory["after_count"] += 1
    plan = build_plan(baseline, source)
    assert not plan["diagnostics"]
    assert all(row["status"] == "verified" for row in plan["projects"])
    assert plan["updates"]["projects"] == [{"monday_id": parent["id"], "total_order_value": "105.00"}]
    assert len(plan["updates"]["subitems"]) == len(plan["updates"]["hidden_items"]) == 5
    assert len(plan["excluded_subitems"]) == 4


@pytest.mark.parametrize("change", ["mapping", "null_evidence", "hidden_inventory", "board_inventory",
                                    "absent_duplicate", "reparented", "inactive", "hidden_link",
                                    "missing_counterpart", "counterpart_parent", "parent_state", "source_amount",
                                    "source_unknown", "source_invoice", "source_date", "database_duplicate",
                                    "stored_link", "stored_financial", "additional_link"])
def test_reviewed_duplicate_validation_fails_closed(change):
    baseline, source = reviewed_duplicate_inputs()
    evidence = source["exclusion_evidence"]
    duplicate = evidence["excluded_subitems"][0]
    child, hidden = source["subitems"][0], source["hidden_items"][0]
    if change == "mapping":
        evidence["approved_mappings"] = {}
    elif change == "null_evidence":
        source["exclusion_evidence"] = None
    elif change == "hidden_inventory":
        evidence["hidden_inventory"]["item_ids"].pop()
    elif change == "board_inventory":
        evidence["subitem_inventory"]["item_ids"].append("unexpected")
    elif change == "absent_duplicate":
        evidence["excluded_subitems"].pop()
    elif change == "reparented":
        duplicate["parent_monday_id"] = child["parent_monday_id"]
    elif change == "inactive":
        duplicate["state"] = "archived"
    elif change == "hidden_link":
        duplicate["hidden_ids"] = ["unexpected"]
    elif change == "missing_counterpart":
        source["subitems"].pop(0)
    elif change == "counterpart_parent":
        child["parent_monday_id"] = "unexpected"
    elif change == "parent_state":
        child["parent_state"] = "archived"
    elif change == "source_amount":
        hidden[ORDER_FIELDS[1]] = "1.00"
    elif change == "source_unknown":
        hidden[ORDER_FIELDS[0]] = None
    elif change == "source_invoice":
        hidden["amount_invoiced"] = "1.00"
    elif change == "source_date":
        hidden["date_order_received"] = "2026-09-30"
    elif change == "database_duplicate":
        baseline["subitems"].append({"monday_id": duplicate["monday_id"]})
    elif change == "stored_link":
        baseline["subitems"][0]["hidden_item_id"] = "unexpected"
    elif change == "stored_financial":
        baseline["hidden_items"][0][ORDER_FIELDS[0]] = "1.00"
    else:
        baseline["subitems"].append({**baseline["subitems"][0], "monday_id": "extra"})
    with pytest.raises(ValueError):
        build_plan(baseline, source)


def test_backfill_replaces_nonzero_totals_and_is_idempotent():
    baseline, source = sample_inputs()
    plan = build_plan(baseline, source)
    assert plan["updates"]["projects"] == [{"monday_id": "p1", "total_order_value": "105.00"}]
    assert plan["projects"][0]["difference"] == "5.00"
    for table, updates in plan["updates"].items():
        for update in updates:
            next(row for row in baseline[table] if row["monday_id"] == update["monday_id"]).update(update)
    assert build_plan(baseline, source)["updates"] == {"projects": [], "subitems": [], "hidden_items": []}


def test_unknown_child_withholds_entire_project():
    baseline, source = sample_inputs()
    source["hidden_items"][0]["cust_additional_charges"] = None
    plan = build_plan(baseline, source)
    assert plan["projects"][0]["status"] == "blocked"
    assert not any(plan["updates"].values())


def test_duplicate_hidden_source_blocks_all_affected_projects():
    baseline, source = sample_inputs()
    baseline["projects"].append({"monday_id": "p2", "total_order_value": "100.00"})
    baseline["subitems"].append({**baseline["subitems"][0], "monday_id": "s2", "parent_monday_id": "p2"})
    source["project_ids"].append("p2")
    source["subitems"].append({**source["subitems"][0], "monday_id": "s2", "parent_monday_id": "p2"})
    plan = build_plan(baseline, source)
    assert all(row["status"] == "blocked" for row in plan["projects"])
    assert not any(plan["updates"].values())


def test_empty_project_needs_explicit_approval_before_clearing():
    baseline, source = sample_inputs()
    baseline["subitems"] = []
    source["subitems"] = []
    assert build_plan(baseline, source)["projects"][0]["status"] == "blocked"
    plan = build_plan(baseline, source, {"p1"})
    assert plan["updates"]["projects"] == [{"monday_id": "p1", "total_order_value": "0.00"}]


def test_formula_mismatch_blocks_project():
    baseline, source = sample_inputs()
    source["hidden_items"][0]["issues"] = ["monday_formula_mismatch"]
    assert not any(build_plan(baseline, source)["updates"].values())


def test_missing_live_child_does_not_clear_stale_total():
    baseline, source = sample_inputs()
    source["subitems"] = []
    plan = build_plan(baseline, source, {"p1"})
    assert plan["projects"][0]["status"] == "blocked"
    assert not any(plan["updates"].values())


def test_numeric_source_parsing_and_monday_formula_reconciliation():
    item = {"id": "123", "column_values": [
        {"id": HIDDEN_ITEMS_COLUMNS[ORDER_FIELDS[0]], "value": '"100.25"'},
        {"id": HIDDEN_ITEMS_COLUMNS[ORDER_FIELDS[1]], "value": '"5.75"'},
        {"id": TOTAL_COLUMN, "display_value": "106"},
    ]}
    assert normalize_hidden(item)["issues"] == []
    item["column_values"][-1]["display_value"] = "100.25"
    assert normalize_hidden(item)["issues"] == ["monday_formula_mismatch"]
    item["column_values"].pop()
    assert normalize_hidden(item)["issues"] == ["monday_total_unavailable_or_invalid"]


def test_source_link_keeps_all_connected_ids_for_duplicate_detection():
    item = {"id": "123", "parent_item": {"id": "456"}, "column_values": [
        {"id": SUBITEM_COLUMNS["hidden_item_id"], "linked_item_ids": ["789", "999"], "value": None}
    ]}
    result = normalize_subitem(item)
    assert result["hidden_ids"] == ["789", "999"]
    assert result["parent_monday_id"] == "456"
    assert not result["link_error"]


class FakeMonday:
    def __init__(self, incomplete=False):
        self.incomplete = incomplete
        self.first_cursors = []

    def get_board_info(self, board_id):
        return {"items_count": 2}

    def get_item_ids_page(self, board_id, limit, cursor):
        self.first_cursors.append(cursor)
        return {"items": [{"id": "1"}], "next_cursor": "next"}

    def get_next_item_ids_page(self, cursor, limit):
        return {"items": [{"id": "2"}], "next_cursor": None}

    def execute_query(self, query, variables):
        return {"data": {"items": [] if self.incomplete else [{"id": item_id} for item_id in variables["ids"]]}}


def test_source_traversal_always_starts_fresh_and_checks_detail_coverage():
    monday = FakeMonday()
    assert fetch_board(monday, "board", []) == [{"id": "1"}, {"id": "2"}]
    assert monday.first_cursors == [None]
    with pytest.raises(ValueError, match="Incomplete Monday detail"):
        fetch_board(FakeMonday(incomplete=True), "board", ["required_column"])


class InventoryMonday:
    def __init__(self, pages=None, counts=None):
        self.pages = pages or [{"items": [{"id": "1"}], "cursor": "next"},
                               {"items": [{"id": "2"}], "cursor": None}]
        self.counts = iter(counts or [1, 1, 1, 1])
        self.first_pages = 0

    def get_board_info(self, board_id):
        return {"items_count": next(self.counts)}

    def execute_query(self, query, variables):
        assert "column_values" not in query
        assert "mutation" not in query
        if "board_id" in variables:
            self.first_pages += 1
            return {"data": {"boards": [{"id": variables["board_id"], "items_page": self.pages[0]}]}}
        assert variables["cursor"] == "next"
        return {"data": {"next_items_page": self.pages[1]}}


def test_inventory_scans_start_fresh_and_record_count_mismatch():
    monday = InventoryMonday()
    first = backfill.scan_board_inventory(monday, "board")
    second = backfill.scan_board_inventory(monday, "board")
    assert monday.first_pages == 2
    assert first["item_ids"] == second["item_ids"] == ["1", "2"]
    assert first["before_count"] == first["after_count"] == 1
    assert first["captured_count"] == 2
    assert first["count_delta"] == 1
    assert first["page_count"] == 2
    assert first["count_stable"]
    assert not first["counts_match"]


def test_inventory_recovers_from_initial_connection_reset(monkeypatch, caplog):
    monday = InventoryMonday()
    attempts = 0
    waits = []

    def flaky_board_info(board_id):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise requests.ConnectionError("Connection reset by peer")
        return {"items_count": 2}

    monkeypatch.setattr(monday, "get_board_info", flaky_board_info)
    monkeypatch.setattr("time.sleep", waits.append)
    result = backfill.scan_board_inventory(monday, "board")
    assert result["item_ids"] == ["1", "2"]
    assert result["counts_match"]
    assert attempts == 3
    assert waits == [5]
    assert "before-count" in caplog.text


@pytest.mark.parametrize("failure", [requests.ConnectionError, requests.ConnectTimeout, requests.ReadTimeout])
def test_inventory_read_retries_are_bounded_and_redact_errors(monkeypatch, caplog, failure):
    attempts = 0
    waits = []

    def failing_read():
        nonlocal attempts
        attempts += 1
        raise failure("private exception detail")

    monkeypatch.setattr("time.sleep", waits.append)
    with pytest.raises(ValueError, match="board test before-count after 6 attempts") as error:
        backfill._inventory_read("board test before-count", failing_read)
    assert attempts == 6
    assert waits == [5, 10, 20, 40, 60]
    assert "private exception detail" not in str(error.value) + caplog.text


@pytest.mark.parametrize("failure", [
    requests.exceptions.SSLError("certificate verification failed"),
    requests.HTTPError("HTTP 401"),
    ValueError("Invalid page"),
    RuntimeError("GraphQL error"),
])
def test_inventory_read_does_not_retry_nontransport_errors(monkeypatch, failure):
    attempts = 0
    waits = []

    def failing_read():
        nonlocal attempts
        attempts += 1
        raise failure

    monkeypatch.setattr("time.sleep", waits.append)
    with pytest.raises(type(failure)) as error:
        backfill._inventory_read("read", failing_read)
    assert error.value is failure
    assert attempts == 1
    assert not waits


@pytest.mark.parametrize("failed_page", ["first", "next"])
def test_inventory_page_retry_preserves_cursor_and_ids(monkeypatch, failed_page):
    monday = InventoryMonday()
    execute_query = monday.execute_query
    calls = []
    waits = []
    target = {"board_id": "board"} if failed_page == "first" else {"cursor": "next"}

    def flaky_query(query, variables):
        calls.append(deepcopy(variables))
        if variables == target and calls.count(target) == 1:
            raise requests.ConnectionError("Connection reset")
        return execute_query(query, variables)

    monkeypatch.setattr(monday, "execute_query", flaky_query)
    monkeypatch.setattr("time.sleep", waits.append)
    result = backfill.scan_board_inventory(monday, "board")
    assert result["item_ids"] == ["1", "2"]
    assert result["page_count"] == 2
    assert len(calls) == 3
    assert calls.count(target) == 2
    assert monday.first_pages == 1
    assert waits == [5]


def test_inventory_after_count_retry_does_not_restart_scan(monkeypatch):
    monday = InventoryMonday()
    attempts = 0
    waits = []

    def flaky_board_info(board_id):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise requests.ReadTimeout("Read timed out")
        return {"items_count": 2}

    monkeypatch.setattr(monday, "get_board_info", flaky_board_info)
    monkeypatch.setattr("time.sleep", waits.append)
    assert backfill.scan_board_inventory(monday, "board")["counts_match"]
    assert monday.first_pages == 1
    assert attempts == 3
    assert waits == [5]


@pytest.mark.parametrize("include_subitems", [False, True])
def test_inventory_metadata_retry_preserves_batch(monkeypatch, include_subitems):
    calls = []
    waits = []
    resets = []

    def flaky_query(query, variables):
        calls.append((query, deepcopy(variables)))
        if len(calls) <= 3:
            raise requests.ConnectionError("Connection reset")
        item = {"id": "1", "state": "active", "board": {"id": "board"}, "parent_item": None,
                "subitems": []}
        return {"data": {"items": [item]}}

    monkeypatch.setattr("time.sleep", waits.append)
    monday = SimpleNamespace(execute_query=flaky_query, session=SimpleNamespace(close=lambda: resets.append(True)))
    result = backfill.fetch_inventory_details(monday, ["1"],
                                              include_subitems=include_subitems)
    assert not result["not_returned_ids"]
    assert len(calls) == 4
    assert all(call == calls[0] for call in calls)
    assert waits == [5, 10, 20]
    assert len(resets) == 3


def test_inventory_retry_exhaustion_preserves_completed_scan(monkeypatch, tmp_path):
    monday = InventoryMonday()
    count_calls = 0
    waits = []

    def failing_second_scan(board_id):
        nonlocal count_calls
        count_calls += 1
        if count_calls > 2:
            raise requests.ConnectionError("Connection reset")
        return {"items_count": 2}

    monkeypatch.setattr(monday, "get_board_info", failing_second_scan)
    monkeypatch.setattr("time.sleep", waits.append)
    run_dir = tmp_path / "failed-second-scan"
    with pytest.raises(ValueError, match="before-count after 6 attempts"):
        backfill.diagnose_inventory(monday, run_dir)
    assert count_calls == 8
    assert monday.first_pages == 1
    assert waits == [5, 10, 20, 40, 60]
    assert {path.name for path in run_dir.iterdir()} == {"inventory-1.json"}


@pytest.mark.parametrize("pages, message", [
    ([None], "Incomplete"),
    ([{"items": []}], "Incomplete"),
    ([{"items": [], "cursor": "next"}], "did not advance"),
    ([{"items": [{"id": "1"}, {"id": "1"}], "cursor": None}], "duplicate"),
    ([{"items": [{"id": "1"}], "cursor": "next"},
      {"items": [{"id": "1"}], "cursor": None}], "Duplicate"),
    ([{"items": [{"id": "1"}], "cursor": "next"},
      {"items": [{"id": "2"}], "cursor": "next"}], "did not advance"),
])
def test_inventory_rejects_broken_pages_and_duplicates(pages, message):
    with pytest.raises(ValueError, match=message):
        backfill.scan_board_inventory(InventoryMonday(pages=pages), "board")


@pytest.mark.parametrize("response", [
    {"data": None}, {"data": {}},
    {"data": {"boards": [], "items": []}, "errors": [{"message": "partial response"}]},
])
@pytest.mark.parametrize("operation", ["scan", "details"])
def test_inventory_rejects_partial_or_missing_graphql_response(response, operation):
    monday = SimpleNamespace(get_board_info=lambda board_id: {"items_count": 1},
                             execute_query=lambda query, variables: response)
    with pytest.raises(ValueError):
        if operation == "scan":
            backfill.scan_board_inventory(monday, "board")
        else:
            backfill.fetch_inventory_details(monday, ["1"])


@pytest.mark.parametrize("first_ids, second_ids, counts, classification", [
    (["1", "2"], ["1", "2"], [1, 1, 1, 1], "stable_inventory_count_mismatch"),
    (["1", "2"], ["1", "3"], [2, 2, 2, 2], "inventory_changed"),
    (["1", "2"], ["1", "2"], [1, 2, 2, 2], "reported_count_changed"),
    (["1", "2"], ["1", "2"], [2, 2, 2, 2], "counts_and_ids_match"),
])
def test_inventory_comparison_checks_sets_not_only_counts(first_ids, second_ids, counts, classification):
    scans = [{"item_ids": item_ids, "before_count": counts[offset], "after_count": counts[offset + 1],
              "counts_match": counts[offset] == counts[offset + 1] == len(item_ids)}
             for item_ids, offset in ((first_ids, 0), (second_ids, 2))]
    result = backfill.compare_inventory_scans(*scans)
    assert result["classification"] == classification
    assert result["only_first_ids"] == sorted(set(first_ids) - set(second_ids))
    assert result["only_second_ids"] == sorted(set(second_ids) - set(first_ids))


def inventory_parent():
    parent = {"id": "p1", "state": "active", "board": {"id": backfill.PARENT_BOARD_ID}, "parent_item": None}
    child = {"id": "1", "state": "active", "board": {"id": backfill.SUBITEM_BOARD_ID},
             "parent_item": {key: parent[key] for key in ("id", "state", "board")}}
    return {**parent, "subitems": [child]}


class ParentInventoryMonday(InventoryMonday):
    def __init__(self):
        super().__init__(counts=[1, 1, 1, 1, 1, 1])
        self.detail_requests = []

    def execute_query(self, query, variables):
        if variables.get("board_id") == backfill.PARENT_BOARD_ID:
            return {"data": {"boards": [{"id": backfill.PARENT_BOARD_ID,
                                         "items_page": {"items": [{"id": "p1"}], "cursor": None}}]}}
        if "ids" in variables:
            assert "column_values" not in query
            assert "exclude_nonactive: false" in query
            self.detail_requests.append(variables["ids"])
            if "subitems {" in query:
                return {"data": {"items": [inventory_parent()]}}
            return {"data": {"items": [{"id": "2", "state": "active", "board": {"id": backfill.SUBITEM_BOARD_ID},
                                        "parent_item": {"id": "p2", "state": "archived",
                                                        "board": {"id": backfill.PARENT_BOARD_ID}}}]}}
        return super().execute_query(query, variables)


def test_inventory_diagnostic_saves_scans_and_parent_discrepancy_evidence(tmp_path):
    monday = ParentInventoryMonday()
    run_dir = tmp_path / "diagnostic"
    summary = backfill.diagnose_inventory(monday, run_dir)
    assert summary["diagnostic_only"]
    assert summary["classification"] == "stable_inventory_count_mismatch"
    assert summary["needs_investigation"]
    assert summary["parent_check"]["only_in_board_scans"] == ["2"]
    assert monday.detail_requests == [["p1"], ["2"]]
    assert {path.name for path in run_dir.iterdir()} == {
        "inventory-1.json", "inventory-2.json", "comparison.json", "parent-inventory.json",
        "parent-details.json", "parent-comparison.json", "discrepancy-details.json", "summary.json",
    }
    inspection = backfill.json.loads((run_dir / "discrepancy-details.json").read_text())
    assert inspection["items"][0]["parent_item"]["state"] == "archived"
    with pytest.raises(ValueError, match="already exists"):
        backfill.diagnose_inventory(monday, run_dir)


def test_inventory_diagnostic_retains_first_scan_when_second_fails(tmp_path):
    monday = InventoryMonday(counts=[1, 1])
    run_dir = tmp_path / "partial"
    with pytest.raises(StopIteration):
        backfill.diagnose_inventory(monday, run_dir)
    assert {path.name for path in run_dir.iterdir()} == {"inventory-1.json"}


def test_parent_inventory_flags_missing_parents_relationships_states_and_boards():
    parent = inventory_parent()
    parent["subitems"][0]["board"]["id"] = "other"
    parent["subitems"][0]["state"] = "deleted"
    parent["subitems"][0]["parent_item"]["id"] = "wrong"
    parent["subitems"].append(deepcopy(parent["subitems"][0]))
    result = backfill.compare_parent_inventory({"2"}, {"counts_match": True},
                                              {"items": [parent], "not_returned_ids": ["p2"]})
    assert not result["consistent"]
    assert result["only_in_board_scans"] == ["2"]
    assert result["only_under_parents"] == ["1"]
    assert result["missing_parent_ids"] == ["p2"]
    issues = {issue for row in result["metadata_issues"] for issue in row["issues"]}
    assert {"unexpected_subitem_board", "subitem_not_active_or_state_unknown",
            "parent_relationship_mismatch", "subitem_listed_more_than_once"} <= issues


def test_parent_inventory_matching_sets_do_not_hide_bad_parent_counts():
    details = {"items": [inventory_parent()], "not_returned_ids": []}
    assert backfill.compare_parent_inventory({"1"}, {"counts_match": True}, details)["consistent"]
    assert not backfill.compare_parent_inventory({"1"}, {"counts_match": False}, details)["consistent"]


def test_inventory_details_batch_ids_and_report_unavailable_items():
    class DetailMonday:
        def __init__(self):
            self.batches = []

        def execute_query(self, query, variables):
            self.batches.append(variables["ids"])
            assert "limit: 100" in query
            return {"data": {"items": []}}

    monday = DetailMonday()
    requested = [str(number) for number in range(205)]
    result = backfill.fetch_inventory_details(monday, requested)
    assert [len(batch) for batch in monday.batches] == [100, 100, 5]
    assert result["not_returned_ids"] == sorted(requested)


def test_inventory_detail_checkpoints_resume_only_completed_batches(tmp_path, monkeypatch):
    calls = []
    failing = True
    requested = [str(number) for number in range(205)]

    def query_items(query, variables):
        calls.append(deepcopy(variables["ids"]))
        if failing and variables["ids"][0] == sorted(requested)[100]:
            raise requests.ConnectionError("interrupted second batch")
        return {"data": {"items": [{"id": item_id, "state": "active", "board": {"id": "board"},
                                    "parent_item": None} for item_id in variables["ids"]]}}

    monday = SimpleNamespace(execute_query=query_items)
    monkeypatch.setattr(backfill.time, "sleep", lambda delay: None)
    with pytest.raises(ValueError, match="after 6 attempts"):
        backfill.fetch_inventory_details(monday, requested, checkpoint_dir=tmp_path)
    assert len(list(tmp_path.glob("*.json"))) == 1
    calls.clear()
    failing = False
    result = backfill.fetch_inventory_details(monday, requested, checkpoint_dir=tmp_path)
    assert len(result["items"]) == 205
    assert [len(batch) for batch in calls] == [100, 5]
    assert len(list(tmp_path.glob("*.json"))) == 3
    checkpoint = next(tmp_path.glob("*.json"))
    record = backfill.json.loads(checkpoint.read_text())
    record["response"]["data"]["items"][0]["state"] = "archived"
    checkpoint.write_text(backfill.json.dumps(record))
    with pytest.raises(ValueError, match="checkpoint integrity"):
        backfill.fetch_inventory_details(monday, requested, checkpoint_dir=tmp_path)


@pytest.mark.parametrize("counts, expected_code", [([1, 1, 1, 1], 2), ([2, 2, 2, 2], 0)])
def test_inventory_cli_requires_no_database_and_ids_only_skips_details(monkeypatch, tmp_path, counts, expected_code):
    monkeypatch.setattr(backfill, "load_dotenv", lambda: None)
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monday = InventoryMonday(counts=counts)
    monkeypatch.setattr(backfill, "MondayClient", lambda: monday)

    def no_database(*args, **kwargs):
        pytest.fail("Inventory diagnostics must never connect to the database")

    monkeypatch.setattr(backfill.psycopg, "connect", no_database)
    run_dir = tmp_path / "ids-only"
    assert backfill.main(["diagnose-inventory", "--run-dir", str(run_dir), "--ids-only"]) == expected_code
    assert monday.first_pages == 2
    assert {path.name for path in run_dir.iterdir()} == {
        "inventory-1.json", "inventory-2.json", "comparison.json", "summary.json",
    }


@pytest.mark.parametrize("count, expected_code", [(37677, 0), (None, 1), (True, 1), (-1, 1), ("37677", 1)])
def test_check_monday_cli_never_connects_to_database(monkeypatch, capsys, count, expected_code):
    calls = []

    def read_count(board_id):
        calls.append(board_id)
        return {"items_count": count}

    monkeypatch.setattr(backfill, "load_dotenv", lambda: None)
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.setattr(backfill, "MondayClient", lambda: SimpleNamespace(get_board_info=read_count))
    monkeypatch.setattr(backfill.psycopg, "connect", lambda *args, **kwargs: pytest.fail("Probe must not open database"))
    assert backfill.main(["check-monday"]) == expected_code
    assert calls == [backfill.SUBITEM_BOARD_ID]
    if expected_code == 0:
        result = backfill.json.loads(capsys.readouterr().out)
        assert result["items_count"] == count
        assert set(result) == {"board_id", "items_count", "checked_at"}


class FakeConnection:
    def __init__(self, baseline):
        self.state = deepcopy(baseline)
        self.info = SimpleNamespace(host="localhost", port=5432, dbname="test", user="test")
        self.statements = []
        self.fail_projects = False
        self.rollbacks = 0

    @contextmanager
    def transaction(self):
        before = deepcopy(self.state)
        try:
            yield
        except Exception:
            self.state = before
            self.rollbacks += 1
            raise

    def execute(self, statement):
        self.statements.append(statement)

    @contextmanager
    def cursor(self, **kwargs):
        yield FakeWriteCursor(self)


class FakeWriteCursor:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, statement, values):
        text = statement.as_string()
        table = next(table for table in self.connection.state if f'public."{table}"' in text)
        if table == "projects" and self.connection.fail_projects:
            raise RuntimeError("simulated database failure")
        fields = ("total_order_value",) if table == "projects" else ORDER_FIELDS
        assert text.startswith("UPDATE ")
        assert "last_synced_at" not in text
        row = next(row for row in self.connection.state[table] if row["monday_id"] == values[-1])
        row.update({field: str(value) for field, value in zip(fields, values)})
        self.result = {"monday_id": values[-1]}

    def fetchone(self):
        return self.result


@pytest.fixture
def prepared_run(tmp_path, monkeypatch):
    baseline, source = sample_inputs()
    connection = FakeConnection(baseline)
    monkeypatch.setattr(backfill, "read_baseline", lambda connection: deepcopy(connection.state))
    monkeypatch.setattr(backfill, "capture_source", lambda monday: deepcopy(source))
    run_dir = tmp_path / "run"
    manifest = backfill.prepare_run(connection, object(), run_dir, set())
    return connection, source, run_dir, manifest


@pytest.fixture
def prepared_duplicate_run(tmp_path, monkeypatch):
    baseline, _ = reviewed_duplicate_inputs()
    connection = FakeConnection(baseline)
    monday = ReviewedDuplicateMonday()
    monkeypatch.setattr(backfill, "read_baseline", lambda connection: deepcopy(connection.state))
    run_dir = tmp_path / "approved-duplicates"
    manifest = backfill.prepare_run(connection, monday, run_dir, set(), approve_reviewed_parentless_duplicates=True)
    return connection, monday, run_dir, manifest


@pytest.fixture
def interrupted_duplicate_run(tmp_path, monkeypatch):
    baseline, _ = reviewed_duplicate_inputs()
    connection = FakeConnection(baseline)
    monday = ReviewedDuplicateMonday()
    execute_query = monday.execute_query
    calls = []
    failing = {"enabled": True}

    def interrupted_query(query, variables):
        calls.append(deepcopy(variables))
        if failing["enabled"] and "columns" in variables:
            raise requests.ConnectionError("simulated transport interruption")
        return execute_query(query, variables)

    monkeypatch.setattr(monday, "execute_query", interrupted_query)
    monkeypatch.setattr(backfill.time, "sleep", lambda delay: None)
    monkeypatch.setattr(backfill, "read_baseline", lambda connection: deepcopy(connection.state))
    run_dir = tmp_path / "interrupted"
    with pytest.raises(ValueError, match="after 6 attempts"):
        backfill.prepare_run(connection, monday, run_dir, set(), approve_reviewed_parentless_duplicates=True)
    assert len(list((run_dir / "capture-batches").glob("*.json"))) == 1
    assert not (run_dir / "manifest.json").exists()
    failing["enabled"] = False
    calls.clear()
    return connection, monday, run_dir, calls


def test_preparation_resume_rechecks_ids_and_apply_never_reuses_checkpoints(interrupted_duplicate_run):
    connection, monday, run_dir, calls = interrupted_duplicate_run
    baseline_bytes = (run_dir / "baseline.json").read_bytes()
    manifest = backfill.prepare_run(connection, monday, run_dir, set(),
                                    approve_reviewed_parentless_duplicates=True, resume=True)
    assert manifest["preparation_resumed"]
    assert (run_dir / "baseline.json").read_bytes() == baseline_bytes
    assert {call["board_id"] for call in calls if "board_id" in call} == {
        backfill.PARENT_BOARD_ID, backfill.SUBITEM_BOARD_ID, backfill.HIDDEN_ITEMS_BOARD_ID}
    assert not any("ids" in call and "columns" not in call for call in calls)
    calls.clear()
    receipt = backfill.apply_run(connection, monday, run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert receipt["status"] == "applied"
    assert any("ids" in call and "columns" not in call for call in calls)
    assert sum("columns" in call for call in calls) == 2
    assert backfill.verify_run(connection, run_dir)["matches_reviewed_result"]


def test_apply_rejects_stale_resumed_details_even_with_allow_blocked(interrupted_duplicate_run, monkeypatch):
    connection, monday, run_dir, _ = interrupted_duplicate_run
    execute_query = monday.execute_query

    def interrupt_hidden_capture(query, variables):
        if TOTAL_COLUMN in variables.get("columns", []):
            raise requests.ConnectionError("interrupted hidden stage")
        return execute_query(query, variables)

    monkeypatch.setattr(monday, "execute_query", interrupt_hidden_capture)
    with pytest.raises(ValueError, match="after 6 attempts"):
        backfill.prepare_run(connection, monday, run_dir, set(), approve_reviewed_parentless_duplicates=True, resume=True)
    assert len(list((run_dir / "capture-batches").glob("*.json"))) == 2
    monkeypatch.setattr(monday, "execute_query", execute_query)
    child_id = next(iter(backfill.REVIEWED_PARENTLESS_DUPLICATES.values()))["subitem_id"]
    monday.rows[child_id]["name"] = "Changed since checkpoint"
    manifest = backfill.prepare_run(connection, monday, run_dir, set(),
                                    approve_reviewed_parentless_duplicates=True, resume=True)
    before = deepcopy(connection.state)
    with pytest.raises(ValueError, match="Monday sources changed"):
        backfill.apply_run(connection, monday, run_dir, confirm_run_id=manifest["run_id"],
                           writers_paused=True, allow_blocked=True)
    assert connection.state == before
    assert not list(run_dir.glob("apply-*.json"))


@pytest.mark.parametrize("change", ["database", "code", "target", "approval", "baseline_file", "completed"])
def test_preparation_resume_refuses_changed_context(interrupted_duplicate_run, monkeypatch, change):
    connection, monday, run_dir, calls = interrupted_duplicate_run
    approved_empty = set()
    if change == "database":
        connection.state["projects"][0]["total_order_value"] = "1.00"
    elif change == "code":
        monkeypatch.setattr(backfill, "code_fingerprint", lambda: "changed")
    elif change == "target":
        connection.info.dbname = "different"
    elif change == "approval":
        approved_empty = {connection.state["projects"][0]["monday_id"]}
    elif change == "baseline_file":
        (run_dir / "baseline.json").write_text("{}")
    else:
        (run_dir / "manifest.json").write_text("{}")
    with pytest.raises(ValueError):
        backfill.prepare_run(connection, monday, run_dir, approved_empty,
                             approve_reviewed_parentless_duplicates=True, resume=True)
    assert not calls


def test_preparation_resume_cannot_upgrade_legacy_runs_or_change_mode(tmp_path):
    run_dir = tmp_path / "legacy"
    run_dir.mkdir()
    (run_dir / "baseline.json").write_text("{}")
    with pytest.raises(ValueError, match="No resumable capture context"):
        backfill.prepare_run(None, None, run_dir, set(), approve_reviewed_parentless_duplicates=True, resume=True)
    with pytest.raises(ValueError, match="Resume requires"):
        backfill.prepare_run(None, None, run_dir, set(), resume=True)


def test_reviewed_duplicate_prepare_apply_verify_and_repeat_are_audited(prepared_duplicate_run):
    connection, monday, run_dir, manifest = prepared_duplicate_run
    assert manifest["approve_reviewed_parentless_duplicates"] is True
    assert manifest["summary"]["excluded_subitems"] == 4
    assert connection.state == reviewed_duplicate_inputs()[0]
    first = backfill.apply_run(connection, monday, run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert first["status"] == "applied"
    assert first["updated_rows"] == {"hidden_items": 4, "subitems": 4, "projects": 0}
    assert first["excluded_subitems"] == manifest["excluded_subitems"]
    assert all(row[ORDER_FIELDS[0]] == "0.00" for row in connection.state["subitems"])
    assert not set(backfill.REVIEWED_PARENTLESS_DUPLICATES) & {row["monday_id"] for row in connection.state["subitems"]}
    second = backfill.apply_run(connection, monday, run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert second["status"] == "already_applied"
    assert not any(second["updated_rows"].values())
    verified = backfill.verify_run(connection, run_dir)
    assert verified["matches_reviewed_result"]
    assert verified["excluded_subitems"] == manifest["excluded_subitems"]


@pytest.mark.parametrize("change", ["reparented", "hidden_link", "inactive_counterpart", "new_source_value",
                                    "database_duplicate", "database_parent", "approval", "audit", "source_evidence"])
def test_reviewed_duplicate_apply_refuses_drift_even_with_allow_blocked(prepared_duplicate_run, change):
    connection, monday, run_dir, manifest = prepared_duplicate_run
    excluded_id, expected = next(iter(backfill.REVIEWED_PARENTLESS_DUPLICATES.items()))
    if change == "reparented":
        monday.rows[excluded_id]["parent_item"] = {"id": expected["parent_id"], "state": "active",
                                                    "board": {"id": backfill.PARENT_BOARD_ID}}
    elif change == "hidden_link":
        monday.rows[excluded_id]["column_values"][0]["linked_item_ids"] = ["99999"]
    elif change == "inactive_counterpart":
        monday.rows[expected["subitem_id"]]["state"] = "archived"
    elif change == "new_source_value":
        monday.rows[expected["hidden_id"]]["column_values"][0]["value"] = '"5"'
    elif change == "database_duplicate":
        connection.state["subitems"].append({"monday_id": excluded_id})
    elif change == "database_parent":
        connection.state["subitems"][0]["parent_monday_id"] = "unexpected"
    elif change == "source_evidence":
        evidence_path = run_dir / "source.json"
        source = backfill.json.loads(evidence_path.read_text())
        source["exclusion_evidence"]["excluded_subitems"].pop()
        evidence_path.write_text(backfill.json.dumps(source))
    else:
        if change == "approval":
            manifest["approve_reviewed_parentless_duplicates"] = False
        else:
            manifest["excluded_subitems"] = []
        (run_dir / "manifest.json").write_text(backfill.json.dumps(manifest))
    before = deepcopy(connection.state)
    with pytest.raises(ValueError):
        backfill.apply_run(connection, monday, run_dir, confirm_run_id=manifest["run_id"],
                           writers_paused=True, allow_blocked=True)
    assert connection.state == before
    assert not list(run_dir.glob("apply-*.json"))


def test_reviewed_duplicates_recheck_database_under_lock(prepared_duplicate_run, monkeypatch):
    connection, monday, run_dir, manifest = prepared_duplicate_run

    def concurrent_duplicate(connection):
        assert any(statement.startswith("LOCK TABLE") for statement in connection.statements)
        current = deepcopy(connection.state)
        current["subitems"].append({"monday_id": next(iter(backfill.REVIEWED_PARENTLESS_DUPLICATES))})
        return current

    monkeypatch.setattr(backfill, "read_baseline", concurrent_duplicate)
    before = deepcopy(connection.state)
    with pytest.raises(ValueError, match="duplicate exists"):
        backfill.apply_run(connection, monday, run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert connection.rollbacks == 1
    assert connection.state == before


def test_reviewed_duplicate_source_requires_explicit_prepare_approval(tmp_path, monkeypatch):
    baseline, source = reviewed_duplicate_inputs()
    connection = FakeConnection(baseline)
    monkeypatch.setattr(backfill, "read_baseline", lambda connection: deepcopy(connection.state))
    monkeypatch.setattr(backfill, "capture_source", lambda monday: source)
    run_dir = tmp_path / "unapproved"
    with pytest.raises(ValueError, match="explicit preparation approval"):
        backfill.prepare_run(connection, object(), run_dir, set())
    assert not (run_dir / "manifest.json").exists()
    default = backfill.argument_parser().parse_args(["prepare", "--run-dir", "unused"])
    approved = backfill.argument_parser().parse_args(["prepare", "--run-dir", "unused",
                                                    "--approve-reviewed-parentless-duplicates"])
    assert not default.approve_reviewed_parentless_duplicates
    assert approved.approve_reviewed_parentless_duplicates


def test_prepare_is_read_only_and_creates_reviewable_artifacts(prepared_run):
    connection, source, run_dir, manifest = prepared_run
    assert connection.state == sample_inputs()[0]
    assert sorted(path.name for path in run_dir.iterdir()) == ["baseline.json", "manifest.json", "plan.json", "review.csv", "source.json"]
    assert manifest["summary"]["updates"] == {"projects": 1, "subitems": 1, "hidden_items": 1}
    assert "READ ONLY" in connection.statements[0]


def test_apply_is_transactional_and_repeat_run_is_noop(prepared_run):
    connection, _, run_dir, manifest = prepared_run
    first = backfill.apply_run(connection, object(), run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert first["status"] == "applied"
    assert connection.state["projects"][0]["total_order_value"] == "105.00"
    assert any(statement.startswith("LOCK TABLE") for statement in connection.statements)
    second = backfill.apply_run(connection, object(), run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert second["status"] == "already_applied"
    assert not any(second["updated_rows"].values())
    assert backfill.verify_run(connection, run_dir)["matches_reviewed_result"]


def test_apply_rolls_back_all_three_tables_on_failure(prepared_run):
    connection, _, run_dir, manifest = prepared_run
    connection.fail_projects = True
    before = deepcopy(connection.state)
    with pytest.raises(RuntimeError):
        backfill.apply_run(connection, object(), run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert connection.state == before
    assert connection.rollbacks == 1
    assert not list(run_dir.glob("apply-*.json"))


@pytest.mark.parametrize("drift", ["source", "database", "artifact", "confirmation", "writers", "target"])
def test_apply_refuses_unreviewed_or_stale_changes(prepared_run, drift):
    connection, source, run_dir, manifest = prepared_run
    run_id = manifest["run_id"]
    if drift == "source":
        source["hidden_items"][0]["cust_additional_charges"] = "6.00"
    elif drift == "database":
        connection.state["projects"][0]["total_order_value"] = "101.00"
    elif drift == "artifact":
        (run_dir / "review.csv").write_text("changed", encoding="utf-8")
    elif drift == "confirmation":
        run_id = "not-the-reviewed-run"
    elif drift == "target":
        connection.info.host = "another-host"
    before = deepcopy(connection.state)
    with pytest.raises(ValueError):
        backfill.apply_run(connection, object(), run_dir, confirm_run_id=run_id, writers_paused=drift != "writers")
    assert connection.state == before


def test_partial_apply_requires_opt_in_and_leaves_blocked_rows_unchanged(tmp_path, monkeypatch):
    baseline, source = sample_inputs()
    baseline["projects"].append({"monday_id": "p2", "total_order_value": "777.00"})
    source["project_ids"].append("p2")
    connection = FakeConnection(baseline)
    monkeypatch.setattr(backfill, "read_baseline", lambda connection: deepcopy(connection.state))
    monkeypatch.setattr(backfill, "capture_source", lambda monday: deepcopy(source))
    run_dir = tmp_path / "partial"
    manifest = backfill.prepare_run(connection, object(), run_dir, set())
    with pytest.raises(ValueError, match="Unresolved"):
        backfill.apply_run(connection, object(), run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    result = backfill.apply_run(connection, object(), run_dir, confirm_run_id=manifest["run_id"],
                                writers_paused=True, allow_blocked=True)
    assert result["blocked_projects"] == 1
    assert connection.state["projects"][1]["total_order_value"] == "777.00"
    assert connection.state["projects"][0]["total_order_value"] == "105.00"


def test_failed_post_write_reconciliation_rolls_back(prepared_run, monkeypatch):
    connection, _, run_dir, manifest = prepared_run
    before = deepcopy(connection.state)
    calls = []

    def inconsistent_read(connection):
        calls.append(True)
        result = deepcopy(connection.state)
        if len(calls) == 2:
            result["projects"][0]["total_order_value"] = "wrong"
        return result

    monkeypatch.setattr(backfill, "read_baseline", inconsistent_read)
    with pytest.raises(ValueError, match="Post-write"):
        backfill.apply_run(connection, object(), run_dir, confirm_run_id=manifest["run_id"], writers_paused=True)
    assert connection.state == before
    assert not list(run_dir.glob("apply-*.json"))


def test_prepare_never_overwrites_an_existing_run(prepared_run):
    connection, _, run_dir, _ = prepared_run
    with pytest.raises(FileExistsError):
        backfill.prepare_run(connection, object(), run_dir, set())


def test_database_baseline_reads_all_keyset_pages_and_serializes_dates():
    class ReadCursor:
        def execute(self, statement, params):
            assert "ORDER BY monday_id LIMIT" in statement.as_string()
            assert params[0] == params[1]
            if params[0] is None:
                self.rows = [{"monday_id": "1", "amount_invoiced": Decimal("1.25"), "invoice_date": date(2026, 1, 1)}]
            elif params[0] == "1":
                self.rows = [{"monday_id": "2", "amount_invoiced": None, "invoice_date": None}]
            else:
                self.rows = []

        def fetchall(self):
            return self.rows

    class ReadConnection:
        @contextmanager
        def cursor(self, **kwargs):
            yield ReadCursor()

    baseline = backfill.read_baseline(ReadConnection())
    for rows in baseline.values():
        assert len(rows) == 2
        assert rows[0]["amount_invoiced"] == "1.25"
        assert rows[0]["invoice_date"] == "2026-01-01"


@pytest.mark.parametrize("counts", [[2, 3], [3, 3], [1, 1]])
def test_source_board_count_drift_and_incomplete_traversal_are_rejected(counts):
    monday = FakeMonday()
    counts_iter = iter(counts)
    monday.get_board_info = lambda board_id: {"items_count": next(counts_iter)}
    with pytest.raises(ValueError, match="board count") as error:
        fetch_board(monday, "board", [])
    assert str(error.value) == (
        "Monday board count changed or traversal was incomplete "
        f"(board_id=board, before_count={counts[0]}, "
        f"after_count={counts[1]}, len(items)=2); prepare a fresh run"
    )


def test_apply_cli_requires_confirmation_and_paused_writers():
    with pytest.raises(SystemExit):
        backfill.argument_parser().parse_args(["apply", "--run-dir", "somewhere"])