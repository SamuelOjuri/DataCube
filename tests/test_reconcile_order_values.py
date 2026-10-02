from copy import deepcopy
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
import re

import pytest

from scripts import backfill_order_values as backfill
from scripts import reconcile_order_values as reconcile
from scripts.reconcile_order_values import build_report


def inputs():
    baseline = {"projects": [{"monday_id": "p1"}], "hidden_items": [{"monday_id": "h1"}],
                "subitems": [{"monday_id": "s1", "parent_monday_id": "p1", "hidden_item_id": "wrong"}]}
    source = {"project_ids": ["p1"],
              "subitems": [{"monday_id": "s1", "parent_monday_id": "p1", "hidden_ids": ["h1"],
                            "state": "active", "board_id": backfill.SUBITEM_BOARD_ID,
                            "parent_state": "active", "parent_board_id": backfill.PARENT_BOARD_ID}],
              "hidden_items": [{"monday_id": "h1", "issues": [], **dict.fromkeys(backfill.ORDER_FIELDS, "0.00")}]}
    plan = {"projects": [{"project_id": "p1", "status": "blocked", "issues": ["stored_hidden_link_mismatch"]}],
            "diagnostics": []}
    return baseline, source, plan


def test_explicit_link_mismatch_is_rehydration_candidate_without_mutation():
    baseline, source, plan = inputs()
    original = deepcopy((baseline, source, plan))
    report = build_report(baseline, source, plan)
    assert report["projects"][0]["action"] == "rehydrate_candidate"
    assert report["subitems"][0]["stored_hidden_id"] == "wrong"
    assert report["subitems"][0]["live_hidden_ids"] == ["h1"]
    assert (baseline, source, plan) == original


@pytest.mark.parametrize("change", ["shared_source", "missing_live_child", "parent_mismatch", "unknown_amount",
                                    "inactive", "missing_metadata", "multiple_links", "missing_parent"])
def test_ambiguous_projects_are_manual_review(change):
    baseline, source, plan = inputs()
    child = source["subitems"][0]
    if change == "shared_source":
        source["subitems"].append({**child, "monday_id": "s2", "parent_monday_id": "p2"})
    elif change == "missing_live_child":
        baseline["subitems"].append({"monday_id": "stale", "parent_monday_id": "p1"})
    elif change == "parent_mismatch":
        baseline["subitems"][0]["parent_monday_id"] = "other"
    elif change == "unknown_amount":
        source["hidden_items"][0][backfill.ORDER_FIELDS[0]] = None
    elif change == "inactive":
        child["state"] = "archived"
    elif change == "missing_metadata":
        del child["parent_state"]
    elif change == "multiple_links":
        child["hidden_ids"].append("h2")
    else:
        source["project_ids"] = []
    assert build_report(baseline, source, plan)["projects"][0]["action"] == "manual_review"


def raw_rows():
    hidden = {"id": "301", "name": "Different name from child", "board": {"id": backfill.HIDDEN_ITEMS_BOARD_ID},
              "column_values": [{"id": column_id, "value": None, "text": ""}
                                for column_id in reconcile.get_hidden_items_extraction_columns() if column_id != "name"]}
    child = {"id": "201", "name": "Child", "board": {"id": backfill.SUBITEM_BOARD_ID}, "parent_item": {"id": "101"},
             "column_values": [{"id": column_id, "value": None, "text": ""}
                               for column_id in reconcile.get_subitems_extraction_columns() if column_id != "name"]}
    for column in child["column_values"]:
        if column["id"] == backfill.SUBITEM_COLUMNS["hidden_item_id"]:
            column["linked_item_ids"] = ["301"]
    return [hidden], [child]


def test_exact_transform_uses_ids_and_updates_link_dependent_totals():
    hidden, children = raw_rows()
    column = next(row for row in hidden[0]["column_values"]
                  if row["id"] == backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[0]])
    column["value"] = '"100.00"'
    result = reconcile.transform_exact_rows(hidden, children, {"101"})
    assert result["subitems"][0]["hidden_item_id"] == "301"
    assert result["projects"][0]["total_order_value"] == "100.00"
    assert result["subitems"][0]["cust_order_value_material"] == 100.0
    assert all("last_synced_at" not in row for rows in result.values() for row in rows)


def test_transform_refuses_name_matching_when_explicit_link_is_missing():
    hidden, children = raw_rows()
    hidden[0]["name"] = children[0]["name"]
    for column in children[0]["column_values"]:
        if column["id"] == backfill.SUBITEM_COLUMNS["hidden_item_id"]:
            column["linked_item_ids"] = []
    with pytest.raises(ValueError, match="Exact parent and hidden links"):
        reconcile.transform_exact_rows(hidden, children, {"101"})


def test_duplicate_source_names_preserve_distinct_links_and_amounts():
    hidden, children = raw_rows()
    hidden.append({**deepcopy(hidden[0]), "id": "302"})
    children.append({**deepcopy(children[0]), "id": "202"})
    for item in hidden + children:
        item["name"] = "16360_24.01 - B"
    for item, amount in zip(hidden, ('"100"', '"200"')):
        next(column for column in item["column_values"]
             if column["id"] == backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[0]])["value"] = amount
    next(column for column in children[1]["column_values"]
         if column["id"] == backfill.SUBITEM_COLUMNS["hidden_item_id"])["linked_item_ids"] = ["302"]
    result = reconcile.transform_exact_rows(hidden, children, {"101"})
    assert [row["hidden_item_id"] for row in result["subitems"]] == ["301", "302"]
    assert [row["cust_order_value_material"] for row in result["subitems"]] == [100.0, 200.0]
    assert result["projects"][0]["total_order_value"] == "300.00"


def test_transform_leaves_generated_invoice_spread_to_database():
    hidden, children = raw_rows()
    result = reconcile.transform_exact_rows(hidden, children, {"101"})
    assert "invoicing_spread_days" not in result["projects"][0]


def test_report_writes_separate_audits_without_modifying_backfill(tmp_path, monkeypatch):
    baseline, source, plan = inputs()
    monkeypatch.setattr(backfill, "load_run", lambda run_dir: ({"run_id": "original"}, baseline, source, plan))
    result = reconcile.report_run(tmp_path / "original", tmp_path / "report")
    assert result["candidate_projects"] == 1
    assert result["manual_projects"] == 0
    assert result["ready_repair_groups"] == 1
    assert result["ready_candidate_projects"] == 1
    assert (tmp_path / "report" / "subitem-links.csv").exists()
    assert (tmp_path / "report" / "repair-groups.csv").exists()
    with pytest.raises(FileExistsError):
        reconcile.report_run(tmp_path / "original", tmp_path / "report")


class RepairDatabase:
    def __init__(self, state, contract):
        self.state = deepcopy(state)
        self.contract = contract
        self.info = SimpleNamespace(host="local", port=5432, dbname="test", user="test")
        self.fail_table = None
        self.statements = []
        self.rollbacks = 0

    @contextmanager
    def transaction(self):
        previous = deepcopy(self.state)
        try:
            yield
        except Exception:
            self.state = previous
            self.rollbacks += 1
            raise

    def execute(self, statement):
        self.statements.append(statement)

    @contextmanager
    def cursor(self, **kwargs):
        database = self

        class Cursor:
            def execute(self, statement, values):
                text = statement if isinstance(statement, str) else statement.as_string()
                database.statements.append(text)
                if "information_schema.columns" in text:
                    self.rows = [{"table_name": table, "column_name": field,
                                  "data_type": column["type"], "numeric_scale": column["scale"],
                                  "is_generated": column.get("generated", "NEVER")}
                                 for table, columns in database.contract.items() for field, column in columns.items()]
                    return
                identifiers = re.findall(r'"([^"]+)"', text)
                table = identifiers[0]
                if text.startswith("SELECT"):
                    self.rows = sorted([deepcopy(row) for row in database.state[table] if row["monday_id"] in values[0]],
                                       key=lambda row: row["monday_id"])
                    return
                if text.startswith("INSERT"):
                    fields = identifiers[1:1 + len(values)]
                    update = dict(zip(fields, values))
                    update = {field: value.obj if isinstance(value, reconcile.Jsonb) else value for field, value in update.items()}
                    row = next((row for row in database.state[table] if row["monday_id"] == update["monday_id"]), None)
                    if row is None:
                        row = dict.fromkeys(database.contract[table])
                        database.state[table].append(row)
                else:
                    assert text.startswith("UPDATE")
                    update = dict(zip(identifiers[1:], values[:-1]))
                    row = next(row for row in database.state[table] if row["monday_id"] == values[-1])
                if any(database.contract[table][field].get("generated") == "ALWAYS" for field in update):
                    raise RuntimeError("cannot write generated column")
                row.update(update)
                if table == "projects":
                    first_date, last_date = row.get("first_date_invoiced"), row.get("last_date_invoiced")
                    row["invoicing_spread_days"] = (
                        max((date.fromisoformat(last_date) - date.fromisoformat(first_date)).days, 0)
                        if first_date and last_date else None)
                self.rowcount = 1
                if database.fail_table == table:
                    raise RuntimeError("simulated write failure")

            def fetchall(self):
                return self.rows

        yield Cursor()


@pytest.fixture
def staged_repair(tmp_path, monkeypatch, request):
    hidden, children = raw_rows()
    for column in hidden[0]["column_values"]:
        if column["id"] == backfill.HIDDEN_ITEMS_COLUMNS[backfill.ORDER_FIELDS[0]]:
            column["value"] = '"100"'
    for item in hidden + children:
        item["state"] = "active"
    children[0]["parent_item"].update({"state": "active", "board": {"id": backfill.PARENT_BOARD_ID}})
    hidden[0]["column_values"] = [column for column in hidden[0]["column_values"] if column["id"] != backfill.TOTAL_COLUMN]
    hidden[0]["column_values"].append({"id": backfill.TOTAL_COLUMN, "value": None, "display_value": "100"})
    transformed = reconcile.transform_exact_rows(hidden, children, {"101"})
    contract = {table: {field: {"type": "numeric" if isinstance(value, float) or field in (
        "total_order_value", "total_amount_invoiced", "new_enquiry_value") else "text", "scale": 2}
        for field, value in rows[0].items()} for table, rows in transformed.items()}
    for table, fields in backfill.BASELINE_COLUMNS.items():
        for field in fields:
            contract[table].setdefault(field, {"type": "text", "scale": None})
    contract["projects"]["invoicing_spread_days"] = {"type": "integer", "scale": 0, "generated": "ALWAYS"}
    state = reconcile.normalize_updates(transformed, contract)
    for table, rows in state.items():
        for row in rows:
            for field in contract[table]:
                row.setdefault(field, None)
    state["subitems"][0]["hidden_item_id"] = "999"
    state["subitems"][0]["cust_order_value_material"] = "50.00"
    state["hidden_items"][0]["cust_order_value_material"] = "0.00"
    state["hidden_items"].append({**state["hidden_items"][0], "monday_id": "999"})
    state["projects"][0]["total_order_value"] = "50.00"
    state["projects"][0].update({"first_date_invoiced": "2024-01-01", "last_date_invoiced": "2024-01-11",
                                 "invoicing_spread_days": 10})
    if getattr(request, "param", None) == "missing_subitem":
        state["subitems"] = []
    elif getattr(request, "param", None) == "missing_hidden":
        state["hidden_items"] = [state["hidden_items"][1]]
    database = RepairDatabase(state, contract)

    def baseline_reader(connection):
        return {table: [{field: row.get(field) for field in fields}
                        for row in sorted(connection.state[table], key=lambda row: row["monday_id"])]
                for table, fields in backfill.BASELINE_COLUMNS.items()}

    monkeypatch.setattr(backfill, "read_baseline", baseline_reader)
    baseline = baseline_reader(database)
    source = {"project_ids": ["101"], "subitems": [{**backfill.normalize_subitem(children[0]),
              "state": "active", "board_id": backfill.SUBITEM_BOARD_ID,
              "parent_state": "active", "parent_board_id": backfill.PARENT_BOARD_ID}],
              "hidden_items": [backfill.normalize_hidden(hidden[0])]}
    plan = backfill.build_plan(baseline, source)
    original = {"run_id": "original", "target": backfill.target_fingerprint(database),
                "approve_reviewed_parentless_duplicates": True}
    saved_source = deepcopy(source)
    monkeypatch.setattr(backfill, "load_run", lambda run_dir: (deepcopy(original), deepcopy(baseline), deepcopy(saved_source), deepcopy(plan)))
    monkeypatch.setattr(backfill, "capture_source_with_reviewed_duplicates", lambda monday: deepcopy(source))
    parent = {"id": "101", "state": "active", "board": {"id": backfill.PARENT_BOARD_ID},
              "parent_item": None, "subitems": deepcopy(children)}
    raw = {item["id"]: item for item in [parent, *children, *hidden]}

    def query_items(query, variables):
        assert "mutation" not in query
        rows = [deepcopy(raw[item_id]) for item_id in variables["ids"]]
        if "columns" in variables:
            for row in rows:
                row["column_values"] = [column for column in row["column_values"] if column["id"] in variables["columns"]]
        return {"data": {"items": rows}}

    monday = SimpleNamespace(execute_query=query_items)
    repair_dir = tmp_path / "repair"
    manifest = reconcile.prepare_repair(database, monday, tmp_path / "original", repair_dir, {"101"})
    return database, monday, repair_dir, manifest, raw, source


def test_repair_prepare_apply_verify_repeat_and_exact_scope(staged_repair):
    database, monday, repair_dir, manifest, _, _ = staged_repair
    original_hidden = deepcopy(database.state["hidden_items"][1])
    assert database.state["subitems"][0]["hidden_item_id"] == "999"
    assert not any(statement.startswith(("INSERT", "UPDATE")) for statement in database.statements)
    assert reconcile.apply_repair(database, monday, repair_dir, manifest["repair_id"], True)["status"] == "applied"
    assert database.state["subitems"][0]["hidden_item_id"] == "301"
    assert database.state["projects"][0]["total_order_value"] == "100.00"
    assert database.state["projects"][0]["invoicing_spread_days"] is None
    assert database.state["hidden_items"][1] == original_hidden
    assert reconcile.verify_repair(database, repair_dir)["matches_reviewed_result"]
    assert reconcile.apply_repair(database, monday, repair_dir, manifest["repair_id"], True)["status"] == "already_applied"


@pytest.mark.parametrize("first_date,last_date,spread", [
    ("2024-01-01", "2024-01-11", 10), ("2024-01-11", "2024-01-01", 0),
    ("2024-01-01", "2024-01-01", 0), (None, "2024-01-11", None),
    ("2024-01-01", None, None),
])
def test_generated_invoice_spread_is_verified_after_date_changes(first_date, last_date, spread):
    before = {"projects": [{"monday_id": "101", "invoicing_spread_days": 99}]}
    update = {"monday_id": "101", "first_date_invoiced": first_date, "last_date_invoiced": last_date}
    current = {"projects": [{**update, "invoicing_spread_days": spread}]}
    assert reconcile.check_rows(before, current, {"projects": [update]})
    current["projects"][0]["invoicing_spread_days"] = 777
    assert not reconcile.check_rows(before, current, {"projects": [update]})


def test_generated_column_cannot_be_staged():
    contract = {"projects": {"invoicing_spread_days": {"type": "integer", "scale": 0, "generated": "ALWAYS"}}}
    with pytest.raises(ValueError, match="Cannot stage generated"):
        reconcile.normalize_updates({"projects": [{"invoicing_spread_days": None}]}, contract)


@pytest.mark.parametrize("table", ["hidden_items", "subitems", "projects"])
def test_repair_failure_rolls_back_every_table(staged_repair, table):
    database, monday, repair_dir, manifest, _, _ = staged_repair
    before = deepcopy(database.state)
    database.fail_table = table
    with pytest.raises(RuntimeError, match="simulated write failure"):
        reconcile.apply_repair(database, monday, repair_dir, manifest["repair_id"], True)
    assert database.state == before
    assert database.rollbacks == 1
    assert not list(repair_dir.glob("apply-*.json"))


@pytest.mark.parametrize("change", ["source", "database", "targeted_value", "schema", "review", "code", "confirmation", "writers"])
def test_repair_refuses_drift_and_missing_authorization(staged_repair, monkeypatch, change):
    database, monday, repair_dir, manifest, raw, source = staged_repair
    confirmation, paused = manifest["repair_id"], True
    if change == "source":
        source["project_ids"].append("902")
    elif change == "database":
        database.state["projects"][0]["total_order_value"] = "1.00"
    elif change == "targeted_value":
        raw["201"]["name"] = "changed"
    elif change == "schema":
        database.contract["subitems"]["new_column"] = {"type": "text", "scale": None}
    elif change == "review":
        (repair_dir / "changes.csv").write_text("altered")
    elif change == "code":
        monkeypatch.setattr(reconcile, "repair_code", lambda: "changed")
    elif change == "confirmation":
        confirmation = "wrong"
    else:
        paused = False
    before = deepcopy(database.state)
    with pytest.raises(ValueError):
        reconcile.apply_repair(database, monday, repair_dir, confirmation, paused)
    assert database.state == before
    assert not list(repair_dir.glob("apply-*.json"))


def test_selection_refuses_partial_resolution_of_shared_stored_source():
    baseline, source, plan = inputs()
    baseline["subitems"].append({"monday_id": "s2", "parent_monday_id": "p2", "hidden_item_id": "h1"})
    with pytest.raises(ValueError, match="shared stored source"):
        reconcile.select_scope(baseline, source, plan, {"p1"})
    report = build_report(baseline, source, plan)
    group = report["repair_groups"][0]
    assert group["action"] == "manual_review"
    assert group["blocking_project_ids"] == ["p2"]
    assert group["blocking_subitem_ids"] == ["s2"]
    assert report["projects"][0]["repair_readiness"] == "manual_review"


def test_repair_groups_include_all_dependent_candidate_owners():
    baseline, source, plan = inputs()
    for position in range(2, 5):
        parent_id, child_id, hidden_id = f"p{position}", f"s{position}", f"h{position}"
        baseline["projects"].append({"monday_id": parent_id})
        baseline["subitems"].append({"monday_id": child_id, "parent_monday_id": parent_id,
                                     "hidden_item_id": f"h{position - 1}" if position < 4 else "wrong"})
        baseline["hidden_items"].append({"monday_id": hidden_id})
        source["project_ids"].append(parent_id)
        source["subitems"].append({**source["subitems"][0], "monday_id": child_id,
                                   "parent_monday_id": parent_id, "hidden_ids": [hidden_id]})
        source["hidden_items"].append({**source["hidden_items"][0], "monday_id": hidden_id})
        plan["projects"].append({**plan["projects"][0], "project_id": parent_id})
    original = deepcopy((baseline, source, plan))
    groups = build_report(baseline, source, plan)["repair_groups"]
    assert [group["project_ids"] for group in groups] == [["p1", "p2", "p3"], ["p4"]]
    for group in groups:
        assert group["action"] == "prepare_candidate"
        assert reconcile.select_scope(baseline, source, plan, set(group["project_ids"]))["projects"] == group["project_ids"]
    assert (baseline, source, plan) == original


def test_oversized_dependency_groups_are_not_split_automatically():
    candidates = [{"project_id": str(position), "action": "rehydrate_candidate"} for position in range(26)]
    source = {"subitems": [{"monday_id": f"s{position}", "parent_monday_id": str(position),
                            "hidden_ids": [f"h{position}"]} for position in range(26)]}
    baseline = {"subitems": [{"monday_id": f"s{position}", "parent_monday_id": str(position),
                              "hidden_item_id": f"h{(position + 1) % 26}"} for position in range(26)]}
    groups = reconcile.build_repair_groups(baseline, source, candidates)
    assert len(groups) == 1
    assert groups[0]["action"] == "manual_review"
    assert groups[0]["blockers"] == ["dependency_group_exceeds_limit"]


def test_prepare_rejects_unknown_group_before_connecting(tmp_path, monkeypatch):
    baseline, source, plan = inputs()
    monkeypatch.setattr(backfill, "load_dotenv", lambda: None)
    monkeypatch.setattr(backfill, "load_run", lambda run_dir: ({}, baseline, source, plan))
    monkeypatch.setattr(reconcile.psycopg, "connect", lambda *args, **kwargs: pytest.fail("Must reject group before connecting"))
    assert reconcile.main(["prepare", "--run-dir", str(tmp_path / "run"), "--repair-dir", str(tmp_path / "repair"),
                           "--repair-group", "not-a-group"]) == 1


def test_prepare_cli_selects_multiple_reviewed_groups(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "load_dotenv", lambda: None)
    monkeypatch.setattr(backfill, "load_run", lambda run_dir: ({}, {}, {}, {}))
    monkeypatch.setattr(reconcile, "build_report", lambda *args: {"repair_groups": [
        {"group_id": "repair-001", "action": "prepare_candidate", "project_ids": ["101", "102"]},
        {"group_id": "repair-002", "action": "prepare_candidate", "project_ids": ["103"]}]})
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://local/test")
    database, monday = object(), object()

    @contextmanager
    def connect(*args, **kwargs):
        yield database

    def prepare(connection, client, run_dir, repair_dir, selected):
        assert connection is database and client is monday
        assert selected == {"101", "102", "103"}
        return {"status": "prepared"}

    monkeypatch.setattr(reconcile.psycopg, "connect", connect)
    monkeypatch.setattr(backfill, "MondayClient", lambda: monday)
    monkeypatch.setattr(reconcile, "prepare_repair", prepare)
    assert reconcile.main(["prepare", "--run-dir", str(tmp_path / "run"), "--repair-dir", str(tmp_path / "repair"),
                           "--repair-group", "repair-001", "--repair-group", "repair-002"]) == 0


@pytest.mark.parametrize("selection", ["blocked", "duplicate", "too_many"])
def test_prepare_cli_rejects_unsafe_group_selection_before_connecting(tmp_path, monkeypatch, selection):
    monkeypatch.setattr(backfill, "load_dotenv", lambda: None)
    monkeypatch.setattr(backfill, "load_run", lambda run_dir: ({}, {}, {}, {}))
    monkeypatch.setattr(reconcile, "build_report", lambda *args: {"repair_groups": [
        {"group_id": "repair-001", "action": "manual_review" if selection == "blocked" else "prepare_candidate",
         "project_ids": [str(parent_id) for parent_id in range(26)] if selection == "too_many" else ["101"]}]})
    monkeypatch.setattr(reconcile.psycopg, "connect", lambda *args, **kwargs: pytest.fail("Must reject selection before connecting"))
    args = ["prepare", "--run-dir", str(tmp_path / "run"), "--repair-dir", str(tmp_path / "repair"),
            "--repair-group", "repair-001"]
    if selection == "duplicate":
        args.extend(["--repair-group", "repair-001"])
    assert reconcile.main(args) == 1


def test_targeted_fetch_rejects_missing_columns_and_inactive_items():
    for item in ({"id": "201", "state": "archived", "board": {"id": backfill.SUBITEM_BOARD_ID}},
                 {"id": "201", "state": "active", "board": {"id": backfill.SUBITEM_BOARD_ID}, "column_values": []}):
        monday = SimpleNamespace(execute_query=lambda query, variables: {"data": {"items": [item]}})
        with pytest.raises(ValueError):
            reconcile.fetch_columns(monday, ["201"], ["required"], backfill.SUBITEM_BOARD_ID)


def test_targeted_fetch_reads_name_as_item_metadata():
    def query_items(query, variables):
        assert "mutation" not in query
        assert variables["columns"] == ["required"]
        return {"data": {"items": [{"id": "201", "name": "Repeated label", "state": "active",
                "board": {"id": backfill.SUBITEM_BOARD_ID},
                "column_values": [{"id": "required", "value": None}]}]}}

    result = reconcile.fetch_columns(SimpleNamespace(execute_query=query_items), ["201"],
                                     ["name", "required"], backfill.SUBITEM_BOARD_ID)
    assert result[0]["name"] == "Repeated label"


@pytest.mark.parametrize("staged_repair", ["missing_subitem", "missing_hidden"], indirect=True)
def test_repair_inserts_only_confirmed_live_missing_rows(staged_repair):
    database, monday, repair_dir, manifest, _, _ = staged_repair
    reconcile.apply_repair(database, monday, repair_dir, manifest["repair_id"], True)
    assert {row["monday_id"] for row in database.state["subitems"]} == {"201"}
    assert {row["monday_id"] for row in database.state["hidden_items"]} == {"301", "999"}
    assert reconcile.verify_repair(database, repair_dir)["matches_reviewed_result"]


def test_failed_post_write_verification_rolls_back(staged_repair, monkeypatch):
    database, monday, repair_dir, manifest, _, _ = staged_repair
    before = deepcopy(database.state)
    read_scope = reconcile.read_scope
    reads = 0

    def corrupted_verification(connection, scope):
        nonlocal reads
        reads += 1
        result = read_scope(connection, scope)
        if reads == 2:
            result["subitems"][0]["item_name"] = "unexpected post-write value"
        return result

    monkeypatch.setattr(reconcile, "read_scope", corrupted_verification)
    with pytest.raises(ValueError, match="reconciliation failed"):
        reconcile.apply_repair(database, monday, repair_dir, manifest["repair_id"], True)
    assert database.state == before
    assert database.rollbacks == 1
    assert not list(repair_dir.glob("apply-*.json"))


def test_report_cli_never_constructs_live_clients(tmp_path, monkeypatch):
    baseline, source, plan = inputs()
    monkeypatch.setattr(backfill, "load_dotenv", lambda: None)
    monkeypatch.setattr(backfill, "load_run", lambda run_dir: ({"run_id": "test"}, baseline, source, plan))
    monkeypatch.setattr(backfill, "MondayClient", lambda: pytest.fail("Offline report must not construct a Monday client"))
    monkeypatch.setattr(reconcile.psycopg, "connect", lambda *args, **kwargs: pytest.fail("Offline report must not connect"))
    assert reconcile.main(["report", "--run-dir", str(tmp_path / "original"),
                           "--output-dir", str(tmp_path / "report")]) == 0


def test_numeric_normalization_uses_database_scale_and_refuses_nonfinite():
    contract = {"projects": {"total_order_value": {"type": "numeric", "scale": 2}}}
    assert reconcile.normalize_updates({"projects": [{"total_order_value": "1.005"}]}, contract) == {
        "projects": [{"total_order_value": "1.01"}]}
    with pytest.raises(ValueError, match="Non-finite"):
        reconcile.normalize_updates({"projects": [{"total_order_value": "NaN"}]}, contract)