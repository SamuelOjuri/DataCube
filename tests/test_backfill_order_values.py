from copy import deepcopy
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import pytest

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


@pytest.mark.parametrize("counts", [[2, 3], [3, 3]])
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