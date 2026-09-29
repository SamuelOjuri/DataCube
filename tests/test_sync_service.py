import sys
from pathlib import Path
import asyncio
import json
import pytest
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import HIDDEN_ITEMS_COLUMNS, get_hidden_items_extraction_columns
from src.core.data_processor import HierarchicalSegmentation, LabelNormalizer, EnhancedMirrorResolver
from src.database.sync_service import DataSyncService


def _make_sync_service() -> DataSyncService:
    service = DataSyncService.__new__(DataSyncService)
    service.segmentation = HierarchicalSegmentation()
    service.label_normalizer = LabelNormalizer()
    service.mirror_resolver = EnhancedMirrorResolver()
    service._hidden_lookup_by_id = {}
    service._hidden_lookup_by_name = {}
    service._hidden_lookup_by_normalized_name = {}
    service._hidden_lookup_by_prefix = {}
    service._product_alias_map = None
    service._category_alias_map = None
    return service


def test_normalize_category_collapses_multi_select_to_first_canonical_label():
    service = _make_sync_service()

    assert service._normalize_category("Education, Commercial") == "Education"
    assert service._normalize_category("Health, Apartments") == "Health"


def test_order_rollup_includes_charges_and_preserves_earliest_order_date():
    service = _make_sync_service()
    totals, dates = service._rollup_order_values_from_subitems([
        {"parent_monday_id": "p1", "cust_order_value_material": "10000.10",
         "cust_additional_charges": "500.20", "date_order_received": "2026-03-20"},
        {"parent_monday_id": "p1", "cust_order_value_material": "200.01",
         "cust_additional_charges": "-50.02", "date_order_received": "2026-03-01"},
        {"parent_monday_id": "charges_only", "cust_order_value_material": 0,
         "cust_additional_charges": "25.50"},
        {"parent_monday_id": "cleared", "cust_order_value_material": 0,
         "cust_additional_charges": 0},
    ])
    assert totals == {"p1": 10650.29, "charges_only": 25.50, "cleared": 0}
    assert dates == {"p1": "2026-03-01"}


@pytest.mark.parametrize("charges", [None, "invalid 50", "NaN", "Infinity", True])
def test_order_rollup_withholds_whole_project_when_one_order_is_incomplete(charges):
    service = _make_sync_service()
    totals, dates = service._rollup_order_values_from_subitems([
        {"parent_monday_id": "p1", "cust_order_value_material": 100,
         "cust_additional_charges": 5},
        {"parent_monday_id": "p1", "cust_order_value_material": 200,
         "cust_additional_charges": charges, "date_order_received": "2026-01-01"},
    ])
    assert totals == {}
    assert dates == {"p1": "2026-01-01"}


def test_order_rollup_does_not_treat_unfetched_charges_as_zero():
    totals, _ = _make_sync_service()._rollup_order_values_from_subitems([
        {"parent_monday_id": "p1", "cust_order_value_material": 100},
    ])
    assert totals == {}


def _hidden_order_item(material='"100"', charges='"5"'):
    return {
        "id": "h1", "name": "123_order",
        "column_values": [
            {"id": HIDDEN_ITEMS_COLUMNS["cust_order_value_material"], "type": "numbers", "value": material},
            {"id": HIDDEN_ITEMS_COLUMNS["cust_additional_charges"], "type": "numbers", "value": charges},
        ],
    }


@pytest.mark.parametrize("material, charges, expected", [
    ('"100.25"', '"5.75"', (100.25, 5.75)),
    (None, '"25"', (0, 25)),
    ('"100"', None, (100, 0)),
    (None, None, (0, 0)),
    ('"100"', '"-2.50"', (100, -2.5)),
    ('"100"', '"invalid 5"', (100, None)),
])
def test_hidden_order_amounts_reach_subitems_from_same_source(material, charges, expected):
    service = _make_sync_service()
    hidden = service._transform_for_hidden_table([_hidden_order_item(material, charges)])[0]
    subitem = service._transform_for_subitems_table([{
        "id": "s1", "name": "123_order", "parent_item": {"id": "p1"},
        "column_values": [{"id": "mirror17__1", "type": "mirror", "display_value": "999"}],
    }])[0]
    for row in (hidden, subitem):
        assert (row["cust_order_value_material"], row["cust_additional_charges"]) == expected


def test_missing_hidden_charge_column_stays_unknown():
    service = _make_sync_service()
    item = _hidden_order_item()
    item["column_values"].pop()
    hidden = service._transform_for_hidden_table([item])[0]
    assert hidden["cust_additional_charges"] is None
    assert HIDDEN_ITEMS_COLUMNS["cust_additional_charges"] in get_hidden_items_extraction_columns()
    assert service._has_transactional_hidden_values({"cust_additional_charges": 25})


@pytest.mark.parametrize("link", [
    {"value": '{"linkedPulseIds": [{"linkedPulseId": 123}]}'},
    {"value": '{"linkedItemIds": [123]}'},
    {"linked_item_ids": ["123"], "value": None},
])
def test_order_source_uses_explicit_hidden_id_instead_of_display_name(link):
    service = _make_sync_service()
    source = _hidden_order_item()
    source["id"] = "123"
    source["name"] = "different name"
    service._transform_for_hidden_table([source])
    row = service._transform_for_subitems_table([{
        "id": "s1", "name": "123_order", "parent_item": {"id": "p1"},
        "column_values": [{"id": "connect_boards8__1", "type": "board_relation", "text": "different name", **link}],
    }])[0]
    assert row["hidden_item_id"] == "123"
    assert row["cust_order_value_material"] == 100
    assert row["cust_additional_charges"] == 5


@pytest.mark.parametrize("linked_ids", [[999], [123, 456]])
def test_order_source_does_not_guess_when_explicit_link_is_unresolved(linked_ids):
    service = _make_sync_service()
    service._transform_for_hidden_table([_hidden_order_item()])
    row = service._transform_for_subitems_table([{
        "id": "s1", "name": "123_order", "parent_item": {"id": "p1"},
        "column_values": [{"id": "connect_boards8__1", "value": json.dumps({"linkedItemIds": linked_ids})}],
    }])[0]
    assert row["cust_order_value_material"] is None
    assert row["cust_additional_charges"] is None


def test_hidden_order_cache_replaces_previous_values_for_same_source():
    service = _make_sync_service()
    service._transform_for_hidden_table([_hidden_order_item()])
    service._transform_for_hidden_table([_hidden_order_item(charges=None)])
    assert len(service._hidden_lookup_by_name["123_order"]) == 1
    row = service._transform_for_subitems_table([{
        "id": "s1", "name": "123_order", "parent_item": {"id": "p1"},
    }])[0]
    assert row["cust_order_value_material"] == 100
    assert row["cust_additional_charges"] == 0


def test_charge_only_order_is_preferred_to_blank_revision_in_fallback_matching():
    service = _make_sync_service()
    blank = _hidden_order_item(material=None, charges=None)
    blank["id"] = "blank"
    order = _hidden_order_item(material=None, charges='"25"')
    service._transform_for_hidden_table([blank, order])
    row = service._transform_for_subitems_table([{
        "id": "s1", "name": "123_order", "parent_item": {"id": "p1"},
    }])[0]
    assert row["hidden_item_id"] == "h1"
    assert row["cust_order_value_material"] == 0
    assert row["cust_additional_charges"] == 25


def test_order_rollup_withholds_duplicate_sources_and_overflow():
    rows = [
        {"parent_monday_id": "duplicate", "hidden_item_id": "h1",
         "cust_order_value_material": 100, "cust_additional_charges": 5},
        {"parent_monday_id": "duplicate", "hidden_item_id": "h1",
         "cust_order_value_material": 100, "cust_additional_charges": 5},
        {"parent_monday_id": "overflow", "cust_order_value_material": "9999999999.99",
         "cust_additional_charges": "0.01"},
    ]
    assert _make_sync_service()._rollup_order_values_from_subitems(rows)[0] == {}


def test_subitem_without_hidden_source_does_not_publish_material_only_order():
    subitem = _make_sync_service()._transform_for_subitems_table([{
        "id": "s1", "name": "unmatched", "parent_item": {"id": "p1"},
        "column_values": [{"id": "mirror17__1", "type": "mirror", "display_value": "999"}],
    }])[0]
    assert subitem["cust_order_value_material"] is None
    assert subitem["cust_additional_charges"] is None


class _PagedSubitemTable:
    def __init__(self, rows, updates):
        self.rows = rows
        self.updates = updates
        self.after = ""
        self.payload = None

    def select(self, columns):
        assert "monday_id" in columns
        return self

    def in_(self, column, values):
        self.rows = [row for row in self.rows if row[column] in values]
        return self

    def order(self, column):
        self.rows = sorted(self.rows, key=lambda row: row[column])
        return self

    def limit(self, count):
        return self

    def gt(self, column, value):
        assert column == "monday_id"
        self.after = value
        return self

    def update(self, payload):
        self.payload = payload
        return self

    def eq(self, column, value):
        assert column == "monday_id"
        self.item_id = value
        return self

    def execute(self):
        if self.payload is not None:
            self.updates.append((self.item_id, self.payload))
            return SimpleNamespace(data=[self.payload])
        return SimpleNamespace(data=[row for row in self.rows if row["monday_id"] > self.after][:2])


def _set_paged_subitems(service, rows):
    updates = []
    service.supabase_client = SimpleNamespace(client=SimpleNamespace(
        table=lambda name: _PagedSubitemTable(rows, updates)
    ))
    return updates


def test_persisted_order_rollups_read_every_page_and_clear_empty_projects():
    service = _make_sync_service()
    rows = [
        {"monday_id": f"s{index}", "parent_monday_id": "p1",
         "cust_order_value_material": 100, "cust_additional_charges": 5}
        for index in range(5)
    ]
    _set_paged_subitems(service, rows)
    assert service._load_persisted_subitems_for_rollups(["p1"]) == rows
    rollups = service._compute_project_rollups_from_persisted_subitems(["p1", "empty"])
    assert rollups[0] == {"p1": 525, "empty": 0}


def test_hidden_order_refresh_propagates_cleared_charges_to_every_linked_subitem():
    service = _make_sync_service()
    rows = [
        {"monday_id": f"s{index}", "parent_monday_id": f"p{index}", "hidden_item_id": "h1"}
        for index in range(5)
    ]
    updates = _set_paged_subitems(service, rows)
    count, parents = asyncio.run(service._refresh_linked_subitem_rollup_fields_from_hidden_items([
        {"monday_id": "h1", "cust_order_value_material": 100, "cust_additional_charges": 0}
    ]))
    assert count == 5
    assert parents == [f"p{index}" for index in range(5)]
    assert all(payload["cust_additional_charges"] == 0 for _, payload in updates)
    assert all(payload["cust_order_value_material"] == 100 for _, payload in updates)


def test_normalize_category_maps_aliases_and_missing_numeric_label():
    service = _make_sync_service()

    assert service._normalize_category("Healthcare") == "Health"
    assert service._normalize_category("13") == "Datacentre"


def test_compute_product_key_returns_first_recognized_canonical_value():
    service = _make_sync_service()

    assert service._compute_product_key("Tissue Faced PIR, Foil Faced PIR") == "pir_tissue"
    assert service._compute_product_key("Torch On PIR (Prebonded), Torch On PIR") == "pir_prebonded"


def test_transform_for_projects_table_emits_canonical_category_and_product_key():
    service = _make_sync_service()

    transformed = service._transform_for_projects_table(
        [
            {
                "monday_id": "123",
                "name": "12345",
                "project_name": "Example",
                "type": "Refurbishment",
                "category": "Education, Commercial",
                "product_type": "Torch On PIR (Prebonded), Torch On PIR",
                "new_enquiry_value": 12000,
            }
        ]
    )

    assert len(transformed) == 1
    assert transformed[0]["category"] == "Education"
    assert transformed[0]["product_key"] == "pir_prebonded"


def test_rollup_invoice_date_ranges_handles_single_multiple_and_null_dates():
    service = _make_sync_service()

    ranges = service._rollup_invoice_date_ranges_from_subitems(
        [
            {"parent_monday_id": "p1", "invoice_date": "2026-01-15"},
            {"parent_monday_id": "p1", "invoice_date": None},
            {"parent_monday_id": "p2", "invoice_date": "2026-03-10"},
            {"parent_monday_id": "p2", "invoice_date": "2026-02-20"},
            {"parent_monday_id": "p3", "invoice_date": ""},
        ]
    )

    assert ranges["p1"] == {
        "first_date_invoiced": "2026-01-15",
        "last_date_invoiced": "2026-01-15",
        "invoice_date_count": 1,
        "invoicing_spread_days": 0,
    }
    assert ranges["p2"] == {
        "first_date_invoiced": "2026-02-20",
        "last_date_invoiced": "2026-03-10",
        "invoice_date_count": 2,
        "invoicing_spread_days": 18,
    }
    assert "p3" not in ranges


def test_apply_project_invoice_date_range_rollup_sets_first_last_and_spread():
    service = _make_sync_service()
    projects = [{"monday_id": "p1", "first_date_invoiced": "2026-02-01"}]

    service._apply_project_invoice_date_range_rollup(
        projects,
        {
            "p1": {
                "first_date_invoiced": "2026-01-15",
                "last_date_invoiced": "2026-02-20",
                "invoicing_spread_days": 36,
            }
        },
    )

    assert projects[0]["first_date_invoiced"] == "2026-01-15"
    assert projects[0]["last_date_invoiced"] == "2026-02-20"
    assert "invoicing_spread_days" not in projects[0]


class _FakeProjectUpdateTable:
    def __init__(self, updates):
        self.updates = updates
        self.payload = None
        self.project_id = None

    def update(self, payload):
        self.payload = payload
        return self

    def eq(self, column, value):
        assert column == "monday_id"
        self.project_id = value
        return self

    def execute(self):
        self.updates.append((self.project_id, self.payload))
        return None


class _FakeSupabasePostgrestClient:
    def __init__(self):
        self.updates = []

    def table(self, table_name):
        assert table_name == "projects"
        return _FakeProjectUpdateTable(self.updates)


class _FakeSupabaseClient:
    def __init__(self):
        self.client = _FakeSupabasePostgrestClient()


def test_batch_update_invoice_date_range_rollups_clears_missing_ranges():
    service = _make_sync_service()
    service.supabase_client = _FakeSupabaseClient()

    updated = asyncio.run(
        service._batch_update_invoice_date_range_rollups(
            ["p1", "p2"],
            {"p1": "2026-01-15"},
            {"p1": "2026-02-20"},
        )
    )

    assert updated == 2
    assert service.supabase_client.client.updates == [
        (
            "p1",
            {
                "first_date_invoiced": "2026-01-15",
                "last_date_invoiced": "2026-02-20",
            },
        ),
        (
            "p2",
            {
                "first_date_invoiced": None,
                "last_date_invoiced": None,
            },
        ),
    ]