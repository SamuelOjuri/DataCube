import sys
from pathlib import Path
import asyncio

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.data_processor import HierarchicalSegmentation
from src.database.sync_service import DataSyncService


def _make_sync_service() -> DataSyncService:
    service = DataSyncService.__new__(DataSyncService)
    service.segmentation = HierarchicalSegmentation()
    service._hidden_lookup_by_name = {}
    service._product_alias_map = None
    service._category_alias_map = None
    return service


def test_normalize_category_collapses_multi_select_to_first_canonical_label():
    service = _make_sync_service()

    assert service._normalize_category("Education, Commercial") == "Education"
    assert service._normalize_category("Health, Apartments") == "Health"


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