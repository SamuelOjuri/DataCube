import sys
from pathlib import Path

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