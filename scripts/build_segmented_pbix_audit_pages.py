"""Add allocation and model-audit pages to a modern-format PBIX copy."""

from __future__ import annotations

import argparse
import json
import secrets
import zipfile
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented-Live.pbix"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented-Audit.pbix"
)

FORECAST_TABLE = "vw_weighted_enquiry_value_monthly_oct2027"
ALLOCATION_TABLE = "ProjectLeafAllocation"
MODEL_SUMMARY_TABLE = "SegmentModelSummary"
AUDIT_PAGE_NAMES = (
    "Segment Mapping Overview",
    "Project Allocation Detail",
    "Model & Forecast Audit",
)
PAGE_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
    "page/2.1.0/schema.json"
)
VISUAL_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
    "visualContainer/2.11.0/schema.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add three audit pages to a modern-format segmented PBIX copy."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _id() -> str:
    return secrets.token_hex(10)


def _literal(value: str) -> dict[str, dict[str, str]]:
    escaped = value.replace("'", "''")
    return {"Literal": {"Value": f"'{escaped}'"}}


def _column(table: str, property_name: str) -> dict[str, Any]:
    return {
        "Column": {
            "Expression": {"SourceRef": {"Entity": table}},
            "Property": property_name,
        }
    }


def _measure(table: str, property_name: str) -> dict[str, Any]:
    return {
        "Measure": {
            "Expression": {"SourceRef": {"Entity": table}},
            "Property": property_name,
        }
    }


def _projection(
    table: str,
    property_name: str,
    display_name: str,
    *,
    measure: bool = False,
    active: bool = False,
) -> dict[str, Any]:
    projection: dict[str, Any] = {
        "field": (
            _measure(table, property_name)
            if measure
            else _column(table, property_name)
        ),
        "queryRef": f"{table}.{property_name}",
        "nativeQueryRef": display_name,
        "displayName": display_name,
    }
    if active:
        projection["active"] = True
    return projection


def _position(
    x: float,
    y: float,
    width: float,
    height: float,
    z: int,
) -> dict[str, float | int]:
    return {
        "x": x,
        "y": y,
        "z": z,
        "height": height,
        "width": width,
        "tabOrder": z,
    }


def _visual_title(text: str) -> dict[str, Any]:
    return {
        "title": [
            {
                "properties": {
                    "text": {"expr": _literal(text)},
                }
            }
        ]
    }


def _visual(
    visual_type: str,
    position: dict[str, float | int],
    *,
    query_state: dict[str, Any] | None = None,
    title: str | None = None,
    objects: dict[str, Any] | None = None,
) -> dict[str, Any]:
    visual_name = _id()
    definition: dict[str, Any] = {
        "$schema": VISUAL_SCHEMA,
        "name": visual_name,
        "position": position,
        "visual": {
            "visualType": visual_type,
            "drillFilterOtherVisuals": True,
        },
    }
    visual = definition["visual"]
    if query_state is not None:
        visual["query"] = {"queryState": query_state}
    if title is not None:
        visual["visualContainerObjects"] = _visual_title(title)
    if objects is not None:
        visual["objects"] = objects
    return definition


def _textbox(text: str, position: dict[str, float | int]) -> dict[str, Any]:
    visual = _visual("textbox", position)
    visual["visual"]["objects"] = {
        "general": [
            {
                "properties": {
                    "paragraphs": [
                        {
                            "textRuns": [
                                {
                                    "value": text,
                                    "textStyle": {
                                        "fontWeight": "bold",
                                        "fontSize": "18pt",
                                    },
                                }
                            ],
                            "horizontalTextAlignment": "left",
                        }
                    ]
                }
            }
        ]
    }
    return visual


def _slicer(
    table: str,
    property_name: str,
    label: str,
    position: dict[str, float | int],
) -> dict[str, Any]:
    return _visual(
        "slicer",
        position,
        query_state={
            "Values": {
                "projections": [
                    _projection(
                        table,
                        property_name,
                        label,
                        active=True,
                    )
                ]
            }
        },
        objects={
            "data": [
                {"properties": {"mode": {"expr": _literal("Dropdown")}}}
            ],
            "header": [
                {"properties": {"text": {"expr": _literal(label)}}}
            ],
        },
    )


def _card(
    table: str,
    measure_name: str,
    label: str,
    position: dict[str, float | int],
) -> dict[str, Any]:
    return _visual(
        "cardVisual",
        position,
        query_state={
            "Data": {
                "projections": [
                    _projection(
                        table,
                        measure_name,
                        label,
                        measure=True,
                    )
                ]
            }
        },
    )


def _table(
    fields: Iterable[tuple[str, str, str, bool]],
    position: dict[str, float | int],
    title: str,
) -> dict[str, Any]:
    projections = [
        _projection(table, property_name, label, measure=is_measure)
        for table, property_name, label, is_measure in fields
    ]
    return _visual(
        "tableEx",
        position,
        query_state={"Values": {"projections": projections}},
        title=title,
        objects={"columnHeaders": [{"properties": {}}]},
    )


def _bar_chart(
    category: tuple[str, str, str],
    value: tuple[str, str, str],
    position: dict[str, float | int],
    title: str,
) -> dict[str, Any]:
    category_table, category_property, category_label = category
    value_table, value_property, value_label = value
    return _visual(
        "clusteredBarChart",
        position,
        query_state={
            "Category": {
                "projections": [
                    _projection(
                        category_table,
                        category_property,
                        category_label,
                        active=True,
                    )
                ]
            },
            "Y": {
                "projections": [
                    _projection(
                        value_table,
                        value_property,
                        value_label,
                        measure=True,
                    )
                ]
            },
        },
        title=title,
    )


def _line_chart(position: dict[str, float | int]) -> dict[str, Any]:
    return _visual(
        "lineChart",
        position,
        query_state={
            "Category": {
                "projections": [
                    _projection(
                        FORECAST_TABLE,
                        "month_start",
                        "Month",
                        active=True,
                    )
                ]
            },
            "Y": {
                "projections": [
                    _projection(
                        FORECAST_TABLE,
                        "Forecast Weighted Enquiry Value - oct2027",
                        "Blended Forecast",
                        measure=True,
                    ),
                    _projection(
                        FORECAST_TABLE,
                        "XGBoost Forecast Component",
                        "XGBoost Component",
                        measure=True,
                    ),
                    _projection(
                        FORECAST_TABLE,
                        "Seasonal Forecast Component",
                        "Seasonal Component",
                        measure=True,
                    ),
                ]
            },
        },
        title="Bottom-Up Forecast Composition by Month",
        objects={
            "lineStyles": [
                {
                    "properties": {
                        "showMarker": {"expr": {"Literal": {"Value": "true"}}},
                        "markerShape": {"expr": _literal("circle")},
                        "markerSize": {"expr": {"Literal": {"Value": "4D"}}},
                    }
                }
            ]
        },
    )


def _page(display_name: str, visuals: list[dict[str, Any]]) -> dict[str, Any]:
    page_name = _id()
    return {
        "definition": {
            "$schema": PAGE_SCHEMA,
            "name": page_name,
            "displayName": display_name,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280,
        },
        "visuals": visuals,
    }


def _mapping_overview_page() -> dict[str, Any]:
    visuals = [
        _textbox("Segment Mapping Overview", _position(24, 14, 560, 46, 0)),
        _slicer(
            ALLOCATION_TABLE,
            "product_segment",
            "Product Segment",
            _position(24, 72, 210, 58, 1),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "category_segment",
            "Category Segment",
            _position(246, 72, 220, 58, 2),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "product_mapping_status",
            "Product Mapping Status",
            _position(478, 72, 226, 58, 3),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "category_mapping_status",
            "Category Mapping Status",
            _position(716, 72, 226, 58, 4),
        ),
        _card(
            ALLOCATION_TABLE,
            "Distinct Project Count",
            "Projects",
            _position(24, 146, 286, 92, 5),
        ),
        _card(
            ALLOCATION_TABLE,
            "Source Weighted Value",
            "Source Weighted Value",
            _position(326, 146, 286, 92, 6),
        ),
        _card(
            ALLOCATION_TABLE,
            "Allocated Weighted Value",
            "Allocated Weighted Value",
            _position(628, 146, 286, 92, 7),
        ),
        _card(
            ALLOCATION_TABLE,
            "Allocation Reconciliation Delta",
            "Reconciliation Delta",
            _position(930, 146, 326, 92, 8),
        ),
        _bar_chart(
            (
                ALLOCATION_TABLE,
                "product_allocation_method",
                "Product Allocation Method",
            ),
            (ALLOCATION_TABLE, "Distinct Project Count", "Projects"),
            _position(24, 254, 596, 214, 9),
            "Projects by Product Allocation Method",
        ),
        _bar_chart(
            (
                ALLOCATION_TABLE,
                "category_allocation_method",
                "Category Allocation Method",
            ),
            (ALLOCATION_TABLE, "Distinct Project Count", "Projects"),
            _position(636, 254, 620, 214, 10),
            "Projects by Category Allocation Method",
        ),
        _table(
            (
                (ALLOCATION_TABLE, "product_segment", "Product", False),
                (ALLOCATION_TABLE, "category_segment", "Category", False),
                (
                    ALLOCATION_TABLE,
                    "Distinct Project Count",
                    "Projects",
                    True,
                ),
                (
                    ALLOCATION_TABLE,
                    "Allocated Weighted Value",
                    "Allocated Weighted Value",
                    True,
                ),
                (
                    ALLOCATION_TABLE,
                    "Allocation % of Selected Total",
                    "% of Selected Total",
                    True,
                ),
            ),
            _position(24, 484, 1232, 218, 11),
            "Eight-Leaf Allocation Summary",
        ),
    ]
    return _page(AUDIT_PAGE_NAMES[0], visuals)


def _allocation_detail_page() -> dict[str, Any]:
    visuals = [
        _textbox("Project Allocation Detail", _position(24, 14, 560, 46, 0)),
        _slicer(
            ALLOCATION_TABLE,
            "project_id",
            "Project ID",
            _position(24, 72, 210, 58, 1),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "product_segment",
            "Product Segment",
            _position(246, 72, 210, 58, 2),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "category_segment",
            "Category Segment",
            _position(468, 72, 220, 58, 3),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "product_allocation_method",
            "Product Allocation Method",
            _position(700, 72, 254, 58, 4),
        ),
        _slicer(
            ALLOCATION_TABLE,
            "category_allocation_method",
            "Category Allocation Method",
            _position(966, 72, 290, 58, 5),
        ),
        _card(
            ALLOCATION_TABLE,
            "Distinct Project Count",
            "Selected Projects",
            _position(24, 146, 286, 92, 6),
        ),
        _card(
            ALLOCATION_TABLE,
            "Source Weighted Value",
            "Source Weighted Value",
            _position(326, 146, 286, 92, 7),
        ),
        _card(
            ALLOCATION_TABLE,
            "Allocated Weighted Value",
            "Allocated Weighted Value",
            _position(628, 146, 286, 92, 8),
        ),
        _card(
            ALLOCATION_TABLE,
            "Allocation Reconciliation Delta",
            "Full-Project Delta",
            _position(930, 146, 326, 92, 9),
        ),
        _table(
            (
                (ALLOCATION_TABLE, "project_id", "Project ID", False),
                (ALLOCATION_TABLE, "enquiry_month", "Enquiry Month", False),
                (ALLOCATION_TABLE, "product_segment", "Product", False),
                (ALLOCATION_TABLE, "category_segment", "Category", False),
                (ALLOCATION_TABLE, "allocation_share", "Allocation Share", False),
                (
                    ALLOCATION_TABLE,
                    "project_weighted_enquiry_value",
                    "Project Weighted Value",
                    False,
                ),
                (
                    ALLOCATION_TABLE,
                    "allocated_weighted_enquiry_value",
                    "Allocated Weighted Value",
                    False,
                ),
                (
                    ALLOCATION_TABLE,
                    "product_allocation_method",
                    "Product Method",
                    False,
                ),
                (
                    ALLOCATION_TABLE,
                    "category_allocation_method",
                    "Category Method",
                    False,
                ),
                (
                    ALLOCATION_TABLE,
                    "product_mapping_status",
                    "Product Status",
                    False,
                ),
                (
                    ALLOCATION_TABLE,
                    "category_mapping_status",
                    "Category Status",
                    False,
                ),
            ),
            _position(24, 254, 1232, 448, 10),
            "Project-to-Leaf Allocation Rows",
        ),
    ]
    return _page(AUDIT_PAGE_NAMES[1], visuals)


def _model_audit_page() -> dict[str, Any]:
    visuals = [
        _textbox("Model & Forecast Audit", _position(24, 14, 560, 46, 0)),
        _card(
            FORECAST_TABLE,
            "Forecast Weighted Enquiry Value - oct2027",
            "Blended Forecast",
            _position(24, 72, 286, 92, 1),
        ),
        _card(
            FORECAST_TABLE,
            "XGBoost Forecast Component",
            "XGBoost Component",
            _position(326, 72, 286, 92, 2),
        ),
        _card(
            FORECAST_TABLE,
            "Seasonal Forecast Component",
            "Seasonal Component",
            _position(628, 72, 286, 92, 3),
        ),
        _card(
            MODEL_SUMMARY_TABLE,
            "Fallback Leaf Count",
            "Fallback Leaves",
            _position(930, 72, 326, 92, 4),
        ),
        _line_chart(_position(24, 180, 816, 288, 5)),
        _bar_chart(
            (MODEL_SUMMARY_TABLE, "model", "Model"),
            (MODEL_SUMMARY_TABLE, "Leaf Count", "Leaves"),
            _position(856, 180, 400, 288, 6),
            "Leaf Count by Model",
        ),
        _table(
            (
                (MODEL_SUMMARY_TABLE, "product_segment", "Product", False),
                (MODEL_SUMMARY_TABLE, "category_segment", "Category", False),
                (MODEL_SUMMARY_TABLE, "model", "Model", False),
                (MODEL_SUMMARY_TABLE, "fallback_reason", "Fallback Reason", False),
                (MODEL_SUMMARY_TABLE, "history_months", "History Months", False),
                (MODEL_SUMMARY_TABLE, "nonzero_months", "Non-Zero Months", False),
                (MODEL_SUMMARY_TABLE, "history_total", "History Total", False),
                (MODEL_SUMMARY_TABLE, "forecast_start", "Forecast Start", False),
                (MODEL_SUMMARY_TABLE, "forecast_end", "Forecast End", False),
                (MODEL_SUMMARY_TABLE, "xgb_weight", "XGB Weight", False),
                (MODEL_SUMMARY_TABLE, "seasonal_weight", "Seasonal Weight", False),
            ),
            _position(24, 484, 1232, 218, 7),
            "Eight Independently Modelled Leaves",
        ),
    ]
    return _page(AUDIT_PAGE_NAMES[2], visuals)


def _audit_pages() -> list[dict[str, Any]]:
    return [
        _mapping_overview_page(),
        _allocation_detail_page(),
        _model_audit_page(),
    ]


def _page_members(page: dict[str, Any]) -> dict[str, bytes]:
    definition = page["definition"]
    page_name = definition["name"]
    members = {
        f"Report/definition/pages/{page_name}/page.json": json.dumps(
            definition,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    }
    for visual in page["visuals"]:
        visual_name = visual["name"]
        members[
            f"Report/definition/pages/{page_name}/visuals/{visual_name}/visual.json"
        ] = json.dumps(
            visual,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    return members


def validate_audit_archive(path: Path, original_page_count: int | None = None) -> None:
    with zipfile.ZipFile(path) as archive:
        corrupt_member = archive.testzip()
        if corrupt_member is not None:
            raise ValueError(f"Generated PBIX contains a corrupt member: {corrupt_member}")
        if "SecurityBindings" in archive.namelist():
            raise ValueError("Generated PBIX retained stale SecurityBindings.")

        pages = json.loads(archive.read("Report/definition/pages/pages.json"))
        page_names: list[str] = []
        visual_names: list[str] = []
        report_text = ""
        for page_id in pages["pageOrder"]:
            page_member = f"Report/definition/pages/{page_id}/page.json"
            page = json.loads(archive.read(page_member))
            if page["name"] != page_id:
                raise ValueError(f"Page member/name mismatch for {page_id}.")
            page_names.append(page["displayName"])
            report_text += json.dumps(page, ensure_ascii=False)

            visual_prefix = f"Report/definition/pages/{page_id}/visuals/"
            for member in archive.namelist():
                if member.startswith(visual_prefix) and member.endswith("/visual.json"):
                    visual = json.loads(archive.read(member))
                    visual_names.append(visual["name"])
                    report_text += json.dumps(visual, ensure_ascii=False)

        if len(page_names) != len(set(page_names)):
            raise ValueError("Generated PBIX contains duplicate page display names.")
        if len(visual_names) != len(set(visual_names)):
            raise ValueError("Generated PBIX contains duplicate visual names.")
        if not set(AUDIT_PAGE_NAMES).issubset(page_names):
            raise ValueError("Generated PBIX is missing one or more audit pages.")
        if original_page_count is not None and len(page_names) != original_page_count + 3:
            raise ValueError("Generated PBIX page count is not source page count plus three.")
        forbidden_name = "overall_" + "benchmark_report"
        if forbidden_name in report_text:
            raise ValueError("An audit visual references the direct-overall benchmark.")


def build_audit_report(source: Path, output: Path) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("The audit PBIX output must differ from the source PBIX.")
    if output.exists():
        raise FileExistsError(
            f"Refusing to overwrite an existing PBIX: {output.resolve()}"
        )

    with zipfile.ZipFile(source, "r") as archive:
        required_members = {
            "Report/definition/report.json",
            "Report/definition/pages/pages.json",
            "DataModel",
        }
        missing = sorted(required_members.difference(archive.namelist()))
        if missing:
            raise ValueError(
                "Source PBIX is not a supported modern report package; "
                f"missing members: {missing}"
            )

        pages_member = "Report/definition/pages/pages.json"
        pages_metadata = json.loads(archive.read(pages_member))
        original_page_count = len(pages_metadata["pageOrder"])
        new_members: dict[str, bytes] = {}
        for page in _audit_pages():
            pages_metadata["pageOrder"].append(page["definition"]["name"])
            new_members.update(_page_members(page))
        pages_metadata["activePageName"] = pages_metadata["pageOrder"][-3]
        new_members[pages_member] = json.dumps(
            pages_metadata,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        output.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output, "w") as derived:
            for info in archive.infolist():
                if info.filename == "SecurityBindings":
                    continue
                if info.filename in new_members:
                    derived.writestr(info, new_members.pop(info.filename))
                else:
                    derived.writestr(info, archive.read(info.filename))
            for member_name, content in new_members.items():
                derived.writestr(member_name, content)

    validate_audit_archive(output, original_page_count=original_page_count)
    print(f"Audit PBIX written to: {output.resolve()}")
    print(f"Page count: {original_page_count + 3}")


def main() -> int:
    args = parse_args()
    build_audit_report(args.source.resolve(), args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())