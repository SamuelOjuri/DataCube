"""Create a derived PBIX archive with six segmented forecast pages.

This script edits only a local copy of the supplied PBIX report layout.  It does
not open, refresh, or modify any remote data source.  The semantic-model table
is populated separately by the model updater.
"""

from __future__ import annotations

import argparse
import copy
import json
import secrets
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = (
    PROJECT_ROOT
    / "data"
    / "Version 3 - Forward-Looking-Monthly-Outlook-for-Projects-in-Pipeline.pbix"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "outputs"
    / "segmented_weighted_enquiry"
    / "Forward-Looking-Monthly-Outlook-Segmented-Live.pbix"
)
FORECAST_TABLE = "vw_weighted_enquiry_value_monthly_oct2027"

PAGE_DEFINITIONS = (
    ("By Product - Non-Combustible", "product_segment", "Non-Combustible"),
    ("By Product - Combustible", "product_segment", "Combustible"),
    ("By Category - Data Centres", "category_segment", "Data Centres"),
    ("By Category - Education", "category_segment", "Education"),
    (
        "By Category - Apartments/Housing",
        "category_segment",
        "Apartments/Housing",
    ),
    ("By Category - Other", "category_segment", "Other"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add six segmented pages to a PBIX copy.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _literal(value: str) -> dict[str, dict[str, str]]:
    escaped = value.replace("'", "''")
    return {"Literal": {"Value": f"'{escaped}'"}}


def _filter_condition(source_alias: str, field: str, value: str) -> dict[str, object]:
    return {
        "Condition": {
            "In": {
                "Expressions": [
                    {
                        "Column": {
                            "Expression": {"SourceRef": {"Source": source_alias}},
                            "Property": field,
                        }
                    }
                ],
                "Values": [[_literal(value)]],
            }
        }
    }


def _page_filter(field: str, value: str) -> str:
    return _compact(
        [
            {
                "name": secrets.token_hex(10),
                "expression": {
                    "Column": {
                        "Expression": {"SourceRef": {"Entity": FORECAST_TABLE}},
                        "Property": field,
                    }
                },
                "filter": {
                    "Version": 2,
                    "From": [{"Name": "v", "Entity": FORECAST_TABLE, "Type": 0}],
                    "Where": [_filter_condition("v", field, value)],
                },
                "type": "Categorical",
                "howCreated": 1,
                "isHiddenInViewMode": False,
            }
        ]
    )


def _add_query_filter(query: dict[str, object], field: str, value: str) -> None:
    from_items = query.get("From", [])
    source_alias = next(
        (
            item["Name"]
            for item in from_items
            if item.get("Entity") == FORECAST_TABLE
        ),
        "v1",
    )
    query.setdefault("Where", []).append(_filter_condition(source_alias, field, value))


def _clone_segment_page(
    template: dict[str, object],
    ordinal: int,
    display_name: str,
    field: str,
    value: str,
) -> dict[str, object]:
    page = copy.deepcopy(template)
    page["name"] = secrets.token_hex(10)
    page["displayName"] = display_name
    page["ordinal"] = ordinal
    page["filters"] = _page_filter(field, value)

    title = f"Actual vs Forecast Weighted Enquiry Value - {value} (15-Month Outlook)"
    for visual in page.get("visualContainers", []):
        config = json.loads(visual["config"])
        config["name"] = secrets.token_hex(10)
        single_visual = config.get("singleVisual", {})
        title_objects = single_visual.get("vcObjects", {}).get("title", [])
        if title_objects:
            title_objects[0]["properties"]["text"]["expr"] = _literal(title)
        prototype = single_visual.get("prototypeQuery")
        if prototype:
            _add_query_filter(prototype, field, value)
        visual["config"] = _compact(config)

        query_payload = json.loads(visual["query"])
        for command in query_payload.get("Commands", []):
            shape = command.get("SemanticQueryDataShapeCommand", {})
            semantic_query = shape.get("Query")
            if semantic_query:
                _add_query_filter(semantic_query, field, value)
        visual["query"] = _compact(query_payload)
    return page


def build_layout(source: Path, output: Path) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("The derived PBIX output must differ from the source PBIX.")
    if output.exists():
        raise FileExistsError(
            f"Refusing to overwrite an existing PBIX: {output.resolve()}"
        )

    with zipfile.ZipFile(source, "r") as archive:
        layout_info = archive.getinfo("Report/Layout")
        layout = json.loads(archive.read("Report/Layout").decode("utf-16-le"))
        template = next(
            section
            for section in layout["sections"]
            if section["displayName"].startswith("Oct 27 Ending")
        )

        template["displayName"] = "Overall - Actual vs Forecast Weighted Enquiry Value"
        for visual in template.get("visualContainers", []):
            config = json.loads(visual["config"])
            title_objects = (
                config.get("singleVisual", {}).get("vcObjects", {}).get("title", [])
            )
            if title_objects:
                title_objects[0]["properties"]["text"]["expr"] = _literal(
                    "Actual vs Forecast Weighted Enquiry Value - 15-Month Outlook"
                )
            visual["config"] = _compact(config)

        next_ordinal = max(section["ordinal"] for section in layout["sections"]) + 1
        for offset, definition in enumerate(PAGE_DEFINITIONS):
            layout["sections"].append(
                _clone_segment_page(template, next_ordinal + offset, *definition)
            )

        output.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output, "w") as derived:
            for info in archive.infolist():
                if info.filename in {"Report/Layout", "SecurityBindings"}:
                    continue
                derived.writestr(info, archive.read(info.filename))
            derived.writestr(layout_info, _compact(layout).encode("utf-16-le"))

    print(f"Derived PBIX layout written to: {output.resolve()}")
    print(f"Page count: {len(layout['sections'])}")


def main() -> int:
    args = parse_args()
    build_layout(args.source.resolve(), args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
