from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from scripts.build_segmented_pbix_audit_pages import (
    ALLOCATION_TABLE,
    AUDIT_PAGE_NAMES,
    DEFAULT_SOURCE,
    FORECAST_TABLE,
    MODEL_SUMMARY_TABLE,
    build_audit_report,
    validate_audit_archive,
)


def _report_definition_text(path: Path) -> tuple[list[str], str]:
    with zipfile.ZipFile(path) as archive:
        pages = json.loads(archive.read("Report/definition/pages/pages.json"))
        display_names: list[str] = []
        definition_parts: list[str] = []
        for page_id in pages["pageOrder"]:
            page_prefix = f"Report/definition/pages/{page_id}/"
            page = json.loads(archive.read(f"{page_prefix}page.json"))
            display_names.append(page["displayName"])
            definition_parts.append(json.dumps(page, ensure_ascii=False))
            for member in archive.namelist():
                if member.startswith(f"{page_prefix}visuals/") and member.endswith(
                    "/visual.json"
                ):
                    definition_parts.append(
                        archive.read(member).decode("utf-8")
                    )
        return display_names, "".join(definition_parts)


def test_builder_adds_three_modern_audit_pages(tmp_path: Path) -> None:
    output = tmp_path / "audit.pbix"

    build_audit_report(DEFAULT_SOURCE, output)
    validate_audit_archive(output, original_page_count=13)

    page_names, report_text = _report_definition_text(output)
    assert len(page_names) == 16
    assert page_names[-3:] == list(AUDIT_PAGE_NAMES)
    assert ALLOCATION_TABLE in report_text
    assert MODEL_SUMMARY_TABLE in report_text
    assert FORECAST_TABLE in report_text
    assert "overall_benchmark_report" not in report_text

    with zipfile.ZipFile(output) as archive:
        assert archive.testzip() is None
        assert "DataModel" in archive.namelist()
        assert "SecurityBindings" not in archive.namelist()


def test_builder_refuses_to_overwrite_an_existing_audit_pbix(
    tmp_path: Path,
) -> None:
    output = tmp_path / "audit.pbix"
    output.write_bytes(b"existing audit artifact")

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        build_audit_report(DEFAULT_SOURCE, output)

    assert output.read_bytes() == b"existing audit artifact"


def test_builder_refuses_to_replace_its_source(tmp_path: Path) -> None:
    source = tmp_path / "source.pbix"
    source.write_bytes(b"source artifact")

    with pytest.raises(ValueError, match="must differ"):
        build_audit_report(source, source)

    assert source.read_bytes() == b"source artifact"