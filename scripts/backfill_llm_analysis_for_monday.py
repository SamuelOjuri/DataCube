#!/usr/bin/env python
"""
Re-run LLM analysis for selected projects (or, if none provided, any whose latest
result is still numeric-baseline) and push the refreshed metrics back into Monday.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Set

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient
from src.services.analysis_service import AnalysisService
from src.services.monday_update_service import MondayUpdateService



logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger("backfill_llm_analysis")


def fetch_baseline_project_ids(db: SupabaseClient) -> List[str]:
    """
    Return all project IDs whose latest analysis is still numeric-only.
    """
    rows = (
        db.client.table("analysis_results")
        .select("project_id", count="exact")
        .in_("llm_model", ["numeric-baseline", None])
        .execute()
        .data
        or []
    )
    ids: Set[str] = {row["project_id"] for row in rows if row.get("project_id")}
    logger.info("Discovered %s projects needing LLM re-analysis", len(ids))
    return sorted(ids)


def load_project_ids(
    explicit_ids: Optional[List[str]],
    ids_file: Optional[Path],
    db: SupabaseClient,
) -> List[str]:
    """
    Combine CLI-provided IDs and/or those read from a file.
    Falls back to the numeric-baseline backlog if nothing was supplied.
    """
    chosen: Set[str] = set()

    if explicit_ids:
        chosen.update(str(pid).strip() for pid in explicit_ids if str(pid).strip())

    if ids_file:
        for line in ids_file.read_text(encoding="utf-8").splitlines():
            value = line.strip()
            if value:
                chosen.add(value)

    if chosen:
        logger.info("Using %s explicitly supplied project id(s)", len(chosen))
        return sorted(chosen)

    logger.info("No project IDs supplied; defaulting to numeric-baseline backlog.")
    return fetch_baseline_project_ids(db)


def backfill_projects(project_ids: List[str]) -> None:
    """
    For each project: re-run analysis with LLM enabled and sync the results to Monday.
    """
    if not project_ids:
        logger.info("Nothing to backfill – no project IDs were provided or discovered.")
        return

    db = SupabaseClient()
    analysis = AnalysisService(db_client=db)
    updater = MondayUpdateService(db_client=db)

    for project_id in project_ids:
        try:
            logger.info("Re-running analysis for project %s", project_id)
            result = analysis.analyze_and_store(project_id, with_llm=True)
            if not result.get("success"):
                logger.warning(
                    "Skipping Monday sync for %s – analysis failed: %s",
                    project_id,
                    result.get("error"),
                )
                continue

            payload = result["result"]
            monday_res = updater.sync_project(
                project_id,
                analysis=payload,
                include_update=True,
                throttle=True,
            )

            if monday_res.get("success"):
                logger.info(
                    "LLM results stored and Monday columns refreshed for %s", project_id
                )
            else:
                logger.warning(
                    "Analysis stored but Monday update failed for %s: %s",
                    project_id,
                    monday_res,
                )

        except Exception as exc:  # noqa: BLE001
            logger.exception("Backfill failed for %s", project_id)
            continue


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Monday projects with LLM analysis and sync CRM columns."
    )
    parser.add_argument(
        "--project-id",
        action="append",
        dest="project_ids",
        help="Explicit Monday project ID to process (repeat for multiple).",
    )
    parser.add_argument(
        "--ids-file",
        type=Path,
        help="Path to a text file containing Monday project IDs (one per line).",
    )

    args = parser.parse_args()

    db = SupabaseClient()
    project_ids = load_project_ids(args.project_ids, args.ids_file, db)
    backfill_projects(project_ids)


if __name__ == "__main__":
    main()