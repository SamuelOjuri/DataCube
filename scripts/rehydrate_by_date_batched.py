"""
Script to run by-id rehydrate in batches with a limit on unique name prefixes per batch, to mitigate potential bottlenecks in the rehydration process. This is designed to be run with a --since date to target recently created projects, and can be further limited with --limit for testing or incremental runs.

Recommended usage: python .\scripts\rehydrate_by_date_batched.py --since 2021-01-01 --batch-prefix-limit 40 --chunk-size 40

"""
import argparse
import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.database.supabase_client import SupabaseClient  # noqa: E402
from src.tasks.pipeline import rehydrate_projects_by_ids  # noqa: E402

logger = logging.getLogger("rehydrate_by_date_batched")

_PREFIX_RE = re.compile(r"^(\d+)")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run by-id rehydrate in prefix-limited batches"
    )
    parser.add_argument(
        "--since",
        type=str,
        required=True,
        help="ISO date, e.g. 2021-01-01",
    )
    parser.add_argument(
        "--batch-prefix-limit",
        type=int,
        default=50,
        help="Maximum unique prefixes per batch",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50,
        help="Chunk size passed to rehydrate_projects_by_ids",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of projects to process",
    )
    return parser.parse_args()


def _leading_digits(name: str) -> Optional[str]:
    if not name:
        return None
    match = _PREFIX_RE.match(name.strip())
    return match.group(1) if match else None


def _load_candidates(
    db: SupabaseClient,
    since: str,
    limit: Optional[int],
) -> List[Dict[str, str]]:
    page_size = 1000
    offset = 0
    rows: List[Dict[str, str]] = []
    page_number = 0

    logger.info(
        "Loading candidates from Supabase | since=%s | limit=%s | page_size=%s",
        since,
        limit,
        page_size,
    )

    while True:
        page_number += 1
        started = time.perf_counter()

        result = (
            db.client.table("projects")
            .select("monday_id, item_name, project_name, date_created")
            .gte("date_created", since)
            .order("date_created", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )

        page = result.data or []
        elapsed = time.perf_counter() - started

        logger.info(
            "Candidate page %d loaded | offset=%d | rows=%d | cumulative=%d | elapsed=%.2fs",
            page_number,
            offset,
            len(page),
            len(rows) + len(page),
            elapsed,
        )

        if not page:
            break

        rows.extend(page)

        if limit is not None and len(rows) >= limit:
            rows = rows[:limit]
            logger.info(
                "Candidate limit reached | limit=%d | truncated_total=%d",
                limit,
                len(rows),
            )
            break

        if len(page) < page_size:
            break

        offset += page_size

    candidates: List[Dict[str, str]] = []
    prefix_set: Set[str] = set()

    for row in rows:
        monday_id = str(row.get("monday_id") or "").strip()
        if not monday_id:
            continue

        name = str(row.get("item_name") or row.get("project_name") or "").strip()
        prefix = _leading_digits(name)
        normalized_prefix = prefix or monday_id

        candidates.append(
            {
                "monday_id": monday_id,
                "name": name,
                "prefix": normalized_prefix,
            }
        )
        prefix_set.add(normalized_prefix)

    logger.info(
        "Candidate load complete | candidates=%d | unique_prefixes=%d",
        len(candidates),
        len(prefix_set),
    )

    if candidates:
        logger.info(
            "Candidate sample | first=%s:%s | last=%s:%s",
            candidates[0]["monday_id"],
            candidates[0]["name"],
            candidates[-1]["monday_id"],
            candidates[-1]["name"],
        )

    return candidates


def _build_batches(
    candidates: List[Dict[str, str]],
    prefix_limit: int,
) -> List[List[Dict[str, str]]]:
    batches: List[List[Dict[str, str]]] = []
    current_batch: List[Dict[str, str]] = []
    current_prefixes: Set[str] = set()

    logger.info(
        "Building batches | candidates=%d | prefix_limit=%d",
        len(candidates),
        prefix_limit,
    )

    for row in candidates:
        prefix = row["prefix"]

        if current_batch and prefix not in current_prefixes and len(current_prefixes) >= prefix_limit:
            batches.append(current_batch)
            current_batch = []
            current_prefixes = set()

        current_batch.append(row)
        current_prefixes.add(prefix)

    if current_batch:
        batches.append(current_batch)

    logger.info("Batch build complete | batches=%d", len(batches))

    for index, batch in enumerate(batches[:5], start=1):
        batch_prefixes = {row["prefix"] for row in batch}
        logger.info(
            "Batch preview %d | projects=%d | unique_prefixes=%d | first_id=%s | last_id=%s",
            index,
            len(batch),
            len(batch_prefixes),
            batch[0]["monday_id"],
            batch[-1]["monday_id"],
        )

    if len(batches) > 5:
        logger.info("Batch preview truncated | remaining_batches=%d", len(batches) - 5)

    return batches


async def main() -> None:
    args = _parse_args()
    started = time.perf_counter()

    logger.info(
        "Starting batched by-id rehydrate | since=%s | batch_prefix_limit=%d | chunk_size=%d | limit=%s",
        args.since,
        args.batch_prefix_limit,
        args.chunk_size,
        args.limit,
    )

    db = SupabaseClient()

    candidates = _load_candidates(db, args.since, args.limit)
    if not candidates:
        logger.info("No candidates found for since=%s", args.since)
        return

    batches = _build_batches(candidates, args.batch_prefix_limit)

    logger.info(
        "Prepared %d batches for %d candidates",
        len(batches),
        len(candidates),
    )

    attempted = 0
    succeeded = 0
    failed = 0
    failed_ids: List[str] = []

    for index, batch in enumerate(batches, start=1):
        batch_ids = [row["monday_id"] for row in batch]
        batch_prefixes = sorted({row["prefix"] for row in batch})
        batch_started = time.perf_counter()

        logger.info(
            "Starting batch %d/%d | projects=%d | unique_prefixes=%d | first_id=%s | last_id=%s | attempted=%d/%d | succeeded=%d | failed=%d",
            index,
            len(batches),
            len(batch_ids),
            len(batch_prefixes),
            batch_ids[0],
            batch_ids[-1],
            attempted,
            len(candidates),
            succeeded,
            failed,
        )
        logger.info(
            "Batch %d prefix sample | %s",
            index,
            ", ".join(batch_prefixes[:10]),
        )

        attempted += len(batch_ids)

        try:
            await rehydrate_projects_by_ids(
                batch_ids,
                chunk_size=max(1, min(args.chunk_size, len(batch_ids))),
                logger=logger,
            )
        except Exception:
            batch_elapsed = time.perf_counter() - batch_started
            failed += len(batch_ids)
            failed_ids.extend(batch_ids)

            total_elapsed = time.perf_counter() - started
            rate = succeeded / total_elapsed if total_elapsed > 0 else 0.0

            logger.exception(
                "Batch %d/%d failed | projects=%d | first_id=%s | last_id=%s | batch_elapsed=%.2fs | progress attempted=%d/%d | succeeded=%d | failed=%d | avg_success_rate=%.1f projects/s",
                index,
                len(batches),
                len(batch_ids),
                batch_ids[0],
                batch_ids[-1],
                batch_elapsed,
                attempted,
                len(candidates),
                succeeded,
                failed,
                rate,
            )
            continue

        batch_elapsed = time.perf_counter() - batch_started
        succeeded += len(batch_ids)
        total_elapsed = time.perf_counter() - started
        rate = succeeded / total_elapsed if total_elapsed > 0 else 0.0

        logger.info(
            "Finished batch %d/%d | batch_elapsed=%.2fs | progress attempted=%d/%d | succeeded=%d | failed=%d | avg_success_rate=%.1f projects/s",
            index,
            len(batches),
            batch_elapsed,
            attempted,
            len(candidates),
            succeeded,
            failed,
            rate,
        )

    total_elapsed = time.perf_counter() - started
    final_rate = succeeded / total_elapsed if total_elapsed > 0 else 0.0

    logger.info(
        "Completed batched by-id rehydrate | candidates=%d | attempted=%d | succeeded=%d | failed=%d | batches=%d | elapsed=%.2fs | avg_success_rate=%.1f projects/s",
        len(candidates),
        attempted,
        succeeded,
        failed,
        len(batches),
        total_elapsed,
        final_rate,
    )

    if failed_ids:
        unique_failed_ids = sorted(set(failed_ids))
        logger.warning(
            "Failed project IDs (%d): %s",
            len(unique_failed_ids),
            ", ".join(unique_failed_ids),
        )
    else:
        logger.info("Failed project IDs (0): none")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main())