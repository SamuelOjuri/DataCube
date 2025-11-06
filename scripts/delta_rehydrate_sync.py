# scripts/rehydrate_recent.py

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.tasks.pipeline import rehydrate_recent  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rehydrate recent Supabase projects only")
    parser.add_argument(
        "--since",
        type=str,
        help="ISO date (YYYY-MM-DD). Rehydrate projects with Supabase date_created >= this value",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="If --since not provided, rehydrate projects created within the past N days (default 7)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of parent IDs to process per chunk",
    )
    return parser.parse_args()


def _resolve_since(raw: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(raw) if raw else None


async def main() -> None:
    args = _parse_args()
    logger = logging.getLogger("rehydrate_recent")
    since_dt = _resolve_since(args.since)

    await rehydrate_recent(
        since=since_dt,
        days_back=args.days_back,
        chunk_size=args.chunk_size,
        logger=logger,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    asyncio.run(main())