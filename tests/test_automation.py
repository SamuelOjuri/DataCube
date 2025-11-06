
import asyncio

import logging
import sys
from pathlib import Path
import pytest

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.tasks.pipeline import rehydrate_projects_by_ids  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

TEST_PROJECT_IDS = [
    "5072605477",
    "5071760777",
    "5071584303",
    "5071568603",
    "5070355738",
    "5069732320",
    "5069390242",
    "5069272487",
    "5069268170",
    "5069137419",
    "5068931481",
]

@pytest.mark.asyncio
async def test_rehydrate_selected_projects():
    """
    Ensure the rehydrate helper can pull the designated test projects without error.
    Adds basic progress logging so we can see which IDs were requested and when
    the helper finishes.
    """
    logger = logging.getLogger("test.rehydrate")
    total = len(TEST_PROJECT_IDS)
    logger.info("Starting rehydrate for %s project(s)", total)
    # Call helper (single async invocation to reuse batching)
    await rehydrate_projects_by_ids(TEST_PROJECT_IDS, chunk_size=25)
    for idx, pid in enumerate(TEST_PROJECT_IDS, start=1):
        logger.info("Rehydrate requested for %s (%s/%s)", pid, idx, total)
    logger.info("Rehydrate helper completed without raising for %s project(s)", total)
