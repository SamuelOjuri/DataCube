import logging
import sys
from pathlib import Path
import pytest

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.app import _scheduled_recent_rehydrate

logger = logging.getLogger("tests.recent_rehydrate")

@pytest.mark.asyncio
async def test_recent_rehydrate_invokes_pipeline(monkeypatch):
    logger.info("Starting happy-path recent rehydrate test")
    captured = {}

    async def fake_rehydrate_recent(*, since=None, days_back, chunk_size, batch_prefix_limit, logger):
        captured["kwargs"] = {
            "since": since,
            "days_back": days_back,
            "chunk_size": chunk_size,
            "batch_prefix_limit": batch_prefix_limit,
            "logger_name": logger.name,
        }

    monkeypatch.setattr("src.api.app.rehydrate_recent", fake_rehydrate_recent)

    await _scheduled_recent_rehydrate()

    assert captured["kwargs"]["since"] is None
    assert captured["kwargs"]["days_back"] == 3
    from src.api.app import REHYDRATE_CHUNK_SIZE, REHYDRATE_BATCH_PREFIX_LIMIT
    assert captured["kwargs"]["chunk_size"] == REHYDRATE_CHUNK_SIZE
    assert captured["kwargs"]["batch_prefix_limit"] == REHYDRATE_BATCH_PREFIX_LIMIT
    assert captured["kwargs"]["logger_name"] == "scheduler.recent_rehydrate"
    logger.info("Completed happy-path recent rehydrate test")

@pytest.mark.asyncio
async def test_recent_rehydrate_logs_exception(monkeypatch, caplog):
    logger.info("Starting failure-path recent rehydrate test")

    async def boom(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("src.api.app.rehydrate_recent", boom)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match='boom'):
            await _scheduled_recent_rehydrate()

    assert "Recent rehydrate job failed" in caplog.text
    assert "boom" in caplog.text

    logger.info("Completed failure-path recent rehydrate test")
