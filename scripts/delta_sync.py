#!/usr/bin/env python3
"""
Delta sync script for periodic updates
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.sync_service import DataSyncService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Run delta sync for recent changes"""
    logger.info("Starting delta sync...")
    
    try:
        sync_service = DataSyncService()
        result = await sync_service.perform_delta_sync(hours_back=24)
        
        if result['success']:
            logger.info("✅ Delta sync completed successfully")
            logger.info(f"Stats: {result['stats']}")
        else:
            logger.error("❌ Delta sync failed")
            logger.error(f"Error: {result['error']}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Delta sync failed with exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
