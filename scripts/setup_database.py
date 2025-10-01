#!/usr/bin/env python3
"""
Database setup and validation script
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Setup and validate database connection"""
    logger.info("Setting up database connection...")
    
    try:
        # Test Supabase connection
        supabase = SupabaseClient()
        
        # Test basic operations
        result = supabase.client.table('projects').select('count').execute()
        logger.info("✅ Database connection successful")
        
        # Test if tables exist
        tables_to_check = ['projects', 'subitems', 'hidden_items', 'sync_log', 'webhook_events']
        
        for table in tables_to_check:
            try:
                supabase.client.table(table).select('count').limit(1).execute()
                logger.info(f"✅ Table '{table}' exists and accessible")
            except Exception as e:
                logger.error(f"❌ Table '{table}' not accessible: {e}")
                logger.error("Please run the schema SQL files in Supabase dashboard")
                sys.exit(1)
        
        logger.info("✅ Database setup validation completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Database setup failed: {e}")
        logger.error("Please check your Supabase credentials and connection")
        sys.exit(1)


if __name__ == "__main__":
    main()
