

## Complete Supabase Implementation Guide

DataCube/
├── .env.example                    # Environment template
├── .env                           # Environment variables (gitignored)
├── .gitignore
├── README.md
├── requirements.txt
├─
├─
├── pyproject.toml                 # Modern Python packaging
│
├── src/
│   ├── __init__.py
│   ├── config.py                  # Enhanced with Supabase config
│   ├── main.py                    # Application entry point
│   │
│   ├── api/                       # FastAPI application
│   │   ├── __init__.py
│   │   ├── app.py                 # FastAPI app factory
│   │   ├── routes/
│   │   │   ├── __init__.py
│   │   │   ├── health.py
│   │   │   ├── analysis.py
│   │   │   └── monitoring.py
│   │   └── middleware/
│   │       ├── __init__.py
│   │       ├── auth.py
│   │       └── logging.py
│   │
│   ├── database/                  # Database layer (NEW)
│   │   ├── __init__.py
│   │   ├── supabase_client.py     # Supabase client
│   │   ├── sync_service.py        # Data sync service
│   │   ├── monitoring.py          # DB monitoring
│   │   └── schema/
│   │       ├── schema.sql         # Main schema
│   │       ├── functions.sql      # Stored procedures
│   │       └── migrations/
│   │           └── 001_initial.sql
│   │
│   ├── webhooks/                  # Webhook handling (NEW)
│   │   ├── __init__.py
│   │   ├── webhook_server.py      # FastAPI webhook server
│   │   ├── handlers.py            # Event handlers
│   │   └── security.py           # HMAC verification
│   │
│   ├── core/                      # Renamed from src root
│   │   ├── __init__.py
│   │   ├── monday_client.py       # Existing
│   │   ├── data_extractor.py      # Enhanced
│   │   ├── data_processor.py      # Existing
│   │   ├── llm_analyzer.py        # Existing
│   │   ├── numeric_analyzer.py    # Existing
│   │   └── models.py              # Existing
│   │
│   ├── services/                  # Business logic layer
│   │   ├── __init__.py
│   │   ├── analysis_service.py    # Analysis orchestration
│   │   ├── sync_service.py        # Data synchronization
│   │   └── notification_service.py
│   │
│   └── utils/                     # Utilities
│       ├── __init__.py
│       ├── cache.py
│       ├── logging.py
│       └── monitoring.py
│
├── scripts/                       # Deployment & maintenance
│   ├── __init__.py
│   ├── setup_database.py         # Database initialization
│   ├── setup_webhooks.py         # Webhook registration
│   ├── initial_sync.py           # Initial data sync
│   ├── delta_sync.py             # Scheduled sync
│   └── health_check.py           # System health
│
├── tests/                         # Test suite (renamed from test_files)
│   ├── __init__.py
│   ├── conftest.py               # Pytest configuration
│   ├── unit/
│   │   ├── test_monday_client.py
│   │   ├── test_data_processor.py
│   │   └── test_supabase_client.py
│   ├── integration/
│   │   ├── test_full_pipeline.py
│   │   └── test_webhook_flow.py
│   └── fixtures/
│       └── sample_data.json
│
├── config/                        # Environment-specific configs
│   ├── development.yaml
│   ├── staging.yaml
│   └── production.yaml
│
│
├── monitoring/                    # Monitoring & observability
│   ├── prometheus/
│   │   └── rules.yml
│   ├── grafana/
│   │   └── dashboards/
│   └── alerts/
│       └── alerts.yml
│
│
├── outputs/                      # Existing outputs
│   └── analysis/
│
└── docs/                         # Documentation
    ├── api/
    ├── deployment.md
    ├── architecture.md
    └── runbooks/

### **Phase 1: Initial Setup & Environment Configuration**

#### Step 1.1: Install Required Dependencies

```bash
# Add to requirements.txt
supabase==2.10.0
fastapi==0.115.5
uvicorn==0.32.1
python-multipart==0.0.12
psycopg2-binary==2.9.10
alembic==1.14.0
```

#### Step 1.2: Update Environment Configuration

```python
# .env file additions
SUPABASE_URL=your_supabase_project_url
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_KEY=your_supabase_service_role_key
WEBHOOK_SECRET=your_secure_webhook_secret
MONDAY_WEBHOOK_URL=https://your-domain.com/webhooks/monday
```

#### Step 1.3: Update Configuration Module

```python
# src/config.py - Add Supabase configuration

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Webhook Configuration
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8000"))

# Sync Configuration
SYNC_BATCH_SIZE = 1000  # Records per batch for Supabase
SYNC_INTERVAL_MINUTES = 15  # Delta sync interval
RETENTION_DAYS = 730  # Keep 2 years of data
```

### **Phase 2: Supabase Database Schema Creation**

#### Step 2.1: Create SQL Schema File

```sql
-- src/database/schema/schema.sql

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For text search

-- Main projects table (Parent Board: 1825117125)
CREATE TABLE projects (
    -- Primary identifiers
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    monday_id TEXT UNIQUE NOT NULL,
    item_name TEXT NOT NULL,
    
    -- Core project fields
    project_name TEXT,
    pipeline_stage TEXT,
    status_category TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN pipeline_stage IN ('Won - Closed (Invoiced)', 'Won - Open (Order Received)', 'Won Via Other Ref') THEN 'Won'
            WHEN pipeline_stage = 'Lost' THEN 'Lost'
            ELSE 'Open'
        END
    ) STORED,
    
    -- Categorical fields
    type TEXT,  -- New Build/Refurbishment
    category TEXT,  -- Apartments, Commercial, etc.
    zip_code TEXT,
    sales_representative TEXT,
    funding TEXT,
    
    -- Mirror resolved fields
    account TEXT,
    product_type TEXT,
    new_enquiry_value NUMERIC(12, 2) DEFAULT 0,
    
    -- Calculated fields
    gestation_period INTEGER CHECK (gestation_period >= 0 AND gestation_period < 1000),
    project_value NUMERIC(12, 2),
    weighted_pipeline NUMERIC(12, 2),
    probability_percent INTEGER DEFAULT 0,
    
    -- Date fields
    date_created DATE,
    expected_start_date DATE,
    follow_up_date DATE,
    first_date_designed DATE,
    last_date_designed DATE,
    first_date_invoiced DATE,
    
    -- Feedback fields
    feedback TEXT,
    lost_to_who_or_why TEXT,
    
    -- Analysis fields
    value_band TEXT,  -- Computed during processing
    
    -- Metadata
    last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_version INTEGER DEFAULT 1,
    
    -- Indexes for performance
    INDEX idx_account (account),
    INDEX idx_type_category (type, category),
    INDEX idx_product_type (product_type),
    INDEX idx_status_category (status_category),
    INDEX idx_date_created (date_created),
    INDEX idx_monday_id (monday_id)
);

-- Subitems table (Subitem Board: 1825117144)
CREATE TABLE subitems (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    monday_id TEXT UNIQUE NOT NULL,
    parent_monday_id TEXT REFERENCES projects(monday_id) ON DELETE CASCADE,
    item_name TEXT,
    
    -- Connected board relations
    hidden_item_id TEXT,  -- Reference to hidden items board
    
    -- Mirror fields from hidden items
    account TEXT,
    product_type TEXT,
    new_enquiry_value NUMERIC(12, 2),
    reason_for_change TEXT,
    
    -- Design and quote fields
    date_received DATE,
    date_design_completed DATE,
    designer TEXT,
    time_taken NUMERIC(6, 2),
    surveyor TEXT,
    
    -- Area and specifications
    area NUMERIC(10, 2),
    fall TEXT,
    min_thickness NUMERIC(8, 2),
    max_thickness NUMERIC(8, 2),
    u_value NUMERIC(6, 4),
    deck_type TEXT,
    layering TEXT,
    
    -- Financial fields
    quote_amount NUMERIC(12, 2),
    m2_rate NUMERIC(10, 2),
    material_value NUMERIC(12, 2),
    transport_cost NUMERIC(10, 2),
    
    -- Order fields
    order_status TEXT,
    date_order_received DATE,
    customer_po TEXT,
    supplier1 TEXT,
    supplier2 TEXT,
    
    -- Delivery fields
    requested_delivery_date DATE,
    final_delivery_date DATE,
    delivery_address TEXT,
    
    -- Invoice fields
    invoice_number TEXT,
    invoice_date DATE,
    amount_invoiced NUMERIC(12, 2),
    
    -- Metadata
    last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    INDEX idx_parent (parent_monday_id),
    INDEX idx_hidden_item (hidden_item_id)
);

-- Hidden items table (Hidden Items Board: 1825138260)
CREATE TABLE hidden_items (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    monday_id TEXT UNIQUE NOT NULL,
    item_name TEXT,
    
    -- Status and files
    status TEXT,
    project_attachments JSONB,  -- Store file references as JSON
    
    -- Project details
    prefix TEXT,
    revision TEXT,
    project_name TEXT,
    urgency TEXT,
    
    -- Dates
    date_received DATE,
    date_design_completed DATE,
    date_quoted DATE,
    date_project_won DATE,
    date_project_closed DATE,
    
    -- Financial calculations
    quote_amount NUMERIC(12, 2),
    material_value NUMERIC(12, 2),
    transport_cost NUMERIC(10, 2),
    target_gpm NUMERIC(6, 2),
    tp_margin NUMERIC(6, 2),
    commission NUMERIC(6, 2),
    distributor_margin NUMERIC(6, 2),
    
    -- Connected accounts and contacts
    account_id TEXT,
    contact_id TEXT,
    
    -- Metadata
    last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    INDEX idx_monday_id (monday_id),
    INDEX idx_date_received (date_received)
);

-- Sync tracking table
CREATE TABLE sync_log (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sync_type TEXT NOT NULL CHECK (sync_type IN ('full', 'delta', 'webhook')),
    board_id TEXT NOT NULL,
    board_name TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status TEXT DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    items_processed INTEGER DEFAULT 0,
    items_created INTEGER DEFAULT 0,
    items_updated INTEGER DEFAULT 0,
    items_deleted INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);

-- Analysis results cache
CREATE TABLE analysis_results (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    project_id TEXT REFERENCES projects(monday_id) ON DELETE CASCADE,
    analysis_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Predictions
    expected_gestation_days INTEGER,
    gestation_confidence NUMERIC(3, 2),
    expected_conversion_rate NUMERIC(3, 2),
    conversion_confidence NUMERIC(3, 2),
    rating_score INTEGER CHECK (rating_score >= 1 AND rating_score <= 10),
    
    -- Reasoning from LLM
    reasoning JSONB,
    adjustments JSONB,
    confidence_notes TEXT,
    special_factors TEXT,
    
    -- Metadata
    llm_model TEXT,
    analysis_version TEXT,
    processing_time_ms INTEGER,
    
    INDEX idx_project_id (project_id),
    INDEX idx_timestamp (analysis_timestamp DESC)
);

-- Webhook events table for tracking
CREATE TABLE webhook_events (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    event_id TEXT UNIQUE,
    board_id TEXT,
    item_id TEXT,
    event_type TEXT,
    webhook_payload JSONB,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    status TEXT DEFAULT 'pending',
    error_message TEXT,
    
    INDEX idx_status (status),
    INDEX idx_received (received_at DESC)
);

-- Create views for common queries
CREATE VIEW project_analytics AS
SELECT 
    p.*,
    COUNT(s.id) as subitem_count,
    SUM(s.quote_amount) as total_quote_value,
    AVG(s.quote_amount) as avg_quote_value
FROM projects p
LEFT JOIN subitems s ON p.monday_id = s.parent_monday_id
GROUP BY p.id;

-- Create materialized view for performance metrics
CREATE MATERIALIZED VIEW conversion_metrics AS
SELECT 
    account,
    type,
    category,
    product_type,
    COUNT(*) as total_projects,
    COUNT(*) FILTER (WHERE status_category = 'Won') as won_projects,
    COUNT(*) FILTER (WHERE status_category = 'Lost') as lost_projects,
    COUNT(*) FILTER (WHERE status_category = 'Open') as open_projects,
    ROUND(
        COUNT(*) FILTER (WHERE status_category = 'Won')::numeric / 
        NULLIF(COUNT(*), 0), 
        3
    ) as win_rate,
    AVG(gestation_period) FILTER (WHERE gestation_period > 0) as avg_gestation,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gestation_period) 
        FILTER (WHERE gestation_period > 0) as median_gestation
FROM projects
WHERE date_created >= CURRENT_DATE - INTERVAL '2 years'
GROUP BY account, type, category, product_type;

-- Create index for full-text search
CREATE INDEX idx_projects_search ON projects USING gin(
    to_tsvector('english', 
        COALESCE(project_name, '') || ' ' || 
        COALESCE(account, '') || ' ' || 
        COALESCE(category, '')
    )
);

-- Row Level Security (RLS) policies
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE subitems ENABLE ROW LEVEL SECURITY;
ALTER TABLE hidden_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE analysis_results ENABLE ROW LEVEL SECURITY;

-- Create policies (adjust based on your auth strategy)
CREATE POLICY "Enable read access for authenticated users" ON projects
    FOR SELECT USING (true);

CREATE POLICY "Enable write access for service role" ON projects
    FOR ALL USING (auth.jwt() ->> 'role' = 'service_role');

-- Triggers for updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_projects_updated_at
    BEFORE UPDATE ON projects
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_subitems_updated_at
    BEFORE UPDATE ON subitems
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();
```

### **Phase 3: Create Supabase Client & Sync Service**

#### Step 3.1: Supabase Client Module

```python
# src/database/supabase_client.py

import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Supabase client for database operations"""
    
    def __init__(self):
        """Initialize Supabase client"""
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_KEY")  # Use service key for admin operations
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL and key must be set in environment variables")
        
        self.client: Client = create_client(self.url, self.key)
        logger.info("Supabase client initialized")
    
    def upsert_projects(self, projects: List[Dict]) -> Dict:
        """Upsert multiple projects"""
        try:
            result = self.client.table('projects').upsert(
                projects,
                on_conflict='monday_id'
            ).execute()
            return {"success": True, "count": len(result.data)}
        except Exception as e:
            logger.error(f"Failed to upsert projects: {e}")
            return {"success": False, "error": str(e)}
    
    def upsert_subitems(self, subitems: List[Dict]) -> Dict:
        """Upsert multiple subitems"""
        try:
            result = self.client.table('subitems').upsert(
                subitems,
                on_conflict='monday_id'
            ).execute()
            return {"success": True, "count": len(result.data)}
        except Exception as e:
            logger.error(f"Failed to upsert subitems: {e}")
            return {"success": False, "error": str(e)}
    
    def upsert_hidden_items(self, hidden_items: List[Dict]) -> Dict:
        """Upsert multiple hidden items"""
        try:
            result = self.client.table('hidden_items').upsert(
                hidden_items,
                on_conflict='monday_id'
            ).execute()
            return {"success": True, "count": len(result.data)}
        except Exception as e:
            logger.error(f"Failed to upsert hidden items: {e}")
            return {"success": False, "error": str(e)}
    
    def get_last_sync_time(self, board_id: str) -> Optional[datetime]:
        """Get last successful sync time for a board"""
        try:
            result = self.client.table('sync_log')\
                .select('completed_at')\
                .eq('board_id', board_id)\
                .eq('status', 'completed')\
                .order('completed_at', desc=True)\
                .limit(1)\
                .execute()
            
            if result.data:
                return datetime.fromisoformat(result.data[0]['completed_at'])
            return None
        except Exception as e:
            logger.error(f"Failed to get last sync time: {e}")
            return None
    
    def log_sync_operation(
        self, 
        sync_type: str, 
        board_id: str, 
        board_name: str = None
    ) -> str:
        """Create a sync log entry"""
        try:
            result = self.client.table('sync_log').insert({
                'sync_type': sync_type,
                'board_id': board_id,
                'board_name': board_name,
                'started_at': datetime.now().isoformat()
            }).execute()
            return result.data[0]['id']
        except Exception as e:
            logger.error(f"Failed to create sync log: {e}")
            return None
    
    def update_sync_log(
        self, 
        log_id: str, 
        status: str, 
        stats: Dict = None, 
        error: str = None
    ):
        """Update sync log with results"""
        try:
            update_data = {
                'status': status,
                'completed_at': datetime.now().isoformat()
            }
            
            if stats:
                update_data.update({
                    'items_processed': stats.get('processed', 0),
                    'items_created': stats.get('created', 0),
                    'items_updated': stats.get('updated', 0),
                    'items_deleted': stats.get('deleted', 0),
                    'metadata': stats
                })
            
            if error:
                update_data['error_message'] = error
            
            self.client.table('sync_log')\
                .update(update_data)\
                .eq('id', log_id)\
                .execute()
        except Exception as e:
            logger.error(f"Failed to update sync log: {e}")
    
    def get_projects_for_analysis(
        self, 
        filters: Dict = None, 
        limit: int = 100
    ) -> List[Dict]:
        """Get projects for analysis with optional filters"""
        try:
            query = self.client.table('projects').select('*')
            
            if filters:
                for key, value in filters.items():
                    if value is not None:
                        query = query.eq(key, value)
            
            result = query.limit(limit).execute()
            return result.data
        except Exception as e:
            logger.error(f"Failed to get projects: {e}")
            return []
    
    def store_analysis_result(
        self, 
        project_id: str, 
        analysis_result: Dict
    ):
        """Store LLM analysis results"""
        try:
            self.client.table('analysis_results').insert({
                'project_id': project_id,
                **analysis_result,
                'analysis_timestamp': datetime.now().isoformat()
            }).execute()
        except Exception as e:
            logger.error(f"Failed to store analysis result: {e}")
```

#### Step 3.2: Data Sync Service

```python
# src/database/sync_service.py

import asyncio
import json
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd

from ..config import (
    PARENT_BOARD_ID,
    SUBITEM_BOARD_ID,
    HIDDEN_ITEMS_BOARD_ID,
    PARENT_COLUMNS,
    SUBITEM_COLUMNS,
    HIDDEN_ITEMS_COLUMNS,
    SYNC_BATCH_SIZE
)
from ..monday_client import MondayClient
from ..data_processor import LabelNormalizer, EnhancedMirrorResolver
from .supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class DataSyncService:
    """Service for syncing data between Monday and Supabase"""
    
    def __init__(self):
        self.monday_client = MondayClient()
        self.supabase_client = SupabaseClient()
        self.label_normalizer = LabelNormalizer()
        self.mirror_resolver = EnhancedMirrorResolver()
    
    async def perform_full_sync(self) -> Dict[str, Any]:
        """Perform full data sync from Monday to Supabase"""
        logger.info("Starting full sync from Monday to Supabase")
        
        # Create sync log entries
        parent_log_id = self.supabase_client.log_sync_operation(
            'full', PARENT_BOARD_ID, 'Tapered Enquiry Maintenance'
        )
        
        try:
            # Step 1: Get board info
            parent_info = self.monday_client.get_board_info(PARENT_BOARD_ID)
            subitem_info = self.monday_client.get_board_info(SUBITEM_BOARD_ID)
            hidden_info = self.monday_client.get_board_info(HIDDEN_ITEMS_BOARD_ID)
            
            logger.info(f"Boards to sync - Parent: {parent_info['items_count']}, "
                       f"Subitems: {subitem_info['items_count']}, "
                       f"Hidden: {hidden_info['items_count']}")
            
            # Step 2: Extract all data from Monday
            parent_items = await self._extract_all_items(
                PARENT_BOARD_ID, 
                list(PARENT_COLUMNS.values()),
                parent_info['items_count']
            )
            
            subitems = await self._extract_all_items(
                SUBITEM_BOARD_ID,
                list(SUBITEM_COLUMNS.values()),
                subitem_info['items_count']
            )
            
            hidden_items = await self._extract_all_items(
                HIDDEN_ITEMS_BOARD_ID,
                list(HIDDEN_ITEMS_COLUMNS.values()),
                hidden_info['items_count']
            )
            
            # Step 3: Resolve mirrors and process data
            processed_data = self._process_and_resolve_mirrors(
                parent_items, subitems, hidden_items
            )
            
            # Step 4: Transform for Supabase
            projects_data = self._transform_for_projects_table(processed_data['projects'])
            subitems_data = self._transform_for_subitems_table(processed_data['subitems'])
            hidden_data = self._transform_for_hidden_table(processed_data['hidden'])
            
            # Step 5: Batch upload to Supabase
            stats = {
                'processed': len(projects_data) + len(subitems_data) + len(hidden_data),
                'created': 0,
                'updated': 0
            }
            
            # Upload in batches
            for i in range(0, len(projects_data), SYNC_BATCH_SIZE):
                batch = projects_data[i:i + SYNC_BATCH_SIZE]
                result = self.supabase_client.upsert_projects(batch)
                if result['success']:
                    stats['updated'] += result['count']
            
            for i in range(0, len(subitems_data), SYNC_BATCH_SIZE):
                batch = subitems_data[i:i + SYNC_BATCH_SIZE]
                result = self.supabase_client.upsert_subitems(batch)
                if result['success']:
                    stats['updated'] += result['count']
            
            for i in range(0, len(hidden_data), SYNC_BATCH_SIZE):
                batch = hidden_data[i:i + SYNC_BATCH_SIZE]
                result = self.supabase_client.upsert_hidden_items(batch)
                if result['success']:
                    stats['updated'] += result['count']
            
            # Update sync log
            self.supabase_client.update_sync_log(parent_log_id, 'completed', stats)
            
            logger.info(f"Full sync completed. Stats: {stats}")
            return {"success": True, "stats": stats}
            
        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            self.supabase_client.update_sync_log(
                parent_log_id, 'failed', error=str(e)
            )
            return {"success": False, "error": str(e)}
    
    async def perform_delta_sync(self, hours_back: int = 24) -> Dict[str, Any]:
        """Perform delta sync for recent changes"""
        logger.info(f"Starting delta sync for last {hours_back} hours")
        
        # Get last sync time or use hours_back
        last_sync = self.supabase_client.get_last_sync_time(PARENT_BOARD_ID)
        if not last_sync:
            last_sync = datetime.now() - timedelta(hours=hours_back)
        
        log_id = self.supabase_client.log_sync_operation(
            'delta', PARENT_BOARD_ID, 'Delta Sync'
        )
        
        try:
            # Query Monday for items updated since last sync
            updated_items = await self._get_updated_items_since(last_sync)
            
            if not updated_items['projects'] and not updated_items['subitems']:
                logger.info("No items to sync")
                self.supabase_client.update_sync_log(
                    log_id, 'completed', {'processed': 0}
                )
                return {"success": True, "stats": {'processed': 0}}
            
            # Process and sync updated items
            stats = await self._sync_updated_items(updated_items)
            
            self.supabase_client.update_sync_log(log_id, 'completed', stats)
            logger.info(f"Delta sync completed. Stats: {stats}")
            
            return {"success": True, "stats": stats}
            
        except Exception as e:
            logger.error(f"Delta sync failed: {e}")
            self.supabase_client.update_sync_log(
                log_id, 'failed', error=str(e)
            )
            return {"success": False, "error": str(e)}
    
    async def _extract_all_items(
        self, 
        board_id: str, 
        column_ids: List[str],
        total_count: int
    ) -> List[Dict]:
        """Extract all items from a board"""
        items = []
        cursor = None
        
        while True:
            try:
                batch = self.monday_client.get_items_page(
                    board_id, column_ids, 
                    limit=500, cursor=cursor
                )
                
                if batch['items']:
                    items.extend(batch['items'])
                
                cursor = batch.get('next_cursor')
                if not cursor:
                    break
                
                await asyncio.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error extracting batch from {board_id}: {e}")
                break
        
        logger.info(f"Extracted {len(items)} items from board {board_id}")
        return items
    
    def _transform_for_projects_table(self, projects: List[Dict]) -> List[Dict]:
        """Transform Monday data to Supabase projects format"""
        transformed = []
        
        for item in projects:
            try:
                # Extract and normalize values
                project = {
                    'monday_id': item.get('id'),
                    'item_name': item.get('name', ''),
                    'project_name': item.get('project_name', ''),
                    'pipeline_stage': item.get('pipeline_stage', ''),
                    'type': item.get('type', ''),
                    'category': item.get('category', ''),
                    'account': item.get('account', ''),
                    'product_type': item.get('product_type', ''),
                    'new_enquiry_value': float(item.get('new_enquiry_value', 0) or 0),
                    'gestation_period': item.get('gestation_period'),
                    'date_created': item.get('date_created'),
                    'follow_up_date': item.get('follow_up_date'),
                    'last_synced_at': datetime.now().isoformat()
                }
                
                # Clean None values
                project = {k: v for k, v in project.items() if v is not None}
                transformed.append(project)
                
            except Exception as e:
                logger.warning(f"Failed to transform project {item.get('id')}: {e}")
        
        return transformed
    
    def _transform_for_subitems_table(self, subitems: List[Dict]) -> List[Dict]:
        """Transform Monday subitems to Supabase format"""
        transformed = []
        
        for item in subitems:
            try:
                subitem = {
                    'monday_id': item.get('id'),
                    'parent_monday_id': item.get('parent_id'),
                    'item_name': item.get('name', ''),
                    'account': item.get('account', ''),
                    'product_type': item.get('product_type', ''),
                    'new_enquiry_value': float(item.get('new_enquiry_value', 0) or 0),
                    'last_synced_at': datetime.now().isoformat()
                }
                
                transformed.append({k: v for k, v in subitem.items() if v is not None})
                
            except Exception as e:
                logger.warning(f"Failed to transform subitem {item.get('id')}: {e}")
        
        return transformed
    
    def _transform_for_hidden_table(self, hidden_items: List[Dict]) -> List[Dict]:
        """Transform Monday hidden items to Supabase format"""
        transformed = []
        
        for item in hidden_items:
            try:
                hidden = {
                    'monday_id': item.get('id'),
                    'item_name': item.get('name', ''),
                    'quote_amount': float(item.get('quote_amount', 0) or 0),
                    'date_design_completed': item.get('date_design_completed'),
                    'invoice_date': item.get('invoice_date'),
                    'last_synced_at': datetime.now().isoformat()
                }
                
                transformed.append({k: v for k, v in hidden.items() if v is not None})
                
            except Exception as e:
                logger.warning(f"Failed to transform hidden item {item.get('id')}: {e}")
        
        return transformed
    
    def _process_and_resolve_mirrors(
        self, 
        parent_items: List[Dict],
        subitems: List[Dict],
        hidden_items: List[Dict]
    ) -> Dict[str, List[Dict]]:
        """Process and resolve mirror columns"""
        # Use existing mirror resolver logic
        resolver = EnhancedMirrorResolver()
        processed_projects = resolver.resolve_mirrors_with_hidden_items(
            parent_items, subitems, hidden_items
        )
        
        return {
            'projects': processed_projects,
            'subitems': subitems,
            'hidden': hidden_items
        }
```

### **Phase 4: Webhook Service for Real-Time Updates**

#### Step 4.1: Create FastAPI Webhook Server

```python
# src/webhooks/webhook_server.py

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import hmac
import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, Any

from ..database.supabase_client import SupabaseClient
from ..database.sync_service import DataSyncService
from ..config import WEBHOOK_SECRET

app = FastAPI(title="DataCube Webhook Server")
logger = logging.getLogger(__name__)

supabase_client = SupabaseClient()
sync_service = DataSyncService()


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify Monday webhook signature"""
    if not WEBHOOK_SECRET:
        logger.warning("No webhook secret configured")
        return True  # Skip verification in development
    
    expected = hmac.new(
        WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(expected, signature)


@app.post("/webhooks/monday")
async def handle_monday_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Handle Monday.com webhook events - Fast acknowledgement pattern"""
    
    # CRITICAL: Fast acknowledgement - respond immediately with 202
    body = await request.body()
    
    # Verify HMAC signature
    signature = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_webhook_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse payload
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    # Store webhook event immediately
    event_id = data.get('event', {}).get('id')
    event_type = data.get('event', {}).get('type')
    board_id = data.get('event', {}).get('boardId')
    item_id = data.get('event', {}).get('pulseId')
    
    # Log webhook event to Supabase (fast insert)
    try:
        supabase_client.client.table('webhook_events').insert({
            'event_id': event_id,
            'board_id': board_id,
            'item_id': item_id,
            'event_type': event_type,
            'webhook_payload': data,
            'received_at': datetime.now().isoformat(),
            'status': 'pending'
        }).execute()
    except Exception as e:
        logger.error(f"Failed to log webhook event: {e}")
    
    # Process in background task - NO AWAIT here
    background_tasks.add_task(
        process_webhook_event_optimized,
        event_type,
        board_id,
        item_id,
        data
    )
    
    # CRITICAL: Return 202 immediately - Monday expects fast response
    return JSONResponse({"status": "accepted"}, status_code=202)


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Enhanced HMAC verification with proper error handling"""
    if not WEBHOOK_SECRET:
        logger.warning("No webhook secret configured - skipping verification")
        return True  # Development mode
    
    try:
        # Create expected signature using SHA256 HMAC
        expected = hmac.new(
            WEBHOOK_SECRET.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        # Use constant-time comparison to prevent timing attacks
        return hmac.compare_digest(f"sha256={expected}", signature)
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        return False


async def process_webhook_event_optimized(
    event_type: str,
    board_id: str,
    item_id: str,
    payload: Dict[str, Any]
):
    """Optimized webhook processing - minimal updates only"""
    
    try:
        if event_type == "change_column_value":
            # CRITICAL: Only update the specific changed field
            await handle_column_changed_minimal(board_id, item_id, payload)
        
        elif event_type == "create_item":
            # Full item sync for new items
            await handle_item_created(board_id, item_id, payload)
        
        elif event_type == "delete_item":
            await handle_item_deleted(board_id, item_id)
        
        # Mark as processed
        supabase_client.client.table('webhook_events')\
            .update({
                'status': 'processed', 
                'processed_at': datetime.now().isoformat()
            })\
            .eq('event_id', payload.get('event', {}).get('id'))\
            .execute()
            
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        # Mark as failed with error details
        supabase_client.client.table('webhook_events')\
            .update({
                'status': 'failed', 
                'error_message': str(e),
                'processed_at': datetime.now().isoformat()
            })\
            .eq('event_id', payload.get('event', {}).get('id'))\
            .execute()


async def handle_item_created(board_id: str, item_id: str, payload: Dict):
    """Handle new item creation"""
    logger.info(f"Processing new item: {item_id} in board: {board_id}")
    
    # Fetch full item data from Monday
    from ..monday_client import MondayClient
    monday = MondayClient()
    
    # Get item details
    query = f"""
    query {{
        items(ids: [{item_id}]) {{
            id
            name
            column_values {{
                id
                text
                value
            }}
        }}
    }}
    """
    
    result = monday.execute_query(query)
    if result['data']['items']:
        item_data = result['data']['items'][0]
        
        # Transform and insert to Supabase
        if board_id == "1825117125":  # Parent board
            transformed = sync_service._transform_for_projects_table([item_data])
            if transformed:
                supabase_client.upsert_projects(transformed)
        
        elif board_id == "1825117144":  # Subitems board
            transformed = sync_service._transform_for_subitems_table([item_data])
            if transformed:
                supabase_client.upsert_subitems(transformed)


async def handle_column_changed_minimal(board_id: str, item_id: str, payload: Dict):
    """Minimal column update - only update the specific field that changed"""
    
    column_id = payload.get('event', {}).get('columnId')
    new_value = payload.get('event', {}).get('value')
    
    # Get enhanced column mappings
    field_mapping = get_enhanced_column_field_mapping(board_id, column_id)
    
    if not field_mapping:
        logger.debug(f"No mapping for column {column_id} in board {board_id}")
        return
    
    table_name = field_mapping['table']
    field_name = field_mapping['field']
    transform_func = field_mapping.get('transform')
    
    # Transform value if needed
    if transform_func:
        try:
            new_value = transform_func(new_value)
        except Exception as e:
            logger.warning(f"Value transformation failed for {column_id}: {e}")
            return
    
    # CRITICAL: Single field update - very fast
    try:
        supabase_client.client.table(table_name)\
            .update({
                field_name: new_value,
                'last_synced_at': datetime.now().isoformat()
            })\
            .eq('monday_id', item_id)\
            .execute()
        
        logger.debug(f"Updated {field_name} for item {item_id}")
        
    except Exception as e:
        logger.error(f"Failed to update {field_name}: {e}")


def get_enhanced_column_field_mapping(board_id: str, column_id: str) -> Optional[Dict]:
    """Enhanced column mapping with transformation functions"""
    
    # Transform functions for different data types
    def parse_status_value(value_str):
        """Parse Monday status column value"""
        if not value_str:
            return None
        try:
            value_data = json.loads(value_str)
            return value_data.get('index')
        except:
            return None
    
    def parse_dropdown_value(value_str):
        """Parse Monday dropdown column value"""
        if not value_str:
            return None
        try:
            value_data = json.loads(value_str)
            ids = value_data.get('ids', [])
            return ids[0] if ids else None
        except:
            return None
    
    def parse_numeric_value(text_value):
        """Parse numeric values from text"""
        if not text_value:
            return None
        try:
            # Remove currency symbols and parse
            clean_value = str(text_value).replace('£', '').replace(',', '').strip()
            return float(clean_value) if clean_value else None
        except:
            return None
    
    def parse_date_value(text_value):
        """Parse date values"""
        if not text_value:
            return None
        try:
            return datetime.strptime(text_value, '%Y-%m-%d').date().isoformat()
        except:
            return text_value  # Return as-is if parsing fails
    
    # Enhanced mappings with transformations
    mappings = {
        "1825117125": {  # Parent board
            'text3__1': {
                'table': 'projects', 
                'field': 'project_name'
            },
            'status4__1': {
                'table': 'projects', 
                'field': 'pipeline_stage',
                'transform': parse_status_value
            },
            'dropdown7__1': {
                'table': 'projects', 
                'field': 'type',
                'transform': parse_dropdown_value
            },
            'dropdown__1': {
                'table': 'projects', 
                'field': 'category',
                'transform': parse_dropdown_value
            },
            'formula_mkpp85yw': {
                'table': 'projects', 
                'field': 'gestation_period',
                'transform': parse_numeric_value
            },
            'date9__1': {
                'table': 'projects', 
                'field': 'date_created',
                'transform': parse_date_value
            },
            'date_mkpx4163': {
                'table': 'projects', 
                'field': 'follow_up_date',
                'transform': parse_date_value
            }
        },
        "1825117144": {  # Subitems board
            'mirror_12__1': {
                'table': 'subitems', 
                'field': 'account'
            },
            'mirror875__1': {
                'table': 'subitems', 
                'field': 'product_type'
            },
            'formula_mkqa31kh': {
                'table': 'subitems', 
                'field': 'new_enquiry_value',
                'transform': parse_numeric_value
            }
        },
        "1825138260": {  # Hidden items board
            'formula63__1': {
                'table': 'hidden_items', 
                'field': 'quote_amount',
                'transform': parse_numeric_value
            },
            'date__1': {
                'table': 'hidden_items', 
                'field': 'date_design_completed',
                'transform': parse_date_value
            },
            'date42__1': {
                'table': 'hidden_items', 
                'field': 'invoice_date',
                'transform': parse_date_value
            }
        }
    }
    
    return mappings.get(board_id, {}).get(column_id)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}
```

#### Step 4.2: Register Webhooks with Monday

```python
# src/scripts/setup_webhooks.py

import requests
import json
from typing import Dict
import os
from dotenv import load_dotenv

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
WEBHOOK_URL = os.getenv("MONDAY_WEBHOOK_URL")
BOARDS_TO_WATCH = ["1825117125", "1825117144", "1825138260"]


def create_webhook(board_id: str, event: str) -> Dict:
    """Create a webhook for a board and event type"""
    
    query = """
    mutation create_webhook($board_id: ID!, $url: String!, $event: WebhookEventType!) {
        create_webhook(
            board_id: $board_id,
            url: $url,
            event: $event
        ) {
            id
            board_id
        }
    }
    """
    
    variables = {
        "board_id": board_id,
        "url": WEBHOOK_URL,
        "event": event
    }
    
    response = requests.post(
        "https://api.monday.com/v2",
        json={"query": query, "variables": variables},
        headers={
            "Authorization": MONDAY_API_KEY,
            "Content-Type": "application/json"
        }
    )
    
    return response.json()


def setup_all_webhooks():
    """Setup webhooks for all boards and events"""
    
    events = [
        "create_item",
        "change_column_value", 
        "delete_item"
    ]
    
    for board_id in BOARDS_TO_WATCH:
        for event in events:
            result = create_webhook(board_id, event)
            print(f"Created webhook for board {board_id}, event {event}: {result}")


if __name__ == "__main__":
    setup_all_webhooks()
```

### **Phase 5: Modified Data Pipeline Using Supabase**

#### Step 5.1: Update Data Extractor to Use Supabase

```python
# src/data_extractor_supabase.py

import logging
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from datetime import datetime, timedelta

from .database.supabase_client import SupabaseClient
from .data_processor import HierarchicalSegmentation
from .models import ProjectFeatures

logger = logging.getLogger(__name__)


class SupabaseDataExtractor:
    """Data extraction from Supabase instead of Monday API"""
    
    def __init__(self):
        self.supabase = SupabaseClient()
        self.segmentation = HierarchicalSegmentation()
    
    async def get_projects_dataframe(
        self, 
        filters: Dict = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """Get projects data from Supabase as DataFrame"""
        
        # Query Supabase
        query = self.supabase.client.table('projects').select('*')
        
        if filters:
            for key, value in filters.items():
                if value is not None:
                    query = query.eq(key, value)
        
        # Add time filter for relevant data
        cutoff_date = (datetime.now() - timedelta(days=730)).isoformat()
        query = query.gte('date_created', cutoff_date)
        
        result = query.execute()
        
        # Convert to DataFrame
        df = pd.DataFrame(result.data)
        
        # Add value bands
        df = self.segmentation.create_value_bands(df)
        
        logger.info(f"Retrieved {len(df)} projects from Supabase")
        return df
    
    async def get_enhanced_project_context(
        self, 
        project_id: str
    ) -> Dict[str, Any]:
        """Get enhanced context for a specific project"""
        
        # Get project details with related data
        project = self.supabase.client.table('projects')\
            .select('*, subitems(*)')\
            .eq('monday_id', project_id)\
            .single()\
            .execute()
        
        if not project.data:
            raise ValueError(f"Project {project_id} not found")
        
        # Get account performance
        account_stats = self.supabase.client.rpc(
            'get_account_performance',
            {'account_name': project.data['account']}
        ).execute()
        
        # Get similar projects
        similar = self.supabase.client.table('projects')\
            .select('*')\
            .eq('category', project.data['category'])\
            .eq('type', project.data['type'])\
            .neq('monday_id', project_id)\
            .limit(100)\
            .execute()
        
        return {
            'project': project.data,
            'account_stats': account_stats.data,
            'similar_projects': similar.data
        }
```

### **Phase 6: Testing & Deployment**

#### Step 6.1: Test Script

```python
# src/scripts/test_supabase.py

import asyncio
import logging
from datetime import datetime

from ..database.sync_service import DataSyncService
from ..database.supabase_client import SupabaseClient
from ..data_extractor_supabase import SupabaseDataExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_full_pipeline():
    """Test the complete Supabase pipeline"""
    
    print("=" * 50)
    print("TESTING SUPABASE INTEGRATION")
    print("=" * 50)
    
    # Step 1: Test connection
    print("\n1. Testing Supabase connection...")
    supabase = SupabaseClient()
    result = supabase.client.table('projects').select('count').execute()
    print(f"   ✓ Connected. Current projects: {result.data}")
    
    # Step 2: Test sync service
    print("\n2. Testing sync service...")
    sync = DataSyncService()
    
    # Test delta sync (small test)
    delta_result = await sync.perform_delta_sync(hours_back=24)
    print(f"   ✓ Delta sync result: {delta_result}")
    
    # Step 3: Test data extraction
    print("\n3. Testing data extraction from Supabase...")
    extractor = SupabaseDataExtractor()
    df = await extractor.get_projects_dataframe()
    print(f"   ✓ Retrieved {len(df)} projects")
    print(f"   Columns: {df.columns.tolist()}")
    
    # Step 4: Test analysis pipeline
    if len(df) > 0:
        print("\n4. Testing analysis with Supabase data...")
        sample = df.iloc[0]
        context = await extractor.get_enhanced_project_context(
            sample['monday_id']
        )
        print(f"   ✓ Enhanced context retrieved")
        print(f"   Account stats: {context['account_stats']}")
    
    print("\n✅ All tests passed!")


if __name__ == "__main__":
    asyncio.run(test_full_pipeline())
```

#### Step 6.2: Deployment Commands

```bash
# Corrected Deployment Commands

# 1. Setup environment variables
cp .env.example .env
# Edit .env with your Supabase and Monday.com credentials

# 2. Install dependencies
pip install -r requirements.txt

# 3. Deploy database schema (use Supabase dashboard SQL editor)
# Copy contents of src/database/schema/schema.sql to Supabase SQL editor
# Copy contents of src/database/schema/functions.sql to Supabase SQL editor

# 4. Test database connection & setup

**First, test the connection:**
python scripts_new/test_supabase.py

**If needed, truncate all data:**
python scripts_new/truncate_and_prepare.py

**Test with a small batch:**
python scripts_new/test_small_batch_sync.py

# 5. Perform initial full sync
python scripts_new/intelligent_initial_sync.py

**Monitor progress in another terminal:**
python scripts_new/monitor_sync_progress.py

# 6. Setup Monday webhooks  
python scripts/setup_webhooks.py

# 7. Start webhook server
uvicorn src.webhooks.webhook_server:app --host 0.0.0.0 --port 8000

# 8. Schedule periodic delta syncs (cron)
# Add to crontab:
*/15 * * * * cd /path/to/datacube && python scripts/delta_sync.py
```

### **Phase 7: Monitoring & Maintenance**

#### Step 7.1: Create Monitoring Dashboard

```sql
-- src/database/monitoring.sql

-- Create monitoring views
CREATE VIEW sync_health AS
SELECT 
    board_id,
    MAX(completed_at) as last_sync,
    COUNT(*) FILTER (WHERE status = 'completed') as successful_syncs,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_syncs,
    AVG(items_processed) as avg_items_processed,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_seconds
FROM sync_log
WHERE started_at > NOW() - INTERVAL '7 days'
GROUP BY board_id;

CREATE VIEW data_freshness AS
SELECT
    'projects' as table_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour') as fresh_1h,
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours') as fresh_24h,
    MIN(last_synced_at) as oldest_sync,
    MAX(last_synced_at) as newest_sync
FROM projects
UNION ALL
SELECT
    'subitems' as table_name,
    COUNT(*),
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour'),
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours'),
    MIN(last_synced_at),
    MAX(last_synced_at)
FROM subitems;
```

## **Critical Implementation Enhancements & Gotchas**

### **Enhanced Webhook Implementation (Rate Limit Avoidance)**

```python
# src/webhooks/webhook_server.py - Enhanced version

@app.post("/webhooks/monday")
async def handle_monday_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Handle Monday.com webhook events - Fast acknowledgement pattern"""
    
    # CRITICAL: Fast acknowledgement - respond immediately with 202
    body = await request.body()
    
    # Verify HMAC signature
    signature = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_webhook_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse payload
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    
    # Store webhook event immediately
    event_id = data.get('event', {}).get('id')
    event_type = data.get('event', {}).get('type')
    board_id = data.get('event', {}).get('boardId')
    item_id = data.get('event', {}).get('pulseId')
    
    # Log webhook event to Supabase (fast insert)
    try:
        supabase_client.client.table('webhook_events').insert({
            'event_id': event_id,
            'board_id': board_id,
            'item_id': item_id,
            'event_type': event_type,
            'webhook_payload': data,
            'received_at': datetime.now().isoformat(),
            'status': 'pending'
        }).execute()
    except Exception as e:
        logger.error(f"Failed to log webhook event: {e}")
    
    # Process in background task - NO AWAIT here
    background_tasks.add_task(
        process_webhook_event_optimized,
        event_type,
        board_id,
        item_id,
        data
    )
    
    # CRITICAL: Return 202 immediately - Monday expects fast response
    return JSONResponse({"status": "accepted"}, status_code=202)


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Enhanced HMAC verification with proper error handling"""
    if not WEBHOOK_SECRET:
        logger.warning("No webhook secret configured - skipping verification")
        return True  # Development mode
    
    try:
        # Create expected signature using SHA256 HMAC
        expected = hmac.new(
            WEBHOOK_SECRET.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        # Use constant-time comparison to prevent timing attacks
        return hmac.compare_digest(f"sha256={expected}", signature)
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        return False


async def process_webhook_event_optimized(
    event_type: str,
    board_id: str,
    item_id: str,
    payload: Dict[str, Any]
):
    """Optimized webhook processing - minimal updates only"""
    
    try:
        if event_type == "change_column_value":
            # CRITICAL: Only update the specific changed field
            await handle_column_changed_minimal(board_id, item_id, payload)
        
        elif event_type == "create_item":
            # Full item sync for new items
            await handle_item_created(board_id, item_id, payload)
        
        elif event_type == "delete_item":
            await handle_item_deleted(board_id, item_id)
        
        # Mark as processed
        supabase_client.client.table('webhook_events')\
            .update({
                'status': 'processed', 
                'processed_at': datetime.now().isoformat()
            })\
            .eq('event_id', payload.get('event', {}).get('id'))\
            .execute()
            
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        # Mark as failed with error details
        supabase_client.client.table('webhook_events')\
            .update({
                'status': 'failed', 
                'error_message': str(e),
                'processed_at': datetime.now().isoformat()
            })\
            .eq('event_id', payload.get('event', {}).get('id'))\
            .execute()


async def handle_column_changed_minimal(board_id: str, item_id: str, payload: Dict):
    """Minimal column update - only update the specific field that changed"""
    
    column_id = payload.get('event', {}).get('columnId')
    new_value = payload.get('event', {}).get('value')
    
    # Get enhanced column mappings
    field_mapping = get_enhanced_column_field_mapping(board_id, column_id)
    
    if not field_mapping:
        logger.debug(f"No mapping for column {column_id} in board {board_id}")
        return
    
    table_name = field_mapping['table']
    field_name = field_mapping['field']
    transform_func = field_mapping.get('transform')
    
    # Transform value if needed
    if transform_func:
        try:
            new_value = transform_func(new_value)
        except Exception as e:
            logger.warning(f"Value transformation failed for {column_id}: {e}")
            return
    
    # CRITICAL: Single field update - very fast
    try:
        supabase_client.client.table(table_name)\
            .update({
                field_name: new_value,
                'last_synced_at': datetime.now().isoformat()
            })\
            .eq('monday_id', item_id)\
            .execute()
        
        logger.debug(f"Updated {field_name} for item {item_id}")
        
    except Exception as e:
        logger.error(f"Failed to update {field_name}: {e}")


def get_enhanced_column_field_mapping(board_id: str, column_id: str) -> Optional[Dict]:
    """Enhanced column mapping with transformation functions"""
    
    # Transform functions for different data types
    def parse_status_value(value_str):
        """Parse Monday status column value"""
        if not value_str:
            return None
        try:
            value_data = json.loads(value_str)
            return value_data.get('index')
        except:
            return None
    
    def parse_dropdown_value(value_str):
        """Parse Monday dropdown column value"""
        if not value_str:
            return None
        try:
            value_data = json.loads(value_str)
            ids = value_data.get('ids', [])
            return ids[0] if ids else None
        except:
            return None
    
    def parse_numeric_value(text_value):
        """Parse numeric values from text"""
        if not text_value:
            return None
        try:
            # Remove currency symbols and parse
            clean_value = str(text_value).replace('£', '').replace(',', '').strip()
            return float(clean_value) if clean_value else None
        except:
            return None
    
    def parse_date_value(text_value):
        """Parse date values"""
        if not text_value:
            return None
        try:
            return datetime.strptime(text_value, '%Y-%m-%d').date().isoformat()
        except:
            return text_value  # Return as-is if parsing fails
    
    # Enhanced mappings with transformations
    mappings = {
        "1825117125": {  # Parent board
            'text3__1': {
                'table': 'projects', 
                'field': 'project_name'
            },
            'status4__1': {
                'table': 'projects', 
                'field': 'pipeline_stage',
                'transform': parse_status_value
            },
            'dropdown7__1': {
                'table': 'projects', 
                'field': 'type',
                'transform': parse_dropdown_value
            },
            'dropdown__1': {
                'table': 'projects', 
                'field': 'category',
                'transform': parse_dropdown_value
            },
            'formula_mkpp85yw': {
                'table': 'projects', 
                'field': 'gestation_period',
                'transform': parse_numeric_value
            },
            'date9__1': {
                'table': 'projects', 
                'field': 'date_created',
                'transform': parse_date_value
            },
            'date_mkpx4163': {
                'table': 'projects', 
                'field': 'follow_up_date',
                'transform': parse_date_value
            }
        },
        "1825117144": {  # Subitems board
            'mirror_12__1': {
                'table': 'subitems', 
                'field': 'account'
            },
            'mirror875__1': {
                'table': 'subitems', 
                'field': 'product_type'
            },
            'formula_mkqa31kh': {
                'table': 'subitems', 
                'field': 'new_enquiry_value',
                'transform': parse_numeric_value
            }
        },
        "1825138260": {  # Hidden items board
            'formula63__1': {
                'table': 'hidden_items', 
                'field': 'quote_amount',
                'transform': parse_numeric_value
            },
            'date__1': {
                'table': 'hidden_items', 
                'field': 'date_design_completed',
                'transform': parse_date_value
            },
            'date42__1': {
                'table': 'hidden_items', 
                'field': 'invoice_date',
                'transform': parse_date_value
            }
        }
    }
    
    return mappings.get(board_id, {}).get(column_id)
```

### **Enhanced Data Transformation with Schema Alignment**

```python
# src/database/sync_service.py - Enhanced transform functions

def _transform_for_projects_table(self, projects: List[Dict]) -> List[Dict]:
    """Enhanced transform function - reuse for both initial sync and webhook upserts"""
    transformed = []
    
    for item in projects:
        try:
            # Use existing normalization logic from your codebase
            normalized_item = self._normalize_monday_item(item)
            
            project = {
                'monday_id': normalized_item.get('id'),
                'item_name': normalized_item.get('name', ''),
                'project_name': normalized_item.get('project_name', ''),
                'pipeline_stage': self._normalize_pipeline_stage(
                    normalized_item.get('pipeline_stage')
                ),
                'type': self._normalize_type(normalized_item.get('type')),
                'category': self._normalize_category(normalized_item.get('category')),
                'account': normalized_item.get('account', ''),
                'product_type': normalized_item.get('product_type', ''),
                'new_enquiry_value': self._parse_numeric_value(
                    normalized_item.get('new_enquiry_value', 0)
                ),
                'gestation_period': self._parse_gestation_period(
                    normalized_item.get('gestation_period')
                ),
                'date_created': self._parse_date_value(
                    normalized_item.get('date_created')
                ),
                'follow_up_date': self._parse_date_value(
                    normalized_item.get('follow_up_date')
                ),
                'last_synced_at': datetime.now().isoformat()
            }
            
            # Remove None values to avoid database constraint issues
            project = {k: v for k, v in project.items() if v is not None}
            transformed.append(project)
            
        except Exception as e:
            logger.warning(f"Failed to transform project {item.get('id')}: {e}")
    
    return transformed

def _normalize_monday_item(self, item: Dict) -> Dict:
    """Normalize Monday item using existing logic from data_processor"""
    # Reuse your existing normalization logic
    return self.label_normalizer.normalize_item(item)

def _normalize_pipeline_stage(self, stage_value: Any) -> str:
    """Normalize pipeline stage using existing mappings"""
    from ..config import PIPELINE_STAGE_LABELS
    
    if isinstance(stage_value, str) and stage_value.isdigit():
        return PIPELINE_STAGE_LABELS.get(stage_value, stage_value)
    return str(stage_value) if stage_value else ''

def _normalize_type(self, type_value: Any) -> str:
    """Normalize type using existing mappings"""
    from ..config import TYPE_LABELS
    
    if isinstance(type_value, str) and type_value.isdigit():
        return TYPE_LABELS.get(type_value, type_value)
    return str(type_value) if type_value else ''

def _normalize_category(self, category_value: Any) -> str:
    """Normalize category using existing mappings"""
    from ..config import CATEGORY_LABELS
    
    if isinstance(category_value, str) and category_value.isdigit():
        return CATEGORY_LABELS.get(category_value, category_value)
    return str(category_value) if category_value else ''

def _parse_numeric_value(self, value: Any) -> Optional[float]:
    """Parse numeric values with proper error handling"""
    if value is None or value == '':
        return None
    
    try:
        if isinstance(value, str):
            # Remove currency symbols and parse
            clean_value = value.replace('£', '').replace(',', '').strip()
            return float(clean_value) if clean_value else None
        return float(value)
    except (ValueError, TypeError):
        return None

def _parse_gestation_period(self, value: Any) -> Optional[int]:
    """Parse gestation period with validation"""
    if value is None:
        return None
    
    try:
        gestation = int(float(value))
        # Validate range (0-1000 days as per schema)
        if 0 <= gestation < 1000:
            return gestation
        else:
            logger.warning(f"Gestation period out of range: {gestation}")
            return None
    except (ValueError, TypeError):
        return None

def _parse_date_value(self, value: Any) -> Optional[str]:
    """Parse date values with multiple format support"""
    if not value:
        return None
    
    try:
        if isinstance(value, str):
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    return datetime.strptime(value, fmt).date().isoformat()
                except ValueError:
                    continue
        return str(value)  # Return as-is if no format matches
    except Exception:
        return None
```

### **Enhanced Monitoring and Observability**

```python
# src/database/monitoring.py - Enhanced monitoring

class SupabaseMonitor:
    """Enhanced monitoring for Supabase operations"""
    
    def __init__(self, supabase_client: SupabaseClient):
        self.supabase = supabase_client
    
    async def get_sync_health_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive sync health metrics"""
        
        # Webhook processing health
        webhook_health = self.supabase.client.table('webhook_events')\
            .select('status, count(*)')\
            .gte('received_at', (datetime.now() - timedelta(hours=24)).isoformat())\
            .execute()
        
        # Data freshness metrics
        freshness_result = self.supabase.client.rpc('get_data_freshness').execute()
        
        # Sync operation metrics
        sync_metrics = self.supabase.client.table('sync_log')\
            .select('*')\
            .gte('started_at', (datetime.now() - timedelta(days=7)).isoformat())\
            .order('started_at', desc=True)\
            .execute()
        
        return {
            'webhook_health': webhook_health.data,
            'data_freshness': freshness_result.data,
            'sync_metrics': sync_metrics.data,
            'timestamp': datetime.now().isoformat()
        }
    
    async def check_data_consistency(self) -> Dict[str, Any]:
        """Check data consistency between tables"""
        
        # Check for orphaned records
        orphaned_subitems = self.supabase.client.rpc(
            'find_orphaned_subitems'
        ).execute()
        
        # Check for missing mirror resolutions
        missing_mirrors = self.supabase.client.table('projects')\
            .select('monday_id, account, product_type')\
            .or('account.is.null,product_type.is.null')\
            .limit(100)\
            .execute()
        
        return {
            'orphaned_subitems': len(orphaned_subitems.data),
            'missing_mirrors': len(missing_mirrors.data),
            'details': {
                'orphaned': orphaned_subitems.data[:10],  # Sample
                'missing_mirrors': missing_mirrors.data[:10]
            }
        }
```

### **Additional SQL Functions for Enhanced Operations**

```sql
-- src/database/enhanced_functions.sql

-- Function to get data freshness metrics
CREATE OR REPLACE FUNCTION get_data_freshness()
RETURNS TABLE(
    table_name text,
    total_records bigint,
    fresh_1h bigint,
    fresh_24h bigint,
    oldest_sync timestamp with time zone,
    newest_sync timestamp with time zone
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'projects'::text,
        COUNT(*),
        COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour'),
        COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours'),
        MIN(last_synced_at),
        MAX(last_synced_at)
    FROM projects
    UNION ALL
    SELECT 
        'subitems'::text,
        COUNT(*),
        COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour'),
        COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours'),
        MIN(last_synced_at),
        MAX(last_synced_at)
    FROM subitems;
END;
$$ LANGUAGE plpgsql;

-- Function to find orphaned subitems
CREATE OR REPLACE FUNCTION find_orphaned_subitems()
RETURNS TABLE(monday_id text, parent_monday_id text) AS $$
BEGIN
    RETURN QUERY
    SELECT s.monday_id, s.parent_monday_id
    FROM subitems s
    LEFT JOIN projects p ON s.parent_monday_id = p.monday_id
    WHERE p.monday_id IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY conversion_metrics;
    -- Add other materialized views as needed
END;
$$ LANGUAGE plpgsql;
```

## **Key Implementation Gotchas Applied:**

1. **Fast Acknowledgement**: Webhook returns 202 immediately, processes in background
2. **HMAC Verification**: Proper SHA256 HMAC signature verification with `Authorization: Bearer <sha256_hmac(body, WEBHOOK_SECRET)>`
3. **Minimal Updates**: Only update the specific field that changed in `change_column_value` events
4. **Schema Alignment**: Transform functions reuse existing normalization logic for consistency
5. **Observability**: Enhanced monitoring with webhook events tracking and data freshness SLAs

These enhancements ensure you avoid Monday API rate limits while maintaining data consistency and providing comprehensive monitoring capabilities.