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
    gestation_period INTEGER CHECK (gestation_period >= 0 AND gestation_period <= 500000),
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
    sync_version INTEGER DEFAULT 1
);

-- Subitems table (Subitem Board: 1825117144)
CREATE TABLE subitems (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    monday_id TEXT UNIQUE NOT NULL,
    parent_monday_id TEXT REFERENCES projects(monday_id) ON DELETE CASCADE,
    item_name TEXT,
    
    -- Connected board relations
    hidden_item_id TEXT REFERENCES hidden_items(monday_id) ON DELETE SET NULL,
    
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
    reason_for_change TEXT,
    
    -- Dates
    date_received DATE,
    date_design_completed DATE,
    date_quoted DATE,
    date_project_won DATE,
    date_project_closed DATE,
    invoice_date DATE,
    
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
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sync tracking table
CREATE TABLE sync_log (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sync_type TEXT NOT NULL CHECK (sync_type IN ('full', 'delta', 'webhook', 'reset')),
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
    rating_score INTEGER CHECK (rating_score >= 1 AND rating_score <= 100),
    
    -- Reasoning from LLM
    reasoning JSONB,
    adjustments JSONB,
    confidence_notes TEXT,
    special_factors TEXT,
    
    -- Metadata
    llm_model TEXT,
    analysis_version TEXT,
    processing_time_ms INTEGER
);

-- Create indexes separately
CREATE INDEX idx_analysis_project_id ON analysis_results (project_id);
CREATE INDEX idx_analysis_timestamp ON analysis_results (analysis_timestamp DESC);

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
    error_message TEXT
);

-- Create all indexes separately
-- Projects table indexes
CREATE INDEX IF NOT EXISTS idx_projects_account ON projects (account);
CREATE INDEX IF NOT EXISTS idx_projects_type_category ON projects (type, category);
CREATE INDEX IF NOT EXISTS idx_projects_product_type ON projects (product_type);
CREATE INDEX IF NOT EXISTS idx_projects_status_category ON projects (status_category);
CREATE INDEX IF NOT EXISTS idx_projects_date_created ON projects (date_created);
CREATE INDEX IF NOT EXISTS idx_projects_monday_id ON projects (monday_id);

-- Subitems table indexes
CREATE INDEX IF NOT EXISTS idx_subitems_parent ON subitems (parent_monday_id);
CREATE INDEX IF NOT EXISTS idx_subitems_hidden_item ON subitems (hidden_item_id);

-- Hidden items table indexes
CREATE INDEX IF NOT EXISTS idx_hidden_items_monday_id ON hidden_items (monday_id);
CREATE INDEX IF NOT EXISTS idx_hidden_items_date_received ON hidden_items (date_received);
CREATE INDEX IF NOT EXISTS idx_hidden_items_invoice_date ON hidden_items (invoice_date);

-- Analysis results indexes
CREATE INDEX IF NOT EXISTS idx_analysis_project_id ON analysis_results (project_id);
CREATE INDEX IF NOT EXISTS idx_analysis_timestamp ON analysis_results (analysis_timestamp DESC);

-- Webhook events indexes
CREATE INDEX IF NOT EXISTS idx_webhook_events_status ON webhook_events (status);
CREATE INDEX IF NOT EXISTS idx_webhook_events_received ON webhook_events (received_at DESC);

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
-- Use closed-won only as the "won" population
DROP MATERIALIZED VIEW IF EXISTS conversion_metrics CASCADE;

CREATE MATERIALIZED VIEW conversion_metrics AS
SELECT 
    NULLIF(TRIM(account), '')        as account,
    NULLIF(TRIM(type), '')           as type,
    NULLIF(TRIM(category), '')       as category,
    NULLIF(TRIM(product_type), '')   as product_type,
    COUNT(*) as total_projects,
    COUNT(*) FILTER (WHERE pipeline_stage = 'Won - Closed (Invoiced)') as won_projects,
    COUNT(*) FILTER (WHERE pipeline_stage = 'Lost') as lost_projects,
    COUNT(*) FILTER (WHERE pipeline_stage NOT IN ('Won - Closed (Invoiced)', 'Lost')) as open_projects,
    ROUND(
        COUNT(*) FILTER (WHERE pipeline_stage = 'Won - Closed (Invoiced)')::numeric /
        NULLIF(COUNT(*), 0), 
        3
    ) as win_rate,  -- inclusive: won_closed / all
    ROUND(
        COUNT(*) FILTER (WHERE pipeline_stage = 'Won - Closed (Invoiced)')::numeric /
        NULLIF((COUNT(*) FILTER (WHERE pipeline_stage IN ('Won - Closed (Invoiced)', 'Lost'))), 0),
        3
    ) as closed_win_rate,
    AVG(gestation_period) FILTER (WHERE gestation_period > 0) as avg_gestation,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY gestation_period)
        FILTER (WHERE gestation_period > 0) as gestation_p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gestation_period) 
        FILTER (WHERE gestation_period > 0) as median_gestation,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY gestation_period)
        FILTER (WHERE gestation_period > 0) as gestation_p75
FROM projects
WHERE date_created >= CURRENT_DATE - INTERVAL '5 years'
GROUP BY 1,2,3,4;

CREATE UNIQUE INDEX IF NOT EXISTS conversion_metrics_unique
  ON conversion_metrics (account, type, category, product_type);


DROP MATERIALIZED VIEW IF EXISTS conversion_metrics_recent CASCADE;

CREATE MATERIALIZED VIEW conversion_metrics_recent AS
SELECT 
    NULLIF(TRIM(account), '')        as account,
    NULLIF(TRIM(type), '')           as type,
    NULLIF(TRIM(category), '')       as category,
    NULLIF(TRIM(product_type), '')   as product_type,
    COUNT(*) as total_projects,
    COUNT(*) FILTER (WHERE pipeline_stage = 'Won - Closed (Invoiced)') as won_projects,
    COUNT(*) FILTER (WHERE pipeline_stage = 'Lost') as lost_projects,
    COUNT(*) FILTER (WHERE pipeline_stage NOT IN ('Won - Closed (Invoiced)', 'Lost')) as open_projects,
    ROUND(
        COUNT(*) FILTER (WHERE pipeline_stage = 'Won - Closed (Invoiced)')::numeric /
        NULLIF(COUNT(*), 0), 
        3
    ) as win_rate,
    ROUND(
        COUNT(*) FILTER (WHERE pipeline_stage = 'Won - Closed (Invoiced)')::numeric /
        NULLIF((COUNT(*) FILTER (WHERE pipeline_stage IN ('Won - Closed (Invoiced)', 'Lost'))), 0),
        3
    ) as closed_win_rate,
    AVG(gestation_period) FILTER (WHERE gestation_period > 0) as avg_gestation,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY gestation_period)
        FILTER (WHERE gestation_period > 0) as gestation_p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gestation_period) 
        FILTER (WHERE gestation_period > 0) as median_gestation,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY gestation_period)
        FILTER (WHERE gestation_period > 0) as gestation_p75
FROM projects
WHERE date_created >= CURRENT_DATE - INTERVAL '2 years'
GROUP BY 1,2,3,4;

CREATE UNIQUE INDEX IF NOT EXISTS conversion_metrics_recent_unique
  ON conversion_metrics_recent (account, type, category, product_type);

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



-- Enforce one row per project
ALTER TABLE analysis_results
  ADD CONSTRAINT IF NOT EXISTS analysis_results_project_unique UNIQUE (project_id);

-- Migration (run on your Supabase/Postgres)
ALTER TABLE analysis_results
  DROP CONSTRAINT IF EXISTS analysis_results_rating_score_check;

ALTER TABLE analysis_results
  ADD CONSTRAINT analysis_results_rating_score_check
  CHECK (rating_score >= 1 AND rating_score <= 100);

-- Optional backfill to scale existing 1–10 scores up to 1–100:
UPDATE analysis_results
SET rating_score = 1 + ((rating_score - 1) * 11)
WHERE rating_score BETWEEN 1 AND 10;

