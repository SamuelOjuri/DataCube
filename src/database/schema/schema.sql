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
            WHEN pipeline_stage = 'Won - Closed (Invoiced)' THEN 'Won'
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

    -- Order fields
    total_order_value NUMERIC(12, 2),
    date_order_received DATE,
    
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
    cust_order_value_material NUMERIC(12, 2),
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
    status TEXT,
    project_attachments JSONB,
    prefix TEXT,
    revision TEXT,
    project_name TEXT,
    urgency TEXT,
    reason_for_change TEXT,
    approx_bonding TEXT,
    date_received DATE,
    date_design_completed DATE,
    date_quoted DATE,
    date_order_received DATE,
    cust_order_value_material NUMERIC(12, 2),
    date_project_won DATE,
    date_project_closed DATE,
    invoice_date DATE,
    quote_amount NUMERIC(12, 2),
    material_value NUMERIC(12, 2),
    transport_cost NUMERIC(10, 2),
    target_gpm NUMERIC(6, 2),
    tp_margin NUMERIC(6, 2),
    commission NUMERIC(6, 2),
    distributor_margin NUMERIC(6, 2),
    time_taken NUMERIC(6, 2),
    min_thickness NUMERIC(8, 2),
    max_thickness NUMERIC(8, 2),
    volume_m3 NUMERIC(12, 3),
    wastage_percent NUMERIC(6, 2),
    account_id TEXT,
    contact_id TEXT,
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
    error_message TEXT,
    client_ip TEXT,
    processing_time_ms NUMERIC,
    retry_count INTEGER
);

-- Create all indexes separately
-- Projects table indexes
CREATE INDEX IF NOT EXISTS idx_projects_account ON projects (account);
CREATE INDEX IF NOT EXISTS idx_projects_type_category ON projects (type, category);
CREATE INDEX IF NOT EXISTS idx_projects_product_type ON projects (product_type);
CREATE INDEX IF NOT EXISTS idx_projects_status_category ON projects (status_category);
CREATE INDEX IF NOT EXISTS idx_projects_date_created ON projects (date_created);
CREATE INDEX IF NOT EXISTS idx_projects_monday_id ON projects (monday_id);
CREATE INDEX IF NOT EXISTS idx_projects_pipeline_stage ON projects (pipeline_stage);

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


-- Migration: Track order values 
ALTER TABLE hidden_items
  ADD COLUMN IF NOT EXISTS date_order_received DATE;
ALTER TABLE hidden_items
  ADD COLUMN IF NOT EXISTS cust_order_value_material NUMERIC(12, 2);

ALTER TABLE subitems
  ADD COLUMN IF NOT EXISTS cust_order_value_material NUMERIC(12, 2);

ALTER TABLE projects
  ADD COLUMN IF NOT EXISTS total_order_value NUMERIC(12, 2);
ALTER TABLE projects
  ADD COLUMN IF NOT EXISTS date_order_received DATE;

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

-- Queue job tracking
CREATE TABLE IF NOT EXISTS job_queue (
    id UUID PRIMARY KEY,
    job_type TEXT NOT NULL,
    project_id TEXT,
    status TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    payload JSONB,
    detail TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_queue_status ON job_queue (status);
CREATE INDEX IF NOT EXISTS idx_job_queue_project ON job_queue (project_id);

CREATE TRIGGER update_job_queue_timestamp
    BEFORE UPDATE ON job_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();


-- =========================================================
-- Pipeline Forecast Snapshot Table
-- =========================================================

CREATE TABLE IF NOT EXISTS pipeline_forecast_snapshot (
    snapshot_date DATE NOT NULL,
    project_id TEXT NOT NULL,
    forecast_month DATE NOT NULL,
    stage_bucket TEXT NOT NULL,
    contract_value NUMERIC(12,2) NOT NULL DEFAULT 0,
    probability NUMERIC(6,5) NOT NULL DEFAULT 0,
    committed_value NUMERIC(12,2) NOT NULL DEFAULT 0,
    expected_value NUMERIC(12,2) NOT NULL DEFAULT 0,
    best_case_value NUMERIC(12,2) NOT NULL DEFAULT 0,
    worst_case_value NUMERIC(12,2) NOT NULL DEFAULT 0,
    analysis_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, project_id, forecast_month)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_forecast_snapshot_snapshot_date
    ON pipeline_forecast_snapshot (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_pipeline_forecast_snapshot_forecast_month
    ON pipeline_forecast_snapshot (forecast_month);

CREATE INDEX IF NOT EXISTS idx_pipeline_forecast_snapshot_project_id
    ON pipeline_forecast_snapshot (project_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_forecast_snapshot_snapshot_forecast
    ON pipeline_forecast_snapshot (snapshot_date, forecast_month);


###############################

-- =========================================================
-- Forecast artifacts rebuild (correct dependency order)
-- =========================================================

-- Drop dependent artifact first, then base view
DROP MATERIALIZED VIEW IF EXISTS mv_pipeline_forecast_monthly_12m_v1 CASCADE;
DROP VIEW IF EXISTS vw_pipeline_forecast_project_v1;

-- =========================================================
-- Stage 1 + Stage 3: Project-level forecast artifact (centralized formulas)
-- =========================================================

CREATE VIEW vw_pipeline_forecast_project_v1 AS
WITH latest_analysis AS (
    SELECT
        ar.project_id,
        ar.analysis_timestamp,
        ar.expected_gestation_days,
        ar.expected_conversion_rate,
        ar.conversion_confidence,
        ar.rating_score,
        ROW_NUMBER() OVER (
            PARTITION BY ar.project_id
            ORDER BY ar.analysis_timestamp DESC NULLS LAST, ar.id DESC
        ) AS rn
    FROM analysis_results ar
),
project_base AS (
    SELECT
        p.id AS project_uuid,
        p.monday_id AS project_id,
        p.item_name,
        COALESCE(NULLIF(p.project_name, ''), p.item_name) AS project_name,
        p.account,
        p.type,
        p.category,
        p.product_type,
        p.pipeline_stage,

        CASE
            WHEN p.pipeline_stage IN ('Won - Closed (Invoiced)', 'Won - Open (Order Received)', 'Won Via Other Ref') THEN 'Committed'
            WHEN p.pipeline_stage = 'Lost' THEN 'Lost'
            ELSE 'Open'
        END AS stage_bucket,

        COALESCE(NULLIF(p.total_order_value, 0), NULLIF(p.new_enquiry_value, 0), 0)::NUMERIC(12,2) AS contract_value,

        p.total_order_value,
        p.new_enquiry_value,
        p.probability_percent,
        p.date_order_received,
        p.expected_start_date,
        p.follow_up_date,
        p.date_created,

        la.analysis_timestamp,
        la.expected_gestation_days,
        la.expected_conversion_rate,
        la.conversion_confidence,
        la.rating_score,

        CASE
            WHEN p.date_order_received IS NOT NULL THEN p.date_order_received
            WHEN p.expected_start_date IS NOT NULL THEN p.expected_start_date
            WHEN p.follow_up_date IS NOT NULL THEN p.follow_up_date
            WHEN p.date_created IS NOT NULL AND la.expected_gestation_days IS NOT NULL
                THEN (p.date_created + GREATEST(la.expected_gestation_days, 0))
            WHEN p.date_created IS NOT NULL THEN p.date_created
            ELSE CURRENT_DATE
        END AS forecast_date,

        CASE
            WHEN p.date_order_received IS NOT NULL THEN 'date_order_received'
            WHEN p.expected_start_date IS NOT NULL THEN 'expected_start_date'
            WHEN p.follow_up_date IS NOT NULL THEN 'follow_up_date'
            WHEN p.date_created IS NOT NULL AND la.expected_gestation_days IS NOT NULL THEN 'date_created_plus_expected_gestation_days'
            WHEN p.date_created IS NOT NULL THEN 'date_created'
            ELSE 'current_date_default'
        END AS forecast_date_source
    FROM projects p
    LEFT JOIN latest_analysis la
        ON la.project_id = p.monday_id
       AND la.rn = 1
),
probability_base AS (
    SELECT
        pb.*,

        GREATEST(
            0.0::NUMERIC,
            LEAST(
                1.0::NUMERIC,
                CASE
                    WHEN pb.stage_bucket = 'Lost' THEN 0.0
                    WHEN pb.stage_bucket = 'Committed' THEN 1.0
                    WHEN pb.expected_conversion_rate IS NOT NULL THEN pb.expected_conversion_rate
                    WHEN pb.probability_percent IS NOT NULL THEN (pb.probability_percent::NUMERIC / 100.0)
                    ELSE
                        CASE pb.pipeline_stage
                            WHEN 'Customer Confident Of Project Success' THEN 0.20
                            WHEN 'Customer Has Order' THEN 0.50
                            WHEN 'Customer Has Order - TP Preferred Supplier' THEN 0.75
                            WHEN 'Open Enquiry' THEN 0.00
                            WHEN 'Check with IS' THEN 0.30
                            ELSE 0.50
                        END
                END
            )
        ) AS probability,

        CASE
            WHEN pb.conversion_confidence IS NOT NULL AND pb.conversion_confidence > 0 THEN
                LEAST(
                    0.35::NUMERIC,
                    GREATEST(0.05::NUMERIC, (1.0::NUMERIC - pb.conversion_confidence) * 0.30::NUMERIC)
                )
            ELSE
                CASE pb.pipeline_stage
                    WHEN 'Customer Has Order - TP Preferred Supplier' THEN 0.08::NUMERIC
                    WHEN 'Customer Has Order' THEN 0.12::NUMERIC
                    WHEN 'Customer Confident Of Project Success' THEN 0.18::NUMERIC
                    WHEN 'Check with IS' THEN 0.20::NUMERIC
                    WHEN 'Open Enquiry' THEN 0.25::NUMERIC
                    ELSE 0.15::NUMERIC
                END
        END AS probability_spread
    FROM project_base pb
),
project_bands AS (
    SELECT
        p.*,

        CASE
            WHEN p.stage_bucket = 'Committed' THEN 1.0::NUMERIC
            WHEN p.stage_bucket = 'Lost' THEN 0.0::NUMERIC
            ELSE GREATEST(0.0::NUMERIC, LEAST(1.0::NUMERIC, p.probability + p.probability_spread))
        END AS best_case_probability,

        CASE
            WHEN p.stage_bucket = 'Committed' THEN 1.0::NUMERIC
            WHEN p.stage_bucket = 'Lost' THEN 0.0::NUMERIC
            ELSE GREATEST(0.0::NUMERIC, LEAST(1.0::NUMERIC, p.probability - p.probability_spread))
        END AS worst_case_probability
    FROM probability_base p
)
SELECT
    b.project_uuid,
    b.project_id,
    b.item_name,
    b.project_name,
    b.account,
    b.type,
    b.category,
    b.product_type,
    b.pipeline_stage,
    b.stage_bucket,
    b.contract_value,
    b.total_order_value,
    b.new_enquiry_value,
    b.date_created,
    b.probability,

    b.forecast_date,
    DATE_TRUNC('month', b.forecast_date)::DATE AS forecast_month,
    b.forecast_date_source,

    b.analysis_timestamp,
    b.expected_gestation_days,
    b.expected_conversion_rate,
    b.conversion_confidence,
    b.rating_score,
    b.probability_percent,

    ROUND(
        CASE WHEN b.stage_bucket = 'Committed' THEN b.contract_value ELSE 0::NUMERIC END,
        2
    )::NUMERIC(12,2) AS committed_value,

    ROUND((b.contract_value * b.probability)::NUMERIC, 2)::NUMERIC(12,2) AS expected_value,
    ROUND((b.contract_value * b.best_case_probability)::NUMERIC, 2)::NUMERIC(12,2) AS best_case_value,
    ROUND((b.contract_value * b.worst_case_probability)::NUMERIC, 2)::NUMERIC(12,2) AS worst_case_value,

    b.best_case_probability,
    b.worst_case_probability,
    b.probability_spread
FROM project_bands b;

-- =========================================================
-- Stage 2 + Stage 3: Monthly aggregate sourced from centralized project formulas
-- =========================================================

CREATE MATERIALIZED VIEW mv_pipeline_forecast_monthly_12m_v1 AS
WITH bounds AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE)::DATE AS window_start,
        (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '12 months')::DATE AS window_end
)
SELECT
    f.forecast_month,
    f.stage_bucket,
    COUNT(*) AS project_count,
    SUM(f.contract_value)::NUMERIC(14,2) AS contract_value,
    SUM(f.committed_value)::NUMERIC(14,2) AS committed_value,
    SUM(f.expected_value)::NUMERIC(14,2) AS expected_value,
    SUM(f.best_case_value)::NUMERIC(14,2) AS best_case_value,
    SUM(f.worst_case_value)::NUMERIC(14,2) AS worst_case_value
FROM vw_pipeline_forecast_project_v1 f
CROSS JOIN bounds b
WHERE f.forecast_month >= b.window_start
  AND f.forecast_month < b.window_end
GROUP BY f.forecast_month, f.stage_bucket
ORDER BY f.forecast_month, f.stage_bucket;

-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pipeline_forecast_monthly_12m_v1_unique
    ON mv_pipeline_forecast_monthly_12m_v1 (forecast_month, stage_bucket);

CREATE INDEX IF NOT EXISTS idx_mv_pipeline_forecast_monthly_12m_v1_forecast_month
    ON mv_pipeline_forecast_monthly_12m_v1 (forecast_month);