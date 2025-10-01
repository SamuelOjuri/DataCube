-- Enhanced monitoring views and functions for DataCube

-- Sync health monitoring view
CREATE OR REPLACE VIEW sync_health AS
SELECT 
    board_id,
    board_name,
    MAX(completed_at) as last_sync,
    COUNT(*) as total_syncs,
    COUNT(*) FILTER (WHERE status = 'completed') as successful_syncs,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_syncs,
    COUNT(*) FILTER (WHERE status = 'running') as running_syncs,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'completed')::numeric / 
        NULLIF(COUNT(*), 0) * 100, 2
    ) as success_rate_percent,
    AVG(items_processed) FILTER (WHERE status = 'completed') as avg_items_processed,
    AVG(
        EXTRACT(EPOCH FROM (completed_at - started_at))
    ) FILTER (WHERE status = 'completed') as avg_duration_seconds,
    MAX(started_at) as last_sync_attempt
FROM sync_log
WHERE started_at > NOW() - INTERVAL '7 days'
GROUP BY board_id, board_name;

-- Data freshness monitoring
CREATE OR REPLACE VIEW data_freshness AS
SELECT
    'projects' as table_name,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour') as fresh_1h,
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours') as fresh_24h,
    COUNT(*) FILTER (WHERE last_synced_at < NOW() - INTERVAL '24 hours') as stale_24h,
    MIN(last_synced_at) as oldest_sync,
    MAX(last_synced_at) as newest_sync,
    ROUND(
        EXTRACT(EPOCH FROM (NOW() - (NOW() - AVG(EXTRACT(EPOCH FROM (NOW() - last_synced_at))) * INTERVAL '1 second'))) / 3600, 2
    ) as avg_age_hours
FROM projects
UNION ALL
SELECT
    'subitems' as table_name,
    COUNT(*),
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour'),
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours'),
    COUNT(*) FILTER (WHERE last_synced_at < NOW() - INTERVAL '24 hours'),
    MIN(last_synced_at),
    MAX(last_synced_at),
    ROUND(
        AVG(EXTRACT(EPOCH FROM (NOW() - last_synced_at))) / 3600, 2
    )
FROM subitems
UNION ALL
SELECT
    'hidden_items' as table_name,
    COUNT(*),
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '1 hour'),
    COUNT(*) FILTER (WHERE last_synced_at > NOW() - INTERVAL '24 hours'),
    COUNT(*) FILTER (WHERE last_synced_at < NOW() - INTERVAL '24 hours'),
    MIN(last_synced_at),
    MAX(last_synced_at),
    ROUND(
        AVG(EXTRACT(EPOCH FROM (NOW() - last_synced_at))) / 3600, 2
    )
FROM hidden_items;

-- Webhook processing metrics
CREATE OR REPLACE VIEW webhook_metrics AS
SELECT
    event_type,
    board_id,
    COUNT(*) as total_events,
    COUNT(*) FILTER (WHERE status = 'processed') as processed_events,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_events,
    COUNT(*) FILTER (WHERE status = 'pending') as pending_events,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'processed')::numeric / 
        NULLIF(COUNT(*), 0) * 100, 2
    ) as success_rate_percent,
    AVG(
        EXTRACT(EPOCH FROM (processed_at - received_at))
    ) FILTER (WHERE processed_at IS NOT NULL) as avg_processing_seconds,
    MAX(received_at) as last_webhook_received
FROM webhook_events
WHERE received_at > NOW() - INTERVAL '24 hours'
GROUP BY event_type, board_id;

-- System performance overview
CREATE OR REPLACE VIEW system_performance AS
SELECT
    'Database Size' as metric,
    pg_size_pretty(pg_database_size(current_database())) as value,
    'info' as status
UNION ALL
SELECT
    'Active Connections' as metric,
    COUNT(*)::text as value,
    CASE 
        WHEN COUNT(*) > 80 THEN 'warning'
        WHEN COUNT(*) > 50 THEN 'caution'
        ELSE 'healthy'
    END as status
FROM pg_stat_activity
WHERE state = 'active'
UNION ALL
SELECT
    'Failed Syncs (24h)' as metric,
    COUNT(*)::text as value,
    CASE 
        WHEN COUNT(*) > 10 THEN 'critical'
        WHEN COUNT(*) > 5 THEN 'warning'
        ELSE 'healthy'
    END as status
FROM sync_log
WHERE started_at > NOW() - INTERVAL '24 hours' AND status = 'failed';

-- Function to get account performance
CREATE OR REPLACE FUNCTION get_account_performance(account_name TEXT)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'account', account_name,
        'total_projects', COUNT(*),
        'won_projects', COUNT(*) FILTER (WHERE status_category = 'Won'),
        'lost_projects', COUNT(*) FILTER (WHERE status_category = 'Lost'),
        'open_projects', COUNT(*) FILTER (WHERE status_category = 'Open'),
        'win_rate', ROUND(
            COUNT(*) FILTER (WHERE status_category = 'Won')::numeric / 
            NULLIF(COUNT(*), 0), 3
        ),
        'avg_project_value', ROUND(AVG(new_enquiry_value), 2),
        'total_value', ROUND(SUM(new_enquiry_value), 2),
        'avg_gestation_days', ROUND(AVG(gestation_period) FILTER (WHERE gestation_period > 0), 1),
        'recent_activity', COUNT(*) FILTER (WHERE date_created > CURRENT_DATE - INTERVAL '90 days')
    ) INTO result
    FROM projects
    WHERE account = account_name
    AND date_created > CURRENT_DATE - INTERVAL '2 years';
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS TEXT AS $$
BEGIN
    REFRESH MATERIALIZED VIEW conversion_metrics;
    REFRESH MATERIALIZED VIEW conversion_metrics_recent;
    RETURN 'Analytics views refreshed at ' || NOW()::text;
END;
$$ LANGUAGE plpgsql;

-- Function to clean old webhook events
CREATE OR REPLACE FUNCTION cleanup_old_webhook_events()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM webhook_events 
    WHERE received_at < NOW() - INTERVAL '30 days'
    AND status IN ('processed', 'failed');
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
