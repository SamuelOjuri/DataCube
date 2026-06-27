-- Database maintenance and snapshot functions

DROP FUNCTION IF EXISTS public.refresh_analytics_views();

CREATE OR REPLACE FUNCTION public.refresh_analytics_views()
RETURNS void AS $$
BEGIN
    IF to_regclass('public.mv_pipeline_velocity_stats_v1') IS NOT NULL THEN
        REFRESH MATERIALIZED VIEW public.mv_pipeline_velocity_stats_v1;
    END IF;

    IF to_regclass('public.mv_quote_conversion_stats_v1') IS NOT NULL THEN
        REFRESH MATERIALIZED VIEW public.mv_quote_conversion_stats_v1;
    END IF;

    IF to_regclass('public.mv_invoice_smoothing_signal_v1') IS NOT NULL
       AND to_regprocedure('public.refresh_invoice_smoothing_signal_v1(date)') IS NOT NULL THEN
        PERFORM public.refresh_invoice_smoothing_signal_v1(CURRENT_DATE);
    END IF;

    IF to_regclass('public.mv_pipeline_smoothed_revenue_monthly_12m_v1') IS NOT NULL THEN
        REFRESH MATERIALIZED VIEW public.mv_pipeline_smoothed_revenue_monthly_12m_v1;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_conversion_views()
RETURNS void AS $$
BEGIN
    IF to_regclass('public.mv_quote_conversion_stats_v1') IS NOT NULL THEN
        REFRESH MATERIALIZED VIEW mv_quote_conversion_stats_v1;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_pipeline_forecast_snapshot()
RETURNS void AS $$
BEGIN
    INSERT INTO pipeline_forecast_snapshot (
        forecast_month,
        forecast_value_net,
        forecast_value_gross,
        project_count
    )
    SELECT
        forecast_month,
        SUM(forecast_value_net),
        SUM(forecast_value_gross),
        COUNT(*)
    FROM vw_pipeline_forecast_monthly_12m_v1
    GROUP BY forecast_month;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS public.cleanup_old_pipeline_smoothing_forecast_snapshots(INTEGER);

CREATE OR REPLACE FUNCTION cleanup_old_pipeline_smoothing_forecast_snapshots(
    retain_days INTEGER DEFAULT 730
)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    IF retain_days IS NULL OR retain_days < 1 THEN
        RAISE EXCEPTION 'retain_days must be >= 1, got %', retain_days;
    END IF;

    DELETE FROM pipeline_smoothing_forecast_snapshot
    WHERE snapshot_date < (CURRENT_DATE - retain_days);

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS public.create_pipeline_smoothing_forecast_snapshot();
DROP FUNCTION IF EXISTS public.create_pipeline_smoothing_forecast_snapshot(DATE);

CREATE OR REPLACE FUNCTION create_pipeline_smoothing_forecast_snapshot(
    target_snapshot_date DATE DEFAULT CURRENT_DATE
)
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER := 0;
BEGIN
    DELETE FROM pipeline_smoothing_forecast_snapshot
    WHERE snapshot_date = target_snapshot_date;

    INSERT INTO pipeline_smoothing_forecast_snapshot (
        snapshot_date,
        project_id,
        forecast_month,
        stage_bucket,
        base_forecast_month,
        forecast_date,
        smoothing_as_of_date,
        forecast_value_net,
        forecast_value_gross,
        expected_value,
        combined_smoothed_probability,
        expected_spread_days,
        risk_band,
        workbook_suggested_treatment,
        default_smoothing_recommended,
        unsmoothed_allocated_value,
        smoothed_allocated_value,
        allocated_expected_value,
        project_count,
        source_view,
        created_at
    )
    WITH bounds AS (
        SELECT
            DATE_TRUNC('month', target_snapshot_date)::DATE AS window_start,
            (DATE_TRUNC('month', target_snapshot_date) + INTERVAL '12 months')::DATE AS window_end
    ),
    scored_projects AS (
        SELECT
            s.project_id,
            s.stage_bucket,
            s.forecast_date::DATE AS forecast_date,
            s.forecast_month::DATE AS base_forecast_month,
            s.smoothing_as_of_date,
            COALESCE(s.expected_value, 0::NUMERIC)::NUMERIC AS expected_value,
            GREATEST(
                0::NUMERIC,
                LEAST(1::NUMERIC, COALESCE(s.combined_smoothed_probability, 0::NUMERIC))
            ) AS combined_smoothed_probability,
            GREATEST(0::NUMERIC, COALESCE(s.expected_spread_days, 0::NUMERIC)) AS expected_spread_days,
            s.risk_band,
            s.workbook_suggested_treatment,
            s.default_smoothing_recommended
        FROM vw_pipeline_smoothing_score_v1 s
        CROSS JOIN bounds b
        WHERE s.forecast_month >= b.window_start
          AND s.forecast_month < b.window_end
    ),
    project_values AS (
        SELECT
            sp.*,
            (sp.expected_value * sp.combined_smoothed_probability) AS smoothed_value,
            (sp.expected_value * (1::NUMERIC - sp.combined_smoothed_probability)) AS unsmoothed_value,
            GREATEST(CEIL(sp.expected_spread_days)::INTEGER, 1) AS spread_day_count
        FROM scored_projects sp
    ),
    allocation_periods AS (
        SELECT
            pv.*,
            b.window_end,
            LEAST((pv.forecast_date + pv.spread_day_count)::DATE, b.window_end) AS allocation_end_date
        FROM project_values pv
        CROSS JOIN bounds b
    ),
    allocation_periods_with_days AS (
        SELECT
            ap.*,
            GREATEST(
                EXTRACT(EPOCH FROM (ap.allocation_end_date::TIMESTAMP - ap.forecast_date::TIMESTAMP))
                    / 86400::NUMERIC,
                1::NUMERIC
            ) AS allocation_day_count
        FROM allocation_periods ap
    ),
    allocation_rows AS (
        SELECT
            ap.project_id,
            ap.stage_bucket,
            ap.base_forecast_month AS forecast_month,
            ap.base_forecast_month,
            ap.forecast_date,
            ap.smoothing_as_of_date,
            ap.expected_value,
            ap.combined_smoothed_probability,
            ap.expected_spread_days,
            ap.risk_band,
            ap.workbook_suggested_treatment,
            ap.default_smoothing_recommended,
            ap.unsmoothed_value AS unsmoothed_allocated_value,
            0::NUMERIC AS smoothed_allocated_value
        FROM allocation_periods_with_days ap

        UNION ALL

        SELECT
            ap.project_id,
            ap.stage_bucket,
            allocation_month.month_start::DATE AS forecast_month,
            ap.base_forecast_month,
            ap.forecast_date,
            ap.smoothing_as_of_date,
            ap.expected_value,
            ap.combined_smoothed_probability,
            ap.expected_spread_days,
            ap.risk_band,
            ap.workbook_suggested_treatment,
            ap.default_smoothing_recommended,
            0::NUMERIC AS unsmoothed_allocated_value,
            (
                ap.smoothed_value *
                GREATEST(
                    EXTRACT(EPOCH FROM (
                        LEAST(
                            ap.allocation_end_date::TIMESTAMP,
                            (allocation_month.month_start + INTERVAL '1 month')::TIMESTAMP
                        ) - GREATEST(
                            ap.forecast_date::TIMESTAMP,
                            allocation_month.month_start::TIMESTAMP
                        )
                    )) / 86400::NUMERIC,
                    0::NUMERIC
                ) / ap.allocation_day_count
            ) AS smoothed_allocated_value
        FROM allocation_periods_with_days ap
        CROSS JOIN LATERAL GENERATE_SERIES(
            DATE_TRUNC('month', ap.forecast_date)::DATE,
            DATE_TRUNC('month', (ap.allocation_end_date - INTERVAL '1 day')::DATE)::DATE,
            INTERVAL '1 month'
        ) AS allocation_month(month_start)
    ),
    grouped_allocations AS (
        SELECT
            ar.project_id,
            ar.forecast_month,
            MIN(ar.stage_bucket) AS stage_bucket,
            MIN(ar.base_forecast_month) AS base_forecast_month,
            MIN(ar.forecast_date) AS forecast_date,
            MIN(ar.smoothing_as_of_date) AS smoothing_as_of_date,
            MAX(ar.expected_value) AS expected_value,
            MAX(ar.combined_smoothed_probability) AS combined_smoothed_probability,
            MAX(ar.expected_spread_days) AS expected_spread_days,
            MAX(ar.risk_band) AS risk_band,
            MAX(ar.workbook_suggested_treatment) AS workbook_suggested_treatment,
            BOOL_OR(COALESCE(ar.default_smoothing_recommended, FALSE)) AS default_smoothing_recommended,
            SUM(ar.unsmoothed_allocated_value) AS unsmoothed_allocated_value,
            SUM(ar.smoothed_allocated_value) AS smoothed_allocated_value,
            SUM(ar.unsmoothed_allocated_value + ar.smoothed_allocated_value) AS allocated_expected_value
        FROM allocation_rows ar
        CROSS JOIN bounds b
        WHERE ar.forecast_month >= b.window_start
          AND ar.forecast_month < b.window_end
        GROUP BY ar.project_id, ar.forecast_month
    )
    SELECT
        target_snapshot_date,
        ga.project_id,
        ga.forecast_month,
        ga.stage_bucket,
        ga.base_forecast_month,
        ga.forecast_date,
        ga.smoothing_as_of_date,
        ROUND(ga.allocated_expected_value, 2)::NUMERIC(18, 2) AS forecast_value_net,
        ROUND(ga.allocated_expected_value, 2)::NUMERIC(18, 2) AS forecast_value_gross,
        ROUND(ga.expected_value, 2)::NUMERIC(12, 2) AS expected_value,
        ROUND(ga.combined_smoothed_probability, 6)::NUMERIC(8, 6) AS combined_smoothed_probability,
        ROUND(ga.expected_spread_days, 2)::NUMERIC(10, 2) AS expected_spread_days,
        ga.risk_band,
        ga.workbook_suggested_treatment,
        ga.default_smoothing_recommended,
        ROUND(ga.unsmoothed_allocated_value, 2)::NUMERIC(12, 2) AS unsmoothed_allocated_value,
        ROUND(ga.smoothed_allocated_value, 2)::NUMERIC(12, 2) AS smoothed_allocated_value,
        ROUND(ga.allocated_expected_value, 2)::NUMERIC(12, 2) AS allocated_expected_value,
        1::INTEGER AS project_count,
        'vw_pipeline_smoothing_score_v1'::TEXT AS source_view,
        CURRENT_TIMESTAMP
    FROM grouped_allocations ga;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;