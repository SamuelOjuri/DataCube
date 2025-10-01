"""
Monitoring API endpoints for DataCube system health
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging
from datetime import datetime

from ...database.monitoring import DatabaseMonitor

router = APIRouter(prefix="/monitoring", tags=["monitoring"])
logger = logging.getLogger(__name__)

# Initialize monitor
db_monitor = DatabaseMonitor()

@router.get("/dashboard")
async def get_monitoring_dashboard() -> Dict[str, Any]:
    """Get comprehensive monitoring dashboard"""
    try:
        dashboard_data = await db_monitor.get_comprehensive_dashboard()
        return dashboard_data
    except Exception as e:
        logger.error(f"Error getting monitoring dashboard: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve monitoring data")

@router.get("/sync-health")
async def get_sync_health() -> Dict[str, Any]:
    """Get sync operation health metrics"""
    return await db_monitor.get_sync_health()

@router.get("/data-freshness")
async def get_data_freshness() -> Dict[str, Any]:
    """Get data freshness metrics"""
    return await db_monitor.get_data_freshness()

@router.get("/webhook-metrics")
async def get_webhook_metrics() -> Dict[str, Any]:
    """Get webhook processing metrics"""
    return await db_monitor.get_webhook_metrics()

@router.get("/system-performance")
async def get_system_performance() -> Dict[str, Any]:
    """Get system performance metrics"""
    return await db_monitor.get_system_performance()

@router.post("/cleanup")
async def cleanup_old_data() -> Dict[str, Any]:
    """Clean up old monitoring data"""
    return await db_monitor.cleanup_old_data()

@router.post("/refresh-analytics")
async def refresh_analytics() -> Dict[str, Any]:
    """Refresh materialized views for analytics"""
    return await db_monitor.refresh_analytics()

@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """Simple health check endpoint"""
    try:
        # Quick database connectivity test
        result = await db_monitor.supabase.client.table('projects').select('count').limit(1).execute()
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
