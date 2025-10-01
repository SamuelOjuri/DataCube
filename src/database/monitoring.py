"""
Database monitoring and health check utilities
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import asyncio

from .supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class DatabaseMonitor:
    """Database monitoring and health checking service"""
    
    def __init__(self):
        self.supabase = SupabaseClient()
    
    async def get_sync_health(self) -> Dict[str, Any]:
        """Get sync operation health metrics"""
        try:
            result = self.supabase.client.table('sync_health').select('*').execute()
            return {
                "status": "healthy",
                "sync_health": result.data,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get sync health: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_data_freshness(self) -> Dict[str, Any]:
        """Get data freshness metrics"""
        try:
            result = self.supabase.client.table('data_freshness').select('*').execute()
            return {
                "status": "healthy",
                "data_freshness": result.data,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get data freshness: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_webhook_metrics(self) -> Dict[str, Any]:
        """Get webhook processing metrics"""
        try:
            result = self.supabase.client.table('webhook_metrics').select('*').execute()
            return {
                "status": "healthy",
                "webhook_metrics": result.data,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get webhook metrics: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_system_performance(self) -> Dict[str, Any]:
        """Get overall system performance metrics"""
        try:
            result = self.supabase.client.table('system_performance').select('*').execute()
            
            # Determine overall health status
            critical_issues = [item for item in result.data if item['status'] == 'critical']
            warning_issues = [item for item in result.data if item['status'] == 'warning']
            
            overall_status = "healthy"
            if critical_issues:
                overall_status = "critical"
            elif warning_issues:
                overall_status = "warning"
            
            return {
                "status": overall_status,
                "system_metrics": result.data,
                "critical_issues": len(critical_issues),
                "warning_issues": len(warning_issues),
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get system performance: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_comprehensive_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive monitoring dashboard data"""
        try:
            # Run all monitoring queries in parallel
            sync_health, data_freshness, webhook_metrics, system_performance = await asyncio.gather(
                self.get_sync_health(),
                self.get_data_freshness(),
                self.get_webhook_metrics(),
                self.get_system_performance(),
                return_exceptions=True
            )
            
            # Determine overall system health
            statuses = []
            if isinstance(sync_health, dict):
                statuses.append(sync_health.get('status', 'error'))
            if isinstance(data_freshness, dict):
                statuses.append(data_freshness.get('status', 'error'))
            if isinstance(webhook_metrics, dict):
                statuses.append(webhook_metrics.get('status', 'error'))
            if isinstance(system_performance, dict):
                statuses.append(system_performance.get('status', 'error'))
            
            # Overall status logic
            if 'critical' in statuses or 'error' in statuses:
                overall_status = "critical"
            elif 'warning' in statuses:
                overall_status = "warning"
            else:
                overall_status = "healthy"
            
            return {
                "overall_status": overall_status,
                "sync_health": sync_health if isinstance(sync_health, dict) else {"error": str(sync_health)},
                "data_freshness": data_freshness if isinstance(data_freshness, dict) else {"error": str(data_freshness)},
                "webhook_metrics": webhook_metrics if isinstance(webhook_metrics, dict) else {"error": str(webhook_metrics)},
                "system_performance": system_performance if isinstance(system_performance, dict) else {"error": str(system_performance)},
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get comprehensive dashboard: {e}")
            return {
                "overall_status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def cleanup_old_data(self) -> Dict[str, Any]:
        """Clean up old monitoring data"""
        try:
            # Call the cleanup function
            result = self.supabase.client.rpc('cleanup_old_webhook_events').execute()
            deleted_count = result.data if result.data else 0
            
            return {
                "status": "success",
                "deleted_webhook_events": deleted_count,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def refresh_analytics(self) -> Dict[str, Any]:
        """Refresh materialized views for analytics"""
        try:
            result = self.supabase.client.rpc('refresh_analytics_views').execute()
            
            return {
                "status": "success",
                "message": result.data,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to refresh analytics: {e}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
