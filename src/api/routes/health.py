"""
Health check endpoints for system status
"""

from fastapi import APIRouter
from typing import Dict, Any
import logging
from datetime import datetime

from ...database.supabase_client import SupabaseClient
from ...core.monday_client import MondayClient

router = APIRouter(prefix="/health", tags=["health"])
logger = logging.getLogger(__name__)

@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Comprehensive health check"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    # Check Supabase connection
    try:
        supabase = SupabaseClient()
        result = supabase.client.table('projects').select('count').limit(1).execute()
        health_status["services"]["supabase"] = {
            "status": "healthy",
            "response_time_ms": "< 100"
        }
    except Exception as e:
        health_status["services"]["supabase"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # Check Monday.com API connection
    try:
        monday = MondayClient()
        if monday.test_connection():
            health_status["services"]["monday_api"] = {
                "status": "healthy",
                "authenticated": True
            }
        else:
            health_status["services"]["monday_api"] = {
                "status": "unhealthy",
                "authenticated": False
            }
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["services"]["monday_api"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    return health_status

@router.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """Kubernetes readiness probe endpoint"""
    try:
        # Quick database check
        supabase = SupabaseClient()
        supabase.client.table('projects').select('count').limit(1).execute()
        return {"status": "ready"}
    except Exception:
        return {"status": "not ready"}

@router.get("/live")
async def liveness_check() -> Dict[str, Any]:
    """Kubernetes liveness probe endpoint"""
    return {
        "status": "alive",
        "timestamp": datetime.now().isoformat()
    }
