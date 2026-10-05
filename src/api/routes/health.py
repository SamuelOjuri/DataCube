"""Fast local health probes. No database or Monday calls on request paths."""
import hmac
import os
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse
from ...services.worker_monitor import monitor

router = APIRouter(prefix='/health', tags=['health'])


@router.get('/live')
async def liveness_check():
    state = monitor.snapshot()
    return JSONResponse({'status': 'alive' if state['live'] else 'unhealthy'}, status_code=200 if state['live'] else 503)


@router.get('')
@router.get('/')
@router.get('/ready')
async def readiness_check():
    state = monitor.snapshot()
    return JSONResponse({'status': 'ready' if state['ready'] else 'not_ready', 'database': state['database']},
                        status_code=200 if state['ready'] else 503)


@router.get('/workers')
async def worker_status(authorization: str | None = Header(default=None)):
    token = os.getenv('WORKER_MONITOR_TOKEN')
    if not token or not hmac.compare_digest(authorization or '', 'Bearer ' + token):
        raise HTTPException(status_code=403, detail='Worker monitoring access denied')
    state = monitor.snapshot()
    return JSONResponse(state, status_code=200 if state['ready'] else 503)
