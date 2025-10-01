from fastapi import APIRouter, HTTPException
from ...services.analysis_service import AnalysisService

router = APIRouter(prefix="/analysis", tags=["analysis"])
svc = AnalysisService()

@router.post("/{monday_id}/run")
def run_analysis(monday_id: str, with_llm: bool = False):
    out = svc.analyze_and_store(monday_id, with_llm=with_llm)
    if not out.get('success'):
        raise HTTPException(status_code=404, detail=out.get('error'))
    return out
