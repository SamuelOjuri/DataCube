import asyncio
import logging
import sys
from pathlib import Path


# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from src.services.analysis_service import AnalysisService
svc = AnalysisService()
print(svc.analyze_and_store('1776022610'))
print(svc.analyze_and_store('1778698588'))
