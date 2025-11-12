import pandas as pd
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.analysis_service import AnalysisService

service = AnalysisService()
project_dict = {

    "monday_id": "5058981399",
    "monday_id": "5059108132",
    "monday_id": "5059165715",
    "monday_id": "5059289864",
    "monday_id": "5059448756",
    "monday_id": "5059937915",
    "monday_id": "5060051623",
    "monday_id": "5060938630",
    "monday_id": "5061688336",
    "monday_id": "5061725410",

}  # fill in the project payload you want to analyze
result = service.analyze_project(project_dict)

pd.DataFrame([result]).to_csv("analysis_output.csv", index=False)