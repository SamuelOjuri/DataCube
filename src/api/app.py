from fastapi import FastAPI
from .routes.analysis import router as analysis_router

app = FastAPI()
app.include_router(analysis_router)  # exposes /analysis/{monday_id}/run



