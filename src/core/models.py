"""
Pydantic models for data validation and type safety.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from pydantic import ConfigDict
from pydantic import field_validator, model_serializer, model_validator

class PipelineStage(str, Enum):
    """Pipeline stage categories"""
    WON_CLOSED = "Won - Closed (Invoiced)"
    WON_OPEN = "Won - Open (Order Received)"
    WON_OTHER = "Won Via Other Ref"
    LOST = "Lost"
    CUSTOMER_CONFIDENT = "Customer Confident Of Project Success"
    CUSTOMER_HAS_ORDER = "Customer Has Order"
    CUSTOMER_PREFERRED = "Customer Has Order - TP Preferred Supplier"
    OPEN_ENQUIRY = "Open Enquiry"

class StatusCategory(str, Enum):
    """Simplified status categories for analysis"""
    WON = "Won"
    LOST = "Lost"
    OPEN = "Open"

class ProjectType(str, Enum):
    """Project type categories"""
    REFURBISHMENT = "Refurbishment"
    NEW_BUILD = "New Build"

BACKOFF_PRIORITY_TABLE: Dict[int, Dict[str, Any]] = {
    1: {
        "label": "Account × Project Type × Building Category × Product Key",
        "min_n": 10,
    },
    2: {
        "label": "Account × Project Type × Building Category",
        "min_n": 8,
    },
    3: {
        "label": "Account × Project Type",
        "min_n": 5,
    },
    4: {
        "label": "Project Type × Building Category",
        "min_n": 5,
    },
    5: {
        "label": "Project Type only",
        "min_n": 3,
    },
    6: {
        "label": "Global median",
        "min_n": 1,
    },
}


def get_backoff_priority_metadata(backoff_tier: Optional[int]) -> Dict[str, Any]:
    """
    Convert internal zero-based backoff tier (0..5) into reporting tier (1..6)
    and attach the priority-table label/min_n metadata.
    """
    if backoff_tier is None:
        priority_tier = 6
    else:
        tier = int(backoff_tier)
        priority_tier = min(max(tier + 1, 1), 6)

    cfg = BACKOFF_PRIORITY_TABLE[priority_tier]
    return {
        "priority_tier": priority_tier,
        "label": cfg["label"],
        "min_n": int(cfg["min_n"]),
    }

class ProjectFeatures(BaseModel):
    """Features of a project for analysis"""
    project_id: str
    name: str
    account: Optional[str] = None
    type: Optional[str] = None
    category: Optional[str] = None
    product_type: Optional[str] = None
    new_enquiry_value: float = Field(default=0.0, ge=0)
    gestation_period: Optional[int] = Field(default=None, ge=0)
    pipeline_stage: Optional[str] = None
    status_category: Optional[StatusCategory] = None
    value_band: Optional[str] = None
    date_created: Optional[datetime] = None

    @field_validator("new_enquiry_value", mode="before")
    @classmethod
    def parse_value(cls, value: Any) -> float:
        if value is None or value == "":
            return 0.0
        if isinstance(value, str):
            clean = value.replace("£", "").replace(",", "").strip()
            return float(clean) if clean else 0.0
        return float(value)

class SegmentStatistics(BaseModel):
    """Statistics for a data segment"""

    segment_keys: List[str] = Field(default_factory=list)
    sample_size: int = Field(ge=0)

    # Internal zero-based tier used by analysis engine (0=most specific, 5=global)
    backoff_tier: int = Field(ge=0, le=5)

    # Reporting-ready priority-table metadata (1=most specific, 6=global median)
    backoff_priority_tier: int = Field(default=6, ge=1, le=6)
    backoff_priority_label: str = Field(default="Global median")
    backoff_min_n: int = Field(default=1, ge=1)

    # Gestation statistics
    gestation_median: Optional[float] = None
    gestation_p25: Optional[float] = None
    gestation_p75: Optional[float] = None
    gestation_count: int = 0

    # Conversion statistics
    wins: int = 0
    losses: int = 0
    open: int = 0
    # Use closed-only as primary conversion_rate
    conversion_rate: Optional[float] = Field(default=None, ge=0, le=1)
    conversion_confidence: float = Field(default=0.0, ge=0, le=1)
    # Report both rates
    inclusive_conversion_rate: Optional[float] = Field(default=None, ge=0, le=1)
    closed_conversion_rate: Optional[float] = Field(default=None, ge=0, le=1)

    # Additional metrics
    average_value: Optional[float] = None
    account_win_rate: Optional[float] = None
    product_win_rate: Optional[float] = None

    @model_validator(mode="after")
    def populate_priority_metadata(self) -> "SegmentStatistics":
        meta = get_backoff_priority_metadata(self.backoff_tier)
        self.backoff_priority_tier = meta["priority_tier"]
        self.backoff_priority_label = meta["label"]
        self.backoff_min_n = meta["min_n"]
        return self

class NumericPredictions(BaseModel):
    """Numeric predictions for a project"""
    expected_gestation_days: Optional[int] = None
    gestation_confidence: float = Field(default=0.0, ge=0, le=1)
    gestation_range: Dict[str, int] = Field(default_factory=dict)
    
    expected_conversion_rate: float = Field(default=0.5, ge=0, le=1)
    conversion_method: str = Field(default="inclusive")
    conversion_confidence: float = Field(default=0.0, ge=0, le=1)
    
    rating_score: int = Field(default=50, ge=1, le=100)
    rating_components: Dict[str, Any] = Field(default_factory=dict)
    
    segment_statistics: Optional[SegmentStatistics] = None

class ProjectAnalysisInput(BaseModel):
    """Input structure for LLM analysis"""
    project_id: str
    features: Dict[str, Any] = Field(..., description="Project characteristics")
    historical: Dict[str, Any] = Field(..., description="Historical segment data")
    numeric_predictions: Dict[str, Any] = Field(..., description="Pre-computed baselines")

class ProjectAnalysisOutput(BaseModel):
    """Expected output structure from LLM"""

    summary: str = Field(
        default="Plain-English summary unavailable.",
        description="Plain-language overview suitable for business stakeholders"
    )
    adjustments: Dict[str, int] = Field(
        default_factory=dict,
        description="Optional ±1 adjustments to baselines"
    )
    reasoning: Dict[str, str] = Field(
        ...,
        description="Explanations for each metric"
    )
    confidence_notes: str = Field(
        ...,
        description="Any caveats or confidence factors"
    )
    # Optional field that models may include; used downstream in metadata
    special_factors: Optional[str] = None

class AnalysisResult(BaseModel):
    """Final analysis result combining numeric and LLM outputs"""
    project_id: str
    analysis_timestamp: datetime
    project_features: ProjectFeatures
    predictions: NumericPredictions
    reasoning: Dict[str, str] = Field(default_factory=dict)
    analysis_metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(json_encoders={datetime: datetime.isoformat})

    @model_serializer
    def serialize_model(self) -> Dict[str, Any]:
        data = self.model_dump()
        # datetime.isoformat already applied via json_encoders / ConfigDict
        return data
