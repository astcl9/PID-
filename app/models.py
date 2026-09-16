from datetime import datetime
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field

class SignalInfo(BaseModel):
    tag: str
    type: str = "other"
    unit: Optional[str] = None
    note: Optional[str] = None


class SignalMappingItem(BaseModel):
    tag: str
    type: Literal["PV", "SV", "MV", "other"]
    unit: Optional[str] = None
    note: Optional[str] = None


class SignalMappingRequest(BaseModel):
    signals: list[SignalMappingItem]

class DatasetInfo(BaseModel):
    id: str
    name: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    rows: int = 0
    signals: list[SignalInfo] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

class Segment(BaseModel):
    id: str
    dataset_id: str
    start: datetime
    end: datetime
    type: str
    confidence: float = 0.0
    reason: str = ""
    loop: Optional[str] = None
    note: Optional[str] = None
    stats: dict[str, Any] = Field(default_factory=dict)
    manual_override: bool = False


class HealthResponse(BaseModel):
    status: str


class StorageHealthResponse(BaseModel):
    backend: str
    ok: bool


class SystemHealthResponse(BaseModel):
    status: str
    disk: dict[str, int | float]
    jobs: dict[str, int]


class ImportAccepted(BaseModel):
    job_id: str
    status: str


class Series(BaseModel):
    name: str
    type: str = "OTHER"
    x: list[Optional[str]] = Field(default_factory=list)
    y: list[Optional[float]] = Field(default_factory=list)


class DataResponse(BaseModel):
    resolution: str
    series: list[Series] = Field(default_factory=list)


class DayInfo(BaseModel):
    date: str
    coverage: float = 0.0
    segmentCount: int = 0
    alarmCount: int = 0
    score: float = 0.0
