"""
models/responses.py — Contratti API tipati (Pydantic)
Tutte le risposte API hanno schema esplicito.
"""
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum


class ConfidenceLevel(str, Enum):
    HIGH   = "high"
    MEDIUM = "medium"
    LOW    = "low"


class AnalyticsStatus(str, Enum):
    ENABLED  = "enabled"
    BLOCKED  = "blocked"
    PARTIAL  = "partial"


# ── Upload & Inspect ────────────────────────────────────────────────

class MappedField(BaseModel):
    canonical: str
    source_column: str
    confidence: float
    method: str
    is_critical: bool = False
    is_optional: bool = False


class BlockedAnalysis(BaseModel):
    analysis: str
    reason: str
    missing_fields: List[str] = []
    severity: str = "optional"


class InspectResponse(BaseModel):
    """Risposta di /upload/inspect — preview senza import."""
    family: str
    family_label: str
    family_confidence: float
    overall_confidence: ConfidenceLevel
    overall_score: float
    sheet_name: str
    header_row: int
    raw_columns: List[str]
    mapped_fields: List[MappedField]
    missing_critical: List[str]
    missing_optional: List[str]
    available_analyses: List[str]
    blocked_analyses: List[BlockedAnalysis]
    warnings: List[str]
    normalization_notes: List[str]
    year_detected: Optional[int]
    years_found: List[int]
    yoy_ready: bool
    yoy_note: str
    can_proceed: bool
    needs_confirmation: bool
    is_blocked: bool


class UploadResponse(BaseModel):
    """Risposta di /upload/* — dopo import."""
    status: str                        # ok | partial | failed
    upload_id: Optional[str]
    rows_inserted: int
    rows_skipped: int
    family: str
    family_label: str
    sheet_used: str
    header_row: int
    year_detected: Optional[int]
    years_found: List[int]
    mapping_confidence: ConfidenceLevel
    mapping_score: float
    available_analyses: List[str]
    blocked_analyses: List[BlockedAnalysis]
    warnings: List[str]
    normalization_notes: List[str]
    yoy_ready: bool
    error: Optional[str] = None


# ── KPI Analytics ────────────────────────────────────────────────────

class KpiSummary(BaseModel):
    listino: float
    impegnato: float
    saving: float
    perc_saving: float
    n_righe: int
    n_doc_neg: int
    n_negoziati: int
    perc_negoziati: float
    n_albo: int
    perc_albo: float


class YoyDelta(BaseModel):
    listino: Optional[float]
    impegnato: Optional[float]
    saving: Optional[float]
    perc_saving: Optional[float]
    perc_negoziati: Optional[float]


class ApiResponse(BaseModel):
    """Wrapper generico per risposte API con metadata."""
    data: Any
    meta: Dict[str, Any] = {}
    errors: List[Dict] = []
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class HealthResponse(BaseModel):
    status: str
    version: str
    environment: str
    database: str
    analytics_ready: bool
    upload_engine_version: str
    kpi_definitions: Dict[str, str]
