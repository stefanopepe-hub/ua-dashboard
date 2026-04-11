"""
config/settings.py — Configuration layer enterprise
Tutto ciò che era hardcoded in main.py/ingestion_engine.py vive qui.
Override da env vars per deploy separation.
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class IngestionSettings:
    # Performance
    inspect_nrows: int = 200          # righe per mapping (non full read)
    sheet_select_nrows: int = 100     # righe per selezione foglio
    batch_insert_size: int = 5000     # righe per batch DB insert
    max_file_size_mb: float = 50.0

    # Confidence thresholds
    auto_proceed_threshold: float = 0.85    # HIGH → procede automaticamente
    confirm_threshold: float = 0.60         # MEDIUM → chiede conferma
    block_threshold: float = 0.20           # LOW → blocca

    # Family classification
    supplier_master_min_score: float = 0.60 # safety threshold supplier_master

@dataclass
class AnalyticsSettings:
    default_top_suppliers: int = 10
    default_top_categories: int = 15
    pareto_limit: int = 80              # mostra fino a 80 fornitori in Pareto
    yoy_min_overlap_months: int = 1     # mesi minimi per confronto YoY

@dataclass
class AppSettings:
    api_version: str = "v1"
    app_name: str = "UA Dashboard — Fondazione Telethon ETS"
    app_version: str = "10.0.0"
    environment: str = os.getenv("ENVIRONMENT", "production")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    allowed_origins: List[str] = field(default_factory=lambda: 
        os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(","))

    ingestion: IngestionSettings = field(default_factory=IngestionSettings)
    analytics: AnalyticsSettings = field(default_factory=AnalyticsSettings)

# Singleton
settings = AppSettings()

# ── Business vocabulary ────────────────────────────────────────────
# Fonte di verità per label, non ripetute ovunque
DOC_TYPE_LABELS: Dict[str, str] = {
    "ORN":    "Ordine Ricerca",
    "ORD":    "Ordine Diretto Ricerca",
    "OPR":    "Ordine Previsionale Ricerca",
    "PS":     "Procedura Straordinaria",
    "OS":     "Ordine Struttura",
    "OSP":    "Ordine Previsionale Struttura",
    "OSD":    "Ordine Diretto Struttura",
    "OSDP01": "Ordine Diretto Struttura (variante)",
}

DOC_TYPE_AREA: Dict[str, str] = {
    "ORN": "RICERCA", "ORD": "RICERCA", "OPR": "RICERCA", "PS": "RICERCA",
    "OS": "STRUTTURA", "OSP": "STRUTTURA", "OSD": "STRUTTURA", "OSDP01": "STRUTTURA",
}

DOC_NEGOTIABLE: frozenset = frozenset({"OS", "OSP", "PS", "OPR", "ORN", "ORD"})

CDC_ORDER: List[str] = ["GD", "TIGEM", "TIGET", "FT", "STRUTTURA"]
