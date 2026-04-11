"""
models/errors.py — Struttura degli errori enterprise
Ogni errore ha: codice, dominio, messaggio utente, dettaglio tecnico (solo log).
"""
from enum import Enum
from dataclasses import dataclass
from typing import Optional

class ErrorDomain(str, Enum):
    UPLOAD     = "UPLOAD"
    INGESTION  = "INGESTION"
    ANALYTICS  = "ANALYTICS"
    DATABASE   = "DATABASE"
    VALIDATION = "VALIDATION"
    AUTH       = "AUTH"
    SYSTEM     = "SYSTEM"

class ErrorCode(str, Enum):
    # Upload
    FILE_TOO_LARGE           = "U001"
    FILE_UNREADABLE          = "U002"
    FILE_UNRECOGNIZED        = "U003"
    FILE_FAMILY_MISMATCH     = "U004"
    FILE_CONFIDENCE_LOW      = "U005"

    # Ingestion
    NORMALIZATION_FAILED     = "I001"
    MAPPING_INSUFFICIENT     = "I002"
    PARTIAL_SUCCESS          = "I003"
    YEAR_DETECTION_FAILED    = "I004"

    # Analytics
    DATA_NOT_AVAILABLE       = "A001"
    FIELD_NOT_MAPPED         = "A002"
    FILTER_YIELDS_EMPTY      = "A003"
    YOY_INSUFFICIENT_DATA    = "A004"

    # Database
    CONSTRAINT_VIOLATION     = "D001"
    DUPLICATE_RECORD         = "D002"
    REFERENCE_ERROR          = "D003"
    CONNECTION_TIMEOUT       = "D004"

    # System
    INTERNAL_ERROR           = "S001"

@dataclass
class AppError:
    code: ErrorCode
    domain: ErrorDomain
    user_message: str           # Mostrato all'utente — mai tecnico
    technical_detail: str = ""  # Solo per log interni
    recoverable: bool = True
    recommended_action: str = ""

    def to_user_dict(self) -> dict:
        """Serializzazione sicura per la risposta API — no dettagli tecnici."""
        return {
            "error_code": self.code.value,
            "domain": self.domain.value,
            "message": self.user_message,
            "recoverable": self.recoverable,
            "action": self.recommended_action,
        }


# ── Error catalog ───────────────────────────────────────────────────
ERRORS = {
    "db_23514": AppError(
        code=ErrorCode.CONSTRAINT_VIOLATION,
        domain=ErrorDomain.DATABASE,
        user_message=(
            "Il file non è stato salvato perché il tipo rilevato non è compatibile "
            "con il formato atteso. Prova con il caricamento automatico."
        ),
        recommended_action="Usa '/upload/auto' per classificazione automatica.",
    ),
    "db_23505": AppError(
        code=ErrorCode.DUPLICATE_RECORD,
        domain=ErrorDomain.DATABASE,
        user_message=(
            "Alcune righe erano già presenti nel database e non sono state reimportate. "
            "Vai in Data Quality per eliminare il caricamento precedente e reimportare."
        ),
        recommended_action="Elimina il caricamento precedente in Data Quality.",
    ),
    "db_23502": AppError(
        code=ErrorCode.CONSTRAINT_VIOLATION,
        domain=ErrorDomain.DATABASE,
        user_message="Import parziale: alcune righe mancavano di campi obbligatori.",
        recommended_action="Le righe valide sono state importate. Verifica il file sorgente.",
    ),
    "file_unrecognized": AppError(
        code=ErrorCode.FILE_UNRECOGNIZED,
        domain=ErrorDomain.UPLOAD,
        user_message="Il file non è stato riconosciuto come file procurement.",
        recommended_action=(
            "Verifica che il file contenga colonne come: Data documento, "
            "Importo, Fornitore. Usa il template standard come guida."
        ),
    ),
    "confidence_low": AppError(
        code=ErrorCode.FILE_CONFIDENCE_LOW,
        domain=ErrorDomain.INGESTION,
        user_message=(
            "Il sistema non è riuscito a riconoscere le colonne con sufficiente "
            "certezza per procedere automaticamente."
        ),
        recommended_action=(
            "Usa il mapping guidato per specificare le colonne manualmente, "
            "oppure scarica il template standard."
        ),
    ),
}


def translate_db_error(raw_error: str, table: str = "") -> AppError:
    """
    Traduce errori PostgreSQL crudi in AppError strutturati.
    Logga il dettaglio tecnico, restituisce messaggio sicuro.
    """
    raw = raw_error.lower()
    if "23514" in raw or ("check" in raw and "violat" in raw):
        return ERRORS["db_23514"]
    if "23505" in raw or "unique" in raw or "duplicate" in raw:
        return ERRORS["db_23505"]
    if "23502" in raw or "not null" in raw:
        return ERRORS["db_23502"]
    if "timeout" in raw or "connection" in raw:
        return AppError(
            code=ErrorCode.CONNECTION_TIMEOUT,
            domain=ErrorDomain.DATABASE,
            user_message="Il database non ha risposto in tempo. L'import potrebbe essere parziale.",
            recommended_action="Controlla Data Quality. Riprova se necessario.",
        )
    return AppError(
        code=ErrorCode.INTERNAL_ERROR,
        domain=ErrorDomain.SYSTEM,
        user_message="Si è verificato un errore durante il salvataggio.",
        technical_detail=raw_error[:500],
        recommended_action="Il team tecnico ha ricevuto i dettagli. Riprova.",
    )
