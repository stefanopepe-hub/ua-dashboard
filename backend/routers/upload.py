"""
routers/upload.py — Upload router
HTTP layer puro. Nessuna business logic qui.
Delega tutto a upload_engine + services.
"""
import logging
from typing import Optional
from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from supabase import create_client
import os

from upload_engine import (
    process_upload, inspect_bytes, inspect_and_load,
    compute_readiness, UploadResult,
)
from ingestion_engine import mapping_result_to_dict
from models.errors import translate_db_error
from models.responses import UploadResponse, InspectResponse

log = logging.getLogger("ua.upload")
router = APIRouter(prefix="/upload", tags=["upload"])


def sb():
    return create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )


@router.post("/inspect")
async def upload_inspect(file: UploadFile = File(...)):
    """
    Preview intelligente — stesso engine del vero import.
    Non importa nulla. Ritorna: family, confidence, mapping, analisi, YoY.
    """
    contents = await file.read()
    try:
        mr = inspect_bytes(contents, file.filename)
        result = mapping_result_to_dict(mr)
        wbi = inspect_and_load(contents, file.filename)
        readiness = compute_readiness(mr, wbi)
        result.update({
            "year_detected":       readiness["year_detected"],
            "years_found":         readiness["years_found"],
            "yoy_ready":           readiness["yoy_ready"],
            "yoy_note":            readiness["yoy_note"],
            "normalization_notes": readiness["normalization_notes"],
            "family_label":        readiness["family_label"],
        })
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.error(f"inspect error {file.filename}: {e}", exc_info=True)
        raise HTTPException(400, f"Errore ispezione file: {str(e)[:200]}")


@router.post("/auto")
async def upload_auto(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = Query(None),
    yoy_mode: bool = Query(False),
    forced_family: Optional[str] = Query(None),
):
    """
    Endpoint unificato — classifica e importa automaticamente.
    Supporta: savings, risorse, non_conformita, tempi.
    """
    contents = await file.read()
    try:
        result = process_upload(
            file_bytes=contents, filename=file.filename, client=sb(),
            cdc_override=cdc_override, yoy_mode=yoy_mode, forced_family=forced_family,
        )
    except Exception as e:
        log.error(f"upload_auto error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "Upload fallito.")
    return result.to_dict()


@router.post("/saving")
async def upload_saving(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = Query(None),
    yoy_mode: bool = Query(False),
):
    """Upload file saving/ordini — compatibilità frontend."""
    contents = await file.read()
    try:
        result = process_upload(
            file_bytes=contents, filename=file.filename, client=sb(),
            cdc_override=cdc_override, yoy_mode=yoy_mode,
        )
    except Exception as e:
        log.error(f"upload_saving error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File non riconoscibile.")
    return result.to_dict()


@router.post("/risorse")
async def upload_risorse(file: UploadFile = File(...)):
    """Upload file risorse/team con guardrail di famiglia obbligatori."""
    contents = await file.read()
    try:
        result = process_upload(
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            forced_family="risorse",
        )
    except Exception as e:
        log.error(f"upload_risorse error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(
            400,
            result.error or "File non riconoscibile come file risorse. Verifica che contenga: Risorsa, Mese, Pratiche Gestite.",
        )

    if result.family != "risorse":
        raise HTTPException(
            400,
            f"File non coerente con il dominio Risorse/Team. Famiglia rilevata: {result.family_label or result.family}.",
        )
    return result.to_dict()


@router.post("/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    """Upload file tempi attraversamento con guardrail di famiglia obbligatori."""
    contents = await file.read()
    try:
        result = process_upload(file_bytes=contents, filename=file.filename, client=sb(), forced_family="tempi")
    except Exception as e:
        log.error(f"upload_tempi error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")
    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File tempi non riconoscibile.")
    if result.family != "tempi":
        raise HTTPException(400, f"File non coerente con il dominio Tempi. Famiglia rilevata: {result.family_label or result.family}.")
    return result.to_dict()


@router.post("/nc")
async def upload_nc(file: UploadFile = File(...)):
    """Upload file non conformità con guardrail di famiglia obbligatori."""
    contents = await file.read()
    try:
        result = process_upload(file_bytes=contents, filename=file.filename, client=sb(), forced_family="nc")
    except Exception as e:
        log.error(f"upload_nc error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")
    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File NC non riconoscibile.")
    if result.family != "nc":
        raise HTTPException(400, f"File non coerente con il dominio Non Conformità. Famiglia rilevata: {result.family_label or result.family}.")
    return result.to_dict()


@router.get("/log")
def upload_log():
    """Storico caricamenti."""
    return sb().table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data


@router.delete("/{upload_id}")
def delete_upload(upload_id: str):
    """Elimina un upload e i dati correlati."""
    sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}
