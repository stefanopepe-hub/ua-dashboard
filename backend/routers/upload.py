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


ALLOWED_BY_TARGET = {
    "saving": {"savings", "saving", "orders_detail", "orders", "detailed_orders"},
    "risorse": {"resources", "risorse", "team", "orders_detail", "orders", "detailed_orders"},
    "tempi": {"tempi", "cycle_times", "tempo_attraversamento", "orders_detail", "orders", "detailed_orders"},
    "nc": {"nc", "non_conformities", "non_conformita"},
}

def _normalize_family(value: str | None) -> str:
    return (value or "").strip().lower()

def _is_family_allowed(target: str, family: str | None) -> bool:
    fam = _normalize_family(family)
    return fam in ALLOWED_BY_TARGET.get(target, set())

def _contextual_response(result, target: str):
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result)
    payload["target_domain"] = target
    payload["family_allowed_for_target"] = _is_family_allowed(target, getattr(result, "family", None))
    return payload

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
    """Upload file per analisi Risorse/Operatività.
    Accetta sia file resources puri sia orders_detail, che verranno interpretati nel dominio Risorse.
    """
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
        raise HTTPException(500, "Errore durante l'elaborazione del file risorse.")

    if result.status == "failed" and not result.upload_id:
        # fallback: alcuni file dettagliati ordini devono poter alimentare Risorse
        try:
            result = process_upload(
                file_bytes=contents,
                filename=file.filename,
                client=sb(),
                forced_family="orders_detail",
                target_domain="risorse",
            )
        except TypeError:
            # compatibilità con versioni di process_upload senza target_domain
            pass

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(
            400,
            result.error or
            "Il file non è compatibile con il dominio Risorse. "
            "Carica un file team dedicato oppure un file ordini dettagliati compatibile."
        )

    return _contextual_response(result, "risorse")


@router.post("/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    """Upload file per analisi Tempi Attraversamento."""
    contents = await file.read()
    try:
        result = process_upload(
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            forced_family="tempi",
        )
    except Exception as e:
        log.error(f"upload_tempi error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file tempi.")

    if result.status == "failed" and not result.upload_id:
        try:
            result = process_upload(
                file_bytes=contents,
                filename=file.filename,
                client=sb(),
                forced_family="orders_detail",
                target_domain="tempi",
            )
        except TypeError:
            pass

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(
            400,
            result.error or
            "Il file non è compatibile con il dominio Tempi Attraversamento."
        )

    return _contextual_response(result, "tempi")


@router.post("/nc")
async def upload_nc(file: UploadFile = File(...)):
    """Upload file per analisi Non Conformità."""
    contents = await file.read()
    try:
        result = process_upload(
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            forced_family="nc",
        )
    except Exception as e:
        log.error(f"upload_nc error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file NC.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "Il file non è compatibile con il dominio NC.")

    return _contextual_response(result, "nc")


@router.get("/log")
def upload_log():
    """Storico caricamenti."""
    return sb().table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data


@router.delete("/{upload_id}")
def delete_upload(upload_id: str):
    """Elimina un upload e i dati correlati."""
    sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}
