"""
routers/upload.py — Upload router
HTTP layer puro. Nessuna business logic qui.
Delega tutto a upload_engine + services.
"""
import logging
import os
from typing import Optional

from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from supabase import create_client

from upload_engine import (
    process_upload,
    inspect_bytes,
    inspect_and_load,
    compute_readiness,
)
from ingestion_engine import mapping_result_to_dict

log = logging.getLogger("ua.upload")
router = APIRouter(prefix="/upload", tags=["upload"])


def sb():
    return create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )


def _result_payload(result, target_domain: Optional[str] = None):
    payload = result.to_dict() if hasattr(result, "to_dict") else dict(result)
    if target_domain:
        payload["target_domain"] = target_domain
    return payload


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
            "year_detected": readiness["year_detected"],
            "years_found": readiness["years_found"],
            "yoy_ready": readiness["yoy_ready"],
            "yoy_note": readiness["yoy_note"],
            "normalization_notes": readiness["normalization_notes"],
            "family_label": readiness["family_label"],
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
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            cdc_override=cdc_override,
            yoy_mode=yoy_mode,
            forced_family=forced_family,
        )
    except Exception as e:
        log.error(f"upload_auto error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "Upload fallito.")

    return _result_payload(result)


@router.post("/saving")
async def upload_saving(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = Query(None),
    yoy_mode: bool = Query(False),
):
    """
    Upload file per dominio Saving / Ordini.
    Il file viene interpretato per le analisi saving anche se è un detailed orders compatibile.
    """
    contents = await file.read()
    try:
        result = process_upload(
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            cdc_override=cdc_override,
            yoy_mode=yoy_mode,
        )
    except Exception as e:
        log.error(f"upload_saving error {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File non riconoscibile per il dominio Saving.")

    return _result_payload(result, target_domain="saving")


@router.post("/risorse")
async def upload_risorse(file: UploadFile = File(...)):
    """
    Upload file per dominio Risorse / Operatività.

    Regola di business:
    - se il file è un vero file risorse/team, viene importato come tale
    - se il file è un detailed orders compatibile, viene comunque accettato
      perché deve poter alimentare le analisi Risorse
    """
    contents = await file.read()

    # Primo tentativo: file risorse puro
    try:
        result = process_upload(
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            forced_family="risorse",
        )
    except Exception as e:
        log.error(f"upload_risorse primary error {file.filename}: {e}", exc_info=True)
        result = None

    # Fallback: alcuni file ordini dettagliati devono essere validi anche per Risorse
    if result is None or (result.status == "failed" and not result.upload_id):
        try:
            result = process_upload(
                file_bytes=contents,
                filename=file.filename,
                client=sb(),
                forced_family="orders_detail",
            )
        except Exception as e:
            log.error(f"upload_risorse fallback error {file.filename}: {e}", exc_info=True)
            raise HTTPException(500, "Errore durante l'elaborazione del file risorse.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(
            400,
            result.error or (
                "File non riconoscibile per il dominio Risorse. "
                "Carica un file team dedicato oppure un file ordini dettagliati compatibile."
            ),
        )

    return _result_payload(result, target_domain="risorse")


@router.post("/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    """
    Upload file per dominio Tempi Attraversamento.

    Accetta anche ordini dettagliati compatibili, se il motore li sa interpretare.
    """
    contents = await file.read()

    try:
        result = process_upload(
            file_bytes=contents,
            filename=file.filename,
            client=sb(),
            forced_family="tempi",
        )
    except Exception as e:
        log.error(f"upload_tempi primary error {file.filename}: {e}", exc_info=True)
        result = None

    if result is None or (result.status == "failed" and not result.upload_id):
        try:
            result = process_upload(
                file_bytes=contents,
                filename=file.filename,
                client=sb(),
                forced_family="orders_detail",
            )
        except Exception as e:
            log.error(f"upload_tempi fallback error {file.filename}: {e}", exc_info=True)
            raise HTTPException(500, "Errore durante l'elaborazione del file tempi.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File tempi non riconoscibile.")

    return _result_payload(result, target_domain="tempi")


@router.post("/nc")
async def upload_nc(file: UploadFile = File(...)):
    """
    Upload file per dominio Non Conformità.
    """
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
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File NC non riconoscibile.")

    return _result_payload(result, target_domain="nc")


@router.get("/log")
def upload_log():
    """
    Storico caricamenti.
    """
    data = (
        sb()
        .table("upload_log")
        .select("*")
        .order("upload_date", desc=True)
        .limit(50)
        .execute()
        .data
    )
    return data if isinstance(data, list) else []


@router.delete("/{upload_id}")
def delete_upload(upload_id: str):
    """
    Elimina un upload e i dati correlati.
    """
    sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}
