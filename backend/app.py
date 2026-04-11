"""
app.py — Enterprise FastAPI Application
Entry point pulito. Registra i router, configura middleware, health check.
main.py rimane per retrocompatibilità deploy ma questo è il file corretto.
"""
import os
import logging
import io
from typing import Optional
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

from config.settings import settings
from routers.upload import router as upload_router
from routers.analytics import router as analytics_router
from models.responses import HealthResponse

load_dotenv()

# ── Logging strutturato ───────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
log = logging.getLogger("ua")

# ── App ───────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    docs_url="/docs" if settings.environment != "production" else None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────
app.include_router(upload_router)
app.include_router(analytics_router)


def sb():
    return create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )


# ── System endpoints ─────────────────────────────────────────────

@app.get("/wake")
def wake():
    """Cold-start wake endpoint per Render free tier."""
    return {"ok": True, "version": settings.app_version}


@app.get("/health", response_model=HealthResponse)
def health():
    """Health check strutturato — database + analytics readiness."""
    try:
        sb().table("upload_log").select("id").limit(1).execute()
        db_ok = True
    except Exception:
        db_ok = False

    # Verifica se ci sono dati analytics
    try:
        from services.analytics import get_anni
        anni = get_anni(sb())
        analytics_ready = len(anni) > 0
    except Exception:
        analytics_ready = False

    return HealthResponse(
        status="ok" if db_ok else "degraded",
        version=settings.app_version,
        environment=settings.environment,
        database="reachable" if db_ok else "unreachable",
        analytics_ready=analytics_ready,
        upload_engine_version="1.0",
        kpi_definitions={
            "listino":   "imp_listino_eur  = Imp. Iniziale € (prezzo di partenza)",
            "impegnato": "imp_impegnato_eur = Imp. Negoziato € (quanto paghiamo)",
            "saving":    "saving_eur = Saving.1 (il nostro lavoro)",
            "perc_saving": "saving / listino × 100",
        },
    )


# ── Filtri e Export ───────────────────────────────────────────────

@app.get("/filtri/disponibili")
def filtri_disponibili(anno: Optional[int] = None):
    from services.analytics import saving_filters, query
    fs = saving_filters(anno)
    rows = query(sb(), "saving", fs,
        "cdc,str_ric,alfa_documento,macro_categoria,prefisso_commessa,utente_presentazione,valuta")
    df = pd.DataFrame(rows)
    if df.empty:
        return {k: [] for k in ["cdc","str_ric","alfa_documento","macro_categoria","prefisso_commessa","utente","valuta"]}
    def uniq(c):
        return sorted(df[c].dropna().str.strip().unique().tolist()) if c in df.columns else []
    return {
        "cdc":               uniq("cdc"),
        "str_ric":           uniq("str_ric"),
        "alfa_documento":    uniq("alfa_documento"),
        "macro_categoria":   [x for x in uniq("macro_categoria") if x],
        "prefisso_commessa": uniq("prefisso_commessa"),
        "utente":            [x for x in uniq("utente_presentazione") if x],
        "valuta":            uniq("valuta"),
    }


@app.post("/export/custom/excel")
def export_excel(body: dict = Body(...)):
    from services.analytics import (
        kpi_riepilogo, kpi_mensile, kpi_per_cdc,
        kpi_per_alfa, kpi_top_fornitori
    )
    filtri_in = body.get("filtri", {})
    sezioni   = body.get("sezioni", ["riepilogo","mensile","cdc","alfa_documento","top_fornitori"])
    anno      = filtri_in.get("anno")
    str_ric   = filtri_in.get("str_ric")
    cdc       = filtri_in.get("cdc")
    client    = sb()

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if "riepilogo" in sezioni:
            k = kpi_riepilogo(client, anno=anno, str_ric=str_ric, cdc=cdc)
            pd.DataFrame([
                ["Listino €",    k["listino"],   "Prezzo di partenza"],
                ["Impegnato €",  k["impegnato"], "Quanto paghiamo"],
                ["Saving €",     k["saving"],    "Il nostro lavoro"],
                ["% Saving",     f"{k['perc_saving']}%", "saving/listino×100"],
                ["N° Righe",     k["n_righe"], ""],
                ["N° Negoziati", k["n_negoziati"], ""],
            ], columns=["KPI","Valore","Note"]).to_excel(writer, index=False, sheet_name="Riepilogo")
        if "mensile" in sezioni:
            d = kpi_mensile(client, anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Mensile")
        if "cdc" in sezioni:
            d = kpi_per_cdc(client, anno=anno, str_ric=str_ric)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Per CDC")
        if "alfa_documento" in sezioni:
            d = kpi_per_alfa(client, anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Per Tipo Documento")
        if "top_fornitori" in sezioni:
            d = kpi_top_fornitori(client, anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Top Fornitori")
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=report_{anno or 'tutti'}.xlsx"})
