"""
UA Dashboard Backend v9 — Fondazione Telethon ETS
Architettura unificata: UN SOLO pipeline ingestion per preview + import + analytics.

FIX v9.1:
  1. upload_auto: aggiunto return result mancante
  2. MESI: dizionario definito (era usato ma mai dichiarato)
  3. CORS: URL frontend V2 aggiunto agli origins
"""
import os, io, logging
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client
from dotenv import load_dotenv

from upload_engine import (
    process_upload, inspect_bytes, inspect_and_load,
    compute_readiness, UploadResult, WorkbookInspection,
)
from ingestion_engine import (
    inspect_workbook, mapping_result_to_dict, FileFamily,
    build_column_map, FieldMapping,
)
from domain import (
    calc_kpi, _s, _b, _f, _fn, _i, _d, clean, safe_pct,
    DOC_NEG, parse_commessa, derive_cdc,
)

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ua")

MESI = {
    1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr",
    5: "Mag", 6: "Giu", 7: "Lug", 8: "Ago",
    9: "Set", 10: "Ott", 11: "Nov", 12: "Dic",
}

app = FastAPI(title="UA Dashboard API", version="9.1.0")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "https://ua-enterprise-v2-frontend.onrender.com,https://ua-acquisti-frontend.onrender.com,http://localhost:5173"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def sb():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE = 1000

def query(table: str, filters=None, select: str = "*") -> list:
    client = sb()
    all_rows, offset = [], 0
    while True:
        q = client.table(table).select(select)
        if filters:
            for fn_filter in filters:
                q = fn_filter(q)
        batch = q.range(offset, offset + PAGE - 1).execute().data
        all_rows.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
    return all_rows

def saving_filters(anno=None, str_ric=None, cdc=None, alfa=None,
                   macro=None, pref_comm=None):
    fs = []
    if anno:
        fs.append(lambda q, a=anno: q.gte("data_doc", f"{a}-01-01")
                                     .lte("data_doc", f"{a}-12-31"))
    if str_ric:    fs.append(lambda q, v=str_ric: q.eq("str_ric", v))
    if cdc:        fs.append(lambda q, v=cdc: q.eq("cdc", v))
    if alfa:       fs.append(lambda q, v=alfa: q.eq("alfa_documento", v))
    if macro:      fs.append(lambda q, v=macro.strip(): q.ilike("macro_categoria", f"%{v}%"))
    if pref_comm:  fs.append(lambda q, v=pref_comm: q.eq("prefisso_commessa", v))
    return fs

def get_saving_df(anno=None, str_ric=None, cdc=None, alfa=None,
                  macro=None, pref_comm=None, cols="*") -> pd.DataFrame:
    rows = query("saving", saving_filters(anno, str_ric, cdc, alfa, macro, pref_comm), cols)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "data_doc" in df.columns:
        df["data_doc"] = pd.to_datetime(df["data_doc"], errors="coerce")
    else:
        df["data_doc"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    for c in ['imp_listino_eur', 'imp_impegnato_eur', 'saving_eur']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    for c in ['negoziazione', 'accred_albo']:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)
    return df

@app.post("/upload/inspect")
async def upload_inspect(file: UploadFile = File(...)):
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
        log.error(f"inspect error for {file.filename}: {e}", exc_info=True)
        raise HTTPException(400, f"Errore ispezione file: {str(e)[:200]}")

@app.post("/upload/auto")
async def upload_auto(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = None,
    yoy_mode: bool = False,
    forced_family: Optional[str] = None,
):
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
        log.error(f"upload_auto unexpected error: {e}", exc_info=True)
        raise HTTPException(400, "Impossibile analizzare il file. Verifica che sia un Excel valido (.xlsx/.xls).")

    # retry smart per file risorse non riconosciuti al primo colpo
    if (
        result.status == "failed"
        and not result.upload_id
        and not forced_family
        and result.mapping_score >= 0.30
    ):
        mapped = {m.get("canonical") for m in (result.mapped_fields or [])}
        risorse_hits = len(mapped.intersection({
            "risorsa", "utente_pres", "year_month", "pratiche_gestite", "pratiche_aperte",
            "pratiche_chiuse", "saving_generato", "negoziazioni_concluse", "tempo_medio_risorsa",
        }))
        if risorse_hits >= 3:
            result = process_upload(
                file_bytes=contents,
                filename=file.filename,
                client=sb(),
                cdc_override=cdc_override,
                yoy_mode=yoy_mode,
                forced_family="risorse",
            )

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File non riconoscibile come file procurement.")

    return {
        "status":              result.status,
        "rows_inserted":       result.rows_inserted,
        "rows_skipped":        result.rows_skipped,
        "upload_id":           result.upload_id,
        "sheet_used":          result.sheet_used,
        "family":              result.family,
        "family_label":        result.family_label,
        "mapping_confidence":  result.mapping_confidence,
        "mapping_score":       result.mapping_score,
        "year_detected":       result.year_detected,
        "years_found":         result.years_found,
        "yoy_ready":           result.yoy_ready,
        "available_analyses":  result.available_analyses,
        "blocked_analyses":    result.blocked_analyses,
        "warnings":            result.warnings,
        "normalization_notes": result.normalization_notes,
    }

GRAN_MAP = {
    "mensile":    [(m, m, f"M{m:02d}") for m in range(1, 13)],
    "bimestrale": [(1,2,"B1"),(3,4,"B2"),(5,6,"B3"),(7,8,"B4"),(9,10,"B5"),(11,12,"B6")],
    "quarter":    [(1,3,"Q1"),(4,6,"Q2"),(7,9,"Q3"),(10,12,"Q4")],
    "semestrale": [(1,6,"S1"),(7,12,"S2")],
    "annuale":    [(1,12,"Anno")],
}

@app.get("/kpi/saving/yoy-granulare")
def kpi_yoy(
    anno: int = Query(...), granularita: str = Query("mensile"),
    str_ric: Optional[str] = Query(None), cdc: Optional[str] = Query(None)
):
    try:
        ap = anno - 1
        periodi = GRAN_MAP.get(granularita, GRAN_MAP["mensile"])
        cols = "data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"

        df_c = get_saving_df(anno, str_ric, cdc, cols=cols)
        df_p = get_saving_df(ap, str_ric, cdc, cols=cols)

        if not df_c.empty:
            df_c["data_doc"] = pd.to_datetime(df_c["data_doc"], errors="coerce")
            df_c["mn"] = df_c["data_doc"].dt.month
            df_c = df_c.dropna(subset=["mn"])
            if not df_c.empty:
                df_c["mn"] = df_c["mn"].astype(int)
        if not df_p.empty:
            df_p["data_doc"] = pd.to_datetime(df_p["data_doc"], errors="coerce")
            df_p["mn"] = df_p["data_doc"].dt.month
            df_p = df_p.dropna(subset=["mn"])
            if not df_p.empty:
                df_p["mn"] = df_p["mn"].astype(int)

        mese_max = int(df_c["mn"].max()) if not df_c.empty else 0
        ult_giorno = 0
        if mese_max and not df_c.empty:
            max_day = df_c[df_c["mn"] == mese_max]["data_doc"].dt.day.max()
            ult_giorno = int(max_day) if pd.notna(max_day) else 0

        def delta(c, p): return round((c - p) / abs(p) * 100, 1) if p else None

        chart = []
        for m1, m2, lbl in periodi:
            gc_df = df_c[(df_c["mn"] >= m1) & (df_c["mn"] <= m2)] if not df_c.empty else pd.DataFrame()
            gp_df = df_p[(df_p["mn"] >= m1) & (df_p["mn"] <= m2)] if not df_p.empty else pd.DataFrame()
            if len(gc_df) == 0 and len(gp_df) == 0:
                continue

            parziale = len(gc_df) > 0 and mese_max < m2
            if   granularita == "mensile":    label = MESI.get(m1, lbl)
            elif granularita == "bimestrale": label = f"{MESI[m1]}–{MESI[m2]}"
            elif granularita == "quarter":    label = lbl
            elif granularita == "semestrale": label = f"{lbl} ({MESI[m1]}–{MESI[m2]})"
            else:                             label = str(anno)

            kc, kp = calc_kpi(gc_df), calc_kpi(gp_df)
            chart.append({
                "label": label, "m_start": m1, "m_end": m2, "parziale": parziale,
                "ha_dati_curr": len(gc_df) > 0, "ha_dati_prev": len(gp_df) > 0,
                f"listino_{anno}": kc["listino"], f"impegnato_{anno}": kc["impegnato"],
                f"saving_{anno}": kc["saving"], f"perc_saving_{anno}": kc["perc_saving"],
                f"listino_{ap}": kp["listino"], f"impegnato_{ap}": kp["impegnato"],
                f"saving_{ap}": kp["saving"], f"perc_saving_{ap}": kp["perc_saving"],
                "delta_saving": delta(kc["saving"], kp["saving"]) if not parziale else None,
                "delta_impegnato": delta(kc["impegnato"], kp["impegnato"]) if not parziale else None,
            })

        return {
            "anno": anno, "anno_precedente": ap, "granularita": granularita,
            "chart_data": chart,
            "kpi_headline": {
                "corrente": calc_kpi(df_c),
                "precedente": calc_kpi(df_p),
                "label_curr": f"Gen–{MESI.get(mese_max, '?')} {anno}",
                "label_prev": f"Gen–{MESI.get(mese_max, '?')} {ap}",
                "delta": {
                    "listino": delta(calc_kpi(df_c)["listino"], calc_kpi(df_p)["listino"]),
                    "impegnato": delta(calc_kpi(df_c)["impegnato"], calc_kpi(df_p)["impegnato"]),
                    "saving": delta(calc_kpi(df_c)["saving"], calc_kpi(df_p)["saving"]),
                }
            },
            "nota": "",
            "mese_max": mese_max,
            "ultimo_giorno": ult_giorno,
        }

    except Exception as e:
        log.error(f"kpi_yoy error anno={anno}: {e}", exc_info=True)
        return {
            "anno": anno,
            "anno_precedente": anno - 1,
            "granularita": granularita,
            "chart_data": [],
            "kpi_headline": {
                "corrente": calc_kpi(pd.DataFrame()),
                "precedente": calc_kpi(pd.DataFrame()),
                "label_curr": f"Gen–? {anno}",
                "label_prev": f"Gen–? {anno - 1}",
                "delta": {
                    "listino": None, "impegnato": None, "saving": None,
                    "perc_saving": None, "perc_negoziati": None,
                },
            },
            "nota": "YoY temporaneamente non disponibile: verifica import e qualità date (data_doc).",
            "mese_max": 0,
            "ultimo_giorno": 0,
            "error": "Errore nel calcolo YoY. Verifica che i dati siano stati importati correttamente.",
        }
