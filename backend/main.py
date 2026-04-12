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

# ── FIX: MESI era usato in kpi_yoy e kpi_mensile_area ma mai definito ──
MESI = {
    1: "Gen", 2: "Feb", 3: "Mar", 4: "Apr",
    5: "Mag", 6: "Giu", 7: "Lug", 8: "Ago",
    9: "Set", 10: "Ott", 11: "Nov", 12: "Dic",
}

app = FastAPI(title="UA Dashboard API", version="9.1.0")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ── FIX: aggiunto URL frontend V2 ──
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

# ─────────────────────────────────────────────────────────────────
# PAGINAZIONE SUPABASE
# ─────────────────────────────────────────────────────────────────
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


def build_saving_record(
    col_map: dict,
    row: pd.Series,
    upload_id: str,
    cdc_override: Optional[str] = None,
) -> Optional[dict]:
    def gcol(canonical: str):
        fm = col_map.get(canonical)
        if not fm:
            return None
        return row.get(fm.source_column)

    dv = _d(gcol('data_doc'))
    if not dv:
        return None

    # FIX: cambio None-safe
    cambio_raw = _f(gcol('cambio'), 1.0)
    cambio = cambio_raw if cambio_raw and cambio_raw > 0 else 1.0
    valuta = _s(gcol('valuta')) or 'EURO'

    has_eur = 'listino_eur' in col_map and 'impegnato_eur' in col_map
    if has_eur:
        lst   = _f(gcol('listino_eur'))
        imp   = _f(gcol('impegnato_eur'))
        sav   = _f(gcol('saving_eur'))
        pct_s = _f(gcol('perc_saving_eur'))
    else:
        lst   = _f(gcol('listino_val')) * cambio
        imp   = _f(gcol('impegnato_val')) * cambio
        sav   = _f(gcol('saving_val')) * cambio
        pct_s = _f(gcol('perc_saving_val'))

    if sav == 0 and lst > 0 and imp > 0:
        sav = lst - imp
    if pct_s == 0 and lst > 0:
        pct_s = sav / lst * 100

    if cdc_override:
        cdc_val = cdc_override
    elif 'cdc' in col_map:
        cdc_val = _s(gcol('cdc'))
    else:
        cdc_val = derive_cdc(
            _s(gcol('centro_costo')) or '',
            _s(gcol('desc_cdc')) or ''
        )

    pc = _s(gcol('protoc_commessa'))
    pref, anno_comm = parse_commessa(pc)

    r = {
        "upload_id":            upload_id,
        "data_doc":             dv,
        "alfa_documento":       _s(gcol('alfa_documento')),
        "str_ric":              _s(gcol('str_ric')),
        "stato_dms":            _s(gcol('stato_dms')),
        "ragione_sociale":      _s(gcol('ragione_sociale')),
        "codice_fornitore":     _i(gcol('codice_fornitore')),
        "accred_albo":          _b(gcol('accred_albo')),
        "utente":               _s(gcol('utente')),
        "utente_presentazione": _s(gcol('utente_pres')),
        "cod_utente":           _i(gcol('cod_utente')),
        "num_doc":              _i(gcol('num_doc')),
        "protoc_ordine":        _fn(gcol('protoc_ordine')),
        "protoc_commessa":      pc,
        "prefisso_commessa":    pref,
        "anno_commessa":        anno_comm,
        "grp_merceol":          _s(gcol('grp_merceol')),
        "desc_gruppo_merceol":  _s(gcol('desc_merceol')),
        "macro_categoria":      _s(gcol('macro_cat')),
        "centro_di_costo":      _s(gcol('centro_costo')),
        "desc_cdc":             _s(gcol('desc_cdc')),
        "cdc":                  cdc_val,
        "valuta":               valuta,
        "cambio":               cambio,
        "imp_listino_eur":      lst,
        "imp_impegnato_eur":    imp,
        "saving_eur":           sav,
        "perc_saving_eur":      pct_s,
        "imp_iniziale":         _f(gcol('listino_val')),
        "imp_negoziato":        _f(gcol('impegnato_val')),
        "saving_val":           _f(gcol('saving_val')),
        "perc_saving":          _f(gcol('perc_saving_val')),
        "negoziazione":         _b(gcol('negoziazione')),
        "tail_spend":           _s(gcol('tail_spend')),
    }
    return {k: clean(v) for k, v in r.items()}


# ─────────────────────────────────────────────────────────────────
# UPLOAD ENDPOINTS
# ─────────────────────────────────────────────────────────────────

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


@app.post("/upload/saving")
async def upload_saving(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = None,
    yoy_mode: bool = False,
):
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
        log.error(f"upload_saving error for {file.filename}: {e}", exc_info=True)
        raise HTTPException(500, "Errore durante l'elaborazione del file.")

    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File non riconoscibile come file saving/ordini.")

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


# ── FIX PRINCIPALE: upload_auto con return aggiunto ──
@app.post("/upload/auto")
async def upload_auto(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = None,
    yoy_mode: bool = False,
    forced_family: Optional[str] = None,
):
    """
    Endpoint upload unificato — classifica e importa automaticamente.
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
        log.error(f"upload_auto unexpected error: {e}", exc_info=True)
        raise HTTPException(400, "Impossibile analizzare il file. Verifica che sia un Excel valido (.xlsx/.xls).")

    # Fallback smart: alcuni file risorse (Buyer/Period) possono essere classificati male.
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

    # ── FIX: return era mancante — causava risposta null al frontend ──
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


@app.post("/upload/risorse")
async def upload_risorse_compat(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        result = process_upload(file_bytes=contents, filename=file.filename, client=sb())
    except Exception as e:
        raise HTTPException(500, "Errore interno. Controlla i log per i dettagli.")

    if result.status == "failed" and not result.upload_id:
        if result.mapping_score >= 0.40:
            result = process_upload(
                file_bytes=contents,
                filename=file.filename,
                client=sb(),
                forced_family='risorse',
            )
        if result.status == "failed":
            raise HTTPException(400, result.error or "File risorse non riconoscibile")

    return {
        "status":             result.status,
        "rows":               result.rows_inserted,
        "family":             result.family,
        "year_detected":      result.year_detected,
        "available_analyses": result.available_analyses,
        "warnings":           result.warnings,
    }


@app.post("/upload/tempi")
async def upload_tempi_compat(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        result = process_upload(file_bytes=contents, filename=file.filename, client=sb())
    except Exception as e:
        raise HTTPException(500, "Errore interno. Il file potrebbe non essere nel formato atteso.")
    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File tempi attraversamento non riconoscibile.")
    return {"status": result.status, "rows": result.rows_inserted,
            "family": result.family, "warnings": result.warnings}


@app.post("/upload/nc")
async def upload_nc_compat(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        result = process_upload(file_bytes=contents, filename=file.filename, client=sb())
    except Exception as e:
        raise HTTPException(500, "Errore interno. Controlla i log per i dettagli.")
    if result.status == "failed" and not result.upload_id:
        raise HTTPException(400, result.error or "File NC non riconoscibile")
    return {"status": result.status, "rows": result.rows_inserted,
            "family": result.family, "warnings": result.warnings}


# ─────────────────────────────────────────────────────────────────
# KPI SAVING
# ─────────────────────────────────────────────────────────────────

@app.get("/kpi/saving/anni")
def get_anni():
    rows = query("saving", select="data_doc")
    df = pd.DataFrame(rows)
    if df.empty: return []
    anni = sorted(
        pd.to_datetime(df["data_doc"]).dt.year.dropna().unique().astype(int).tolist(),
        reverse=True
    )
    return [{"anno": a} for a in anni]

@app.get("/kpi/saving/riepilogo")
def kpi_riepilogo(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), alfa: Optional[str] = Query(None),
    macro: Optional[str] = Query(None)
):
    try:
        df = get_saving_df(anno, str_ric, cdc, alfa, macro,
            cols="imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
        return calc_kpi(df)
    except Exception as e:
        log.error(f"kpi_riepilogo error: {e}", exc_info=True)
        raise HTTPException(500, "Errore nel calcolo KPI. Verifica i dati caricati.")

@app.get("/kpi/saving/mensile")
def kpi_mensile(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    return sorted([{"mese": m, **calc_kpi(g)} for m, g in df.groupby("mese")], key=lambda x: x["mese"])

@app.get("/kpi/saving/mensile-con-area")
def kpi_mensile_area(anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None)):
    df = get_saving_df(anno, cdc=cdc,
        cols="data_doc,str_ric,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    MLBL = {f"{anno}-{m:02d}": MESI[m] for m in range(1, 13)} if anno else {}
    result = []
    for mese, grp in df.groupby("mese"):
        result.append({
            "mese": mese, "label": MLBL.get(mese, mese),
            **{f"tot_{k}": v for k, v in calc_kpi(grp).items()},
            **{f"ric_{k}": v for k, v in calc_kpi(grp[grp["str_ric"] == "RICERCA"]).items()},
            **{f"str_{k}": v for k, v in calc_kpi(grp[grp["str_ric"] == "STRUTTURA"]).items()},
        })
    return sorted(result, key=lambda x: x["mese"])

@app.get("/kpi/saving/per-cdc")
def kpi_per_cdc(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    df = get_saving_df(anno, str_ric,
        cols="cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    return sorted([{"cdc": c, **calc_kpi(g)} for c, g in df.groupby("cdc") if c],
                  key=lambda x: x["saving"], reverse=True)

@app.get("/kpi/saving/per-buyer")
def kpi_per_buyer(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="utente_presentazione,utente,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["buyer"] = df["utente_presentazione"].fillna(df["utente"])
    return sorted(
        [{"utente": b, **calc_kpi(g)} for b, g in df.groupby("buyer")
         if b and str(b).strip() not in ('nan', 'none', '')],
        key=lambda x: x["saving"], reverse=True
    )

@app.get("/kpi/saving/per-alfa-documento")
def kpi_per_alfa(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="alfa_documento,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo")
    if df.empty: return []
    return sorted(
        [{"alfa_documento": a, **calc_kpi(g)} for a, g in df.groupby("alfa_documento") if a],
        key=lambda x: x["listino"], reverse=True
    )

@app.get("/kpi/saving/per-macro-categoria")
def kpi_per_macro(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="macro_categoria,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["macro_categoria"] = df["macro_categoria"].fillna("Non classificato").str.strip()
    return sorted(
        [{"macro_categoria": m, **calc_kpi(g)} for m, g in df.groupby("macro_categoria")],
        key=lambda x: x["saving"], reverse=True
    )

@app.get("/kpi/saving/per-commessa")
def kpi_per_commessa(
    anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None),
    limit: int = Query(20)
):
    df = get_saving_df(
        anno,
        "RICERCA",
        cdc,
        cols="prefisso_commessa,desc_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento"
    )
    if df.empty:
        return []

    df = df.dropna(subset=["prefisso_commessa"])
    result = []

    for pref, g in df.groupby("prefisso_commessa"):
        k = calc_kpi(g)
        desc = g["desc_commessa"].dropna().mode()
        result.append({
            "prefisso_commessa": pref,
            "desc_commessa": desc.iloc[0] if not desc.empty else "—",
            **k
        })

    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]

@app.get("/kpi/saving/per-categoria")
def kpi_per_categoria(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), limit: int = Query(15)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="desc_gruppo_merceol,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["desc_gruppo_merceol"])
    result = [{"desc_gruppo_merceol": c, **calc_kpi(g)} for c, g in df.groupby("desc_gruppo_merceol")]
    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]

@app.get("/kpi/saving/top-fornitori")
def kpi_top_fornitori(
    anno: Optional[int] = Query(None), per: str = Query("saving"),
    limit: int = Query(10), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="ragione_sociale,accred_albo,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento")
    if df.empty: return []
    result = []
    for forn, g in df.groupby("ragione_sociale"):
        if not forn: continue
        k = calc_kpi(g)
        k["ragione_sociale"] = forn
        k["albo"] = bool(g["accred_albo"].mode().iloc[0]) if not g.empty else False
        result.append(k)
    sort_key = per if per in ("saving", "listino", "impegnato") else "saving"
    result.sort(key=lambda x: x.get(sort_key, 0), reverse=True)
    return result[:limit]

@app.get("/kpi/saving/pareto-fornitori")
def kpi_pareto(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    df = get_saving_df(anno, str_ric, cols="ragione_sociale,imp_impegnato_eur")
    if df.empty: return []
    grp = (df.groupby("ragione_sociale")["imp_impegnato_eur"].sum()
            .sort_values(ascending=False).reset_index())
    total = grp["imp_impegnato_eur"].sum()
    grp["cum_perc"] = (grp["imp_impegnato_eur"].cumsum() / total * 100).round(2)
    grp["rank"] = range(1, len(grp) + 1)
    return grp.to_dict(orient="records")

@app.get("/kpi/saving/valute")
def kpi_valute(anno: Optional[int] = Query(None)):
    df = get_saving_df(anno, cols="valuta,imp_listino_eur,imp_impegnato_eur")
    if df.empty: return []
    grp = df.groupby("valuta").agg(
        listino_eur=("imp_listino_eur", "sum"),
        impegnato_eur=("imp_impegnato_eur", "sum"),
        n_ordini=("imp_impegnato_eur", "count")
    ).reset_index()
    total = grp["impegnato_eur"].sum()
    grp["perc"] = (grp["impegnato_eur"] / total * 100).round(2)
    return grp.sort_values("impegnato_eur", ascending=False).to_dict(orient="records")


# ─────────────────────────────────────────────────────────────────
# YOY
# ─────────────────────────────────────────────────────────────────

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
            if len(gc_df) == 0 and len(gp_df) == 0: continue

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
                f"listino_{anno}":     kc["listino"],
                f"impegnato_{anno}":   kc["impegnato"],
                f"saving_{anno}":      kc["saving"],
                f"perc_saving_{anno}": kc["perc_saving"],
                f"n_neg_{anno}":       kc["n_negoziati"],
                f"listino_{ap}":       kp["listino"],
                f"impegnato_{ap}":     kp["impegnato"],
                f"saving_{ap}":        kp["saving"],
                f"perc_saving_{ap}":   kp["perc_saving"],
                f"n_neg_{ap}":         kp["n_negoziati"],
                "delta_saving":       delta(kc["saving"], kp["saving"])    if not parziale else None,
                "delta_impegnato":    delta(kc["impegnato"], kp["impegnato"]) if not parziale else None,
                "delta_perc_saving":  round(kc["perc_saving"] - kp["perc_saving"], 2)
                                      if kp["perc_saving"] and not parziale else None,
            })

        mesi_interi = set()
        for r in chart:
            if not r["parziale"] and r["ha_dati_curr"] and r["ha_dati_prev"]:
                for m in range(r["m_start"], r["m_end"] + 1):
                    mesi_interi.add(m)

        kc_hl = calc_kpi(df_c[df_c["mn"].isin(mesi_interi)] if not df_c.empty and mesi_interi else df_c)
        kp_hl = calc_kpi(df_p[df_p["mn"].isin(mesi_interi)] if not df_p.empty and mesi_interi else df_p)
        mc = max(mesi_interi) if mesi_interi else mese_max

        nota = ""
        if mese_max and mese_max < 12:
            nota = f"Dati {anno} disponibili fino al {df_c['data_doc'].max().date() if not df_c.empty else '—'}."
            if ult_giorno < 20 and mese_max > 1:
                nota += f" {MESI.get(mese_max, '')} è parziale ed è escluso dal confronto."

        return {
            "anno": anno, "anno_precedente": ap, "granularita": granularita,
            "chart_data": chart,
            "kpi_headline": {
                "corrente": kc_hl, "precedente": kp_hl,
                "label_curr": f"Gen–{MESI.get(mc, '?')} {anno}",
                "label_prev": f"Gen–{MESI.get(mc, '?')} {ap}",
                "delta": {
                    "listino":        delta(kc_hl["listino"],       kp_hl["listino"]),
                    "impegnato":      delta(kc_hl["impegnato"],     kp_hl["impegnato"]),
                    "saving":         delta(kc_hl["saving"],        kp_hl["saving"]),
                    "perc_saving":    round(kc_hl["perc_saving"] - kp_hl["perc_saving"], 2)
                                      if kp_hl["perc_saving"] else None,
                    "perc_negoziati": round(kc_hl["perc_negoziati"] - kp_hl["perc_negoziati"], 2)
                                      if kp_hl["perc_negoziati"] else None,
                }
            },
            "nota": nota, "mese_max": mese_max, "ultimo_giorno": ult_giorno,
        }

    except Exception as e:
        log.error(f"kpi_yoy error anno={anno}: {e}", exc_info=True)
        # Fallback graceful: evita 500 lato frontend e restituisce struttura consistente.
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
                    "listino": None,
                    "impegnato": None,
                    "saving": None,
                    "perc_saving": None,
                    "perc_negoziati": None,
                },
            },
            "nota": "YoY temporaneamente non disponibile: verifica import e qualità date (data_doc).",
            "mese_max": 0,
            "ultimo_giorno": 0,
            "error": "Errore nel calcolo YoY. Verifica che i dati siano stati importati correttamente.",
        }

@app.get("/kpi/saving/yoy-cdc")
def kpi_yoy_cdc(anno: int = Query(...)):
    ap = anno - 1
    cols = "cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = get_saving_df(anno, cols=cols)
    df_p = get_saving_df(ap,   cols=cols)
    def by_cdc(df):
        if df.empty: return {}
        return {c: calc_kpi(g) for c, g in df.groupby("cdc") if c}
    curr, prev = by_cdc(df_c), by_cdc(df_p)
    all_cdc = sorted(set(list(curr) + list(prev)))
    return [{
        "cdc": c,
        f"saving_{anno}":    curr.get(c, {}).get("saving", 0),
        f"saving_{ap}":      prev.get(c, {}).get("saving", 0),
        f"impegnato_{anno}": curr.get(c, {}).get("impegnato", 0),
        f"impegnato_{ap}":   prev.get(c, {}).get("impegnato", 0),
        f"listino_{anno}":   curr.get(c, {}).get("listino", 0),
        f"listino_{ap}":     prev.get(c, {}).get("listino", 0),
    } for c in all_cdc]


# ─────────────────────────────────────────────────────────────────
# RISORSE
# ─────────────────────────────────────────────────────────────────

@app.get("/kpi/risorse/riepilogo")
def kpi_risorse():
    rows = query("resource_performance")
    df = pd.DataFrame(rows)
    if df.empty:
        return {"available": False, "reason": "Nessun file risorse caricato."}
    n = len(df)
    risorse = df["risorsa"].dropna().nunique()
    avg_pratiche = df["pratiche_gestite"].dropna().mean()
    tot_saving = df["saving_generato"].dropna().sum()
    return {
        "available": True,
        "n_record": n,
        "n_risorse": risorse,
        "avg_pratiche_gestite": round(float(avg_pratiche), 1) if avg_pratiche else 0,
        "tot_saving_generato": round(float(tot_saving), 2) if tot_saving else 0,
    }

@app.get("/kpi/risorse/per-risorsa")
def kpi_risorse_per_risorsa(anno: Optional[int] = Query(None)):
    rows = query("resource_performance")
    df = pd.DataFrame(rows)
    if df.empty: return []
    if anno:
        df = df[df["year"] == anno]
    if df.empty: return []
    result = []
    for risorsa, g in df.groupby("risorsa"):
        result.append({
                      "risorsa": risorsa,
            "struttura": g["struttura"].dropna().mode().iloc[0] if not g["struttura"].dropna().empty else None,
            "pratiche_gestite":      int(g["pratiche_gestite"].sum()),
            "pratiche_aperte":       int(g["pratiche_aperte"].sum()),
            "pratiche_chiuse":       int(g["pratiche_chiuse"].sum()),
            "saving_generato":       round(float(g["saving_generato"].sum()), 2),
            "negoziazioni_concluse": int(g["negoziazioni_concluse"].sum()),
            "tempo_medio_giorni":    round(float(g["tempo_medio_giorni"].mean()), 1) if not g["tempo_medio_giorni"].dropna().empty else None,
            "efficienza":            round(float(g["efficienza"].mean()), 1) if not g["efficienza"].dropna().empty else None,
        })
    return sorted(result, key=lambda x: x["saving_generato"], reverse=True)

@app.get("/kpi/risorse/mensile")
def kpi_risorse_mensile(anno: Optional[int] = Query(None)):
    rows = query("resource_performance")
    df = pd.DataFrame(rows)
    if df.empty: return []
    if anno:
        df = df[df["year"] == anno]
    if df.empty: return []
    result = []
    for mese, g in df.groupby("mese_label"):
        result.append({
            "mese": mese,
            "pratiche_totali": int(g["pratiche_gestite"].sum()),
            "saving_totale":   round(float(g["saving_generato"].sum()), 2),
            "n_risorse_attive": g["risorsa"].nunique(),
        })
    return sorted(result, key=lambda x: x["mese"])


# ─────────────────────────────────────────────────────────────────
# TEMPI & NC
# ─────────────────────────────────────────────────────────────────

@app.get("/kpi/tempi/riepilogo")
def kpi_tempi():
    rows = query("tempo_attraversamento")
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n = len(df)
    return {
        "avg_total_days":             round(float(df["total_days"].mean()), 1),
        "avg_purchasing":             round(float(df["days_purchasing"].mean()), 1),
        "avg_auto":                   round(float(df["days_auto"].mean()), 1),
        "avg_other":                  round(float(df["days_other"].mean()), 1),
        "n_ordini": n,
        "perc_bottleneck_purchasing": safe_pct(int((df["bottleneck"] == "PURCHASING").sum()), n),
        "perc_bottleneck_auto":       safe_pct(int((df["bottleneck"] == "AUTO").sum()), n),
    }

@app.get("/kpi/tempi/mensile")
def kpi_tempi_mensile():
    rows = query("tempo_attraversamento")
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for ym, g in df.groupby("year_month"):
        n = len(g)
        result.append({
            "mese": ym,
            "avg_total":      round(float(g["total_days"].mean()), 1),
            "avg_purchasing": round(float(g["days_purchasing"].mean()), 1),
            "avg_auto":       round(float(g["days_auto"].mean()), 1),
            "avg_other":      round(float(g["days_other"].mean()), 1) if "days_other" in g else 0,
            "n_ordini":       n,
            "n_bottleneck_purchasing": int((g["bottleneck"] == "PURCHASING").sum()) if "bottleneck" in g else 0,
            "n_bottleneck_auto":       int((g["bottleneck"] == "AUTO").sum()) if "bottleneck" in g else 0,
            "n_bottleneck_other":      int((g["bottleneck"] == "OTHER").sum()) if "bottleneck" in g else 0,
        })
    return sorted(result, key=lambda x: x["mese"])

@app.get("/kpi/tempi/distribuzione")
def kpi_tempi_dist():
    rows = query("tempo_attraversamento", select="total_days")
    df = pd.DataFrame(rows)
    if df.empty: return []
    bins = [0, 7, 15, 30, 60, 9999]
    labels = ["≤7 gg", "8-15 gg", "16-30 gg", "31-60 gg", ">60 gg"]
    df["f"] = pd.cut(df["total_days"], bins=bins, labels=labels, right=True)
    return [{"fascia": k, "n_ordini": int(v)}
            for k, v in df["f"].value_counts().reindex(labels).fillna(0).items()]

@app.get("/kpi/nc/riepilogo")
def kpi_nc():
    rows = query("non_conformita", select="non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n = len(df); nnc = int(df["non_conformita"].sum())
    df_nc = df[df["non_conformita"] == True]
    avg_delta_nc = round(float(df_nc["delta_giorni"].mean()), 1) if len(df_nc) > 0 else 0.0
    return {
        "n_totale": n, "n_nc": nnc,
        "perc_nc": safe_pct(nnc, n),
        "avg_delta_giorni": round(float(df["delta_giorni"].mean()), 1),
        "avg_delta_nc": avg_delta_nc,
    }

@app.get("/kpi/nc/mensile")
def kpi_nc_mensile():
    rows = query("non_conformita", select="data_origine,non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return []
    df["mese"] = pd.to_datetime(df["data_origine"], errors="coerce").dt.strftime("%Y-%m")
    df = df.dropna(subset=["mese"])
    result = []
    for m, g in df.groupby("mese"):
        n = len(g); nnc = int(g["non_conformita"].sum())
        result.append({"mese": m, "n_totale": n, "n_nc": nnc,
                       "perc_nc": safe_pct(nnc, n),
                       "avg_delta": round(float(g["delta_giorni"].mean()), 1)})
    return sorted(result, key=lambda x: x["mese"])

@app.get("/kpi/nc/top-fornitori")
def kpi_nc_top(limit: int = Query(10)):
    rows = query("non_conformita", select="ragione_sociale,non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("ragione_sociale").agg(
        n_totale=("non_conformita", "count"),
        n_nc=("non_conformita", "sum"),
        avg_delta=("delta_giorni", "mean")
    ).reset_index()
    grp["perc_nc"] = (grp["n_nc"] / grp["n_totale"] * 100).round(2)
    grp["avg_delta"] = grp["avg_delta"].round(1)
    return grp[grp["n_nc"] > 0].nlargest(limit, "n_nc").to_dict(orient="records")

@app.get("/kpi/nc/per-tipo")
def kpi_nc_tipo():
    rows = query("non_conformita", select="tipo_origine,non_conformita,delta_giorni")
    df = pd.DataFrame(rows)
    if df.empty: return []
    return [{"tipo": t, "n_totale": len(g), "n_nc": int(g["non_conformita"].sum()),
             "perc_nc": safe_pct(int(g["non_conformita"].sum()), len(g)),
             "avg_delta": round(float(g["delta_giorni"].mean()), 1)}
            for t, g in df.groupby("tipo_origine")]


# ─────────────────────────────────────────────────────────────────
# FILTRI + EXPORT + UTILITY
# ─────────────────────────────────────────────────────────────────

@app.get("/kpi/saving/per-protocollo-commessa")
def kpi_per_protocollo_commessa(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), limit: int = Query(20)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="protoc_commessa,prefisso_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo,ragione_sociale")
    if df.empty: return []
    df = df.dropna(subset=["protoc_commessa"])
    result = []
    for prot, g in df.groupby("protoc_commessa"):
        k = calc_kpi(g)
        result.append({
            "protocollo": prot,
            "prefisso": g["prefisso_commessa"].dropna().mode().iloc[0] if not g["prefisso_commessa"].dropna().empty else None,
            "n_fornitori": g["ragione_sociale"].nunique(),
            **k
        })
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)[:limit]

@app.get("/kpi/saving/per-protocollo-ordine")
def kpi_per_protocollo_ordine(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    limit: int = Query(20)
):
    df = get_saving_df(anno, str_ric,
        cols="protoc_ordine,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df = df.dropna(subset=["protoc_ordine"])
    df["protoc_ordine"] = df["protoc_ordine"].astype(str)
    result = []
    for prot, g in df.groupby("protoc_ordine"):
        k = calc_kpi(g)
        result.append({"protocollo_ordine": prot, **k})
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)[:limit]

@app.get("/kpi/saving/concentration-index")
def kpi_concentration(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    df = get_saving_df(anno, str_ric, cols="ragione_sociale,imp_impegnato_eur")
    if df.empty: return {}
    total = float(df["imp_impegnato_eur"].fillna(0).sum())
    if total == 0: return {}
    grp = (df.groupby("ragione_sociale")["imp_impegnato_eur"].sum()
            .sort_values(ascending=False).reset_index())
    grp["share"] = (grp["imp_impegnato_eur"] / total * 100).round(2)
    n = len(grp)
    def cumshare(k): return round(float(grp.head(k)["share"].sum()), 2) if k <= n else 100.0
    hhi = round(float((grp["share"] ** 2).sum()), 1)
    return {
        "n_fornitori_totali": n,
        "total_impegnato": round(total, 2),
        "share_top_5":  cumshare(5),
        "share_top_10": cumshare(10),
        "share_top_20": cumshare(20),
        "hhi": hhi,
        "hhi_interpretation": (
            "Mercato molto concentrato" if hhi > 2500 else
            "Mercato concentrato" if hhi > 1500 else
            "Mercato moderatamente concentrato" if hhi > 1000 else
            "Mercato non concentrato"
        ),
        "top_5": grp.head(5)[["ragione_sociale","imp_impegnato_eur","share"]].to_dict(orient="records"),
    }

@app.get("/kpi/saving/per-buyer-cdc")
def kpi_per_buyer_cdc(anno: Optional[int] = Query(None)):
    df = get_saving_df(anno,
        cols="utente_presentazione,utente,cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["buyer"] = df["utente_presentazione"].fillna(df["utente"])
    df["buyer"] = df["buyer"].fillna("N/D")
    result = []
    for (buyer, cdc_v), g in df.groupby(["buyer", "cdc"]):
        if not buyer or not cdc_v: continue
        k = calc_kpi(g)
        result.append({"buyer": buyer, "cdc": cdc_v, **k})
    return sorted(result, key=lambda x: x["saving"], reverse=True)

@app.get("/filtri/disponibili")
def filtri_disponibili(anno: Optional[int] = Query(None)):
    fs = [lambda q, a=anno: q.gte("data_doc", f"{a}-01-01").lte("data_doc", f"{a}-12-31")] if anno else []
    rows = query("saving", fs,
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
    filtri_in = body.get("filtri", {})
    sezioni   = body.get("sezioni", ["riepilogo","mensile","cdc","alfa_documento","top_fornitori"])
    anno      = filtri_in.get("anno")
    str_ric   = filtri_in.get("str_ric")
    cdc       = filtri_in.get("cdc")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if "riepilogo" in sezioni:
            k = kpi_riepilogo(anno=anno, str_ric=str_ric, cdc=cdc)
            pd.DataFrame([
                ["Listino €",    k["listino"],   "Prezzo di partenza"],
                ["Impegnato €",  k["impegnato"], "Quanto paghiamo"],
                ["Saving €",     k["saving"],    "Il nostro lavoro"],
                ["% Saving",     f"{k['perc_saving']}%", "saving/listino×100"],
                ["N° Righe",     k["n_righe"],   ""],
                ["N° Negoziabili", k["n_doc_neg"], ""],
                ["N° Negoziati", k["n_negoziati"], ""],
                ["% Negoziati",  f"{k['perc_negoziati']}%", ""],
            ], columns=["KPI","Valore","Note"]).to_excel(writer, index=False, sheet_name="Riepilogo")
        if "mensile" in sezioni:
            d = kpi_mensile(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Mensile")
        if "cdc" in sezioni:
            d = kpi_per_cdc(anno=anno, str_ric=str_ric)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Per CDC")
        if "alfa_documento" in sezioni:
            d = kpi_per_alfa(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Per Tipo Documento")
        if "top_fornitori" in sezioni:
            d = kpi_top_fornitori(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Top Fornitori")
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=report_{anno or 'tutti'}.xlsx"})

@app.get("/upload/log")
def upload_log():
    rows = sb().table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data
    if not isinstance(rows, list):
        return []
    import json as _json
    def _safe_list(v):
        if isinstance(v, list): return v
        if isinstance(v, str):
            try: parsed = _json.loads(v); return parsed if isinstance(parsed, list) else []
            except: return []
        return []
    for row in rows:
        row["available_analyses"] = _safe_list(row.get("available_analyses"))
        row["blocked_analyses"]   = _safe_list(row.get("blocked_analyses"))
        row["warnings"]           = _safe_list(row.get("warnings"))
    return rows

@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}

@app.get("/wake")
def wake():
    return {"ok": True, "version": "9.1.0"}

@app.get("/health")
def health():
    try:
        client = sb()
        client.table("upload_log").select("id").limit(1).execute()
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "version": "9.1.0",
        "database": "reachable" if db_ok else "unreachable",
    }
