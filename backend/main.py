"""
UA Dashboard Backend v8 — Fondazione Telethon ETS
Architettura pulita: logica business in domain.py, API in main.py.

KPI DEFINITIVI:
  listino    = Imp. Iniziale €   (prezzo di partenza)
  impegnato  = Imp. Negoziato €  (quanto paghiamo)
  saving     = Saving.1          (il nostro lavoro: listino - impegnato)
  % saving   = saving / listino × 100

NUMERI DI RIFERIMENTO 2025:
  10.413 righe | listino €77.47M | impegnato €69.68M | saving €7.79M | 10.06%
"""
import os, io, logging
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client
from dotenv import load_dotenv

# Import dal modulo domain (testabile separatamente)
from domain import (
    map_cols, gcol, best_sheet, build_record, calc_kpi,
    validate_mapping, _s, _b, _f, _fn, _i, _d, clean, safe_pct,
    DOC_NEG,
)

MESI_IT = {1:"Gen",2:"Feb",3:"Mar",4:"Apr",5:"Mag",6:"Giu",
           7:"Lug",8:"Ago",9:"Set",10:"Ott",11:"Nov",12:"Dic"}

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ua")

app = FastAPI(title="UA Dashboard API", version="8.0.0")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")

app.add_middleware(
    CORSMiddleware, allow_origins=ORIGINS,
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

def sb():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────────────────────────
# QUERY CON PAGINAZIONE AUTOMATICA
# Supabase: max 1000 righe per request. Iteriamo finché < PAGE.
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

def saving_filters(anno=None, str_ric=None, cdc=None, alfa=None, macro=None, pref_comm=None):
    fs = []
    if anno:       fs.append(lambda q, a=anno: q.gte("data_doc", f"{a}-01-01").lte("data_doc", f"{a}-12-31"))
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
    df['data_doc'] = pd.to_datetime(df['data_doc'], errors='coerce')
    for c in ['imp_listino_eur', 'imp_impegnato_eur', 'saving_eur']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    for c in ['negoziazione', 'accred_albo']:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)
    return df


# ─────────────────────────────────────────────────────────────────
# UPLOAD — SAVING
# ─────────────────────────────────────────────────────────────────
@app.post("/upload/saving")
async def upload_saving(file: UploadFile = File(...), cdc_override: Optional[str] = None):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        sheet = best_sheet(xl)
        df = pd.read_excel(xl, sheet_name=sheet)
    except Exception as e:
        raise HTTPException(400, f"Errore lettura file: {e}")

    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
    df.columns = [c.strip() for c in df.columns]
    col = map_cols(df.columns)

    # Validazione mapping
    validation = validate_mapping(col)
    if not validation['valid']:
        raise HTTPException(400,
            f"File non valido. Campi mancanti: {validation['missing_critical']}. "
            f"Scarica il template standard per un formato garantito.")

    date_col = col.get('data_doc')
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])

    # Auto-filtro anno dominante
    yr = df[date_col].dt.year.value_counts()
    if len(yr) > 1 and yr.iloc[0] / len(df) >= 0.95:
        anno_dom = int(yr.index[0])
        df = df[df[date_col].dt.year == anno_dom]
        log.info(f"Auto-filtro anno {anno_dom}: {len(df)} righe")

    client = sb()
    lr = client.table("upload_log").insert({
        "filename": file.filename, "tipo": "saving", "cdc_filter": cdc_override
    }).execute()
    upload_id = lr.data[0]["id"]

    records = [build_record(col, row, upload_id, cdc_override)
               for _, row in df.iterrows()
               if _d(row.get(date_col)) is not None]

    inserted = 0
    BATCH = 5000
    for i in range(0, len(records), BATCH):
        batch = records[i:i + BATCH]
        try:
            client.table("saving").insert(batch).execute()
            inserted += len(batch)
            log.info(f"Batch {i//BATCH+1}: {inserted}/{len(records)}")
        except Exception as e:
            log.error(f"Insert error batch {i}: {str(e)[:400]}")
            if i == 0:
                client.table("upload_log").delete().eq("id", upload_id).execute()
                raise HTTPException(500, f"Errore DB: {str(e)[:400]}")

    client.table("upload_log").update({"rows_inserted": inserted}).eq("id", upload_id).execute()
    log.info(f"Upload OK: {inserted} righe, foglio='{sheet}', warnings={validation['warnings']}")

    return {
        "status": "ok",
        "rows_inserted": inserted,
        "upload_id": upload_id,
        "sheet_used": sheet,
        "mapping_confidence": validation['confidence'],
        "warnings": validation['warnings'],
    }


@app.post("/upload/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        sh = max(xl.sheet_names, key=lambda s: len(pd.read_excel(xl, sheet_name=s)))
        df = pd.read_excel(xl, sheet_name=sh)
    except Exception as e:
        raise HTTPException(400, str(e))
    df.columns = [c.strip() for c in df.columns]
    n = {c.lower(): c for c in df.columns}
    def gc(k): return n.get(k)
    client = sb()
    lr = client.table("upload_log").insert({"filename": file.filename, "tipo": "tempi"}).execute()
    uid = lr.data[0]["id"]
    recs = [{
        "upload_id":        uid,
        "protocol":         _s(row.get(gc("protocol"))),
        "year_month":       _s(row.get(gc("year_month")) or row.get(gc("anno_mese"))),
        "days_purchasing":  _f(row.get(gc("days_purchasing"))),
        "days_auto":        _f(row.get(gc("days_auto"))),
        "days_other":       _f(row.get(gc("days_other"))),
        "total_days":       _f(row.get(gc("total_days"))),
        "perc_purchasing":  _f(row.get(gc("perc_purchasing"))),
        "perc_auto":        _f(row.get(gc("perc_auto"))),
        "perc_other":       _f(row.get(gc("perc_other"))),
        "bottleneck":       _s(row.get(gc("bottleneck"))),
    } for _, row in df.iterrows()]
    for i in range(0, len(recs), 500):
        client.table("tempo_attraversamento").insert(recs[i:i+500]).execute()
    client.table("upload_log").update({"rows_inserted": len(recs)}).eq("id", uid).execute()
    return {"status": "ok", "rows": len(recs)}


@app.post("/upload/nc")
async def upload_nc(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        sh = max(xl.sheet_names, key=lambda s: len(pd.read_excel(xl, sheet_name=s)))
        df = pd.read_excel(xl, sheet_name=sh)
    except Exception as e:
        raise HTTPException(400, str(e))
    df.columns = [c.strip() for c in df.columns]
    n = {c.lower(): c for c in df.columns}
    def gc(k): return n.get(k)
    client = sb()
    lr = client.table("upload_log").insert({"filename": file.filename, "tipo": "nc"}).execute()
    uid = lr.data[0]["id"]
    recs = [{
        "upload_id":             uid,
        "protocollo_commessa":   _s(row.get(gc("protocollo commessa"))),
        "ragione_sociale":       _s(row.get(gc("ragione sociale anagrafica") or gc("ragione sociale") or "")),
        "tipo_origine":          _s(row.get(gc("tipo origine"))),
        "data_origine":          _d(row.get(gc("data origine"))),
        "utente_origine":        _s(row.get(gc("utente origine"))),
        "codice_prima_fattura":  _s(row.get(gc("codice prima fattura"))),
        "data_prima_fattura":    _d(row.get(gc("data prima fattura"))),
        "importo_prima_fattura": _fn(row.get(gc("importo prima fattura"))),
        "delta_giorni":          _fn(row.get(gc("delta giorni (fattura - origine)") or gc("delta giorni"))),
        "non_conformita":        _b(row.get(gc("non conformità") or gc("non conformita"))),
    } for _, row in df.iterrows()]
    for i in range(0, len(recs), 500):
        client.table("non_conformita").insert(recs[i:i+500]).execute()
    client.table("upload_log").update({"rows_inserted": len(recs)}).eq("id", uid).execute()
    return {"status": "ok", "rows": len(recs)}


# ─────────────────────────────────────────────────────────────────
# KPI SAVING
# ─────────────────────────────────────────────────────────────────
@app.get("/kpi/saving/anni")
def get_anni():
    rows = query("saving", select="data_doc")
    df = pd.DataFrame(rows)
    if df.empty: return []
    anni = sorted(pd.to_datetime(df["data_doc"]).dt.year.dropna().unique().astype(int).tolist(), reverse=True)
    return [{"anno": a} for a in anni]

@app.get("/kpi/saving/riepilogo")
def kpi_riepilogo(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), alfa: Optional[str] = Query(None),
    macro: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc, alfa, macro,
        cols="imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    return calc_kpi(df)

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

@app.get("/kpi/saving/per-cdc")
def kpi_per_cdc(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)
):
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
         if b and str(b).strip() and str(b).strip().lower() not in ('nan', 'none')],
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
    return sorted([{"alfa_documento": a, **calc_kpi(g)} for a, g in df.groupby("alfa_documento") if a],
        key=lambda x: x["listino"], reverse=True)

@app.get("/kpi/saving/per-macro-categoria")
def kpi_per_macro(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)
):
    df = get_saving_df(anno, str_ric, cdc,
        cols="macro_categoria,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["macro_categoria"] = df["macro_categoria"].fillna("Non classificato").str.strip()
    return sorted([{"macro_categoria": m, **calc_kpi(g)} for m, g in df.groupby("macro_categoria")],
        key=lambda x: x["saving"], reverse=True)

@app.get("/kpi/saving/per-commessa")
def kpi_per_commessa(
    anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None),
    limit: int = Query(20)
):
    df = get_saving_df(anno, "RICERCA", cdc,
        cols="prefisso_commessa,desc_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["prefisso_commessa"])
    result = []
    for pref, g in df.groupby("prefisso_commessa"):
        k = calc_kpi(g)
        desc = g["desc_commessa"].dropna().mode()
        result.append({"prefisso_commessa": pref, "desc_commessa": desc.iloc[0] if not desc.empty else "—", **k})
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
def kpi_pareto(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)
):
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
# YOY GRANULARE
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
    ap = anno - 1
    periodi = GRAN_MAP.get(granularita, GRAN_MAP["mensile"])
    cols = "data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"

    df_c = get_saving_df(anno, str_ric, cdc, cols=cols)
    df_p = get_saving_df(ap,   str_ric, cdc, cols=cols)

    if not df_c.empty: df_c["mn"] = df_c["data_doc"].dt.month
    if not df_p.empty: df_p["mn"] = df_p["data_doc"].dt.month

    mese_max   = int(df_c["mn"].max()) if not df_c.empty else 0
    ult_giorno = int(df_c[df_c["mn"] == mese_max]["data_doc"].dt.day.max()) if mese_max else 0

    def delta(c, p): return round((c - p) / abs(p) * 100, 1) if p else None

    chart = []
    for m1, m2, lbl in periodi:
        gc = df_c[(df_c["mn"] >= m1) & (df_c["mn"] <= m2)] if not df_c.empty else pd.DataFrame()
        gp = df_p[(df_p["mn"] >= m1) & (df_p["mn"] <= m2)] if not df_p.empty else pd.DataFrame()
        if len(gc) == 0 and len(gp) == 0: continue

        parziale = len(gc) > 0 and mese_max < m2
        if   granularita == "mensile":    label = MESI_IT.get(m1, lbl)
        elif granularita == "bimestrale": label = f"{MESI_IT[m1]}–{MESI_IT[m2]}"
        elif granularita == "quarter":    label = lbl
        elif granularita == "semestrale": label = f"{lbl} ({MESI_IT[m1]}–{MESI_IT[m2]})"
        else:                             label = str(anno)

        kc, kp = calc_kpi(gc), calc_kpi(gp)
        chart.append({
            "label": label, "m_start": m1, "m_end": m2, "parziale": parziale,
            "ha_dati_curr": len(gc) > 0, "ha_dati_prev": len(gp) > 0,
            f"listino_{anno}":    kc["listino"],
            f"impegnato_{anno}":  kc["impegnato"],
            f"saving_{anno}":     kc["saving"],
            f"perc_saving_{anno}":kc["perc_saving"],
            f"n_neg_{anno}":      kc["n_negoziati"],
            f"listino_{ap}":      kp["listino"],
            f"impegnato_{ap}":    kp["impegnato"],
            f"saving_{ap}":       kp["saving"],
            f"perc_saving_{ap}":  kp["perc_saving"],
            f"n_neg_{ap}":        kp["n_negoziati"],
            "delta_saving":       delta(kc["saving"],    kp["saving"])    if not parziale else None,
            "delta_impegnato":    delta(kc["impegnato"], kp["impegnato"]) if not parziale else None,
            "delta_perc_saving":  round(kc["perc_saving"] - kp["perc_saving"], 2) if kp["perc_saving"] and not parziale else None,
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
            nota += f" {MESI_IT.get(mese_max,'')} è parziale ed è escluso dal confronto."

    return {
        "anno": anno, "anno_precedente": ap, "granularita": granularita,
        "chart_data": chart,
        "kpi_headline": {
            "corrente": kc_hl, "precedente": kp_hl,
            "label_curr": f"Gen–{MESI_IT.get(mc,'?')} {anno}",
            "label_prev": f"Gen–{MESI_IT.get(mc,'?')} {ap}",
            "delta": {
                "listino":        delta(kc_hl["listino"],       kp_hl["listino"]),
                "impegnato":      delta(kc_hl["impegnato"],     kp_hl["impegnato"]),
                "saving":         delta(kc_hl["saving"],        kp_hl["saving"]),
                "perc_saving":    round(kc_hl["perc_saving"]    - kp_hl["perc_saving"],    2) if kp_hl["perc_saving"]    else None,
                "perc_negoziati": round(kc_hl["perc_negoziati"] - kp_hl["perc_negoziati"], 2) if kp_hl["perc_negoziati"] else None,
            }
        },
        "nota": nota, "mese_max": mese_max, "ultimo_giorno": ult_giorno,
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

@app.get("/kpi/saving/mensile-con-area")
def kpi_mensile_area(anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None)):
    df = get_saving_df(anno, cdc=cdc,
        cols="data_doc,str_ric,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    MLBL = {f"{anno}-{m:02d}": MESI_IT[m] for m in range(1, 13)} if anno else {}
    result = []
    for mese, grp in df.groupby("mese"):
        result.append({
            "mese": mese,
            "label": MLBL.get(mese, mese),
            **{f"tot_{k}": v for k, v in calc_kpi(grp).items()},
            **{f"ric_{k}": v for k, v in calc_kpi(grp[grp["str_ric"] == "RICERCA"]).items()},
            **{f"str_{k}": v for k, v in calc_kpi(grp[grp["str_ric"] == "STRUTTURA"]).items()},
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
    return sorted([{
        "mese": ym,
        "avg_total":      round(float(g["total_days"].mean()), 1),
        "avg_purchasing": round(float(g["days_purchasing"].mean()), 1),
        "avg_auto":       round(float(g["days_auto"].mean()), 1),
        "n_ordini":       len(g),
    } for ym, g in df.groupby("year_month")], key=lambda x: x["mese"])

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
    return {"n_totale": n, "n_nc": nnc, "perc_nc": safe_pct(nnc, n),
            "avg_delta_giorni": round(float(df["delta_giorni"].mean()), 1)}

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
                ["Listino €",    k["listino"],              "Prezzo di partenza"],
                ["Impegnato €",  k["impegnato"],            "Quanto paghiamo"],
                ["Saving €",     k["saving"],               "Il nostro lavoro"],
                ["% Saving",     f"{k['perc_saving']}%",    "saving/listino×100"],
                ["N° Righe",     k["n_righe"],              ""],
                ["N° Negoziabili", k["n_doc_neg"],          ""],
                ["N° Negoziati", k["n_negoziati"],          ""],
                ["% Negoziati",  f"{k['perc_negoziati']}%", ""],
                ["N° Albo",      k["n_albo"],               ""],
                ["% Albo",       f"{k['perc_albo']}%",      ""],
            ], columns=["KPI", "Valore", "Note"]).to_excel(writer, index=False, sheet_name="Riepilogo")
        if "mensile" in sezioni:
            d = kpi_mensile(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Mensile")
        if "cdc" in sezioni:
            d = kpi_per_cdc(anno=anno, str_ric=str_ric)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Per CDC")
        if "alfa_documento" in sezioni:
            d = kpi_per_alfa(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Per Tipo Documento")
        if "macro_categoria" in sezioni:
            d = kpi_per_macro(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Macro Categoria")
        if "top_fornitori" in sezioni:
            d = kpi_top_fornitori(anno=anno, str_ric=str_ric, cdc=cdc)
            if d: pd.DataFrame(d).to_excel(writer, index=False, sheet_name="Top Fornitori")
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=report_{anno or 'tutti'}.xlsx"})

@app.get("/upload/log")
def upload_log():
    return sb().table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data

@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}

@app.get("/wake")
def wake():
    return {"ok": True}

@app.get("/health")
def health():
    return {
        "status": "ok", "version": "8.0.0",
        "kpi_definitions": {
            "listino":   "Imp. Iniziale € — prezzo di partenza senza negoziazione",
            "impegnato": "Imp. Negoziato € — quanto paghiamo effettivamente",
            "saving":    "Saving.1 — differenza (listino - impegnato) = lavoro UA",
            "perc_saving": "saving / listino × 100",
        },
        "reference_2025": {
            "righe": 10413,
            "listino": 77465963.18,
            "impegnato": 69676501.89,
            "saving": 7789461.29,
            "perc_saving": 10.06,
        }
    }
