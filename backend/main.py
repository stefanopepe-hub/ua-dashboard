"""
UA Dashboard Backend v9 — Fondazione Telethon ETS
Architettura unificata: UN SOLO pipeline ingestion per preview + import + analytics.

ROOT CAUSES RISOLTI:
  1. Split-brain: upload_saving() ora usa ingestion_engine (non domain.COL_MAP)
  2. Field mismatch: colonne DB canoniche usate ovunque
  3. Resource file: nuovo endpoint + tabella
  4. /wake timeout: timeout esteso a 60s nel frontend
  5. Missing endpoints: tutte le routes esistono e funzionano

CANONICAL DB FIELDS (tabella saving):
  imp_listino_eur    = Imp. Iniziale €   (listino)
  imp_impegnato_eur  = Imp. Negoziato €  (impegnato)
  saving_eur         = Saving.1          (saving)
  macro_categoria    = macro categorie
  utente_presentazione = utente per presentazione
"""
import os, io, logging
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client
from dotenv import load_dotenv

# ── Import moduli interni ─────────────────────────────────────────
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

app = FastAPI(title="UA Dashboard API", version="9.0.0")

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
# PAGINAZIONE SUPABASE — max 1000 righe per request
# ─────────────────────────────────────────────────────────────────
PAGE = 1000

def query(table: str, filters=None, select: str = "*") -> list:
    """Query con paginazione automatica."""
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
    """Costruisce filtri Supabase per la tabella saving."""
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
    """Carica saving dal DB con paginazione e normalizzazione."""
    rows = query("saving", saving_filters(anno, str_ric, cdc, alfa, macro, pref_comm), cols)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Normalizza tipi
    df['data_doc'] = pd.to_datetime(df.get('data_doc', pd.Series()), errors='coerce')
    for c in ['imp_listino_eur', 'imp_impegnato_eur', 'saving_eur']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    for c in ['negoziazione', 'accred_albo']:
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)
    return df


# ─────────────────────────────────────────────────────────────────
# UTILITY: costruisce record canonical dalla riga usando ingestion_engine
# ─────────────────────────────────────────────────────────────────

def build_saving_record(
    col_map: dict,       # canonical → FieldMapping
    row: pd.Series,
    upload_id: str,
    cdc_override: Optional[str] = None,
) -> Optional[dict]:
    """
    Costruisce il record DB canonico da una riga.
    Usa ingestion_engine col_map (FieldMapping objects).
    """
    def gcol(canonical: str):
        fm = col_map.get(canonical)
        if not fm:
            return None
        return row.get(fm.source_column)

    # Data — obbligatoria
    dv = _d(gcol('data_doc'))
    if not dv:
        return None

    cambio = _f(gcol('cambio'), 1.0) or 1.0
    valuta = _s(gcol('valuta')) or 'EURO'

    # Importi EUR (priorità) o valuta originale convertita
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

    # Ricalcola se mancante
    if sav == 0 and lst > 0 and imp > 0:
        sav = lst - imp
    if pct_s == 0 and lst > 0:
        pct_s = sav / lst * 100

    # CDC
    if cdc_override:
        cdc_val = cdc_override
    elif 'cdc' in col_map:
        cdc_val = _s(gcol('cdc'))
    else:
        cdc_val = derive_cdc(
            _s(gcol('centro_costo')) or '',
            _s(gcol('desc_cdc')) or ''
        )

    # Commessa
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
        # Canonical financial fields (DB canonical names)
        "imp_listino_eur":      lst,
        "imp_impegnato_eur":    imp,
        "saving_eur":           sav,
        "perc_saving_eur":      pct_s,
        # Legacy fields (valuta originale)
        "imp_iniziale":         _f(gcol('listino_val')),
        "imp_negoziato":        _f(gcol('impegnato_val')),
        "saving_val":           _f(gcol('saving_val')),
        "perc_saving":          _f(gcol('perc_saving_val')),
        "negoziazione":         _b(gcol('negoziazione')),
        "tail_spend":           _s(gcol('tail_spend')),
    }
    return {k: clean(v) for k, v in r.items()}


# ─────────────────────────────────────────────────────────────────
# UPLOAD — SAVING  (pipeline unificata, usa ingestion_engine)
# ─────────────────────────────────────────────────────────────────

@app.post("/upload/saving")
async def upload_saving(file: UploadFile = File(...), cdc_override: Optional[str] = None):
    """
    Pipeline unificata: ingestion_engine → canonical → DB.
    Stesso mapping usato da /upload/inspect e da tutte le analytics.
    """
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, f"Errore apertura file: {e}")

    # ── Step 1: ispeziona con ingestion_engine (STESSO motore del preview) ──
    mr = inspect_workbook(xl)

    # ── Step 2: valida ──
    if mr.overall_score < 0.30:
        raise HTTPException(400,
            f"File non riconoscibile (confidence {mr.overall_score:.0%}). "
            f"Tipo rilevato: {mr.family.value}. "
            f"Campi mancanti: {mr.missing_critical}. "
            f"Scarica il template standard per un formato garantito.")

    if mr.family not in (FileFamily.SAVINGS, FileFamily.ORDERS_DETAIL):
        raise HTTPException(400,
            f"Tipo file rilevato: '{mr.family.value}' — questo endpoint è per file Saving/Ordini. "
            f"Usa /upload/risorse per file risorse, /upload/nc per non conformità.")

    # ── Step 3: rileggi il foglio con header corretto ──
    df = pd.read_excel(xl, sheet_name=mr.sheet_name, header=mr.header_row)
    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
    df.columns = [str(c).strip() for c in df.columns]

    # ── Step 4: usa il col_map dell'engine (stesso del preview) ──
    col_map = mr.fields  # Dict[canonical → FieldMapping]

    # ── Step 5: filtro anno (auto-detect anno dominante) ──
    date_fm = col_map.get('data_doc')
    if date_fm:
        date_col = date_fm.source_column
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])
        yr = df[date_col].dt.year.value_counts()
        if len(yr) > 1 and yr.iloc[0] / len(df) >= 0.95:
            anno_dom = int(yr.index[0])
            df = df[df[date_col].dt.year == anno_dom]
            log.info(f"Auto-filtro anno {anno_dom}: {len(df)} righe")

    # ── Step 6: crea upload_log entry ──
    preview_dict = mapping_result_to_dict(mr)
    client = sb()
    lr = client.table("upload_log").insert({
        "filename":           file.filename,
        "tipo":               "saving",
        "cdc_filter":         cdc_override,
        "family_detected":    mr.family.value,
        "mapping_confidence": mr.overall_confidence.value,
        "mapping_score":      mr.overall_score,
        "sheet_used":         mr.sheet_name,
        "header_row":         mr.header_row,
        "available_analyses": preview_dict.get('available_analyses', []),
        "blocked_analyses":   preview_dict.get('blocked_analyses', []),
        "warnings":           preview_dict.get('warnings', []),
    }).execute()
    upload_id = lr.data[0]["id"]

    # ── Step 7: costruisci records canonical ──
    records = []
    skipped = 0
    for _, row in df.iterrows():
        rec = build_saving_record(col_map, row, upload_id, cdc_override)
        if rec:
            records.append(rec)
        else:
            skipped += 1

    # ── Step 8: insert in batch ──
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
    log.info(f"Upload OK: {inserted} righe, {skipped} saltate")

    return {
        "status": "ok",
        "rows_inserted": inserted,
        "rows_skipped": skipped,
        "upload_id": upload_id,
        "sheet_used": mr.sheet_name,
        "family": mr.family.value,
        "mapping_confidence": mr.overall_confidence.value,
        "mapping_score": mr.overall_score,
        "available_analyses": mr.available_analyses,
        "blocked_analyses": mr.blocked_analyses,
        "warnings": mr.warnings,
    }


@app.post("/upload/inspect")
async def upload_inspect(file: UploadFile = File(...)):
    """Preview intelligente senza importare. Stesso engine del vero import."""
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        mr = inspect_workbook(xl)
        return mapping_result_to_dict(mr)
    except Exception as e:
        raise HTTPException(400, f"Errore ispezione: {str(e)[:300]}")


@app.post("/upload/risorse")
async def upload_risorse(file: UploadFile = File(...)):
    """Upload file analytics risorse / team."""
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, f"Errore apertura file: {e}")

    mr = inspect_workbook(xl)
    # Accetta anche se classificato come altro, se ha almeno il campo 'risorsa'
    has_risorsa = 'risorsa' in mr.fields or 'pratiche_gestite' in mr.fields
    if not has_risorsa and mr.family != FileFamily.RISORSE:
        fam_scores = mr.family_candidate_scores
        raise HTTPException(400,
            f"File non riconoscibile come file risorse. "
            f"Tipo rilevato: '{mr.family.value}' (confidence {mr.family_confidence:.0%}). "
            f"Score risorse: {fam_scores.get('risorse', 0):.0%}. "
            f"Il file deve contenere colonne: Risorsa, Mese, Pratiche Gestite. "
            f"Colonne trovate: {mr.raw_columns[:10]}")

    df = pd.read_excel(xl, sheet_name=mr.sheet_name, header=mr.header_row)
    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
    df.columns = [str(c).strip() for c in df.columns]
    col_map = mr.fields

    def gcol(canonical: str, row):
        """Safe gcol con row esplicita — no closure bug."""
        fm = col_map.get(canonical)
        if not fm: return None
        return row.get(fm.source_column)

    client = sb()
    lr = client.table("upload_log").insert({
        "filename": file.filename, "tipo": "risorse",
        "family_detected": mr.family.value,
        "mapping_confidence": mr.overall_confidence.value,
        "sheet_used": mr.sheet_name,
    }).execute()
    uid = lr.data[0]["id"]

    records = []
    for _, row in df.iterrows():
        mese_raw = _s(gcol('year_month', row)) or ''
        year = month = quarter = None
        try:
            parts = mese_raw.split('-')
            if len(parts) == 2:
                year, month = int(parts[0]), int(parts[1])
                quarter = (month - 1) // 3 + 1
        except Exception:
            pass

        records.append({
            "upload_id":             uid,
            "year":                  year,
            "month":                 month,
            "quarter":               quarter,
            "mese_label":            mese_raw,
            "risorsa":               _s(gcol('risorsa', row)) or 'N/D',
            "struttura":             _s(gcol('str_ric', row)),
            "pratiche_gestite":      _i(gcol('pratiche_gestite', row)),
            "pratiche_aperte":       _i(gcol('pratiche_aperte', row)),
            "pratiche_chiuse":       _i(gcol('pratiche_chiuse', row)),
            "saving_generato":       _fn(gcol('saving_generato', row)),
            "negoziazioni_concluse": _i(gcol('negoziazioni_concluse', row)),
            "tempo_medio_giorni":    _fn(gcol('tempo_medio_risorsa', row)),
            "efficienza":            _fn(gcol('efficienza', row)),
        })

    records_clean = [{k: clean(v) for k, v in r.items()} for r in records]
    for i in range(0, len(records_clean), 500):
        client.table("resource_performance").insert(records_clean[i:i+500]).execute()
    client.table("upload_log").update({"rows_inserted": len(records_clean)}).eq("id", uid).execute()

    return {"status": "ok", "rows": len(records_clean), "family": mr.family.value}


@app.post("/upload/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        mr = inspect_workbook(xl)
        df = pd.read_excel(xl, sheet_name=mr.sheet_name, header=mr.header_row)
        df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
        df.columns = [str(c).strip() for c in df.columns]
    except Exception as e:
        raise HTTPException(400, str(e))

    col_map = mr.fields
    def gcol(canonical):
        fm = col_map.get(canonical)
        return row.get(fm.source_column) if fm else None

    client = sb()
    lr = client.table("upload_log").insert({
        "filename": file.filename, "tipo": "tempi",
        "family_detected": mr.family.value,
    }).execute()
    uid = lr.data[0]["id"]

    recs = []
    for _, row in df.iterrows():
        recs.append({
            "upload_id":        uid,
            "protocol":         _s(gcol('protoc_commessa')),
            "year_month":       _s(gcol('year_month')),
            "days_purchasing":  _f(gcol('days_purchasing')),
            "days_auto":        _f(gcol('days_auto')),
            "days_other":       _f(gcol('days_other')),
            "total_days":       _f(gcol('total_days')),
            "bottleneck":       _s(gcol('bottleneck')),
        })

    for i in range(0, len(recs), 500):
        client.table("tempo_attraversamento").insert(
            [{k: clean(v) for k, v in r.items()} for r in recs[i:i+500]]
        ).execute()
    client.table("upload_log").update({"rows_inserted": len(recs)}).eq("id", uid).execute()
    return {"status": "ok", "rows": len(recs)}


@app.post("/upload/nc")
async def upload_nc(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        mr = inspect_workbook(xl)
        df = pd.read_excel(xl, sheet_name=mr.sheet_name, header=mr.header_row)
        df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
        df.columns = [str(c).strip() for c in df.columns]
    except Exception as e:
        raise HTTPException(400, str(e))

    col_map = mr.fields
    def gcol(canonical):
        fm = col_map.get(canonical)
        return row.get(fm.source_column) if fm else None

    client = sb()
    lr = client.table("upload_log").insert({
        "filename": file.filename, "tipo": "nc",
        "family_detected": mr.family.value,
    }).execute()
    uid = lr.data[0]["id"]

    recs = []
    for _, row in df.iterrows():
        recs.append({
            "upload_id":             uid,
            "ragione_sociale":       _s(gcol('ragione_sociale')),
            "tipo_origine":          _s(gcol('tipo_origine')),
            "data_origine":          _d(gcol('data_origine')) or _d(gcol('data_doc')),
            "utente_origine":        _s(gcol('utente')),
            "delta_giorni":          _fn(gcol('delta_giorni')),
            "non_conformita":        _b(gcol('non_conformita')),
        })

    for i in range(0, len(recs), 500):
        client.table("non_conformita").insert(
            [{k: clean(v) for k, v in r.items()} for r in recs[i:i+500]]
        ).execute()
    client.table("upload_log").update({"rows_inserted": len(recs)}).eq("id", uid).execute()
    return {"status": "ok", "rows": len(recs)}


# ─────────────────────────────────────────────────────────────────
# KPI SAVING — tutte le analytics usano campi DB canonici
# ─────────────────────────────────────────────────────────────────

MESI = {1:"Gen",2:"Feb",3:"Mar",4:"Apr",5:"Mag",6:"Giu",
        7:"Lug",8:"Ago",9:"Set",10:"Ott",11:"Nov",12:"Dic"}

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
    ap = anno - 1
    periodi = GRAN_MAP.get(granularita, GRAN_MAP["mensile"])
    cols = "data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"

    df_c = get_saving_df(anno, str_ric, cdc, cols=cols)
    df_p = get_saving_df(ap, str_ric, cdc, cols=cols)

    if not df_c.empty: df_c["mn"] = df_c["data_doc"].dt.month
    if not df_p.empty: df_p["mn"] = df_p["data_doc"].dt.month

    mese_max   = int(df_c["mn"].max()) if not df_c.empty else 0
    ult_giorno = int(df_c[df_c["mn"] == mese_max]["data_doc"].dt.day.max()) if mese_max else 0

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
# RISORSE ANALYTICS
# ─────────────────────────────────────────────────────────────────

@app.get("/kpi/risorse/riepilogo")
def kpi_risorse():
    rows = query("resource_performance")
    df = pd.DataFrame(rows)
    if df.empty:
        return {"available": False, "reason": "Nessun file risorse caricato. Vai in Carica Dati e importa il file team analytics."}
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
# FILTRI + EXPORT
# ─────────────────────────────────────────────────────────────────

@app.get("/kpi/saving/per-protocollo-commessa")
def kpi_per_protocollo_commessa(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), limit: int = Query(20)
):
    """Analisi per protocollo commessa — ricerca."""
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
    """Analisi per protocollo ordine."""
    df = get_saving_df(anno, str_ric,
        cols="protoc_ordine,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df = df.dropna(subset=["protoc_ordine"])
    # protoc_ordine è numerico
    df["protoc_ordine"] = df["protoc_ordine"].astype(str)
    result = []
    for prot, g in df.groupby("protoc_ordine"):
        k = calc_kpi(g)
        result.append({"protocollo_ordine": prot, **k})
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)[:limit]

@app.get("/kpi/saving/concentration-index")
def kpi_concentration(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    """
    Indice di concentrazione fornitori:
    - share top 5 / top 10 / top 20
    - HHI (Herfindahl-Hirschman Index) semplificato
    """
    df = get_saving_df(anno, str_ric, cols="ragione_sociale,imp_impegnato_eur")
    if df.empty: return {}
    total = float(df["imp_impegnato_eur"].fillna(0).sum())
    if total == 0: return {}
    grp = (df.groupby("ragione_sociale")["imp_impegnato_eur"].sum()
            .sort_values(ascending=False).reset_index())
    grp["share"] = (grp["imp_impegnato_eur"] / total * 100).round(2)
    n = len(grp)
    def cumshare(k): return round(float(grp.head(k)["share"].sum()), 2) if k <= n else 100.0
    # HHI semplificato (somma quadrati delle share in %)
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
    """Matrice saving per buyer × CDC."""
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
    return sb().table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data

@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}

@app.get("/wake")
def wake():
    return {"ok": True, "version": "9.0.0"}

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
        "version": "9.0.0",
        "database": "reachable" if db_ok else "unreachable",
        "kpi_definitions": {
            "listino":   "imp_listino_eur  = Imp. Iniziale €",
            "impegnato": "imp_impegnato_eur = Imp. Negoziato €",
            "saving":    "saving_eur = Saving.1",
        },
    }
