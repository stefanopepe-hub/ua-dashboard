import os
import io
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="UA Dashboard API", version="2.0.0")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────
# COLUMN MAPPING — flessibile per nome colonna
# ─────────────────────────────────────────────

# Ogni entry: nome_interno -> lista di possibili nomi nel file Excel (case-insensitive, strip)
SAVING_COL_MAP = {
    "cod_utente":          ["cod.utente", "codice utente", "cod utente"],
    "utente":              ["utente", "buyer", "user"],
    "num_doc":             ["num.doc.", "numero documento", "num doc", "n.doc"],
    "data_doc":            ["data doc.", "data documento", "data", "date"],
    "alfa_documento":      ["alfa documento", "tipo documento", "alfa doc", "tipo doc"],
    "str_ric":             ["str./ric.", "str/ric", "struttura/ricerca", "area"],
    "stato_dms":           ["stato dms", "stato", "status dms"],
    "codice_fornitore":    ["codice fornitore", "cod.fornitore", "cod fornitore"],
    "ragione_sociale":     ["ragione sociale fornitore", "ragione sociale", "fornitore", "supplier"],
    "accred_albo":         ["accred.albo", "accreditato albo", "albo", "accred albo"],
    "protoc_ordine":       ["protoc.ordine", "protocollo ordine", "prot.ordine"],
    "protoc_commessa":     ["protoc.commessa", "protocollo commessa", "prot.commessa"],
    "grp_merceol":         ["grp.merceol.", "gruppo merceologico", "grp merceol", "gruppo merc"],
    "desc_gruppo_merceol": ["descrizione gruppo merceologic", "desc gruppo merceol", "categoria merceologica", "categoria"],
    "centro_di_costo":     ["centro di costo", "cdc", "cost center"],
    "desc_cdc":            ["descrizione centro di costo", "desc cdc", "desc centro di costo"],
    "valuta":              ["valuta", "currency", "devise"],
    "imp_iniziale":        ["imp.iniziale", "importo iniziale", "imp iniziale", "valore iniziale"],
    "imp_negoziato":       ["imp.negoziato", "importo negoziato", "imp negoziato"],
    "saving_val":          ["saving", "risparmio"],
    "perc_saving":         ["% saving", "perc saving", "% risparmio", "%saving"],
    "negoziazione":        ["negoziazione", "negoziato", "negotiated"],
    "imp_iniziale_eur":    ["imp. iniziale €", "imp iniziale eur", "importo iniziale eur", "imp. iniziale e"],
    "imp_negoziato_eur":   ["imp. negoziato €", "imp negoziato eur", "importo negoziato eur", "imp. negoziato e"],
    "saving_eur":          ["saving.1", "saving eur", "risparmio eur"],
    "perc_saving_eur":     ["%saving", "% saving eur", "perc saving eur"],
    "cdc":                 ["cdc ", "cdc", "centro"],
    "cambio":              ["cambio", "exchange rate", "tasso cambio"],
}

def build_col_lookup(df_columns):
    """Mappa nome_interno -> nome_colonna_effettivo nel DataFrame (case-insensitive)"""
    normalized = {c.strip().lower(): c for c in df_columns}
    lookup = {}
    for internal, candidates in SAVING_COL_MAP.items():
        for cand in candidates:
            if cand.lower() in normalized:
                lookup[internal] = normalized[cand.lower()]
                break
    return lookup

def detect_saving_sheet(xl: pd.ExcelFile) -> pd.DataFrame:
    """
    Trova automaticamente il foglio giusto:
    1. Cerca il foglio con più colonne del mapping saving
    2. Fallback: foglio con più righe
    """
    best_sheet = None
    best_score = -1
    best_rows = -1

    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(xl, sheet_name=sheet, nrows=5)
            if df.empty or len(df.columns) < 3:
                continue
            lookup = build_col_lookup(df.columns)
            # Score = colonne chiave trovate
            key_cols = ["data_doc", "imp_iniziale_eur", "saving_eur", "ragione_sociale"]
            score = sum(1 for k in key_cols if k in lookup)
            rows = len(pd.read_excel(xl, sheet_name=sheet))
            if score > best_score or (score == best_score and rows > best_rows):
                best_score = score
                best_rows = rows
                best_sheet = sheet
        except Exception:
            continue

    if best_sheet is None or best_score < 2:
        raise HTTPException(400, "Nessun foglio compatibile trovato nel file Excel. Verifica che il file contenga le colonne attese (Data, Importo, Saving, Fornitore).")

    return pd.read_excel(xl, sheet_name=best_sheet)


# ─────────────────────────────────────────────
# TYPE HELPERS
# ─────────────────────────────────────────────

def si(v, d=None):
    try: return int(float(str(v))) if pd.notna(v) else d
    except: return d

def sf(v, d=0):
    try: return float(v) if pd.notna(v) else d
    except: return d

def ss(v):
    try:
        s = str(v).strip() if pd.notna(v) else None
        return s if s and s.lower() not in ('nan', 'none', '') else None
    except: return None

def sb_bool(v, true_vals=("si", "sì", "yes", "true", "1")):
    return str(v).strip().lower() in true_vals if pd.notna(v) else False

def safe_date(v):
    try: return pd.to_datetime(v).date().isoformat()
    except: return None


# ─────────────────────────────────────────────
# UPLOAD — SAVING
# ─────────────────────────────────────────────

@app.post("/upload/saving")
async def upload_saving(file: UploadFile = File(...), cdc_override: Optional[str] = None):
    """
    Carica file Excel saving da Alyante.
    Rileva automaticamente il foglio e le colonne — non dipende dal nome del file o del foglio.
    Accetta più upload (uno per CDC) e li aggrega.
    """
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        df = detect_saving_sheet(xl)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Errore lettura file: {str(e)}")

    df.columns = [c.strip() for c in df.columns]
    col = build_col_lookup(df.columns)

    # Colonna data obbligatoria
    date_col = col.get("data_doc")
    if not date_col:
        raise HTTPException(400, "Colonna data non trovata. Il file deve contenere una colonna 'Data doc.' o 'Data documento'.")

    df = df.dropna(subset=[date_col])

    sb = get_supabase()
    log = sb.table("upload_log").insert({
        "filename": file.filename,
        "tipo": "saving",
        "cdc_filter": cdc_override,
    }).execute()
    upload_id = log.data[0]["id"]

    def gcol(key, row):
        """Ottieni valore dalla riga usando il mapping"""
        col_name = col.get(key)
        return row.get(col_name) if col_name else None

    records = []
    for _, row in df.iterrows():
        data_doc = safe_date(row.get(date_col))
        if not data_doc:
            continue

        accred = sb_bool(gcol("accred_albo", row))
        neg = sb_bool(gcol("negoziazione", row))

        # Se imp_iniziale_eur non c'è, usa imp_iniziale (stesso valore se valuta EUR)
        imp_eur = sf(gcol("imp_iniziale_eur", row)) or sf(gcol("imp_iniziale", row))
        neg_eur = sf(gcol("imp_negoziato_eur", row)) or sf(gcol("imp_negoziato", row))
        sav_eur = sf(gcol("saving_eur", row)) or sf(gcol("saving_val", row))

        records.append({
            "upload_id": upload_id,
            "cod_utente": si(gcol("cod_utente", row)),
            "utente": ss(gcol("utente", row)),
            "num_doc": si(gcol("num_doc", row)),
            "data_doc": data_doc,
            "alfa_documento": ss(gcol("alfa_documento", row)),
            "str_ric": ss(gcol("str_ric", row)),
            "stato_dms": ss(gcol("stato_dms", row)),
            "codice_fornitore": si(gcol("codice_fornitore", row)),
            "ragione_sociale": ss(gcol("ragione_sociale", row)),
            "accred_albo": accred,
            "protoc_ordine": sf(gcol("protoc_ordine", row), None),
            "protoc_commessa": ss(gcol("protoc_commessa", row)),
            "grp_merceol": ss(gcol("grp_merceol", row)),
            "desc_gruppo_merceol": ss(gcol("desc_gruppo_merceol", row)),
            "centro_di_costo": ss(gcol("centro_di_costo", row)),
            "desc_cdc": ss(gcol("desc_cdc", row)),
            "valuta": ss(gcol("valuta", row)) or "EURO",
            "imp_iniziale": sf(gcol("imp_iniziale", row)),
            "imp_negoziato": sf(gcol("imp_negoziato", row)),
            "saving_val": sf(gcol("saving_val", row)),
            "perc_saving": sf(gcol("perc_saving", row)),
            "negoziazione": neg,
            "imp_iniziale_eur": imp_eur,
            "imp_negoziato_eur": neg_eur,
            "saving_eur": sav_eur,
            "perc_saving_eur": sf(gcol("perc_saving_eur", row)),
            "cdc": cdc_override or ss(gcol("cdc", row)),
            "cambio": sf(gcol("cambio", row), 1),
        })

    for i in range(0, len(records), 500):
        sb.table("saving").insert(records[i:i+500]).execute()

    sb.table("upload_log").update({"rows_inserted": len(records)}).eq("id", upload_id).execute()
    return {"status": "ok", "rows": len(records), "upload_id": upload_id, "sheet_detected": xl.sheet_names[0]}


# ─────────────────────────────────────────────
# UPLOAD — TEMPI
# ─────────────────────────────────────────────

TEMPI_COL_MAP = {
    "protocol":         ["protocol", "protocollo", "prot"],
    "year_month":       ["year_month", "anno_mese", "mese", "month"],
    "days_purchasing":  ["days_purchasing", "giorni_acquisti", "gg_acquisti", "purchasing"],
    "days_auto":        ["days_auto", "giorni_auto", "gg_auto", "auto"],
    "days_other":       ["days_other", "giorni_altro", "gg_altro", "other"],
    "total_days":       ["total_days", "giorni_totali", "totale_giorni", "total"],
    "perc_purchasing":  ["perc_purchasing", "%_purchasing", "perc purchasing"],
    "perc_auto":        ["perc_auto", "%_auto", "perc auto"],
    "perc_other":       ["perc_other", "%_other", "perc other"],
    "bottleneck":       ["bottleneck", "collo_di_bottiglia", "fase_critica"],
}

@app.post("/upload/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        # Prendi il foglio con più righe
        best, best_rows = xl.sheet_names[0], 0
        for s in xl.sheet_names:
            try:
                n = len(pd.read_excel(xl, sheet_name=s))
                if n > best_rows:
                    best_rows, best = n, s
            except: pass
        df = pd.read_excel(xl, sheet_name=best)
    except Exception as e:
        raise HTTPException(400, f"Errore lettura file: {str(e)}")

    df.columns = [c.strip() for c in df.columns]
    normalized = {c.lower(): c for c in df.columns}
    col = {}
    for internal, candidates in TEMPI_COL_MAP.items():
        for cand in candidates:
            if cand.lower() in normalized:
                col[internal] = normalized[cand.lower()]
                break

    sb = get_supabase()
    log = sb.table("upload_log").insert({"filename": file.filename, "tipo": "tempi"}).execute()
    upload_id = log.data[0]["id"]

    records = []
    for _, row in df.iterrows():
        records.append({
            "upload_id": upload_id,
            "protocol":        ss(row.get(col.get("protocol", ""))),
            "year_month":      ss(row.get(col.get("year_month", ""))),
            "days_purchasing": sf(row.get(col.get("days_purchasing", ""))),
            "days_auto":       sf(row.get(col.get("days_auto", ""))),
            "days_other":      sf(row.get(col.get("days_other", ""))),
            "total_days":      sf(row.get(col.get("total_days", ""))),
            "perc_purchasing": sf(row.get(col.get("perc_purchasing", ""))),
            "perc_auto":       sf(row.get(col.get("perc_auto", ""))),
            "perc_other":      sf(row.get(col.get("perc_other", ""))),
            "bottleneck":      ss(row.get(col.get("bottleneck", ""))),
        })

    for i in range(0, len(records), 500):
        sb.table("tempo_attraversamento").insert(records[i:i+500]).execute()

    sb.table("upload_log").update({"rows_inserted": len(records)}).eq("id", upload_id).execute()
    return {"status": "ok", "rows": len(records), "upload_id": upload_id}


# ─────────────────────────────────────────────
# UPLOAD — NON CONFORMITÀ
# ─────────────────────────────────────────────

NC_COL_MAP = {
    "protocollo_commessa":    ["protocollo commessa", "prot commessa", "protocollo"],
    "ragione_sociale":        ["ragione sociale anagrafica", "ragione sociale", "fornitore"],
    "tipo_origine":           ["tipo origine", "tipo_origine", "tipo"],
    "data_origine":           ["data origine", "data_origine", "data"],
    "utente_origine":         ["utente origine", "utente_origine", "utente"],
    "codice_prima_fattura":   ["codice prima fattura", "cod fattura", "fattura"],
    "data_prima_fattura":     ["data prima fattura", "data fattura"],
    "importo_prima_fattura":  ["importo prima fattura", "importo fattura", "importo"],
    "delta_giorni":           ["delta giorni (fattura - origine)", "delta giorni", "delta_giorni", "delta"],
    "non_conformita":         ["non conformità", "non conformita", "nc", "non_conformita"],
}

@app.post("/upload/nc")
async def upload_nc(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        best, best_rows = xl.sheet_names[0], 0
        for s in xl.sheet_names:
            try:
                n = len(pd.read_excel(xl, sheet_name=s))
                if n > best_rows:
                    best_rows, best = n, s
            except: pass
        df = pd.read_excel(xl, sheet_name=best)
    except Exception as e:
        raise HTTPException(400, f"Errore lettura file: {str(e)}")

    df.columns = [c.strip() for c in df.columns]
    normalized = {c.lower(): c for c in df.columns}
    col = {}
    for internal, candidates in NC_COL_MAP.items():
        for cand in candidates:
            if cand.lower() in normalized:
                col[internal] = normalized[cand.lower()]
                break

    sb = get_supabase()
    log = sb.table("upload_log").insert({"filename": file.filename, "tipo": "nc"}).execute()
    upload_id = log.data[0]["id"]

    records = []
    for _, row in df.iterrows():
        nc_val = sb_bool(row.get(col.get("non_conformita", "")))
        records.append({
            "upload_id": upload_id,
            "protocollo_commessa":   ss(row.get(col.get("protocollo_commessa", ""))),
            "ragione_sociale":       ss(row.get(col.get("ragione_sociale", ""))),
            "tipo_origine":          ss(row.get(col.get("tipo_origine", ""))),
            "data_origine":          safe_date(row.get(col.get("data_origine", ""))),
            "utente_origine":        ss(row.get(col.get("utente_origine", ""))),
            "codice_prima_fattura":  ss(row.get(col.get("codice_prima_fattura", ""))),
            "data_prima_fattura":    safe_date(row.get(col.get("data_prima_fattura", ""))),
            "importo_prima_fattura": sf(row.get(col.get("importo_prima_fattura", "")), None),
            "delta_giorni":          sf(row.get(col.get("delta_giorni", "")), None),
            "non_conformita":        nc_val,
        })

    for i in range(0, len(records), 500):
        sb.table("non_conformita").insert(records[i:i+500]).execute()

    sb.table("upload_log").update({"rows_inserted": len(records)}).eq("id", upload_id).execute()
    return {"status": "ok", "rows": len(records), "upload_id": upload_id}


# ─────────────────────────────────────────────
# ENDPOINT: PREVIEW COLONNE
# ─────────────────────────────────────────────

@app.post("/upload/preview")
async def preview_file(file: UploadFile = File(...)):
    """
    Anteprima del file: restituisce fogli, colonne rilevate e mapping automatico.
    Utile per verificare prima del caricamento definitivo.
    """
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, f"File non leggibile: {str(e)}")

    result = {"sheets": [], "detected_type": None, "col_mapping": {}}
    best_score = 0

    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(xl, sheet_name=sheet, nrows=3)
            col_lookup = build_col_lookup(df.columns)
            key_cols = ["data_doc", "imp_iniziale_eur", "saving_eur", "ragione_sociale"]
            score = sum(1 for k in key_cols if k in col_lookup)
            n_rows = len(pd.read_excel(xl, sheet_name=sheet))
            result["sheets"].append({
                "name": sheet,
                "rows": n_rows,
                "cols": df.columns.tolist(),
                "saving_score": score,
            })
            if score > best_score:
                best_score = score
                result["col_mapping"] = col_lookup
                result["detected_sheet"] = sheet
        except Exception:
            continue

    if best_score >= 2:
        result["detected_type"] = "saving"
    elif any("bottleneck" in s["cols"] for s in result["sheets"] if s.get("cols")):
        result["detected_type"] = "tempi"
    elif any("non conform" in " ".join(str(c).lower() for c in s.get("cols", [])) for s in result["sheets"]):
        result["detected_type"] = "nc"

    return result


# ─────────────────────────────────────────────
# KPI — SAVING
# ─────────────────────────────────────────────

@app.get("/kpi/saving/riepilogo")
def kpi_saving_riepilogo(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
):
    sb = get_supabase()
    q = sb.table("saving").select("imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento,data_doc,str_ric,cdc")
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if str_ric:
        q = q.eq("str_ric", str_ric)
    if cdc:
        q = q.eq("cdc", cdc)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return {"impegnato":0,"saving":0,"perc_saving":0,"n_ordini":0,"n_negoziati":0,"perc_negoziati":0,"n_fornitori_albo":0,"perc_albo":0}

    doc_neg = ["OS","OSP","PS","OPR","ORN"]
    df_neg = df[df["alfa_documento"].isin(doc_neg)]
    impegnato = df["imp_iniziale_eur"].sum()
    saving = df["saving_eur"].sum()
    n_ordini = len(df_neg)
    n_negoziati = int(df_neg["negoziazione"].sum())
    n_albo = int(df["accred_albo"].sum())
    return {
        "impegnato": round(impegnato, 2),
        "saving": round(saving, 2),
        "perc_saving": round(saving/impegnato*100, 2) if impegnato else 0,
        "n_ordini": n_ordini,
        "n_negoziati": n_negoziati,
        "perc_negoziati": round(n_negoziati/n_ordini*100, 2) if n_ordini else 0,
        "n_fornitori_albo": n_albo,
        "perc_albo": round(n_albo/len(df)*100, 2) if len(df) else 0,
    }


@app.get("/kpi/saving/mensile")
def kpi_saving_mensile(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None)):
    sb = get_supabase()
    q = sb.table("saving").select("data_doc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,str_ric,cdc")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if str_ric: q = q.eq("str_ric", str_ric)
    if cdc: q = q.eq("cdc", cdc)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    df["mese"] = pd.to_datetime(df["data_doc"]).dt.strftime("%Y-%m")
    doc_neg = ["OS","OSP","PS","OPR","ORN"]
    df_neg = df[df["alfa_documento"].isin(doc_neg)]
    result = []
    for mese, grp in df.groupby("mese"):
        grp_neg = df_neg[df_neg["mese"]==mese]
        imp = grp["imp_iniziale_eur"].sum()
        sav = grp["saving_eur"].sum()
        n_ord = len(grp_neg)
        n_neg = int(grp_neg["negoziazione"].sum())
        result.append({"mese":mese,"impegnato":round(imp,2),"saving":round(sav,2),
            "perc_saving":round(sav/imp*100,2) if imp else 0,
            "n_ordini":n_ord,"n_negoziati":n_neg,
            "perc_negoziati":round(n_neg/n_ord*100,2) if n_ord else 0})
    return sorted(result, key=lambda x: x["mese"])


@app.get("/kpi/saving/per-cdc")
def kpi_saving_per_cdc(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None)):
    sb = get_supabase()
    q = sb.table("saving").select("cdc,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,str_ric")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if str_ric: q = q.eq("str_ric", str_ric)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for cdc, grp in df.groupby("cdc"):
        imp = grp["imp_iniziale_eur"].sum()
        sav = grp["saving_eur"].sum()
        result.append({"cdc":cdc,"impegnato":round(imp,2),"saving":round(sav,2),
            "perc_saving":round(sav/imp*100,2) if imp else 0,
            "n_ordini":len(grp),"n_negoziati":int(grp["negoziazione"].sum())})
    return sorted(result, key=lambda x: x["impegnato"], reverse=True)


@app.get("/kpi/saving/per-buyer")
def kpi_saving_per_buyer(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None)):
    sb = get_supabase()
    q = sb.table("saving").select("utente,imp_iniziale_eur,saving_eur,negoziazione,cdc,str_ric")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if str_ric: q = q.eq("str_ric", str_ric)
    if cdc: q = q.eq("cdc", cdc)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for utente, grp in df.groupby("utente"):
        imp = grp["imp_iniziale_eur"].sum()
        sav = grp["saving_eur"].sum()
        result.append({"utente":utente,"impegnato":round(imp,2),"saving":round(sav,2),
            "perc_saving":round(sav/imp*100,2) if imp else 0,
            "n_ordini":len(grp),"n_negoziati":int(grp["negoziazione"].sum())})
    return sorted(result, key=lambda x: x["saving"], reverse=True)


@app.get("/kpi/saving/top-fornitori")
def kpi_top_fornitori(anno: Optional[int]=Query(None), per: str=Query("saving"), limit: int=Query(10), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None)):
    sb = get_supabase()
    q = sb.table("saving").select("ragione_sociale,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,cdc")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if str_ric: q = q.eq("str_ric", str_ric)
    if cdc: q = q.eq("cdc", cdc)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("ragione_sociale").agg(
        impegnato=("imp_iniziale_eur","sum"), saving=("saving_eur","sum"),
        n_ordini=("imp_iniziale_eur","count"), albo=("accred_albo","first")).reset_index()
    grp["perc_saving"] = (grp["saving"]/grp["impegnato"]*100).round(2)
    sort_col = "saving" if per=="saving" else "impegnato"
    return grp.nlargest(limit, sort_col).to_dict(orient="records")


@app.get("/kpi/saving/pareto-fornitori")
def kpi_pareto_fornitori(anno: Optional[int]=Query(None)):
    sb = get_supabase()
    q = sb.table("saving").select("ragione_sociale,imp_iniziale_eur")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("ragione_sociale")["imp_iniziale_eur"].sum().sort_values(ascending=False).reset_index()
    total = grp["imp_iniziale_eur"].sum()
    grp["cum_perc"] = (grp["imp_iniziale_eur"].cumsum()/total*100).round(2)
    grp["rank"] = range(1, len(grp)+1)
    grp["imp_iniziale_eur"] = grp["imp_iniziale_eur"].round(2)
    return grp.to_dict(orient="records")


@app.get("/kpi/saving/per-categoria")
def kpi_saving_per_categoria(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None), limit: int=Query(15)):
    sb = get_supabase()
    q = sb.table("saving").select("desc_gruppo_merceol,imp_iniziale_eur,saving_eur,negoziazione,str_ric,cdc")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if str_ric: q = q.eq("str_ric", str_ric)
    if cdc: q = q.eq("cdc", cdc)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    df = df.dropna(subset=["desc_gruppo_merceol"])
    grp = df.groupby("desc_gruppo_merceol").agg(
        impegnato=("imp_iniziale_eur","sum"), saving=("saving_eur","sum"),
        n_ordini=("imp_iniziale_eur","count"), n_negoziati=("negoziazione","sum")).reset_index()
    grp["perc_saving"] = (grp["saving"]/grp["impegnato"]*100).fillna(0).round(2)
    grp["perc_negoziati"] = (grp["n_negoziati"]/grp["n_ordini"]*100).fillna(0).round(2)
    return grp.nlargest(limit,"impegnato").to_dict(orient="records")


@app.get("/kpi/saving/valute")
def kpi_valute(anno: Optional[int]=Query(None)):
    sb = get_supabase()
    q = sb.table("saving").select("valuta,imp_iniziale_eur,imp_iniziale")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("valuta").agg(impegnato_eur=("imp_iniziale_eur","sum"),
        impegnato_orig=("imp_iniziale","sum"), n_ordini=("imp_iniziale_eur","count")).reset_index()
    total = grp["impegnato_eur"].sum()
    grp["perc"] = (grp["impegnato_eur"]/total*100).round(2)
    return grp.sort_values("impegnato_eur", ascending=False).to_dict(orient="records")


# ─────────────────────────────────────────────
# KPI — TEMPI
# ─────────────────────────────────────────────

@app.get("/kpi/tempi/riepilogo")
def kpi_tempi_riepilogo():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return {}
    return {"avg_total_days":round(df["total_days"].mean(),1),"avg_purchasing":round(df["days_purchasing"].mean(),1),
        "avg_auto":round(df["days_auto"].mean(),1),"avg_other":round(df["days_other"].mean(),1),"n_ordini":len(df),
        "perc_bottleneck_purchasing":round(len(df[df["bottleneck"]=="PURCHASING"])/len(df)*100,1),
        "perc_bottleneck_auto":round(len(df[df["bottleneck"]=="AUTO"])/len(df)*100,1),
        "perc_bottleneck_other":round(len(df[df["bottleneck"]=="OTHER"])/len(df)*100,1)}


@app.get("/kpi/tempi/mensile")
def kpi_tempi_mensile():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for ym, grp in df.groupby("year_month"):
        result.append({"mese":ym,"avg_total":round(grp["total_days"].mean(),1),
            "avg_purchasing":round(grp["days_purchasing"].mean(),1),
            "avg_auto":round(grp["days_auto"].mean(),1),"avg_other":round(grp["days_other"].mean(),1),
            "n_ordini":len(grp),
            "n_bottleneck_purchasing":int((grp["bottleneck"]=="PURCHASING").sum()),
            "n_bottleneck_auto":int((grp["bottleneck"]=="AUTO").sum()),
            "n_bottleneck_other":int((grp["bottleneck"]=="OTHER").sum())})
    return sorted(result, key=lambda x: x["mese"])


@app.get("/kpi/tempi/distribuzione")
def kpi_tempi_distribuzione():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("total_days").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    bins = [0,7,15,30,60,9999]
    labels = ["≤7 gg","8-15 gg","16-30 gg","31-60 gg",">60 gg"]
    df["fascia"] = pd.cut(df["total_days"], bins=bins, labels=labels, right=True)
    counts = df["fascia"].value_counts().reindex(labels).fillna(0)
    return [{"fascia":k,"n_ordini":int(v)} for k,v in counts.items()]


# ─────────────────────────────────────────────
# KPI — NON CONFORMITÀ
# ─────────────────────────────────────────────

@app.get("/kpi/nc/riepilogo")
def kpi_nc_riepilogo():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("non_conformita,delta_giorni,tipo_origine").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n = len(df); n_nc = int(df["non_conformita"].sum())
    return {"n_totale":n,"n_nc":n_nc,"perc_nc":round(n_nc/n*100,2) if n else 0,
        "avg_delta_giorni":round(df["delta_giorni"].mean(),1),
        "avg_delta_nc":round(df[df["non_conformita"]==True]["delta_giorni"].mean(),1)}


@app.get("/kpi/nc/mensile")
def kpi_nc_mensile():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("data_origine,non_conformita,delta_giorni,tipo_origine").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    df["mese"] = pd.to_datetime(df["data_origine"], errors="coerce").dt.strftime("%Y-%m")
    df = df.dropna(subset=["mese"])
    result = []
    for mese, grp in df.groupby("mese"):
        n = len(grp); n_nc = int(grp["non_conformita"].sum())
        result.append({"mese":mese,"n_totale":n,"n_nc":n_nc,
            "perc_nc":round(n_nc/n*100,2) if n else 0,
            "avg_delta_giorni":round(grp["delta_giorni"].mean(),1)})
    return sorted(result, key=lambda x: x["mese"])


@app.get("/kpi/nc/top-fornitori")
def kpi_nc_top_fornitori(limit: int=Query(10)):
    sb = get_supabase()
    rows = sb.table("non_conformita").select("ragione_sociale,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("ragione_sociale").agg(n_totale=("non_conformita","count"),
        n_nc=("non_conformita","sum"), avg_delta=("delta_giorni","mean")).reset_index()
    grp["perc_nc"] = (grp["n_nc"]/grp["n_totale"]*100).round(2)
    grp["avg_delta"] = grp["avg_delta"].round(1)
    return grp[grp["n_nc"]>0].nlargest(limit,"n_nc").to_dict(orient="records")


@app.get("/kpi/nc/per-tipo")
def kpi_nc_per_tipo():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("tipo_origine,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for tipo, grp in df.groupby("tipo_origine"):
        n = len(grp); n_nc = int(grp["non_conformita"].sum())
        result.append({"tipo":tipo,"n_totale":n,"n_nc":n_nc,
            "perc_nc":round(n_nc/n*100,2) if n else 0,
            "avg_delta_giorni":round(grp["delta_giorni"].mean(),1)})
    return result


# ─────────────────────────────────────────────
# EXPORT — EXCEL
# ─────────────────────────────────────────────

@app.get("/export/saving/excel")
def export_saving_excel(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None)):
    """Esporta dati saving in Excel"""
    sb = get_supabase()
    q = sb.table("saving").select("*")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if str_ric: q = q.eq("str_ric", str_ric)
    if cdc: q = q.eq("cdc", cdc)
    rows = q.execute().data
    df = pd.DataFrame(rows)

    col_rename = {
        "utente":"Buyer","data_doc":"Data","alfa_documento":"Tipo Doc",
        "ragione_sociale":"Fornitore","accred_albo":"Albo","desc_gruppo_merceol":"Categoria",
        "str_ric":"Area","cdc":"CDC","valuta":"Valuta",
        "imp_iniziale_eur":"Importo Iniziale €","imp_negoziato_eur":"Importo Negoziato €",
        "saving_eur":"Saving €","perc_saving_eur":"% Saving","negoziazione":"Negoziato",
    }
    export_cols = [c for c in col_rename if c in df.columns]
    df_out = df[export_cols].rename(columns=col_rename)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Saving")
        # KPI sheet
        kpi = kpi_saving_riepilogo(anno=anno, str_ric=str_ric, cdc=cdc)
        pd.DataFrame([kpi]).to_excel(writer, index=False, sheet_name="KPI Riepilogo")
    buf.seek(0)

    filename = f"saving_{anno or 'all'}_{cdc or 'tutti'}.xlsx"
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/export/tempi/excel")
def export_tempi_excel():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Tempi Attraversamento")
        mensile = pd.DataFrame(kpi_tempi_mensile())
        mensile.to_excel(writer, index=False, sheet_name="KPI Mensile")
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=tempi_attraversamento.xlsx"})


@app.get("/export/nc/excel")
def export_nc_excel():
    sb = get_supabase()
    rows = sb.table("non_conformita").select("*").execute().data
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Non Conformità")
        mensile = pd.DataFrame(kpi_nc_mensile())
        mensile.to_excel(writer, index=False, sheet_name="KPI Mensile")
    buf.seek(0)
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=non_conformita.xlsx"})


# ─────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────

@app.get("/upload/log")
def get_upload_log():
    sb = get_supabase()
    rows = sb.table("upload_log").select("*").order("upload_date", desc=True).limit(50).execute().data
    return rows


@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    sb = get_supabase()
    sb.table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}


@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0.0"}
