"""
UA Dashboard Backend v4.0
Sistema a prova di bomba per analisi acquisti Fondazione Telethon ETS
"""
import os
import io
import re
import json
import logging
from semantic import build_semantic_map, gcol as semantic_gcol
from typing import Optional, List
from datetime import datetime, date
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ua-dashboard")

app = FastAPI(title="UA Dashboard API", version="4.0.0")

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
# TIPO HELPERS — robusti, mai crashano
# ─────────────────────────────────────────────

def si(v, d=None):
    try: return int(float(str(v))) if pd.notna(v) else d
    except: return d

def sf(v, d=0.0):
    try: return float(v) if pd.notna(v) else d
    except: return d

def sfn(v):
    try: return float(v) if pd.notna(v) else None
    except: return None

def ss(v):
    try:
        s = str(v).strip() if pd.notna(v) else None
        return s if s and s.lower() not in ('nan','none','nat','') else None
    except: return None

def sb(v):
    return str(v).strip().lower() in ('si','sì','yes','true','1') if pd.notna(v) else False

def sd(v):
    try: return pd.to_datetime(v).date().isoformat()
    except: return None

def safe_pct(num, den):
    try: return round(num/den*100, 2) if den else 0.0
    except: return 0.0


# ─────────────────────────────────────────────
# BANCA D'ITALIA — TASSI DI CAMBIO
# ─────────────────────────────────────────────

# Cache in memoria per i tassi (evita chiamate ripetute)
_exchange_cache: dict = {}

def get_exchange_rate(currency: str, ref_date: date) -> float:
    """
    Restituisce il tasso di cambio EUR/valuta per la data specificata.
    Usa la colonna 'cambio' dal file se disponibile.
    Fallback: tasso fisso di riferimento aggiornato.
    """
    if currency == 'EURO' or currency == 'EUR':
        return 1.0
    
    # Tassi di riferimento BCE aggiornati (fallback)
    FALLBACK_RATES = {
        'USD': 1.08, 'GBP': 0.855, 'CHF': 0.945, 'JPY': 161.5,
        'AUD': 1.65, 'CAD': 1.46, 'SEK': 11.2, 'AED': 3.97,
        'DKK': 7.46, 'NOK': 11.7, 'CNY': 7.82,
    }
    
    cache_key = f"{currency}_{ref_date.strftime('%Y-%m')}"
    if cache_key in _exchange_cache:
        return _exchange_cache[cache_key]
    
    # Prova Banca d'Italia API
    try:
        import httpx
        # Banca d'Italia Statistical Data Warehouse
        base_date = ref_date.strftime('%Y-%m-01')
        url = (
            f"https://tassidicambio.bancaditalia.it/terzevalute-wf-web/rest/v1.0/dailyTimeSeries"
            f"?startDate={base_date}&endDate={ref_date.isoformat()}"
            f"&baseCurrencyIsoCode=EUR&currencyIsoCode={currency}&lang=it"
        )
        resp = httpx.get(url, timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            rates = data.get('rates', [])
            if rates:
                rate = float(rates[-1].get('avgRate', FALLBACK_RATES.get(currency, 1.0)))
                _exchange_cache[cache_key] = rate
                log.info(f"Banca d'Italia rate {currency} {base_date}: {rate}")
                return rate
    except Exception as e:
        log.warning(f"Banca d'Italia API error for {currency}: {e}")
    
    rate = FALLBACK_RATES.get(currency, 1.0)
    _exchange_cache[cache_key] = rate
    return rate


# ─────────────────────────────────────────────
# COLUMN MAPPING — mappa per nome, non posizione
# ─────────────────────────────────────────────

# Mappa: nome_interno -> [possibili nomi nel file, case-insensitive]
COL_MAP = {
    # Identificativi
    "cod_utente":           ["cod.utente","codice utente"],
    "utente":               ["utente","buyer","user"],
    "utente_presentazione": ["utente per presentazione","utente presentazione","nome presentazione"],
    "num_doc":              ["num.doc.","numero documento","num doc"],
    "data_doc":             ["data doc.","data documento","data","date"],
    # Documento
    "alfa_documento":       ["alfa documento","tipo documento","alfa doc","tipo doc"],
    "str_ric":              ["str./ric.","str/ric","struttura/ricerca","area"],
    "stato_dms":            ["stato dms","stato","status"],
    # Fornitore
    "codice_fornitore":     ["codice fornitore","cod.fornitore"],
    "ragione_sociale":      ["ragione sociale fornitore","ragione sociale","fornitore"],
    "accred_albo":          ["accred.albo","accreditato albo","albo","accred albo"],
    # Commessa
    "protoc_ordine":        ["protoc.ordine","protocollo ordine"],
    "protoc_commessa":      ["protoc.commessa","protocollo commessa","prot.commessa"],
    "protoc_origine":       ["protocollo origine","prot.origine"],
    # Merceologia
    "grp_merceol":          ["grp.merceol.","gruppo merceologico","grp merceol"],
    "desc_gruppo_merceol":  ["descrizione gruppo merceologic","desc gruppo merceol","categoria merceologica","categoria"],
    # Centro di costo
    "centro_di_costo":      ["centro di costo"],
    "desc_cdc":             ["descrizione centro di costo","desc cdc","desc centro di costo"],
    "macro_categoria":      ["macro categorie","macro categoria","macro cat"],
    # Valuta e importi
    "valuta":               ["valuta","currency"],
    "imp_iniziale":         ["imp.iniziale","importo iniziale"],
    "imp_negoziato":        ["imp.negoziato","importo negoziato"],
    "saving_val":           ["saving","risparmio"],
    "perc_saving":          ["% saving","perc saving"],
    "negoziazione":         ["negoziazione","negoziato"],
    # Importi EUR (presenti solo in alcuni file)
    "imp_iniziale_eur":     ["imp. iniziale €","imp iniziale eur","imp. iniziale e"],
    "imp_negoziato_eur":    ["imp. negoziato €","imp negoziato eur","imp. negoziato e"],
    "saving_eur":           ["saving.1","saving eur"],
    "perc_saving_eur":      ["%saving","% saving eur","perc saving eur"],
    # CDC e cambio
    "cdc":                  ["cdc"],
    "cambio":               ["cambio","exchange rate","tasso cambio"],
    # Extra
    "tail_spend":           ["tail spend","tail"],
}

def build_col_map(df: pd.DataFrame) -> dict:
    """
    Costruisce la mappa colonne usando il rilevamento semantico.
    Analizza il CONTENUTO delle celle, non il nome.
    Compatibile con qualsiasi versione del file Alyante.
    """
    return build_semantic_map(df, min_confidence=60)

def gcol(col_lookup: dict, key: str, row) -> any:
    """Legge valore da riga usando il mapping."""
    cn = col_lookup.get(key)
    return row.get(cn) if cn is not None else None

def detect_best_sheet(xl: pd.ExcelFile) -> tuple:
    """
    Trova il foglio con più colonne compatibili.
    Ritorna (DataFrame, sheet_name).
    Priorità esplicita per fogli con '2025 (2)' nel nome.
    """
    best_sheet, best_score, best_rows = None, -1, -1
    
    for sheet in xl.sheet_names:
        try:
            df_sample = pd.read_excel(xl, sheet_name=sheet, nrows=100)
            if df_sample.empty or len(df_sample.columns) < 5:
                continue
            col_lookup = build_col_map(df_sample)
            key_cols = ["data_doc","imp_iniziale","ragione_sociale","alfa_documento"]
            score = sum(1 for k in key_cols if k in col_lookup)
            # Bonus se ha colonne EUR
            if "imp_iniziale_eur" in col_lookup:
                score += 2
            if "macro_categoria" in col_lookup:
                score += 1
            rows = len(pd.read_excel(xl, sheet_name=sheet))
            if score > best_score or (score == best_score and rows > best_rows):
                best_score, best_rows, best_sheet = score, rows, sheet
        except Exception as e:
            log.warning(f"Sheet '{sheet}' error: {e}")
            continue
    
    if best_sheet is None or best_score < 2:
        raise HTTPException(400, 
            "Nessun foglio compatibile. Il file deve contenere colonne: Data doc., Imp.iniziale, Ragione sociale fornitore, Alfa documento.")
    
    log.info(f"Best sheet: '{best_sheet}' (score={best_score}, rows={best_rows})")
    return pd.read_excel(xl, sheet_name=best_sheet), best_sheet


# ─────────────────────────────────────────────
# CDC DERIVATION
# ─────────────────────────────────────────────

def derive_cdc(centro_di_costo: str, desc_cdc: str) -> str:
    """
    Deriva CDC dal codice centro di costo e dalla descrizione.
    Usato quando il file non ha colonna CDC esplicita.
    """
    cdc   = str(centro_di_costo or '').strip().upper()
    desc  = str(desc_cdc or '').upper()
    
    if 'TIGEM' in desc or 'TIGEM' in cdc:
        return 'TIGEM'
    if 'TIGET' in desc or 'TIGET' in cdc:
        return 'TIGET'
    if cdc.startswith(('RCRIIR','RCREER')):
        return 'GD'
    if cdc.startswith('STR'):
        return 'STRUTTURA'
    return 'FT'

def extract_commessa_info(protoc_commessa: str) -> tuple:
    """
    Estrae prefisso e anno dalla commessa.
    Es: 'GMR24T2072/00053' -> ('GMR', '24')
    Es: 'TMA23X1234/00001' -> ('TMA', '23')
    """
    if not protoc_commessa or protoc_commessa.lower() in ('nan','none',''):
        return None, None
    s = str(protoc_commessa).strip()
    pref = s[:3] if len(s) >= 3 else None
    # Anno: caratteri 3-4 se numerici
    anno = s[3:5] if len(s) >= 5 and s[3:5].isdigit() else None
    return pref, anno


# ─────────────────────────────────────────────
# KPI CALCULATION — funzione centralizzata
# ─────────────────────────────────────────────

# Tipi documento considerati "negoziabili"
DOC_NEGOZIABILI = {'OS','OSP','PS','OPR','ORN','ORD'}

def calc_kpi(df: pd.DataFrame) -> dict:
    """
    Calcola tutti i KPI in modo uniforme.
    Usato ovunque — unica fonte di verità per i calcoli.
    
    Definizioni:
    - impegnato:      SUM(imp_iniziale_eur)  — su tutte le righe
    - saving:         SUM(saving_eur)        — su tutte le righe
    - % saving:       saving / impegnato * 100
    - n_righe:        conteggio totale righe
    - n_doc_neg:      righe con alfa_documento in DOC_NEGOZIABILI
    - n_negoziati:    righe con negoziazione=True (su tutte le righe)
    - % negoziati:    n_negoziati / n_doc_neg * 100
    - n_albo:         righe con accred_albo=True
    - % albo:         n_albo / n_righe * 100
    """
    if df.empty:
        return {k:0 for k in [
            "impegnato","saving","perc_saving","n_righe",
            "n_doc_neg","n_negoziati","perc_negoziati","n_albo","perc_albo"
        ]}
    
    impegnato = float(df["imp_iniziale_eur"].fillna(0).sum())
    saving    = float(df["saving_eur"].fillna(0).sum())
    n_righe   = len(df)
    n_doc_neg = int(df["alfa_documento"].isin(DOC_NEGOZIABILI).sum())
    n_neg     = int(df["negoziazione"].fillna(False).sum())
    n_albo    = int(df["accred_albo"].fillna(False).sum())
    
    return {
        "impegnato":      round(impegnato, 2),
        "saving":         round(saving, 2),
        "perc_saving":    safe_pct(saving, impegnato),
        "n_righe":        n_righe,
        "n_doc_neg":      n_doc_neg,
        "n_negoziati":    n_neg,
        "perc_negoziati": safe_pct(n_neg, n_doc_neg),
        "n_albo":         n_albo,
        "perc_albo":      safe_pct(n_albo, n_righe),
    }


# ─────────────────────────────────────────────
# QUERY HELPER — centralizzato
# ─────────────────────────────────────────────

def query_saving(
    anno: int = None,
    str_ric: str = None,
    cdc: str = None,
    alfa_documento: str = None,
    macro_categoria: str = None,
    prefisso_commessa: str = None,
    utente: str = None,
    data_da: str = None,
    data_a: str = None,
    cols: str = "*"
) -> pd.DataFrame:
    """Query centralizzata con tutti i filtri disponibili."""
    sb = get_supabase()
    q = sb.table("saving").select(cols)
    
    if anno:
        q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
    if data_da:
        q = q.gte("data_doc", data_da)
    if data_a:
        q = q.lte("data_doc", data_a)
    if str_ric:
        q = q.eq("str_ric", str_ric)
    if cdc:
        q = q.eq("cdc", cdc)
    if alfa_documento:
        q = q.eq("alfa_documento", alfa_documento)
    if macro_categoria:
        q = q.eq("macro_categoria", macro_categoria.strip())
    if prefisso_commessa:
        q = q.eq("prefisso_commessa", prefisso_commessa)
    if utente:
        q = q.ilike("utente_presentazione", f"%{utente}%")
    
    rows = q.execute().data
    df = pd.DataFrame(rows)
    
    if not df.empty:
        df["data_doc"] = pd.to_datetime(df["data_doc"], errors="coerce")
        # Assicura che le colonne booleane siano bool
        for col in ["negoziazione","accred_albo"]:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)
        # Assicura che le colonne numeriche siano float
        for col in ["imp_iniziale_eur","saving_eur","imp_iniziale","saving_val"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    return df


# ─────────────────────────────────────────────
# UPLOAD — SAVING (robusto)
# ─────────────────────────────────────────────

@app.post("/upload/saving")
async def upload_saving(
    file: UploadFile = File(...),
    cdc_override: Optional[str] = None,
    anno_filter: Optional[int] = None,
):
    """
    Carica file Excel saving.
    - Rileva automaticamente il foglio migliore
    - Mappa colonne per nome (compatibile con tutti i formati Alyante)
    - Converte valute non-EUR tramite colonna cambio o Banca d'Italia
    - Deriva CDC automaticamente se non presente
    - Estrae prefisso/anno commessa
    - Filtra per anno se specificato
    """
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        df, sheet_name = detect_best_sheet(xl)
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(400, f"Errore lettura file: {e}")

    df.columns = [c.strip() for c in df.columns]
    col = build_col_map(df)

    date_col = col.get("data_doc")
    if not date_col:
        raise HTTPException(400, "Colonna data non trovata nel file.")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    
    if anno_filter:
        df = df[df[date_col].dt.year == anno_filter]
        if df.empty:
            raise HTTPException(400, f"Nessuna riga per l'anno {anno_filter} nel file.")

    has_eur  = "imp_iniziale_eur" in col
    has_cdc  = "cdc" in col
    has_macro = "macro_categoria" in col
    has_tail  = "tail_spend" in col
    has_utente_pres = "utente_presentazione" in col

    log.info(f"Upload '{file.filename}': sheet='{sheet_name}', rows={len(df)}, "
             f"has_eur={has_eur}, has_cdc={has_cdc}, has_macro={has_macro}")

    sb = get_supabase()
    log_row = sb.table("upload_log").insert({
        "filename": file.filename,
        "tipo": "saving",
        "cdc_filter": cdc_override,
    }).execute()
    upload_id = log_row.data[0]["id"]

    records = []
    skipped = 0

    for idx, row in df.iterrows():
        data_doc_val = sd(row.get(date_col))
        if not data_doc_val:
            skipped += 1
            continue

        # Valuta e cambio
        valuta = ss(gcol(col,"valuta",row)) or "EURO"
        cambio_col = sfn(gcol(col,"cambio",row))
        
        if cambio_col and cambio_col != 1.0:
            cambio = cambio_col
        elif valuta != "EURO":
            try:
                ref = pd.to_datetime(data_doc_val).date()
                cambio = get_exchange_rate(valuta, ref)
            except:
                cambio = 1.0
        else:
            cambio = 1.0

        # Importi
        imp_raw = sf(gcol(col,"imp_iniziale",row))
        neg_raw = sf(gcol(col,"imp_negoziato",row))
        sav_raw = sf(gcol(col,"saving_val",row))
        pct_raw = sf(gcol(col,"perc_saving",row))

        if has_eur:
            imp_eur = sf(gcol(col,"imp_iniziale_eur",row))
            neg_eur = sf(gcol(col,"imp_negoziato_eur",row))
            sav_eur = sf(gcol(col,"saving_eur",row))
            pct_eur = sf(gcol(col,"perc_saving_eur",row))
            # Fallback: se la colonna EUR è zero ma raw non lo è, usa raw * cambio
            if imp_eur == 0 and imp_raw != 0:
                imp_eur = imp_raw * cambio
                neg_eur = neg_raw * cambio
                sav_eur = sav_raw * cambio
                pct_eur = pct_raw
        else:
            imp_eur = imp_raw * cambio
            neg_eur = neg_raw * cambio
            sav_eur = sav_raw * cambio
            pct_eur = pct_raw

        # CDC
        if cdc_override:
            cdc_val = cdc_override
        elif has_cdc:
            cdc_val = ss(gcol(col,"cdc",row))
        else:
            cdc_val = derive_cdc(
                ss(gcol(col,"centro_di_costo",row)) or "",
                ss(gcol(col,"desc_cdc",row)) or ""
            )

        # Commessa
        protoc_comm = ss(gcol(col,"protoc_commessa",row))
        pref_comm, anno_comm = extract_commessa_info(protoc_comm)

        records.append({
            "upload_id":            upload_id,
            "cod_utente":           si(gcol(col,"cod_utente",row)),
            "utente":               ss(gcol(col,"utente",row)),
            "utente_presentazione": ss(gcol(col,"utente_presentazione",row)) if has_utente_pres else None,
            "num_doc":              si(gcol(col,"num_doc",row)),
            "data_doc":             data_doc_val,
            "alfa_documento":       ss(gcol(col,"alfa_documento",row)),
            "str_ric":              ss(gcol(col,"str_ric",row)),
            "stato_dms":            ss(gcol(col,"stato_dms",row)),
            "codice_fornitore":     si(gcol(col,"codice_fornitore",row)),
            "ragione_sociale":      ss(gcol(col,"ragione_sociale",row)),
            "accred_albo":          sb(gcol(col,"accred_albo",row)),
            "protoc_ordine":        sfn(gcol(col,"protoc_ordine",row)),
            "protoc_commessa":      protoc_comm,
            "prefisso_commessa":    pref_comm,
            "anno_commessa":        anno_comm,
            "desc_commessa":        ss(gcol(col,"desc_cdc",row)),
            "grp_merceol":          ss(gcol(col,"grp_merceol",row)),
            "desc_gruppo_merceol":  ss(gcol(col,"desc_gruppo_merceol",row)),
            "macro_categoria":      ss(gcol(col,"macro_categoria",row)).strip() if has_macro and ss(gcol(col,"macro_categoria",row)) else None,
            "centro_di_costo":      ss(gcol(col,"centro_di_costo",row)),
            "desc_cdc":             ss(gcol(col,"desc_cdc",row)),
            "cdc":                  cdc_val,
            "valuta":               valuta,
            "cambio":               cambio,
            "imp_iniziale":         imp_raw,
            "imp_negoziato":        neg_raw,
            "saving_val":           sav_raw,
            "perc_saving":          pct_raw,
            "negoziazione":         sb(gcol(col,"negoziazione",row)),
            "imp_iniziale_eur":     imp_eur,
            "imp_negoziato_eur":    neg_eur,
            "saving_eur":           sav_eur,
            "perc_saving_eur":      pct_eur,
            "tail_spend":           ss(gcol(col,"tail_spend",row)) if has_tail else None,
        })

    # Insert a batch
    inserted = 0
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        sb.table("saving").insert(batch).execute()
        inserted += len(batch)

    sb.table("upload_log").update({"rows_inserted": inserted}).eq("id", upload_id).execute()
    
    return {
        "status":        "ok",
        "rows_inserted": inserted,
        "rows_skipped":  skipped,
        "upload_id":     upload_id,
        "sheet_used":    sheet_name,
        "has_eur_cols":  has_eur,
        "has_cdc_col":   has_cdc,
        "has_macro":     has_macro,
        "years_found":   sorted(df[date_col].dt.year.unique().tolist()),
    }


# ─────────────────────────────────────────────
# UPLOAD — TEMPI & NC
# ─────────────────────────────────────────────

TEMPI_MAP = {
    "protocol":        ["protocol","protocollo"],
    "year_month":      ["year_month","anno_mese","mese","month"],
    "days_purchasing": ["days_purchasing","giorni_acquisti"],
    "days_auto":       ["days_auto","giorni_auto"],
    "days_other":      ["days_other","giorni_altro"],
    "total_days":      ["total_days","giorni_totali","total"],
    "perc_purchasing": ["perc_purchasing","%_purchasing"],
    "perc_auto":       ["perc_auto","%_auto"],
    "perc_other":      ["perc_other","%_other"],
    "bottleneck":      ["bottleneck","fase_critica"],
}

NC_MAP = {
    "protocollo_commessa":   ["protocollo commessa","prot commessa"],
    "ragione_sociale":       ["ragione sociale anagrafica","ragione sociale","fornitore"],
    "tipo_origine":          ["tipo origine","tipo_origine"],
    "data_origine":          ["data origine","data_origine","data"],
    "utente_origine":        ["utente origine","utente_origine","utente"],
    "codice_prima_fattura":  ["codice prima fattura","cod fattura"],
    "data_prima_fattura":    ["data prima fattura","data fattura"],
    "importo_prima_fattura": ["importo prima fattura","importo fattura","importo"],
    "delta_giorni":          ["delta giorni (fattura - origine)","delta giorni","delta_giorni"],
    "non_conformita":        ["non conformità","non conformita","nc"],
}

def _best_sheet(xl):
    best, best_n = xl.sheet_names[0], 0
    for s in xl.sheet_names:
        try:
            n = len(pd.read_excel(xl, sheet_name=s))
            if n > best_n: best_n, best = n, s
        except: pass
    return pd.read_excel(xl, sheet_name=best), best

def _col_lookup(df_cols, col_map):
    norm = {c.lower(): c for c in df_cols}
    out = {}
    for k, candidates in col_map.items():
        for c in candidates:
            if c.lower() in norm:
                out[k] = norm[c.lower()]; break
    return out

@app.post("/upload/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        df, sheet = _best_sheet(xl)
    except Exception as e:
        raise HTTPException(400, str(e))
    df.columns = [c.strip() for c in df.columns]
    col = _col_lookup(df.columns, TEMPI_MAP)
    sb = get_supabase()
    log_row = sb.table("upload_log").insert({"filename": file.filename, "tipo": "tempi"}).execute()
    uid = log_row.data[0]["id"]
    records = [{"upload_id":uid,
        "protocol":        ss(row.get(col.get("protocol",""))),
        "year_month":      ss(row.get(col.get("year_month",""))),
        "days_purchasing": sf(row.get(col.get("days_purchasing",""))),
        "days_auto":       sf(row.get(col.get("days_auto",""))),
        "days_other":      sf(row.get(col.get("days_other",""))),
        "total_days":      sf(row.get(col.get("total_days",""))),
        "perc_purchasing": sf(row.get(col.get("perc_purchasing",""))),
        "perc_auto":       sf(row.get(col.get("perc_auto",""))),
        "perc_other":      sf(row.get(col.get("perc_other",""))),
        "bottleneck":      ss(row.get(col.get("bottleneck",""))),
    } for _,row in df.iterrows()]
    for i in range(0,len(records),500):
        sb.table("tempo_attraversamento").insert(records[i:i+500]).execute()
    sb.table("upload_log").update({"rows_inserted":len(records)}).eq("id",uid).execute()
    return {"status":"ok","rows":len(records),"sheet":sheet}

@app.post("/upload/nc")
async def upload_nc(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        df, sheet = _best_sheet(xl)
    except Exception as e:
        raise HTTPException(400, str(e))
    df.columns = [c.strip() for c in df.columns]
    col = _col_lookup(df.columns, NC_MAP)
    sb = get_supabase()
    log_row = sb.table("upload_log").insert({"filename": file.filename, "tipo": "nc"}).execute()
    uid = log_row.data[0]["id"]
    records = [{"upload_id":uid,
        "protocollo_commessa":   ss(row.get(col.get("protocollo_commessa",""))),
        "ragione_sociale":       ss(row.get(col.get("ragione_sociale",""))),
        "tipo_origine":          ss(row.get(col.get("tipo_origine",""))),
        "data_origine":          sd(row.get(col.get("data_origine",""))),
        "utente_origine":        ss(row.get(col.get("utente_origine",""))),
        "codice_prima_fattura":  ss(row.get(col.get("codice_prima_fattura",""))),
        "data_prima_fattura":    sd(row.get(col.get("data_prima_fattura",""))),
        "importo_prima_fattura": sfn(row.get(col.get("importo_prima_fattura",""))),
        "delta_giorni":          sfn(row.get(col.get("delta_giorni",""))),
        "non_conformita":        sb(row.get(col.get("non_conformita",""))),
    } for _,row in df.iterrows()]
    for i in range(0,len(records),500):
        sb.table("non_conformita").insert(records[i:i+500]).execute()
    sb.table("upload_log").update({"rows_inserted":len(records)}).eq("id",uid).execute()
    return {"status":"ok","rows":len(records),"sheet":sheet}


# ─────────────────────────────────────────────
# PREVIEW FILE
# ─────────────────────────────────────────────

@app.post("/upload/preview")
async def preview_file(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, str(e))
    result = {"sheets":[], "detected_type":None, "detected_sheet":None, "notes":[]}
    best_score = 0
    for sheet in xl.sheet_names:
        try:
            df = pd.read_excel(xl, sheet_name=sheet, nrows=100)
            col = build_col_map(df)
            score = sum(1 for k in ["data_doc","imp_iniziale","ragione_sociale","alfa_documento"] if k in col)
            if "imp_iniziale_eur" in col: score += 2
            n_rows = len(pd.read_excel(xl, sheet_name=sheet))
            result["sheets"].append({"name":sheet,"rows":n_rows,"cols":df.columns.tolist(),"score":score})
            if score > best_score:
                best_score = score
                result["detected_sheet"] = sheet
                result["col_mapping"] = col
                if "imp_iniziale_eur" not in col:
                    result["notes"].append("Colonne EUR non trovate: importi convertiti tramite cambio o Banca d'Italia")
                if "cdc" not in col:
                    result["notes"].append("Colonna CDC non trovata: derivata automaticamente dal Centro di costo")
                if "macro_categoria" not in col:
                    result["notes"].append("Colonna 'macro categorie' non trovata")
                anni = pd.to_datetime(df["Data doc."] if "Data doc." in df.columns else df.iloc[:,4], errors="coerce").dt.year.dropna().unique().tolist()
                result["anni_rilevati"] = [int(a) for a in anni]
        except: continue
    result["detected_type"] = "saving" if best_score >= 2 else None
    return result


# ─────────────────────────────────────────────
# KPI ENDPOINTS — tutti con filtri uniformi
# ─────────────────────────────────────────────

# Parametri comuni riusati in tutti gli endpoint
def _common_params(
    anno: Optional[int] = Query(None),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
    alfa_documento: Optional[str] = Query(None),
    macro_categoria: Optional[str] = Query(None),
    prefisso_commessa: Optional[str] = Query(None),
    utente: Optional[str] = Query(None),
    data_da: Optional[str] = Query(None),
    data_a: Optional[str] = Query(None),
):
    return dict(anno=anno,str_ric=str_ric,cdc=cdc,alfa_documento=alfa_documento,
        macro_categoria=macro_categoria,prefisso_commessa=prefisso_commessa,
        utente=utente,data_da=data_da,data_a=data_a)

@app.get("/kpi/saving/anni")
def get_anni():
    sb = get_supabase()
    rows = sb.table("saving").select("data_doc").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    anni = sorted(pd.to_datetime(df["data_doc"]).dt.year.dropna().unique().astype(int).tolist(), reverse=True)
    return [{"anno":a} for a in anni]

@app.get("/kpi/saving/riepilogo")
def kpi_riepilogo(
    anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None), alfa_documento: Optional[str]=Query(None),
    macro_categoria: Optional[str]=Query(None), prefisso_commessa: Optional[str]=Query(None),
    utente: Optional[str]=Query(None), data_da: Optional[str]=Query(None), data_a: Optional[str]=Query(None),
):
    df = query_saving(anno,str_ric,cdc,alfa_documento,macro_categoria,prefisso_commessa,utente,data_da,data_a,
        cols="imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    return calc_kpi(df)

@app.get("/kpi/saving/mensile")
def kpi_mensile(
    anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None), alfa_documento: Optional[str]=Query(None),
    macro_categoria: Optional[str]=Query(None), prefisso_commessa: Optional[str]=Query(None),
    utente: Optional[str]=Query(None),
):
    df = query_saving(anno,str_ric,cdc,alfa_documento,macro_categoria,prefisso_commessa,utente,
        cols="data_doc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    return sorted([{"mese":m, **calc_kpi(g)} for m,g in df.groupby("mese")], key=lambda x:x["mese"])

@app.get("/kpi/saving/per-cdc")
def kpi_per_cdc(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None)):
    df = query_saving(anno,str_ric,cols="cdc,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    return sorted([{"cdc":c, **calc_kpi(g)} for c,g in df.groupby("cdc") if c],
        key=lambda x:x["impegnato"],reverse=True)

@app.get("/kpi/saving/per-buyer")
def kpi_per_buyer(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None)):
    df = query_saving(anno,str_ric,cdc,
        cols="utente_presentazione,utente,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    # Usa utente_presentazione se disponibile, altrimenti utente
    df["buyer"] = df["utente_presentazione"].fillna(df["utente"])
    return sorted([{"utente":b, **calc_kpi(g)} for b,g in df.groupby("buyer") if b],
        key=lambda x:x["saving"],reverse=True)

@app.get("/kpi/saving/per-alfa-documento")
def kpi_per_alfa(
    anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None),
):
    df = query_saving(anno,str_ric,cdc,
        cols="alfa_documento,imp_iniziale_eur,saving_eur,negoziazione,accred_albo")
    if df.empty: return []
    return sorted([{"alfa_documento":a, **calc_kpi(g)} for a,g in df.groupby("alfa_documento") if a],
        key=lambda x:x["impegnato"],reverse=True)

@app.get("/kpi/saving/per-macro-categoria")
def kpi_per_macro(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None), cdc: Optional[str]=Query(None)):
    df = query_saving(anno,str_ric,cdc,
        cols="macro_categoria,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["macro_categoria"] = df["macro_categoria"].fillna("Non classificato").str.strip()
    return sorted([{"macro_categoria":m, **calc_kpi(g)} for m,g in df.groupby("macro_categoria")],
        key=lambda x:x["impegnato"],reverse=True)

@app.get("/kpi/saving/per-commessa")
def kpi_per_commessa(
    anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None), limit: int=Query(20),
):
    """Analisi per prefisso commessa (solo Ricerca)"""
    df = query_saving(anno, str_ric or "RICERCA", cdc,
        cols="prefisso_commessa,anno_commessa,desc_commessa,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["prefisso_commessa"])
    result = []
    for pref, g in df.groupby("prefisso_commessa"):
        kpi = calc_kpi(g)
        desc = g["desc_commessa"].dropna().mode()
        result.append({
            "prefisso_commessa": pref,
            "desc_commessa": desc.iloc[0] if not desc.empty else "—",
            **kpi
        })
    return sorted(result, key=lambda x:x["impegnato"], reverse=True)[:limit]

@app.get("/kpi/saving/per-categoria")
def kpi_per_categoria(
    anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None), limit: int=Query(15),
):
    df = query_saving(anno,str_ric,cdc,
        cols="desc_gruppo_merceol,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["desc_gruppo_merceol"])
    result = [{"desc_gruppo_merceol":c, **calc_kpi(g)} for c,g in df.groupby("desc_gruppo_merceol")]
    return sorted(result,key=lambda x:x["impegnato"],reverse=True)[:limit]

@app.get("/kpi/saving/top-fornitori")
def kpi_top_fornitori(
    anno: Optional[int]=Query(None), per: str=Query("saving"),
    limit: int=Query(10), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None),
):
    df = query_saving(anno,str_ric,cdc,
        cols="ragione_sociale,imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    result = []
    for forn, g in df.groupby("ragione_sociale"):
        if not forn: continue
        kpi = calc_kpi(g)
        kpi["ragione_sociale"] = forn
        kpi["albo"] = bool(g["accred_albo"].iloc[0])
        result.append(kpi)
    sort_col = "saving" if per=="saving" else "impegnato"
    result.sort(key=lambda x:x[sort_col],reverse=True)
    return result[:limit]

@app.get("/kpi/saving/pareto-fornitori")
def kpi_pareto(anno: Optional[int]=Query(None), str_ric: Optional[str]=Query(None)):
    df = query_saving(anno,str_ric,cols="ragione_sociale,imp_iniziale_eur")
    if df.empty: return []
    grp = df.groupby("ragione_sociale")["imp_iniziale_eur"].sum().sort_values(ascending=False).reset_index()
    total = grp["imp_iniziale_eur"].sum()
    grp["cum_perc"] = (grp["imp_iniziale_eur"].cumsum()/total*100).round(2)
    grp["rank"] = range(1,len(grp)+1)
    return grp.to_dict(orient="records")

@app.get("/kpi/saving/valute")
def kpi_valute(anno: Optional[int]=Query(None)):
    df = query_saving(anno,cols="valuta,imp_iniziale_eur,imp_iniziale,cambio")
    if df.empty: return []
    grp = df.groupby("valuta").agg(
        impegnato_eur=("imp_iniziale_eur","sum"),
        impegnato_orig=("imp_iniziale","sum"),
        n_ordini=("imp_iniziale_eur","count")).reset_index()
    total = grp["impegnato_eur"].sum()
    grp["perc"] = (grp["impegnato_eur"]/total*100).round(2)
    return grp.sort_values("impegnato_eur",ascending=False).to_dict(orient="records")


# ─────────────────────────────────────────────
# YoY
# ─────────────────────────────────────────────

@app.get("/kpi/saving/yoy")
def kpi_yoy(
    anno: int=Query(...), str_ric: Optional[str]=Query(None),
    cdc: Optional[str]=Query(None),
):
    ap = anno - 1
    cols = "data_doc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = query_saving(anno,str_ric,cdc,cols=cols)
    df_p = query_saving(ap,  str_ric,cdc,cols=cols)
    
    if not df_c.empty: df_c["mese_num"] = df_c["data_doc"].dt.month
    if not df_p.empty: df_p["mese_num"] = df_p["data_doc"].dt.month
    
    MESI = ["Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    chart = []
    for i, nome in enumerate(MESI,1):
        c = calc_kpi(df_c[df_c["mese_num"]==i]) if not df_c.empty else {}
        p = calc_kpi(df_p[df_p["mese_num"]==i]) if not df_p.empty else {}
        chart.append({
            "mese":nome,"mese_num":i,
            f"saving_{anno}":      c.get("saving",0),
            f"saving_{ap}":        p.get("saving",0),
            f"impegnato_{anno}":   c.get("impegnato",0),
            f"impegnato_{ap}":     p.get("impegnato",0),
            f"perc_saving_{anno}": c.get("perc_saving",0),
            f"perc_saving_{ap}":   p.get("perc_saving",0),
            f"n_ordini_{anno}":    c.get("n_doc_neg",0),
            f"n_ordini_{ap}":      p.get("n_doc_neg",0),
            f"n_negoziati_{anno}": c.get("n_negoziati",0),
            f"n_negoziati_{ap}":   p.get("n_negoziati",0),
        })
    
    kc, kp = calc_kpi(df_c), calc_kpi(df_p)
    def d(c,p): return round((c-p)/abs(p)*100,1) if p else None
    
    return {
        "anno":anno,"anno_precedente":ap,
        "chart_data":chart,
        "kpi_corrente":kc,"kpi_precedente":kp,
        "delta":{
            "impegnato":     d(kc["impegnato"],  kp["impegnato"]),
            "saving":        d(kc["saving"],     kp["saving"]),
            "perc_saving":   round(kc["perc_saving"]-kp["perc_saving"],2) if kp["perc_saving"] else None,
            "n_ordini":      d(kc["n_doc_neg"],  kp["n_doc_neg"]),
            "perc_negoziati":round(kc["perc_negoziati"]-kp["perc_negoziati"],2) if kp["perc_negoziati"] else None,
        }
    }

@app.get("/kpi/saving/yoy-cdc")
def kpi_yoy_cdc(anno: int=Query(...)):
    ap = anno-1
    cols = "cdc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = query_saving(anno,cols=cols)
    df_p = query_saving(ap,  cols=cols)
    def by_cdc(df):
        if df.empty: return {}
        return {c:calc_kpi(g) for c,g in df.groupby("cdc") if c}
    curr, prev = by_cdc(df_c), by_cdc(df_p)
    all_cdc = sorted(set(list(curr)+list(prev)))
    return [{"cdc":c,
        f"impegnato_{anno}": curr.get(c,{}).get("impegnato",0),
        f"impegnato_{ap}":   prev.get(c,{}).get("impegnato",0),
        f"saving_{anno}":    curr.get(c,{}).get("saving",0),
        f"saving_{ap}":      prev.get(c,{}).get("saving",0),
    } for c in all_cdc]


# ─────────────────────────────────────────────
# KPI — TEMPI & NC
# ─────────────────────────────────────────────

@app.get("/kpi/tempi/riepilogo")
def kpi_tempi_riepilogo():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return {}
    return {
        "avg_total_days":             round(float(df["total_days"].mean()),1),
        "avg_purchasing":             round(float(df["days_purchasing"].mean()),1),
        "avg_auto":                   round(float(df["days_auto"].mean()),1),
        "avg_other":                  round(float(df["days_other"].mean()),1),
        "n_ordini":                   len(df),
        "perc_bottleneck_purchasing": safe_pct(len(df[df["bottleneck"]=="PURCHASING"]),len(df)),
        "perc_bottleneck_auto":       safe_pct(len(df[df["bottleneck"]=="AUTO"]),len(df)),
        "perc_bottleneck_other":      safe_pct(len(df[df["bottleneck"]=="OTHER"]),len(df)),
    }

@app.get("/kpi/tempi/mensile")
def kpi_tempi_mensile():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for ym, g in df.groupby("year_month"):
        result.append({"mese":ym,
            "avg_total":      round(float(g["total_days"].mean()),1),
            "avg_purchasing": round(float(g["days_purchasing"].mean()),1),
            "avg_auto":       round(float(g["days_auto"].mean()),1),
            "avg_other":      round(float(g["days_other"].mean()),1),
            "n_ordini":       len(g),
            "n_bottleneck_purchasing": int((g["bottleneck"]=="PURCHASING").sum()),
            "n_bottleneck_auto":       int((g["bottleneck"]=="AUTO").sum()),
            "n_bottleneck_other":      int((g["bottleneck"]=="OTHER").sum()),
        })
    return sorted(result,key=lambda x:x["mese"])

@app.get("/kpi/tempi/distribuzione")
def kpi_tempi_dist():
    sb = get_supabase()
    rows = sb.table("tempo_attraversamento").select("total_days").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    bins=[0,7,15,30,60,9999]; labels=["≤7 gg","8-15 gg","16-30 gg","31-60 gg",">60 gg"]
    df["f"] = pd.cut(df["total_days"],bins=bins,labels=labels,right=True)
    return [{"fascia":k,"n_ordini":int(v)} for k,v in df["f"].value_counts().reindex(labels).fillna(0).items()]

@app.get("/kpi/nc/riepilogo")
def kpi_nc_rie():
    sb=get_supabase(); rows=sb.table("non_conformita").select("non_conformita,delta_giorni").execute().data
    df=pd.DataFrame(rows)
    if df.empty: return {}
    n=len(df); n_nc=int(df["non_conformita"].sum())
    return {"n_totale":n,"n_nc":n_nc,"perc_nc":safe_pct(n_nc,n),
        "avg_delta_giorni":round(float(df["delta_giorni"].mean()),1),
        "avg_delta_nc":round(float(df[df["non_conformita"]==True]["delta_giorni"].mean()),1)}

@app.get("/kpi/nc/mensile")
def kpi_nc_mensile():
    sb=get_supabase(); rows=sb.table("non_conformita").select("data_origine,non_conformita,delta_giorni").execute().data
    df=pd.DataFrame(rows)
    if df.empty: return []
    df["mese"]=pd.to_datetime(df["data_origine"],errors="coerce").dt.strftime("%Y-%m")
    df=df.dropna(subset=["mese"])
    result=[]
    for m,g in df.groupby("mese"):
        n=len(g); n_nc=int(g["non_conformita"].sum())
        result.append({"mese":m,"n_totale":n,"n_nc":n_nc,"perc_nc":safe_pct(n_nc,n),
            "avg_delta_giorni":round(float(g["delta_giorni"].mean()),1)})
    return sorted(result,key=lambda x:x["mese"])

@app.get("/kpi/nc/top-fornitori")
def kpi_nc_top(limit:int=Query(10)):
    sb=get_supabase(); rows=sb.table("non_conformita").select("ragione_sociale,non_conformita,delta_giorni").execute().data
    df=pd.DataFrame(rows)
    if df.empty: return []
    grp=df.groupby("ragione_sociale").agg(n_totale=("non_conformita","count"),n_nc=("non_conformita","sum"),avg_delta=("delta_giorni","mean")).reset_index()
    grp["perc_nc"]=(grp["n_nc"]/grp["n_totale"]*100).round(2)
    grp["avg_delta"]=grp["avg_delta"].round(1)
    return grp[grp["n_nc"]>0].nlargest(limit,"n_nc").to_dict(orient="records")

@app.get("/kpi/nc/per-tipo")
def kpi_nc_tipo():
    sb=get_supabase(); rows=sb.table("non_conformita").select("tipo_origine,non_conformita,delta_giorni").execute().data
    df=pd.DataFrame(rows)
    if df.empty: return []
    return [{"tipo":t,"n_totale":len(g),"n_nc":int(g["non_conformita"].sum()),
        "perc_nc":safe_pct(int(g["non_conformita"].sum()),len(g)),
        "avg_delta_giorni":round(float(g["delta_giorni"].mean()),1)}
        for t,g in df.groupby("tipo_origine")]


# ─────────────────────────────────────────────
# REPORT BUILDER — analisi multiparametrica
# ─────────────────────────────────────────────

@app.post("/report/build")
def build_report(
    body: dict = Body(...),
):
    """
    Report builder multiparametrico.
    
    Parametri body:
    {
      "filtri": {
        "anno": 2026, "str_ric": "RICERCA", "cdc": "TIGEM",
        "alfa_documento": "ORN", "macro_categoria": "Ricerca",
        "prefisso_commessa": "GMR", "utente": "Silvana",
        "data_da": "2026-01-01", "data_a": "2026-04-30"
      },
      "dimensioni": ["cdc", "alfa_documento", "macro_categoria", "prefisso_commessa"],
      "metriche": ["impegnato", "saving", "perc_saving", "n_righe", "n_negoziati", "perc_negoziati", "perc_albo"],
      "yoy": true,
      "anno_confronto": 2025,
      "limit": 50
    }
    """
    filtri = body.get("filtri", {})
    dimensioni = body.get("dimensioni", ["cdc"])
    metriche = body.get("metriche", ["impegnato","saving","perc_saving"])
    do_yoy = body.get("yoy", False)
    anno_conf = body.get("anno_confronto")
    limit = body.get("limit", 100)

    # Mappa dimensioni -> colonna DB
    DIM_MAP = {
        "cdc":               "cdc",
        "alfa_documento":    "alfa_documento",
        "macro_categoria":   "macro_categoria",
        "prefisso_commessa": "prefisso_commessa",
        "str_ric":           "str_ric",
        "utente":            "utente_presentazione",
        "valuta":            "valuta",
        "desc_gruppo_merceol": "desc_gruppo_merceol",
        "ragione_sociale":   "ragione_sociale",
        "mese":              None,  # gestito separatamente
        "anno":              None,
    }

    dim_cols = ",".join(filter(None, [DIM_MAP.get(d) for d in dimensioni]))
    needed = f"{dim_cols},imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento,data_doc"

    df = query_saving(
        anno=filtri.get("anno"),
        str_ric=filtri.get("str_ric"),
        cdc=filtri.get("cdc"),
        alfa_documento=filtri.get("alfa_documento"),
        macro_categoria=filtri.get("macro_categoria"),
        prefisso_commessa=filtri.get("prefisso_commessa"),
        utente=filtri.get("utente"),
        data_da=filtri.get("data_da"),
        data_a=filtri.get("data_a"),
        cols=needed,
    )

    if df.empty:
        return {"rows":[], "totale": calc_kpi(df), "filtri_applicati": filtri}

    # Gestisci dimensioni temporali
    if "mese" in dimensioni:
        df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    if "anno" in dimensioni:
        df["anno"] = df["data_doc"].dt.year.astype(str)

    # Raggruppa per dimensioni
    group_cols = []
    for d in dimensioni:
        db_col = DIM_MAP.get(d)
        if db_col and db_col in df.columns:
            group_cols.append(db_col)
        elif d in ["mese","anno"] and d in df.columns:
            group_cols.append(d)

    if not group_cols:
        return {"rows":[calc_kpi(df)], "totale": calc_kpi(df)}

    rows = []
    for keys, grp in df.groupby(group_cols):
        kpi = calc_kpi(grp)
        row = {}
        if isinstance(keys, str):
            keys = [keys]
        for col, val in zip(group_cols, keys):
            row[col] = val
        for m in metriche:
            row[m] = kpi.get(m)
        rows.append(row)

    rows.sort(key=lambda x: x.get("impegnato", 0), reverse=True)
    rows = rows[:limit]

    # YoY
    if do_yoy and anno_conf and filtri.get("anno"):
        filtri_conf = {**filtri, "anno": anno_conf}
        df_conf = query_saving(
            anno=anno_conf, str_ric=filtri.get("str_ric"), cdc=filtri.get("cdc"),
            cols=needed
        )
        if "mese" in dimensioni and not df_conf.empty:
            df_conf["mese"] = df_conf["data_doc"].dt.strftime("%Y-%m")
        if not df_conf.empty:
            for r in rows:
                mask = pd.Series([True]*len(df_conf))
                for col in group_cols:
                    if col in df_conf.columns:
                        mask &= df_conf[col] == r.get(col)
                grp_conf = df_conf[mask]
                kpi_conf = calc_kpi(grp_conf)
                for m in metriche:
                    r[f"{m}_prev"] = kpi_conf.get(m)
                    curr = r.get(m, 0) or 0
                    prev = kpi_conf.get(m, 0) or 0
                    if prev and m in ["impegnato","saving","n_righe","n_negoziati"]:
                        r[f"{m}_delta_pct"] = round((curr-prev)/abs(prev)*100,1)

    return {
        "rows": rows,
        "totale": calc_kpi(df),
        "n_righe_totali": len(df),
        "filtri_applicati": filtri,
        "dimensioni": dimensioni,
        "metriche": metriche,
    }


# ─────────────────────────────────────────────
# EXPORT — EXCEL MULTI-FOGLIO
# ─────────────────────────────────────────────

@app.post("/export/custom/excel")
def export_custom_excel(body: dict = Body(...)):
    """
    Export Excel custom basato sui parametri del report builder.
    Genera un file Excel con un foglio per ogni sezione selezionata.
    """
    filtri = body.get("filtri", {})
    sezioni = body.get("sezioni", ["riepilogo","mensile","cdc","alfa_documento","top_fornitori"])

    anno    = filtri.get("anno")
    str_ric = filtri.get("str_ric")
    cdc     = filtri.get("cdc")
    macro   = filtri.get("macro_categoria")
    pref    = filtri.get("prefisso_commessa")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:

        if "riepilogo" in sezioni:
            df = query_saving(anno,str_ric,cdc,macro_categoria=macro,prefisso_commessa=pref,
                cols="imp_iniziale_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
            kpi = calc_kpi(df)
            rows_kpi = [
                ["KPI","Valore","Nota"],
                ["Impegnato Totale €", kpi["impegnato"], ""],
                ["Saving Totale €",    kpi["saving"],    ""],
                ["% Saving",           f"{kpi['perc_saving']}%","saving/impegnato"],
                ["N° Righe Totali",    kpi["n_righe"],   ""],
                ["N° Doc. Negoziabili",kpi["n_doc_neg"], f"Tipi: {', '.join(sorted(DOC_NEGOZIABILI))}"],
                ["N° Negoziati",       kpi["n_negoziati"],""],
                ["% Negoziati",        f"{kpi['perc_negoziati']}%","n_negoziati/n_doc_neg"],
                ["% Albo Fornitori",   f"{kpi['perc_albo']}%",""],
            ]
            pd.DataFrame(rows_kpi[1:], columns=rows_kpi[0]).to_excel(writer,index=False,sheet_name="KPI Riepilogo")

        if "mensile" in sezioni:
            data = kpi_mensile(anno=anno,str_ric=str_ric,cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Mensile")

        if "cdc" in sezioni:
            data = kpi_per_cdc(anno=anno,str_ric=str_ric)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Per CDC")

        if "alfa_documento" in sezioni:
            data = kpi_per_alfa(anno=anno,str_ric=str_ric,cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Per Tipo Documento")

        if "macro_categoria" in sezioni:
            data = kpi_per_macro(anno=anno,str_ric=str_ric,cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Per Macro Categoria")

        if "commessa" in sezioni:
            data = kpi_per_commessa(anno=anno,str_ric=str_ric,cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Per Commessa")

        if "top_fornitori" in sezioni:
            data = kpi_top_fornitori(anno=anno,str_ric=str_ric,cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Top Fornitori")

        if "valute" in sezioni:
            data = kpi_valute(anno=anno)
            if data: pd.DataFrame(data).to_excel(writer,index=False,sheet_name="Valute")

        if "dati_raw" in sezioni:
            df = query_saving(anno,str_ric,cdc,macro_categoria=macro,prefisso_commessa=pref)
            if not df.empty:
                cols_out = [c for c in ["data_doc","utente_presentazione","alfa_documento",
                    "ragione_sociale","accred_albo","protoc_commessa","prefisso_commessa",
                    "desc_gruppo_merceol","macro_categoria","cdc","str_ric","valuta",
                    "imp_iniziale_eur","saving_eur","perc_saving_eur","negoziazione",
                    "tail_spend"] if c in df.columns]
                df[cols_out].to_excel(writer,index=False,sheet_name="Dati Raw")

    buf.seek(0)
    label = "_".join(filter(None,[str(anno) if anno else "tutti",cdc,str_ric,macro,pref]))
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition":f"attachment; filename=report_{label}.xlsx"})


# ─────────────────────────────────────────────
# EXPORT — PDF REPORT FORMATTATO
# ─────────────────────────────────────────────

@app.post("/export/custom/pdf")
def export_custom_pdf(body: dict = Body(...)):
    """
    PDF report formattato con sezioni selezionabili.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.colors import HexColor, white, lightgrey
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
            Table, TableStyle, HRFlowable, PageBreak)
    except ImportError:
        raise HTTPException(500, "reportlab non installato")

    filtri  = body.get("filtri", {})
    sezioni = body.get("sezioni", ["riepilogo","cdc","alfa_documento","top_fornitori"])
    note    = body.get("note", "")
    titolo  = body.get("titolo", "Report Ufficio Acquisti")

    anno    = filtri.get("anno")
    str_ric = filtri.get("str_ric")
    cdc     = filtri.get("cdc")
    anno_prec = filtri.get("anno_confronto")

    BLUE  = HexColor("#0057A8"); RED = HexColor("#D81E1E")
    LBL   = HexColor("#E8F0FA"); GRAY = HexColor("#6b7280")

    P = lambda txt,s: Paragraph(txt,s)
    def mkstyle(name,**kw):
        return ParagraphStyle(name,**kw)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf,pagesize=A4,
        leftMargin=1.5*cm,rightMargin=1.5*cm,topMargin=1.5*cm,bottomMargin=1.5*cm)

    ts = mkstyle("ts",fontSize=18,textColor=BLUE,fontName="Helvetica-Bold",spaceAfter=4)
    ss = mkstyle("ss",fontSize=9, textColor=GRAY,fontName="Helvetica",spaceAfter=2)
    hs = mkstyle("hs",fontSize=11,textColor=BLUE,fontName="Helvetica-Bold",spaceBefore=12,spaceAfter=6)
    bs = mkstyle("bs",fontSize=8, fontName="Helvetica",spaceAfter=3,leading=12)
    ns = mkstyle("ns",fontSize=8, textColor=GRAY,fontName="Helvetica-Oblique",spaceAfter=3)
    ws = mkstyle("ws",fontSize=7, textColor=RED,fontName="Helvetica-Bold",spaceAfter=6)
    fs = mkstyle("fs",fontSize=6, textColor=GRAY,fontName="Helvetica")

    def tbl(rows_data, col_widths):
        t = Table(rows_data,colWidths=col_widths)
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),BLUE),("TEXTCOLOR",(0,0),(-1,0),white),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),7),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[white,LBL]),
            ("GRID",(0,0),(-1,-1),0.3,lightgrey),
            ("ALIGN",(1,0),(-1,-1),"RIGHT"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ("PADDING",(0,0),(-1,-1),4),
        ]))
        return t

    def fe(v): 
        if v is None: return "—"
        if abs(v)>=1e6: return f"€{v/1e6:.2f}M"
        if abs(v)>=1e3: return f"€{v/1e3:.1f}K"
        return f"€{v:.0f}"
    def fp(v): return f"{v:.2f}%" if v is not None else "—"
    def fn(v): return f"{v:,}" if v is not None else "—"

    df_curr = query_saving(anno,str_ric,cdc)
    kc = calc_kpi(df_curr)
    kp = {}
    if anno_prec:
        df_prev = query_saving(anno_prec,str_ric,cdc)
        kp = calc_kpi(df_prev)

    def delta_s(c,p,tipo="pct"):
        if not p: return "n/d"
        if tipo=="pp": d=round(c-p,2); return f"{'▲' if d>=0 else '▼'}{abs(d):.1f}pp"
        d=round((c-p)/abs(p)*100,1); return f"{'▲' if d>=0 else '▼'}{abs(d):.1f}%"

    area = f" — {str_ric}" if str_ric else ""
    cdcl = f" — {cdc}" if cdc else ""
    story = [
        P(f"{titolo}{area}{cdcl}", ts),
        P(f"Anno {anno}{f' vs {anno_prec}' if anno_prec else ''} | Fondazione Telethon ETS", ss),
        P("RISERVATO – USO INTERNO", ws),
        HRFlowable(width="100%",thickness=2,color=BLUE,spaceAfter=10),
    ]

    if "riepilogo" in sezioni:
        story.append(P("KPI Principali", hs))
        header = ["Indicatore", f"{anno}"]
        if anno_prec: header += [str(anno_prec), "Δ"]
        kpi_rows = [header]
        for label, key, tipo in [
            ("Impegnato €",       "impegnato",      "eur"),
            ("Saving €",          "saving",         "eur"),
            ("% Saving",          "perc_saving",    "pct"),
            ("N° Righe",          "n_righe",        "num"),
            ("N° Doc.Negoziabili","n_doc_neg",      "num"),
            ("N° Negoziati",      "n_negoziati",    "num"),
            ("% Negoziati",       "perc_negoziati", "pct"),
            ("% Albo",            "perc_albo",      "pct"),
        ]:
            c_val = kc.get(key,0)
            fmt = fe(c_val) if tipo=="eur" else fp(c_val) if tipo=="pct" else fn(c_val)
            row = [label, fmt]
            if anno_prec:
                p_val = kp.get(key,0)
                pfmt = fe(p_val) if tipo=="eur" else fp(p_val) if tipo=="pct" else fn(p_val)
                row += [pfmt, delta_s(c_val,p_val,"pp" if tipo=="pct" else "pct")]
            kpi_rows.append(row)
        cw = [5.5*cm,3.5*cm] + ([3.5*cm,2.5*cm] if anno_prec else [])
        story.append(tbl(kpi_rows,cw))
        story.append(Spacer(1,6))
        if note: story.append(P(f"📝 {note}", ns))
        story.append(HRFlowable(width="100%",thickness=0.5,color=lightgrey,spaceBefore=8,spaceAfter=8))

    if "cdc" in sezioni and not df_curr.empty and "cdc" in df_curr.columns:
        story.append(P("Breakdown per CDC", hs))
        cdc_rows = [["CDC","Impegnato","Saving","% Saving","N°Righe","Neg."]]
        for c,g in df_curr.groupby("cdc"):
            k=calc_kpi(g)
            cdc_rows.append([c,fe(k["impegnato"]),fe(k["saving"]),fp(k["perc_saving"]),fn(k["n_righe"]),fn(k["n_negoziati"])])
        story.append(tbl(cdc_rows,[2.5*cm,3.5*cm,3*cm,2.5*cm,2*cm,2*cm]))
        story.append(HRFlowable(width="100%",thickness=0.5,color=lightgrey,spaceBefore=8,spaceAfter=8))

    if "alfa_documento" in sezioni:
        story.append(P("Per Tipologia Documentale", hs))
        alfa_data = kpi_per_alfa(anno=anno,str_ric=str_ric,cdc=cdc)
        if alfa_data:
            a_rows=[["Tipo","N°Righe","Impegnato","Saving","% Saving","Neg."]]
            for r in alfa_data:
                a_rows.append([r["alfa_documento"],fn(r["n_righe"]),fe(r["impegnato"]),fe(r["saving"]),fp(r["perc_saving"]),fn(r["n_negoziati"])])
            story.append(tbl(a_rows,[2*cm,2.5*cm,4*cm,3.5*cm,2.5*cm,2*cm]))
            story.append(HRFlowable(width="100%",thickness=0.5,color=lightgrey,spaceBefore=8,spaceAfter=8))

    if "macro_categoria" in sezioni:
        story.append(P("Per Macro Categoria", hs))
        mc_data = kpi_per_macro(anno=anno,str_ric=str_ric,cdc=cdc)
        if mc_data:
            mc_rows=[["Categoria","N°Righe","Impegnato","Saving","% Saving"]]
            for r in mc_data:
                mc_rows.append([str(r["macro_categoria"])[:30],fn(r["n_righe"]),fe(r["impegnato"]),fe(r["saving"]),fp(r["perc_saving"])])
            story.append(tbl(mc_rows,[5*cm,2.5*cm,3.5*cm,3*cm,2.5*cm]))
            story.append(HRFlowable(width="100%",thickness=0.5,color=lightgrey,spaceBefore=8,spaceAfter=8))

    if "commessa" in sezioni:
        story.append(P("Per Commessa (Ricerca)", hs))
        comm_data = kpi_per_commessa(anno=anno,str_ric=str_ric,cdc=cdc,limit=15)
        if comm_data:
            c_rows=[["Prefisso","Descrizione","Impegnato","Saving","% Saving","N°Righe"]]
            for r in comm_data:
                c_rows.append([r["prefisso_commessa"],str(r.get("desc_commessa",""))[:25],
                    fe(r["impegnato"]),fe(r["saving"]),fp(r["perc_saving"]),fn(r["n_righe"])])
            story.append(tbl(c_rows,[2*cm,5.5*cm,3*cm,2.5*cm,2*cm,1.5*cm]))
            story.append(HRFlowable(width="100%",thickness=0.5,color=lightgrey,spaceBefore=8,spaceAfter=8))

    if "top_fornitori" in sezioni:
        story.append(P("Top 10 Fornitori per Saving", hs))
        forn_data = kpi_top_fornitori(anno=anno,str_ric=str_ric,cdc=cdc,limit=10)
        if forn_data:
            f_rows=[["Fornitore","Impegnato","Saving","% Saving","N°","Albo"]]
            for r in forn_data:
                nome=str(r["ragione_sociale"])
                nome=nome[:35]+"…" if len(nome)>35 else nome
                f_rows.append([nome,fe(r["impegnato"]),fe(r["saving"]),fp(r["perc_saving"]),fn(r["n_righe"]),"✓" if r.get("albo") else ""])
            story.append(tbl(f_rows,[6*cm,3*cm,2.5*cm,2*cm,1.5*cm,1*cm]))

    # Footer
    story += [Spacer(1,16),HRFlowable(width="100%",thickness=0.5,color=lightgrey),
        P(f"Generato il {datetime.now().strftime('%d/%m/%Y %H:%M')} — Fondazione Telethon ETS — Ufficio Acquisti — Uso interno riservato", fs)]

    doc.build(story)
    buf.seek(0)
    label = "_".join(filter(None,[str(anno) if anno else "tutti",cdc,str_ric]))
    return StreamingResponse(buf,media_type="application/pdf",
        headers={"Content-Disposition":f"attachment; filename=report_{label}.pdf"})


# ─────────────────────────────────────────────
# FILTRI DISPONIBILI — per popolare i menu
# ─────────────────────────────────────────────

@app.get("/filtri/disponibili")
def filtri_disponibili(anno: Optional[int]=Query(None)):
    """Restituisce i valori unici disponibili per ogni dimensione di filtro"""
    sb = get_supabase()
    q = sb.table("saving").select(
        "cdc,str_ric,alfa_documento,macro_categoria,prefisso_commessa,utente_presentazione,valuta"
    )
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return {k:[] for k in ["cdc","str_ric","alfa_documento","macro_categoria","prefisso_commessa","utente","valuta"]}
    
    def uniq(col):
        return sorted(df[col].dropna().str.strip().unique().tolist()) if col in df.columns else []
    
    return {
        "cdc":               uniq("cdc"),
        "str_ric":           uniq("str_ric"),
        "alfa_documento":    uniq("alfa_documento"),
        "macro_categoria":   [x for x in uniq("macro_categoria") if x],
        "prefisso_commessa": uniq("prefisso_commessa"),
        "utente":            [x for x in uniq("utente_presentazione") if x],
        "valuta":            uniq("valuta"),
    }


# ─────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────

@app.get("/upload/log")
def get_upload_log():
    sb=get_supabase()
    rows=sb.table("upload_log").select("*").order("upload_date",desc=True).limit(50).execute().data
    return rows

@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    sb=get_supabase()
    sb.table("upload_log").delete().eq("id",upload_id).execute()
    return {"status":"deleted"}

@app.get("/health")
def health():
    return {"status":"ok","version":"4.0.0",
        "endpoints":["/kpi/saving/riepilogo","/kpi/saving/mensile","/kpi/saving/per-cdc",
            "/kpi/saving/per-buyer","/kpi/saving/per-alfa-documento","/kpi/saving/per-macro-categoria",
            "/kpi/saving/per-commessa","/kpi/saving/yoy","/report/build",
            "/export/custom/excel","/export/custom/pdf","/filtri/disponibili"]}


# ─────────────────────────────────────────────
# YoY CORRETTO — confronto periodo omogeneo
# ─────────────────────────────────────────────

@app.get("/kpi/saving/periodo-disponibile")
def periodo_disponibile(anno: int = Query(...)):
    """
    Restituisce il primo e ultimo mese disponibile per un anno.
    Usato dal frontend per comunicare il periodo di confronto.
    """
    sb = get_supabase()
    rows = sb.table("saving").select("data_doc").gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31").execute().data
    df = pd.DataFrame(rows)
    if df.empty:
        return {"anno": anno, "mese_min": None, "mese_max": None, "n_mesi": 0, "ultima_data": None}
    df["data_doc"] = pd.to_datetime(df["data_doc"])
    return {
        "anno": anno,
        "mese_min": int(df["data_doc"].dt.month.min()),
        "mese_max": int(df["data_doc"].dt.month.max()),
        "n_mesi": int(df["data_doc"].dt.month.nunique()),
        "ultima_data": df["data_doc"].max().date().isoformat(),
        "primo_giorno_ultimo_mese": int(df[df["data_doc"].dt.month == df["data_doc"].dt.month.max()]["data_doc"].dt.day.min()),
        "ultimo_giorno_ultimo_mese": int(df[df["data_doc"].dt.month == df["data_doc"].dt.month.max()]["data_doc"].dt.day.max()),
    }


@app.get("/kpi/saving/yoy-omogeneo")
def kpi_yoy_omogeneo(
    anno: int = Query(...),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
):
    """
    Confronto YoY su periodo omogeneo:
    - Rileva automaticamente i mesi disponibili per l'anno corrente
    - Se l'ultimo mese è parziale (ultimo giorno < 20), escludilo dal confronto KPI headline
    - Confronta con lo stesso periodo dell'anno precedente
    - I grafici mensili mostrano mese-per-mese con nota sul periodo
    """
    ap = anno - 1

    # Rileva periodo anno corrente
    sb = get_supabase()
    rows_curr = sb.table("saving").select("data_doc").gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31").execute().data
    df_curr_dates = pd.DataFrame(rows_curr)

    if df_curr_dates.empty:
        return {"anno": anno, "anno_precedente": ap, "periodo": None, "chart_data": [], "kpi_corrente": calc_kpi(pd.DataFrame()), "kpi_precedente": calc_kpi(pd.DataFrame()), "delta": {}}

    df_curr_dates["data_doc"] = pd.to_datetime(df_curr_dates["data_doc"])
    mese_max = int(df_curr_dates["data_doc"].dt.month.max())
    ultimo_giorno = int(df_curr_dates["data_doc"][df_curr_dates["data_doc"].dt.month == mese_max].dt.day.max())

    # Mese parziale: ultimo giorno < 20 → escludi dal confronto KPI headline
    mese_confronto = mese_max - 1 if ultimo_giorno < 20 and mese_max > 1 else mese_max
    ultima_data = df_curr_dates["data_doc"].max().date().isoformat()

    # Carica dati anno corrente e precedente con filtri
    cols = "data_doc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = query_saving(anno, str_ric, cdc, cols=cols)
    df_p = query_saving(ap, str_ric, cdc, cols=cols)

    if not df_c.empty:
        df_c["mese_num"] = df_c["data_doc"].dt.month
    if not df_p.empty:
        df_p["mese_num"] = df_p["data_doc"].dt.month

    # KPI headline: solo mesi interi (fino a mese_confronto)
    df_c_kpi = df_c[df_c["mese_num"] <= mese_confronto] if not df_c.empty else df_c
    df_p_kpi = df_p[df_p["mese_num"] <= mese_confronto] if not df_p.empty else df_p

    kc = calc_kpi(df_c_kpi)
    kp = calc_kpi(df_p_kpi)

    # Delta
    def d(c, p):
        return round((c - p) / abs(p) * 100, 1) if p else None

    # Grafici mensili: mese-per-mese (tutti i mesi disponibili)
    MESI = ["Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"]
    chart = []
    for i, nome in enumerate(MESI, 1):
        c_grp = df_c[df_c["mese_num"] == i] if not df_c.empty else pd.DataFrame()
        p_grp = df_p[df_p["mese_num"] == i] if not df_p.empty else pd.DataFrame()
        c_kpi = calc_kpi(c_grp)
        p_kpi = calc_kpi(p_grp)
        chart.append({
            "mese": nome,
            "mese_num": i,
            "ha_dati_curr": len(c_grp) > 0,
            "ha_dati_prev": len(p_grp) > 0,
            "parziale": i == mese_max and ultimo_giorno < 20,
            f"saving_{anno}":      c_kpi["saving"],
            f"saving_{ap}":        p_kpi["saving"],
            f"impegnato_{anno}":   c_kpi["impegnato"],
            f"impegnato_{ap}":     p_kpi["impegnato"],
            f"perc_saving_{anno}": c_kpi["perc_saving"],
            f"perc_saving_{ap}":   p_kpi["perc_saving"],
            f"n_ordini_{anno}":    c_kpi["n_doc_neg"],
            f"n_ordini_{ap}":      p_kpi["n_doc_neg"],
            f"n_negoziati_{anno}": c_kpi["n_negoziati"],
            f"n_negoziati_{ap}":   p_kpi["n_negoziati"],
        })

    mesi_it = {1:"gennaio",2:"febbraio",3:"marzo",4:"aprile",5:"maggio",
               6:"giugno",7:"luglio",8:"agosto",9:"settembre",10:"ottobre",
               11:"novembre",12:"dicembre"}

    return {
        "anno": anno,
        "anno_precedente": ap,
        "periodo": {
            "mese_min": 1,
            "mese_max": mese_max,
            "mese_confronto": mese_confronto,
            "ultimo_giorno": ultimo_giorno,
            "ultima_data": ultima_data,
            "label_curr": f"Gen–{MESI[mese_confronto-1]} {anno}",
            "label_prev": f"Gen–{MESI[mese_confronto-1]} {ap}",
            "nota": f"Confronto KPI su mesi interi (Gen–{mesi_it[mese_confronto]}). Aprile {anno} è parziale (dati fino al {ultimo_giorno})." if ultimo_giorno < 20 and mese_max == 4 else f"Dati {anno} disponibili fino al {ultima_data}."
        },
        "chart_data": chart,
        "kpi_corrente": kc,
        "kpi_precedente": kp,
        "delta": {
            "impegnato":      d(kc["impegnato"], kp["impegnato"]),
            "saving":         d(kc["saving"], kp["saving"]),
            "perc_saving":    round(kc["perc_saving"] - kp["perc_saving"], 2) if kp["perc_saving"] else None,
            "n_ordini":       d(kc["n_doc_neg"], kp["n_doc_neg"]),
            "perc_negoziati": round(kc["perc_negoziati"] - kp["perc_negoziati"], 2) if kp["perc_negoziati"] else None,
        }
    }


@app.get("/kpi/saving/mensile-con-area")
def kpi_mensile_con_area(
    anno: Optional[int] = Query(None),
    cdc: Optional[str] = Query(None),
):
    """
    Andamento mensile segmentato per area (RICERCA / STRUTTURA).
    Usato per i grafici mensili con breakdown.
    """
    cols = "data_doc,str_ric,cdc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df = query_saving(anno, cdc=cdc, cols=cols)
    if df.empty:
        return []

    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    MESI_LABEL = {f"{anno}-{m:02d}": ["Gen","Feb","Mar","Apr","Mag","Giu","Lug","Ago","Set","Ott","Nov","Dic"][m-1] for m in range(1,13)} if anno else {}

    result = []
    for mese, grp in df.groupby("mese"):
        kpi_tot = calc_kpi(grp)
        kpi_ric = calc_kpi(grp[grp["str_ric"] == "RICERCA"])
        kpi_str = calc_kpi(grp[grp["str_ric"] == "STRUTTURA"])
        result.append({
            "mese": mese,
            "label": MESI_LABEL.get(mese, mese),
            **{f"tot_{k}": v for k,v in kpi_tot.items()},
            **{f"ric_{k}": v for k,v in kpi_ric.items()},
            **{f"str_{k}": v for k,v in kpi_str.items()},
        })
    return sorted(result, key=lambda x: x["mese"])


# ─────────────────────────────────────────────
# YoY MULTI-GRANULARITÀ
# ─────────────────────────────────────────────

GRANULARITA_MAP = {
    "mensile":    [(m, m,   f"M{m:02d}") for m in range(1, 13)],
    "bimestrale": [(1,2,"B1"),(3,4,"B2"),(5,6,"B3"),(7,8,"B4"),(9,10,"B5"),(11,12,"B6")],
    "quarter":    [(1,3,"Q1"),(4,6,"Q2"),(7,9,"Q3"),(10,12,"Q4")],
    "semestrale": [(1,6,"S1"),(7,12,"S2")],
    "annuale":    [(1,12,"Anno")],
}

MESI_IT = {1:"Gen",2:"Feb",3:"Mar",4:"Apr",5:"Mag",6:"Giu",
           7:"Lug",8:"Ago",9:"Set",10:"Ott",11:"Nov",12:"Dic"}

@app.get("/kpi/saving/yoy-granulare")
def kpi_yoy_granulare(
    anno: int = Query(...),
    granularita: str = Query("mensile", description="mensile|bimestrale|quarter|semestrale|annuale"),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None),
):
    """
    Confronto YoY con granularità selezionabile.
    Mostra solo periodi con dati in almeno uno dei due anni.
    Segnala i periodi parziali (dove l'anno corrente non ha il mese completo).
    """
    ap = anno - 1
    periodi = GRANULARITA_MAP.get(granularita, GRANULARITA_MAP["mensile"])

    cols = "data_doc,imp_iniziale_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = query_saving(anno, str_ric, cdc, cols=cols)
    df_p = query_saving(ap,   str_ric, cdc, cols=cols)

    if not df_c.empty: df_c["mese_n"] = df_c["data_doc"].dt.month
    if not df_p.empty: df_p["mese_n"] = df_p["data_doc"].dt.month

    # Ultimo mese/giorno disponibile anno corrente
    mese_max_curr = int(df_c["mese_n"].max()) if not df_c.empty else 0
    ultimo_giorno = int(df_c[df_c["mese_n"] == mese_max_curr]["data_doc"].dt.day.max()) if not df_c.empty and mese_max_curr else 0

    chart = []
    for m_start, m_end, label in periodi:
        mask_c = (df_c["mese_n"] >= m_start) & (df_c["mese_n"] <= m_end) if not df_c.empty else pd.Series([], dtype=bool)
        mask_p = (df_p["mese_n"] >= m_start) & (df_p["mese_n"] <= m_end) if not df_p.empty else pd.Series([], dtype=bool)

        grp_c = df_c[mask_c] if not df_c.empty else pd.DataFrame()
        grp_p = df_p[mask_p] if not df_p.empty else pd.DataFrame()

        ha_dati_c = len(grp_c) > 0
        ha_dati_p = len(grp_p) > 0

        if not ha_dati_c and not ha_dati_p:
            continue  # salta periodi senza dati in nessuno dei due anni

        # Parziale: l'anno corrente non copre tutto il periodo
        parziale = ha_dati_c and mese_max_curr < m_end
        parziale_label = ""
        if parziale:
            if granularita == "mensile":
                parziale_label = f"(dati fino al {ultimo_giorno})"
            else:
                mesi_coperti = [MESI_IT[m] for m in range(m_start, min(mese_max_curr, m_end)+1)]
                parziale_label = f"(solo {', '.join(mesi_coperti)})"

        kc = calc_kpi(grp_c)
        kp = calc_kpi(grp_p)

        # Label leggibile
        if granularita == "mensile":
            label_full = MESI_IT.get(m_start, label)
        elif granularita == "bimestrale":
            label_full = f"{MESI_IT[m_start]}–{MESI_IT[m_end]}"
        elif granularita == "quarter":
            label_full = label
        elif granularita == "semestrale":
            label_full = f"{label} ({MESI_IT[m_start]}–{MESI_IT[m_end]})"
        else:
            label_full = str(anno)

        def d(c, p):
            return round((c-p)/abs(p)*100, 1) if p else None

        chart.append({
            "label":       label_full,
            "periodo":     label,
            "m_start":     m_start,
            "m_end":       m_end,
            "parziale":    parziale,
            "parziale_label": parziale_label,
            "ha_dati_curr": ha_dati_c,
            "ha_dati_prev": ha_dati_p,
            # Anno corrente
            f"saving_{anno}":      kc["saving"],
            f"impegnato_{anno}":   kc["impegnato"],
            f"perc_saving_{anno}": kc["perc_saving"],
            f"n_ordini_{anno}":    kc["n_doc_neg"],
            f"n_neg_{anno}":       kc["n_negoziati"],
            # Anno precedente
            f"saving_{ap}":        kp["saving"],
            f"impegnato_{ap}":     kp["impegnato"],
            f"perc_saving_{ap}":   kp["perc_saving"],
            f"n_ordini_{ap}":      kp["n_doc_neg"],
            f"n_neg_{ap}":         kp["n_negoziati"],
            # Delta (solo se non parziale)
            "delta_saving":      d(kc["saving"],   kp["saving"])   if not parziale else None,
            "delta_impegnato":   d(kc["impegnato"],kp["impegnato"]) if not parziale else None,
            "delta_perc_saving": round(kc["perc_saving"]-kp["perc_saving"],2) if kp["perc_saving"] and not parziale else None,
        })

    # KPI headline: solo periodi NON parziali
    periodi_interi = [r for r in chart if not r["parziale"] and r["ha_dati_curr"] and r["ha_dati_prev"]]
    if periodi_interi:
        mesi_interi = set()
        for r in periodi_interi:
            for m in range(r["m_start"], r["m_end"]+1):
                mesi_interi.add(m)
        mask_kc = df_c["mese_n"].isin(mesi_interi) if not df_c.empty else pd.Series([], dtype=bool)
        mask_kp = df_p["mese_n"].isin(mesi_interi) if not df_p.empty else pd.Series([], dtype=bool)
        kc_hl = calc_kpi(df_c[mask_kc] if not df_c.empty else pd.DataFrame())
        kp_hl = calc_kpi(df_p[mask_kp] if not df_p.empty else pd.DataFrame())
    else:
        kc_hl = calc_kpi(df_c)
        kp_hl = calc_kpi(df_p)

    def d(c, p): return round((c-p)/abs(p)*100, 1) if p else None

    # Nota periodo
    if mese_max_curr < 12 and not df_c.empty:
        label_curr = f"Gen–{MESI_IT[mese_max_curr]} {anno}"
        label_prev = f"Gen–{MESI_IT[mese_max_curr]} {ap}"
        nota = f"Dati {anno} disponibili fino al {df_c['data_doc'].max().date().isoformat()}."
        if ultimo_giorno < 20:
            mese_confronto = mese_max_curr - 1
            nota += f" I KPI headline escludono {MESI_IT[mese_max_curr]} (parziale) e confrontano Gen–{MESI_IT[mese_confronto]}."
            label_curr = f"Gen–{MESI_IT[mese_confronto]} {anno}"
            label_prev = f"Gen–{MESI_IT[mese_confronto]} {ap}"
    else:
        label_curr = f"Anno {anno}"
        label_prev = f"Anno {ap}"
        nota = ""

    return {
        "anno": anno,
        "anno_precedente": ap,
        "granularita": granularita,
        "chart_data": chart,
        "kpi_headline": {
            "corrente":  kc_hl,
            "precedente": kp_hl,
            "label_curr": label_curr,
            "label_prev": label_prev,
            "delta": {
                "impegnato":      d(kc_hl["impegnato"],   kp_hl["impegnato"]),
                "saving":         d(kc_hl["saving"],      kp_hl["saving"]),
                "perc_saving":    round(kc_hl["perc_saving"]-kp_hl["perc_saving"],2) if kp_hl["perc_saving"] else None,
                "n_ordini":       d(kc_hl["n_doc_neg"],   kp_hl["n_doc_neg"]),
                "perc_negoziati": round(kc_hl["perc_negoziati"]-kp_hl["perc_negoziati"],2) if kp_hl["perc_negoziati"] else None,
            }
        },
        "nota": nota,
        "mese_max_curr": mese_max_curr,
        "ultimo_giorno": ultimo_giorno,
    }
