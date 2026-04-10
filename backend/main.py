"""
UA Dashboard Backend v6.0 — Fondazione Telethon ETS
Logica KPI definitiva:
  LISTINO   = Imp. Iniziale € (prezzo di partenza)
  IMPEGNATO = Imp. Negoziato € (quanto effettivamente paghiamo)
  SAVING    = Saving.1 (differenza, il nostro lavoro di acquisti)
  % SAVING  = Saving / Listino × 100
"""
import os, io, logging
from typing import Optional
from datetime import datetime, date
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ua")

app = FastAPI(title="UA Dashboard API v6", version="6.0.0")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173").split(",")

app.add_middleware(CORSMiddleware, allow_origins=ORIGINS,
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def get_sb(): return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Conversione tipi ─────────────────────────────────────────────
def sf(v, d=0.0):
    try: return float(v) if pd.notna(v) else d
    except: return d
def sfn(v):
    try: return float(v) if pd.notna(v) else None
    except: return None
def si(v):
    try: return int(float(str(v))) if pd.notna(v) else None
    except: return None
def ss(v):
    try:
        s = str(v).strip() if pd.notna(v) else None
        return s if s and s.lower() not in ('nan','none','nat','') else None
    except: return None
def to_bool(v):
    return str(v).strip().upper() in ('SI','SÌ','YES','TRUE','1') if pd.notna(v) else False
def sd(v):
    try: return pd.to_datetime(v).date().isoformat()
    except: return None
def safe_pct(num, den):
    try: return round(num / den * 100, 2) if den else 0.0
    except: return 0.0

# ── Mappa colonne (case-insensitive, prima occorrenza vince) ─────
# LOGICA DEFINITIVA:
#   listino_eur  = Imp. Iniziale €   (prezzo di partenza)
#   impegnato    = Imp. Negoziato €  (quanto paghiamo)
#   saving       = Saving.1          (differenza = nostro lavoro)
COL_MAP = [
    # Importi EUR — PRIORITÀ MASSIMA
    ('imp. iniziale €',               'listino_eur'),
    ('imp. iniziale e',               'listino_eur'),
    ('imp iniziale €',                'listino_eur'),
    ('imp. negoziato €',              'impegnato_eur'),
    ('imp. negoziato e',              'impegnato_eur'),
    ('imp negoziato €',               'impegnato_eur'),
    ('saving.1',                      'saving_eur'),
    ('%saving',                       'perc_saving_eur'),
    # Importi in valuta originale (fallback)
    ('imp.iniziale',                  'listino_val'),
    ('imp.negoziato',                 'impegnato_val'),
    ('saving',                        'saving_val'),
    ('% saving',                      'perc_saving'),
    # Data
    ('data doc.',                     'data_doc'),
    ('data documento',                'data_doc'),
    # Anagrafica
    ('cod.utente',                    'cod_utente'),
    ('utente per presentazione',      'utente_pres'),
    ('utente',                        'utente'),
    ('num.doc.',                      'num_doc'),
    ('alfa documento',                'alfa_documento'),
    ('str./ric.',                     'str_ric'),
    ('stato dms',                     'stato_dms'),
    ('codice fornitore',              'codice_fornitore'),
    ('ragione sociale fornitore',     'ragione_sociale'),
    ('ragione sociale',               'ragione_sociale'),
    ('accred.albo',                   'accred_albo'),
    ('protoc.ordine',                 'protoc_ordine'),
    ('protoc.commessa',               'protoc_commessa'),
    ('protocollo commessa',           'protoc_commessa'),
    ('protocollo origine',            'protoc_origine'),
    ('grp.merceol.',                  'grp_merceol'),
    ('descrizione gruppo merceologic','desc_merceol'),
    ('descrizione gruppo merceologico','desc_merceol'),
    ('centro di costo',               'centro_costo'),
    ('descrizione centro di costo',   'desc_cdc'),
    ('macro categorie',               'macro_cat'),
    ('macro categoria',               'macro_cat'),
    ('negoziazione',                  'negoziazione'),
    ('valuta',                        'valuta'),
    ('cdc',                           'cdc'),
    ('cambio',                        'cambio'),
    ('tail spend',                    'tail_spend'),
]

def map_cols(df_columns):
    """Mappa colonne reali → tipo interno. Case-insensitive."""
    norm = {c.strip().lower(): c for c in df_columns}
    result = {}
    for nome, tipo in COL_MAP:
        if nome in norm and tipo not in result:
            result[tipo] = norm[nome]
    log.info(f"Mapped: {sorted(result.keys())}")
    return result

def gcol(col_map, key, row):
    cn = col_map.get(key)
    return row.get(cn) if cn else None

def best_sheet(xl):
    """Foglio con più righe contenente una colonna data."""
    best, best_n = xl.sheet_names[0], 0
    for s in xl.sheet_names:
        try:
            df = pd.read_excel(xl, sheet_name=s, nrows=5)
            df.columns = [c.strip() for c in df.columns]
            col = map_cols(df.columns)
            if 'data_doc' not in col: continue
            n = len(pd.read_excel(xl, sheet_name=s))
            if n > best_n: best_n, best = n, s
        except: pass
    return best

def derive_cdc(centro, desc):
    c = str(centro or '').upper()
    d = str(desc or '').upper()
    if 'TIGEM' in d: return 'TIGEM'
    if 'TIGET' in d: return 'TIGET'
    if c.startswith(('RCRIIR','RCREER')): return 'GD'
    if c.startswith('STR'): return 'STRUTTURA'
    return 'FT'

def parse_commessa(s):
    if not s: return None, None
    s = str(s).strip()
    return (s[:3] if len(s)>=3 else None), (s[3:5] if len(s)>=5 and s[3:5].isdigit() else None)

# ── KPI — definizioni fisse ──────────────────────────────────────
DOC_NEG = {'OS','OSP','PS','OPR','ORN','ORD'}

def calc_kpi(df: pd.DataFrame) -> dict:
    """
    DEFINIZIONI:
    - listino   = imp_listino_eur   (Imp. Iniziale €)
    - impegnato = imp_impegnato_eur (Imp. Negoziato €)
    - saving    = saving_eur        (Saving.1)
    - % saving  = saving / listino × 100
    """
    if df.empty:
        return dict(listino=0, impegnato=0, saving=0, perc_saving=0,
                    n_righe=0, n_doc_neg=0, n_negoziati=0, perc_negoziati=0,
                    n_albo=0, perc_albo=0)
    listino  = float(df['imp_listino_eur'].fillna(0).sum())
    impegnato = float(df['imp_impegnato_eur'].fillna(0).sum())
    saving   = float(df['saving_eur'].fillna(0).sum())
    n        = len(df)
    neg      = int(df['alfa_documento'].isin(DOC_NEG).sum())
    nn       = int(df['negoziazione'].fillna(False).sum())
    alb      = int(df['accred_albo'].fillna(False).sum())
    return dict(
        listino=round(listino, 2),
        impegnato=round(impegnato, 2),
        saving=round(saving, 2),
        perc_saving=safe_pct(saving, listino),
        n_righe=n,
        n_doc_neg=neg,
        n_negoziati=nn,
        perc_negoziati=safe_pct(nn, neg),
        n_albo=alb,
        perc_albo=safe_pct(alb, n),
    )

def query_saving(anno=None, str_ric=None, cdc=None,
                 alfa=None, macro=None, pref_comm=None,
                 data_da=None, data_a=None, cols="*") -> pd.DataFrame:
    sb = get_sb()
    q = sb.table("saving").select(cols)
    if anno:      q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    if data_da:   q = q.gte("data_doc", data_da)
    if data_a:    q = q.lte("data_doc", data_a)
    if str_ric:   q = q.eq("str_ric", str_ric)
    if cdc:       q = q.eq("cdc", cdc)
    if alfa:      q = q.eq("alfa_documento", alfa)
    if macro:     q = q.ilike("macro_categoria", f"%{macro.strip()}%")
    if pref_comm: q = q.eq("prefisso_commessa", pref_comm)
    rows = q.execute().data
    df = pd.DataFrame(rows)
    if not df.empty:
        df['data_doc'] = pd.to_datetime(df['data_doc'], errors='coerce')
        for c in ['imp_listino_eur','imp_impegnato_eur','saving_eur']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        for c in ['negoziazione','accred_albo']:
            if c in df.columns:
                df[c] = df[c].fillna(False).astype(bool)
    return df


# ════════════════════════════════════════════════════════════════
# UPLOAD
# ════════════════════════════════════════════════════════════════

@app.post("/upload/saving")
async def upload_saving(file: UploadFile = File(...),
                        cdc_override: Optional[str] = None):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        sheet = best_sheet(xl)
        df = pd.read_excel(xl, sheet_name=sheet)
    except Exception as e:
        raise HTTPException(400, f"Errore lettura file: {e}")

    # Rimuovi colonne senza nome
    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
    df.columns = [c.strip() for c in df.columns]
    col = map_cols(df.columns)

    date_col = col.get('data_doc')
    if not date_col:
        raise HTTPException(400, "Colonna data non trovata. Assicurati che il file contenga 'Data doc.'")

    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])

    # Auto-filtro: se ≥95% righe è dello stesso anno, prendi solo quello
    yr_counts = df[date_col].dt.year.value_counts()
    if len(yr_counts) > 1 and yr_counts.iloc[0] / len(df) >= 0.95:
        anno_dom = int(yr_counts.index[0])
        df = df[df[date_col].dt.year == anno_dom]
        log.info(f"Auto-filtro anno {anno_dom}: {len(df)} righe")

    # Verifica colonne importo
    has_eur  = 'listino_eur' in col and 'impegnato_eur' in col
    has_val  = 'listino_val' in col and 'impegnato_val' in col
    has_cdc  = 'cdc' in col

    if not has_eur and not has_val:
        raise HTTPException(400,
            "Colonne importo non trovate. Servono: 'Imp. Iniziale €' e 'Imp. Negoziato €'")

    sb = get_sb()
    lr = sb.table("upload_log").insert({
        "filename": file.filename, "tipo": "saving", "cdc_filter": cdc_override
    }).execute()
    upload_id = lr.data[0]["id"]

    records, skipped = [], 0
    for _, row in df.iterrows():
        dv = sd(row.get(date_col))
        if not dv: skipped += 1; continue

        cambio = sf(gcol(col,'cambio',row), 1.0) or 1.0
        valuta = ss(gcol(col,'valuta',row)) or 'EURO'

        if has_eur:
            listino_eur   = sf(gcol(col,'listino_eur',row))
            impegnato_eur = sf(gcol(col,'impegnato_eur',row))
            saving_eur    = sf(gcol(col,'saving_eur',row))
            pct_saving    = sf(gcol(col,'perc_saving_eur',row))
        else:
            listino_eur   = sf(gcol(col,'listino_val',row)) * cambio
            impegnato_eur = sf(gcol(col,'impegnato_val',row)) * cambio
            saving_eur    = sf(gcol(col,'saving_val',row)) * cambio
            pct_saving    = sf(gcol(col,'perc_saving',row))

        # Fallback saving dal calcolo
        if saving_eur == 0 and listino_eur > 0 and impegnato_eur > 0:
            saving_eur = listino_eur - impegnato_eur
        if pct_saving == 0 and listino_eur > 0:
            pct_saving = saving_eur / listino_eur * 100

        if cdc_override:
            cdc_val = cdc_override
        elif has_cdc:
            cdc_val = ss(gcol(col,'cdc',row))
        else:
            cdc_val = derive_cdc(
                ss(gcol(col,'centro_costo',row)) or '',
                ss(gcol(col,'desc_cdc',row)) or '')

        pc = ss(gcol(col,'protoc_commessa',row))
        pref, anno_comm = parse_commessa(pc)

        records.append({
            "upload_id":            upload_id,
            "cod_utente":           si(gcol(col,'cod_utente',row)),
            "utente":               ss(gcol(col,'utente',row)),
            "utente_presentazione": ss(gcol(col,'utente_pres',row)),
            "num_doc":              si(gcol(col,'num_doc',row)),
            "data_doc":             dv,
            "alfa_documento":       ss(gcol(col,'alfa_documento',row)),
            "str_ric":              ss(gcol(col,'str_ric',row)),
            "stato_dms":            ss(gcol(col,'stato_dms',row)),
            "codice_fornitore":     si(gcol(col,'codice_fornitore',row)),
            "ragione_sociale":      ss(gcol(col,'ragione_sociale',row)),
            "accred_albo":          to_bool(gcol(col,'accred_albo',row)),
            "protoc_ordine":        sfn(gcol(col,'protoc_ordine',row)),
            "protoc_commessa":      pc,
            "prefisso_commessa":    pref,
            "anno_commessa":        anno_comm,
            "desc_commessa":        ss(gcol(col,'desc_cdc',row)),
            "grp_merceol":          ss(gcol(col,'grp_merceol',row)),
            "desc_gruppo_merceol":  ss(gcol(col,'desc_merceol',row)),
            "macro_categoria":      ss(gcol(col,'macro_cat',row)),
            "centro_di_costo":      ss(gcol(col,'centro_costo',row)),
            "desc_cdc":             ss(gcol(col,'desc_cdc',row)),
            "cdc":                  cdc_val,
            "valuta":               valuta,
            "cambio":               cambio,
            # Colonne DB: imp_listino_eur = Imp. Iniziale €
            #             imp_impegnato_eur = Imp. Negoziato €
            "imp_listino_eur":      listino_eur,
            "imp_impegnato_eur":    impegnato_eur,
            "saving_eur":           saving_eur,
            "perc_saving_eur":      pct_saving,
            # Colonne legacy (in valuta originale)
            "imp_iniziale":         sf(gcol(col,'listino_val',row)),
            "imp_negoziato":        sf(gcol(col,'impegnato_val',row)),
            "saving_val":           sf(gcol(col,'saving_val',row)),
            "perc_saving":          sf(gcol(col,'perc_saving',row)),
            "negoziazione":         to_bool(gcol(col,'negoziazione',row)),
            "tail_spend":           ss(gcol(col,'tail_spend',row)),
        })

    inserted = 0
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            sb.table("saving").insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            log.error(f"Insert error batch {i}: {str(e)[:300]}")
            if i == 0:
                sb.table("upload_log").delete().eq("id", upload_id).execute()
                raise HTTPException(500, f"Errore inserimento dati: {str(e)[:400]}")

    sb.table("upload_log").update({"rows_inserted": inserted}).eq("id", upload_id).execute()
    log.info(f"Upload OK: {inserted} rows, {skipped} skipped, sheet='{sheet}'")
    return {
        "status": "ok",
        "rows_inserted": inserted,
        "rows_skipped": skipped,
        "upload_id": upload_id,
        "sheet_used": sheet,
    }


@app.post("/upload/tempi")
async def upload_tempi(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        sh = max(xl.sheet_names, key=lambda s: len(pd.read_excel(xl, sheet_name=s)))
        df = pd.read_excel(xl, sheet_name=sh)
    except Exception as e: raise HTTPException(400, str(e))
    df.columns = [c.strip() for c in df.columns]
    n = {c.lower():c for c in df.columns}
    def gc(k): return n.get(k)
    sb = get_sb()
    lr = sb.table("upload_log").insert({"filename":file.filename,"tipo":"tempi"}).execute()
    uid = lr.data[0]["id"]
    recs = [{"upload_id":uid,
        "protocol":        ss(row.get(gc("protocol"))),
        "year_month":      ss(row.get(gc("year_month") or gc("anno_mese"))),
        "days_purchasing": sf(row.get(gc("days_purchasing"))),
        "days_auto":       sf(row.get(gc("days_auto"))),
        "days_other":      sf(row.get(gc("days_other"))),
        "total_days":      sf(row.get(gc("total_days"))),
        "perc_purchasing": sf(row.get(gc("perc_purchasing"))),
        "perc_auto":       sf(row.get(gc("perc_auto"))),
        "perc_other":      sf(row.get(gc("perc_other"))),
        "bottleneck":      ss(row.get(gc("bottleneck"))),
    } for _,row in df.iterrows()]
    for i in range(0,len(recs),500): sb.table("tempo_attraversamento").insert(recs[i:i+500]).execute()
    sb.table("upload_log").update({"rows_inserted":len(recs)}).eq("id",uid).execute()
    return {"status":"ok","rows":len(recs)}


@app.post("/upload/nc")
async def upload_nc(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(contents))
        sh = max(xl.sheet_names, key=lambda s: len(pd.read_excel(xl, sheet_name=s)))
        df = pd.read_excel(xl, sheet_name=sh)
    except Exception as e: raise HTTPException(400, str(e))
    df.columns = [c.strip() for c in df.columns]
    n = {c.lower():c for c in df.columns}
    def gc(k): return n.get(k)
    sb = get_sb()
    lr = sb.table("upload_log").insert({"filename":file.filename,"tipo":"nc"}).execute()
    uid = lr.data[0]["id"]
    recs = [{"upload_id":uid,
        "protocollo_commessa":   ss(row.get(gc("protocollo commessa"))),
        "ragione_sociale":       ss(row.get(gc("ragione sociale anagrafica") or gc("ragione sociale"))),
        "tipo_origine":          ss(row.get(gc("tipo origine"))),
        "data_origine":          sd(row.get(gc("data origine"))),
        "utente_origine":        ss(row.get(gc("utente origine"))),
        "codice_prima_fattura":  ss(row.get(gc("codice prima fattura"))),
        "data_prima_fattura":    sd(row.get(gc("data prima fattura"))),
        "importo_prima_fattura": sfn(row.get(gc("importo prima fattura"))),
        "delta_giorni":          sfn(row.get(gc("delta giorni (fattura - origine)") or gc("delta giorni"))),
        "non_conformita":        to_bool(row.get(gc("non conformità") or gc("non conformita"))),
    } for _,row in df.iterrows()]
    for i in range(0,len(recs),500): sb.table("non_conformita").insert(recs[i:i+500]).execute()
    sb.table("upload_log").update({"rows_inserted":len(recs)}).eq("id",uid).execute()
    return {"status":"ok","rows":len(recs)}


@app.get("/wake")
def wake(): return {"ok": True}


# ════════════════════════════════════════════════════════════════
# KPI ENDPOINTS
# ════════════════════════════════════════════════════════════════

@app.get("/kpi/saving/anni")
def get_anni():
    rows = get_sb().table("saving").select("data_doc").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    anni = sorted(pd.to_datetime(df["data_doc"]).dt.year.dropna().unique().astype(int).tolist(), reverse=True)
    return [{"anno": a} for a in anni]

@app.get("/kpi/saving/riepilogo")
def kpi_riepilogo(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), alfa: Optional[str] = Query(None),
    macro: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cdc, alfa, macro,
        cols="imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    return calc_kpi(df)

@app.get("/kpi/saving/mensile")
def kpi_mensile(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cdc,
        cols="data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    return sorted([{"mese": m, **calc_kpi(g)} for m, g in df.groupby("mese")], key=lambda x: x["mese"])

@app.get("/kpi/saving/per-cdc")
def kpi_per_cdc(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric,
        cols="cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    return sorted([{"cdc": c, **calc_kpi(g)} for c, g in df.groupby("cdc") if c],
        key=lambda x: x["saving"], reverse=True)

@app.get("/kpi/saving/per-buyer")
def kpi_per_buyer(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cdc,
        cols="utente_presentazione,utente,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["buyer"] = df["utente_presentazione"].fillna(df["utente"])
    return sorted([{"utente": b, **calc_kpi(g)} for b, g in df.groupby("buyer") if b],
        key=lambda x: x["saving"], reverse=True)

@app.get("/kpi/saving/per-alfa-documento")
def kpi_per_alfa(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cdc,
        cols="alfa_documento,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo")
    if df.empty: return []
    return sorted([{"alfa_documento": a, **calc_kpi(g)} for a, g in df.groupby("alfa_documento") if a],
        key=lambda x: x["listino"], reverse=True)

@app.get("/kpi/saving/per-macro-categoria")
def kpi_per_macro(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cdc,
        cols="macro_categoria,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df["macro_categoria"] = df["macro_categoria"].fillna("Non classificato").str.strip()
    return sorted([{"macro_categoria": m, **calc_kpi(g)} for m, g in df.groupby("macro_categoria")],
        key=lambda x: x["saving"], reverse=True)

@app.get("/kpi/saving/per-commessa")
def kpi_per_commessa(
    anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None),
    limit: int = Query(20)):
    df = query_saving(anno, "RICERCA", cdc,
        cols="prefisso_commessa,desc_commessa,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["prefisso_commessa"])
    result = []
    for pref, g in df.groupby("prefisso_commessa"):
        kpi = calc_kpi(g)
        desc = g["desc_commessa"].dropna().mode()
        result.append({"prefisso_commessa": pref, "desc_commessa": desc.iloc[0] if not desc.empty else "—", **kpi})
    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]

@app.get("/kpi/saving/per-categoria")
def kpi_per_categoria(
    anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None), limit: int = Query(15)):
    df = query_saving(anno, str_ric, cdc,
        cols="desc_gruppo_merceol,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,accred_albo,alfa_documento")
    if df.empty: return []
    df = df.dropna(subset=["desc_gruppo_merceol"])
    result = [{"desc_gruppo_merceol": c, **calc_kpi(g)} for c, g in df.groupby("desc_gruppo_merceol")]
    return sorted(result, key=lambda x: x["saving"], reverse=True)[:limit]

@app.get("/kpi/saving/top-fornitori")
def kpi_top_fornitori(
    anno: Optional[int] = Query(None), per: str = Query("saving"),
    limit: int = Query(10), str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cdc,
        cols="ragione_sociale,accred_albo,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento")
    if df.empty: return []
    result = []
    for forn, g in df.groupby("ragione_sociale"):
        if not forn: continue
        kpi = calc_kpi(g)
        kpi["ragione_sociale"] = forn
        kpi["albo"] = bool(g["accred_albo"].iloc[0]) if not g.empty else False
        result.append(kpi)
    result.sort(key=lambda x: x.get(per, x.get("saving", 0)), reverse=True)
    return result[:limit]

@app.get("/kpi/saving/pareto-fornitori")
def kpi_pareto(anno: Optional[int] = Query(None), str_ric: Optional[str] = Query(None)):
    df = query_saving(anno, str_ric, cols="ragione_sociale,imp_impegnato_eur")
    if df.empty: return []
    grp = df.groupby("ragione_sociale")["imp_impegnato_eur"].sum().sort_values(ascending=False).reset_index()
    total = grp["imp_impegnato_eur"].sum()
    grp["cum_perc"] = (grp["imp_impegnato_eur"].cumsum() / total * 100).round(2)
    grp["rank"] = range(1, len(grp) + 1)
    return grp.to_dict(orient="records")

@app.get("/kpi/saving/valute")
def kpi_valute(anno: Optional[int] = Query(None)):
    df = query_saving(anno, cols="valuta,imp_listino_eur,imp_impegnato_eur,cambio")
    if df.empty: return []
    grp = df.groupby("valuta").agg(
        listino_eur=("imp_listino_eur","sum"),
        impegnato_eur=("imp_impegnato_eur","sum"),
        n_ordini=("imp_impegnato_eur","count")).reset_index()
    total = grp["impegnato_eur"].sum()
    grp["perc"] = (grp["impegnato_eur"] / total * 100).round(2)
    return grp.sort_values("impegnato_eur", ascending=False).to_dict(orient="records")


# ── YoY granulare ────────────────────────────────────────────────
GRAN_MAP = {
    "mensile":    [(m, m, f"M{m:02d}") for m in range(1,13)],
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
    granularita: str = Query("mensile"),
    str_ric: Optional[str] = Query(None),
    cdc: Optional[str] = Query(None)):

    ap = anno - 1
    periodi = GRAN_MAP.get(granularita, GRAN_MAP["mensile"])
    cols = "data_doc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"

    df_c = query_saving(anno, str_ric, cdc, cols=cols)
    df_p = query_saving(ap,   str_ric, cdc, cols=cols)

    if not df_c.empty: df_c["mn"] = df_c["data_doc"].dt.month
    if not df_p.empty: df_p["mn"] = df_p["data_doc"].dt.month

    mese_max = int(df_c["mn"].max()) if not df_c.empty else 0
    ult_giorno = int(df_c[df_c["mn"]==mese_max]["data_doc"].dt.day.max()) if mese_max else 0

    chart = []
    for m1, m2, lbl in periodi:
        gc = df_c[(df_c["mn"]>=m1)&(df_c["mn"]<=m2)] if not df_c.empty else pd.DataFrame()
        gp = df_p[(df_p["mn"]>=m1)&(df_p["mn"]<=m2)] if not df_p.empty else pd.DataFrame()
        if len(gc)==0 and len(gp)==0: continue

        parziale = len(gc)>0 and mese_max < m2
        if granularita == "mensile": label = MESI_IT.get(m1, lbl)
        elif granularita == "bimestrale": label = f"{MESI_IT[m1]}–{MESI_IT[m2]}"
        elif granularita == "quarter": label = lbl
        elif granularita == "semestrale": label = f"{lbl} ({MESI_IT[m1]}–{MESI_IT[m2]})"
        else: label = str(anno)

        kc, kp = calc_kpi(gc), calc_kpi(gp)
        def d(c,p): return round((c-p)/abs(p)*100,1) if p else None

        chart.append({
            "label": label, "periodo": lbl, "m_start": m1, "m_end": m2,
            "parziale": parziale,
            "parziale_label": f"(solo {', '.join(MESI_IT[m] for m in range(m1,min(mese_max,m2)+1))})" if parziale else "",
            "ha_dati_curr": len(gc)>0, "ha_dati_prev": len(gp)>0,
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
            "delta_saving":      d(kc["saving"],kp["saving"])   if not parziale else None,
            "delta_impegnato":   d(kc["impegnato"],kp["impegnato"]) if not parziale else None,
            "delta_perc_saving": round(kc["perc_saving"]-kp["perc_saving"],2) if kp["perc_saving"] and not parziale else None,
        })

    # KPI headline: solo periodi interi
    mesi_interi = set()
    for r in chart:
        if not r["parziale"] and r["ha_dati_curr"] and r["ha_dati_prev"]:
            for m in range(r["m_start"], r["m_end"]+1): mesi_interi.add(m)

    if mesi_interi:
        kc_hl = calc_kpi(df_c[df_c["mn"].isin(mesi_interi)] if not df_c.empty else pd.DataFrame())
        kp_hl = calc_kpi(df_p[df_p["mn"].isin(mesi_interi)] if not df_p.empty else pd.DataFrame())
    else:
        kc_hl, kp_hl = calc_kpi(df_c), calc_kpi(df_p)

    MESI_NM = {1:"gennaio",2:"febbraio",3:"marzo",4:"aprile",5:"maggio",6:"giugno",
               7:"luglio",8:"agosto",9:"settembre",10:"ottobre",11:"novembre",12:"dicembre"}
    mc = max(mesi_interi) if mesi_interi else mese_max
    label_curr = f"Gen–{MESI_IT.get(mc,'?')} {anno}"
    label_prev = f"Gen–{MESI_IT.get(mc,'?')} {ap}"

    nota = ""
    if mese_max and mese_max < 12:
        nota = f"Dati {anno} disponibili fino al {df_c['data_doc'].max().date() if not df_c.empty else '—'}."
        if ult_giorno < 20 and mese_max > 1:
            nota += f" {MESI_NM.get(mese_max,'').capitalize()} è parziale (dati fino al giorno {ult_giorno}) ed è escluso dal confronto KPI headline."

    def d(c,p): return round((c-p)/abs(p)*100,1) if p else None
    return {
        "anno": anno, "anno_precedente": ap,
        "granularita": granularita,
        "chart_data": chart,
        "kpi_headline": {
            "corrente": kc_hl, "precedente": kp_hl,
            "label_curr": label_curr, "label_prev": label_prev,
            "delta": {
                "listino":      d(kc_hl["listino"],   kp_hl["listino"]),
                "impegnato":    d(kc_hl["impegnato"],  kp_hl["impegnato"]),
                "saving":       d(kc_hl["saving"],    kp_hl["saving"]),
                "perc_saving":  round(kc_hl["perc_saving"]-kp_hl["perc_saving"],2) if kp_hl["perc_saving"] else None,
                "perc_negoziati": round(kc_hl["perc_negoziati"]-kp_hl["perc_negoziati"],2) if kp_hl["perc_negoziati"] else None,
            }
        },
        "nota": nota,
        "mese_max": mese_max,
        "ultimo_giorno": ult_giorno,
    }

@app.get("/kpi/saving/yoy-cdc")
def kpi_yoy_cdc(anno: int = Query(...)):
    ap = anno - 1
    cols = "cdc,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo"
    df_c = query_saving(anno, cols=cols)
    df_p = query_saving(ap,   cols=cols)
    def by_cdc(df):
        if df.empty: return {}
        return {c: calc_kpi(g) for c, g in df.groupby("cdc") if c}
    curr, prev = by_cdc(df_c), by_cdc(df_p)
    all_cdc = sorted(set(list(curr)+list(prev)))
    return [{
        "cdc": c,
        f"saving_{anno}":    curr.get(c,{}).get("saving",0),
        f"saving_{ap}":      prev.get(c,{}).get("saving",0),
        f"impegnato_{anno}": curr.get(c,{}).get("impegnato",0),
        f"impegnato_{ap}":   prev.get(c,{}).get("impegnato",0),
    } for c in all_cdc]

@app.get("/kpi/saving/mensile-con-area")
def kpi_mensile_area(anno: Optional[int] = Query(None), cdc: Optional[str] = Query(None)):
    df = query_saving(anno, cdc=cdc,
        cols="data_doc,str_ric,imp_listino_eur,imp_impegnato_eur,saving_eur,negoziazione,alfa_documento,accred_albo")
    if df.empty: return []
    df["mese"] = df["data_doc"].dt.strftime("%Y-%m")
    MLBL = {f"{anno}-{m:02d}": MESI_IT[m] for m in range(1,13)} if anno else {}
    result = []
    for mese, grp in df.groupby("mese"):
        kt = calc_kpi(grp)
        kr = calc_kpi(grp[grp["str_ric"]=="RICERCA"])
        ks = calc_kpi(grp[grp["str_ric"]=="STRUTTURA"])
        result.append({
            "mese": mese, "label": MLBL.get(mese, mese),
            **{f"tot_{k}":v for k,v in kt.items()},
            **{f"ric_{k}":v for k,v in kr.items()},
            **{f"str_{k}":v for k,v in ks.items()},
        })
    return sorted(result, key=lambda x: x["mese"])


# ── Tempi & NC KPI ───────────────────────────────────────────────
@app.get("/kpi/tempi/riepilogo")
def kpi_tempi():
    rows = get_sb().table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n = len(df)
    return {
        "avg_total_days":  round(float(df["total_days"].mean()),1),
        "avg_purchasing":  round(float(df["days_purchasing"].mean()),1),
        "avg_auto":        round(float(df["days_auto"].mean()),1),
        "avg_other":       round(float(df["days_other"].mean()),1),
        "n_ordini": n,
        "perc_bottleneck_purchasing": safe_pct(len(df[df["bottleneck"]=="PURCHASING"]),n),
        "perc_bottleneck_auto":       safe_pct(len(df[df["bottleneck"]=="AUTO"]),n),
    }

@app.get("/kpi/tempi/mensile")
def kpi_tempi_mensile():
    rows = get_sb().table("tempo_attraversamento").select("*").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    result = []
    for ym, g in df.groupby("year_month"):
        n = len(g)
        result.append({"mese":ym,
            "avg_total":      round(float(g["total_days"].mean()),1),
            "avg_purchasing": round(float(g["days_purchasing"].mean()),1),
            "avg_auto":       round(float(g["days_auto"].mean()),1),
            "n_ordini": n,
        })
    return sorted(result, key=lambda x: x["mese"])

@app.get("/kpi/tempi/distribuzione")
def kpi_tempi_dist():
    rows = get_sb().table("tempo_attraversamento").select("total_days").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    bins=[0,7,15,30,60,9999]; labels=["≤7 gg","8-15 gg","16-30 gg","31-60 gg",">60 gg"]
    df["f"] = pd.cut(df["total_days"],bins=bins,labels=labels,right=True)
    return [{"fascia":k,"n_ordini":int(v)} for k,v in df["f"].value_counts().reindex(labels).fillna(0).items()]

@app.get("/kpi/nc/riepilogo")
def kpi_nc():
    rows = get_sb().table("non_conformita").select("non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return {}
    n=len(df); nnc=int(df["non_conformita"].sum())
    return {"n_totale":n,"n_nc":nnc,"perc_nc":safe_pct(nnc,n),
        "avg_delta_giorni":round(float(df["delta_giorni"].mean()),1)}

@app.get("/kpi/nc/mensile")
def kpi_nc_mensile():
    rows = get_sb().table("non_conformita").select("data_origine,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    df["mese"] = pd.to_datetime(df["data_origine"],errors="coerce").dt.strftime("%Y-%m")
    df = df.dropna(subset=["mese"])
    result = []
    for m,g in df.groupby("mese"):
        n=len(g); nnc=int(g["non_conformita"].sum())
        result.append({"mese":m,"n_totale":n,"n_nc":nnc,"perc_nc":safe_pct(nnc,n),
            "avg_delta":round(float(g["delta_giorni"].mean()),1)})
    return sorted(result, key=lambda x: x["mese"])

@app.get("/kpi/nc/top-fornitori")
def kpi_nc_top(limit:int=Query(10)):
    rows = get_sb().table("non_conformita").select("ragione_sociale,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    grp = df.groupby("ragione_sociale").agg(n_totale=("non_conformita","count"),n_nc=("non_conformita","sum"),
        avg_delta=("delta_giorni","mean")).reset_index()
    grp["perc_nc"]=(grp["n_nc"]/grp["n_totale"]*100).round(2)
    return grp[grp["n_nc"]>0].nlargest(limit,"n_nc").to_dict(orient="records")

@app.get("/kpi/nc/per-tipo")
def kpi_nc_tipo():
    rows = get_sb().table("non_conformita").select("tipo_origine,non_conformita,delta_giorni").execute().data
    df = pd.DataFrame(rows)
    if df.empty: return []
    return [{"tipo":t,"n_totale":len(g),"n_nc":int(g["non_conformita"].sum()),
        "perc_nc":safe_pct(int(g["non_conformita"].sum()),len(g)),
        "avg_delta":round(float(g["delta_giorni"].mean()),1)}
        for t,g in df.groupby("tipo_origine")]


# ── Filtri disponibili ───────────────────────────────────────────
@app.get("/filtri/disponibili")
def filtri(anno: Optional[int] = Query(None)):
    q = get_sb().table("saving").select(
        "cdc,str_ric,alfa_documento,macro_categoria,prefisso_commessa,utente_presentazione,valuta")
    if anno: q = q.gte("data_doc",f"{anno}-01-01").lte("data_doc",f"{anno}-12-31")
    df = pd.DataFrame(q.execute().data)
    if df.empty: return {k:[] for k in ["cdc","str_ric","alfa_documento","macro_categoria","prefisso_commessa","utente","valuta"]}
    def uniq(c): return sorted(df[c].dropna().str.strip().unique().tolist()) if c in df.columns else []
    return {
        "cdc":               uniq("cdc"),
        "str_ric":           uniq("str_ric"),
        "alfa_documento":    uniq("alfa_documento"),
        "macro_categoria":   [x for x in uniq("macro_categoria") if x],
        "prefisso_commessa": uniq("prefisso_commessa"),
        "utente":            [x for x in uniq("utente_presentazione") if x],
        "valuta":            uniq("valuta"),
    }


# ── Export Excel multi-foglio ────────────────────────────────────
@app.post("/export/custom/excel")
def export_excel(body: dict = Body(...)):
    filtri = body.get("filtri", {})
    sezioni = body.get("sezioni", ["riepilogo","mensile","cdc","alfa_documento","top_fornitori"])
    anno = filtri.get("anno"); str_ric = filtri.get("str_ric"); cdc = filtri.get("cdc")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if "riepilogo" in sezioni:
            kpi = kpi_riepilogo(anno=anno, str_ric=str_ric, cdc=cdc)
            pd.DataFrame([
                ["Listino Totale €",   kpi["listino"],   "Prezzo di partenza senza negoziazione"],
                ["Impegnato Totale €", kpi["impegnato"], "Quanto paghiamo effettivamente"],
                ["Saving €",          kpi["saving"],    "Il risultato del nostro lavoro"],
                ["% Saving",          f"{kpi['perc_saving']}%", "Saving / Listino × 100"],
                ["N° Righe",          kpi["n_righe"],   ""],
                ["N° Negoziabili",    kpi["n_doc_neg"], ""],
                ["N° Negoziati",      kpi["n_negoziati"],""],
                ["% Negoziati",       f"{kpi['perc_negoziati']}%",""],
            ], columns=["KPI","Valore","Nota"]).to_excel(writer, index=False, sheet_name="KPI Riepilogo")

        if "mensile" in sezioni:
            data = kpi_mensile(anno=anno, str_ric=str_ric, cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer, index=False, sheet_name="Mensile")

        if "cdc" in sezioni:
            data = kpi_per_cdc(anno=anno, str_ric=str_ric)
            if data: pd.DataFrame(data).to_excel(writer, index=False, sheet_name="Per CDC")

        if "alfa_documento" in sezioni:
            data = kpi_per_alfa(anno=anno, str_ric=str_ric, cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer, index=False, sheet_name="Per Tipo Documento")

        if "macro_categoria" in sezioni:
            data = kpi_per_macro(anno=anno, str_ric=str_ric, cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer, index=False, sheet_name="Macro Categoria")

        if "top_fornitori" in sezioni:
            data = kpi_top_fornitori(anno=anno, str_ric=str_ric, cdc=cdc)
            if data: pd.DataFrame(data).to_excel(writer, index=False, sheet_name="Top Fornitori")

    buf.seek(0)
    fn = f"report_{anno or 'tutti'}.xlsx"
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fn}"})


# ── Upload log ───────────────────────────────────────────────────
@app.get("/upload/log")
def upload_log():
    return get_sb().table("upload_log").select("*").order("upload_date",desc=True).limit(50).execute().data

@app.delete("/upload/{upload_id}")
def delete_upload(upload_id: str):
    get_sb().table("upload_log").delete().eq("id", upload_id).execute()
    return {"status": "deleted"}

@app.get("/health")
def health():
    return {"status":"ok","version":"6.0.0",
        "kpi_logic":"listino=Imp.Iniziale€, impegnato=Imp.Negoziato€, saving=Saving.1"}
