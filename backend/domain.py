"""
domain.py — Funzioni pure, testabili, senza dipendenze FastAPI.
Tutta la logica di business e ingestion vive qui.
"""
import re
import pandas as pd
from typing import Optional, Tuple

# ─────────────────────────────────────────────────────────────────
# TYPE CONVERTERS — deterministici, mai crashano
# ─────────────────────────────────────────────────────────────────

def _f(v, d: float = 0.0) -> float:
    """Converte in float. NaN/None -> d."""
    try:
        return float(v) if pd.notna(v) else d
    except (TypeError, ValueError):
        return d

def _fn(v) -> Optional[float]:
    """Converte in float opzionale."""
    try:
        return float(v) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None

def _i(v) -> Optional[int]:
    """Converte in int opzionale."""
    try:
        return int(float(str(v))) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None

def _s(v) -> Optional[str]:
    """Converte in stringa pulita. None/nan/'' -> None."""
    try:
        s = str(v).strip() if pd.notna(v) else None
        return s if s and s.lower() not in ('nan', 'none', 'nat', '') else None
    except (TypeError, ValueError):
        return None

def _b(v) -> bool:
    """Converte SI/NO in bool. Case-insensitive."""
    if v is None:
        return False
    return str(v).strip().upper() in ('SI', 'SÌ', 'YES', 'TRUE', '1')

def _d(v) -> Optional[str]:
    """Converte in data ISO string. None se non valida o NaT."""
    try:
        ts = pd.to_datetime(v)
        if pd.isna(ts):
            return None
        return ts.date().isoformat()
    except (TypeError, ValueError):
        return None

def clean(v):
    """Serializza valore per Supabase JSON. NaN -> None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return bool(v)
    if isinstance(v, float):
        return None if v != v else round(v, 6)  # NaN check
    if isinstance(v, int):
        return int(v)
    s = str(v).strip()
    return s if s else None

def safe_pct(num: float, den: float) -> float:
    """% con divisione sicura."""
    try:
        return round(num / den * 100, 2) if den else 0.0
    except (TypeError, ZeroDivisionError):
        return 0.0

# ─────────────────────────────────────────────────────────────────
# COLUMN MAPPING — deterministic, priority-based
# ─────────────────────────────────────────────────────────────────

COL_MAP = [
    # Importi EUR — PRIORITÀ ASSOLUTA
    ('imp. iniziale €',                 'listino_eur'),
    ('imp. iniziale e',                 'listino_eur'),
    ('imp iniziale €',                  'listino_eur'),
    ('imp. negoziato €',                'impegnato_eur'),
    ('imp. negoziato e',                'impegnato_eur'),
    ('imp negoziato €',                 'impegnato_eur'),
    ('saving.1',                        'saving_eur'),
    ('%saving',                         'perc_saving_eur'),
    # Importi in valuta originale
    ('imp.iniziale',                    'listino_val'),
    ('imp.negoziato',                   'impegnato_val'),
    ('saving',                          'saving_val'),
    ('% saving',                        'perc_saving_val'),
    # Data
    ('data doc.',                       'data_doc'),
    ('data documento',                  'data_doc'),
    # Anagrafica
    ('cod.utente',                      'cod_utente'),
    ('utente per presentazione',        'utente_pres'),
    ('utente',                          'utente'),
    ('num.doc.',                        'num_doc'),
    ('alfa documento',                  'alfa_documento'),
    ('str./ric.',                       'str_ric'),
    ('stato dms',                       'stato_dms'),
    ('codice fornitore',                'codice_fornitore'),
    ('ragione sociale fornitore',       'ragione_sociale'),
    ('ragione sociale',                 'ragione_sociale'),
    ('accred.albo',                     'accred_albo'),
    ('protoc.ordine',                   'protoc_ordine'),
    ('protoc.commessa',                 'protoc_commessa'),
    ('protocollo commessa',             'protoc_commessa'),
    ('grp.merceol.',                    'grp_merceol'),
    ('descrizione gruppo merceologic',  'desc_merceol'),
    ('descrizione gruppo merceologico', 'desc_merceol'),
    ('centro di costo',                 'centro_costo'),
    ('descrizione centro di costo',     'desc_cdc'),
    ('macro categorie',                 'macro_cat'),
    ('macro categoria',                 'macro_cat'),
    ('negoziazione',                    'negoziazione'),
    ('valuta',                          'valuta'),
    ('cdc',                             'cdc'),
    ('cambio',                          'cambio'),
    ('tail spend',                      'tail_spend'),
]

def map_cols(columns) -> dict:
    """
    Mappa nomi colonne reali → tipo interno.
    Case-insensitive. Prima occorrenza vince.
    """
    norm = {c.strip().lower(): c for c in columns}
    result = {}
    for nome, tipo in COL_MAP:
        if nome in norm and tipo not in result:
            result[tipo] = norm[nome]
    return result

def gcol(m: dict, key: str, row) -> any:
    """Legge valore da riga pandas usando mapping."""
    cn = m.get(key)
    return row.get(cn) if cn else None

def validate_mapping(col: dict) -> dict:
    """
    Valida il mapping. Ritorna:
    {
      'valid': bool,
      'confidence': 'high'|'medium'|'low',
      'missing_critical': list[str],
      'missing_optional': list[str],
      'warnings': list[str],
    }
    """
    CRITICAL = ['data_doc', 'listino_eur', 'impegnato_eur', 'saving_eur',
                'ragione_sociale', 'alfa_documento', 'str_ric']
    OPTIONAL = ['cdc', 'negoziazione', 'accred_albo', 'macro_cat',
                'utente_pres', 'protoc_commessa', 'valuta', 'cambio']

    missing_critical = [f for f in CRITICAL if f not in col]
    missing_optional = [f for f in OPTIONAL if f not in col]
    warnings = []

    if 'accred_albo' not in col:
        warnings.append("Colonna 'Accred.albo' non trovata: analisi fornitori accreditati non disponibile")
    if 'macro_cat' not in col:
        warnings.append("Colonna 'macro categorie' non trovata: analisi per macro categoria non disponibile")
    if 'cdc' not in col and 'centro_costo' not in col:
        warnings.append("CDC non trovato: analisi per Centro di Costo non disponibile")

    if not missing_critical:
        confidence = 'high' if len(missing_optional) <= 2 else 'medium'
    else:
        confidence = 'low'

    return {
        'valid': len(missing_critical) == 0,
        'confidence': confidence,
        'missing_critical': missing_critical,
        'missing_optional': missing_optional,
        'warnings': warnings,
        'mapped_count': len(col),
    }

# ─────────────────────────────────────────────────────────────────
# SHEET DETECTION
# ─────────────────────────────────────────────────────────────────

def best_sheet(xl) -> str:
    """
    Trova il foglio Excel con più righe che contiene una colonna data.
    """
    best, best_n = xl.sheet_names[0], 0
    for s in xl.sheet_names:
        try:
            df = pd.read_excel(xl, sheet_name=s, nrows=3)
            df.columns = [c.strip() for c in df.columns]
            col = map_cols(df.columns)
            if 'data_doc' not in col:
                continue
            n = len(pd.read_excel(xl, sheet_name=s))
            if n > best_n:
                best_n, best = n, s
        except Exception:
            pass
    return best

# ─────────────────────────────────────────────────────────────────
# DOMAIN LOGIC
# ─────────────────────────────────────────────────────────────────

def derive_cdc(centro: str, desc: str) -> str:
    """Deriva il CDC (Business Unit) dal codice centro di costo."""
    c = str(centro or '').upper()
    d = str(desc or '').upper()
    if 'TIGEM' in d:                       return 'TIGEM'
    if 'TIGET' in d:                       return 'TIGET'
    if c.startswith(('RCRIIR', 'RCREER')): return 'GD'
    if c.startswith('STR'):               return 'STRUTTURA'
    return 'FT'

def parse_commessa(s: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Estrae prefisso e anno dalla commessa.
    'GMR24T2072/00053' -> ('GMR', '24')
    """
    if not s:
        return None, None
    s = str(s).strip()
    if len(s) < 3:
        return None, None
    pref = s[:3]
    anno = s[3:5] if len(s) >= 5 and s[3:5].isdigit() else None
    return pref, anno

# ─────────────────────────────────────────────────────────────────
# KPI CALCULATION — unica fonte di verità
# ─────────────────────────────────────────────────────────────────

DOC_NEG = frozenset({'OS', 'OSP', 'PS', 'OPR', 'ORN', 'ORD'})

def calc_kpi(df: pd.DataFrame) -> dict:
    """
    Calcola tutti i KPI dal DataFrame normalizzato.
    
    Definizioni:
      listino    = imp_listino_eur    (Imp. Iniziale € — prezzo di partenza)
      impegnato  = imp_impegnato_eur  (Imp. Negoziato € — quanto paghiamo)
      saving     = saving_eur         (Saving.1 — il nostro lavoro)
      % saving   = saving / listino × 100
    """
    if df is None or df.empty:
        return _empty_kpi()

    lst = float(df['imp_listino_eur'].fillna(0).sum())
    imp = float(df['imp_impegnato_eur'].fillna(0).sum())
    sav = float(df['saving_eur'].fillna(0).sum())
    n   = len(df)

    neg = int(df['alfa_documento'].isin(DOC_NEG).sum()) \
        if 'alfa_documento' in df.columns else 0
    nn  = int(df['negoziazione'].fillna(False).sum()) \
        if 'negoziazione' in df.columns else 0
    alb = int(df['accred_albo'].fillna(False).sum()) \
        if 'accred_albo' in df.columns else 0

    return dict(
        listino=round(lst, 2),
        impegnato=round(imp, 2),
        saving=round(sav, 2),
        perc_saving=safe_pct(sav, lst),
        n_righe=n,
        n_doc_neg=neg,
        n_negoziati=nn,
        perc_negoziati=safe_pct(nn, neg),
        n_albo=alb,
        perc_albo=safe_pct(alb, n),
    )

def _empty_kpi() -> dict:
    return dict(listino=0, impegnato=0, saving=0, perc_saving=0,
                n_righe=0, n_doc_neg=0, n_negoziati=0, perc_negoziati=0,
                n_albo=0, perc_albo=0)

def build_record(col: dict, row, upload_id: str, cdc_override: Optional[str] = None) -> dict:
    """
    Costruisce il record DB da una riga DataFrame.
    Centralizzato qui per essere testabile.
    """
    has_eur = 'listino_eur' in col and 'impegnato_eur' in col
    has_cdc = 'cdc' in col

    cambio = _f(gcol(col, 'cambio', row), 1.0) or 1.0
    valuta = _s(gcol(col, 'valuta', row)) or 'EURO'

    if has_eur:
        lst   = _f(gcol(col, 'listino_eur', row))
        imp   = _f(gcol(col, 'impegnato_eur', row))
        sav   = _f(gcol(col, 'saving_eur', row))
        pct_s = _f(gcol(col, 'perc_saving_eur', row))
    else:
        lst   = _f(gcol(col, 'listino_val', row)) * cambio
        imp   = _f(gcol(col, 'impegnato_val', row)) * cambio
        sav   = _f(gcol(col, 'saving_val', row)) * cambio
        pct_s = _f(gcol(col, 'perc_saving_val', row))

    # Ricalcola saving se mancante
    if sav == 0 and lst > 0 and imp > 0:
        sav = lst - imp
    if pct_s == 0 and lst > 0:
        pct_s = sav / lst * 100

    if cdc_override:
        cdc_val = cdc_override
    elif has_cdc:
        cdc_val = _s(gcol(col, 'cdc', row))
    else:
        cdc_val = derive_cdc(
            _s(gcol(col, 'centro_costo', row)) or '',
            _s(gcol(col, 'desc_cdc', row)) or ''
        )

    pc = _s(gcol(col, 'protoc_commessa', row))
    pref, anno_comm = parse_commessa(pc)

    r = {
        "upload_id":            upload_id,
        "cod_utente":           _i(gcol(col, 'cod_utente', row)),
        "utente":               _s(gcol(col, 'utente', row)),
        "utente_presentazione": _s(gcol(col, 'utente_pres', row)),
        "num_doc":              _i(gcol(col, 'num_doc', row)),
        "data_doc":             _d(gcol(col, 'data_doc', row)),
        "alfa_documento":       _s(gcol(col, 'alfa_documento', row)),
        "str_ric":              _s(gcol(col, 'str_ric', row)),
        "stato_dms":            _s(gcol(col, 'stato_dms', row)),
        "codice_fornitore":     _i(gcol(col, 'codice_fornitore', row)),
        "ragione_sociale":      _s(gcol(col, 'ragione_sociale', row)),
        "accred_albo":          _b(gcol(col, 'accred_albo', row)),
        "protoc_ordine":        _fn(gcol(col, 'protoc_ordine', row)),
        "protoc_commessa":      pc,
        "prefisso_commessa":    pref,
        "anno_commessa":        anno_comm,
        "desc_commessa":        _s(gcol(col, 'desc_cdc', row)),
        "grp_merceol":          _s(gcol(col, 'grp_merceol', row)),
        "desc_gruppo_merceol":  _s(gcol(col, 'desc_merceol', row)),
        "macro_categoria":      _s(gcol(col, 'macro_cat', row)),
        "centro_di_costo":      _s(gcol(col, 'centro_costo', row)),
        "desc_cdc":             _s(gcol(col, 'desc_cdc', row)),
        "cdc":                  cdc_val,
        "valuta":               valuta,
        "cambio":               cambio,
        "imp_listino_eur":      lst,
        "imp_impegnato_eur":    imp,
        "saving_eur":           sav,
        "perc_saving_eur":      pct_s,
        "imp_iniziale":         _f(gcol(col, 'listino_val', row)),
        "imp_negoziato":        _f(gcol(col, 'impegnato_val', row)),
        "saving_val":           _f(gcol(col, 'saving_val', row)),
        "perc_saving":          _f(gcol(col, 'perc_saving_val', row)),
        "negoziazione":         _b(gcol(col, 'negoziazione', row)),
        "tail_spend":           _s(gcol(col, 'tail_spend', row)),
    }
    return {k: clean(v) for k, v in r.items()}
