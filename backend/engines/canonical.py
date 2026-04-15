"""
engines/canonical.py — Motore dati canonico UA Dashboard v2.1
Fondazione Telethon ETS

REGOLE VERIFICATE SUI FILE REALI:
  2025: Z=Imp.Iniziale€  AA=Imp.Negoziato€  AB=Saving.1  AC=%saving  AE=CDC
        Cambio già applicato (col AF). Saving.1 = saving EUR corretto (€15.8M)
        Valute originali: USD/GBP/JPY/CHF presenti → mantenute per esposizione valutaria
  2026: Imp.iniziale / Imp.negoziato / Saving — tutti in EUR (valuta=EURO)
        CDC: derive da Descrizione centro di costo

REGOLA VALUTA:
  - Analytics EUR: sempre usa colonne EUR (con cambio applicato)
  - Esposizione valutaria: mantieni Imp.iniziale/Imp.negoziato/Saving (valuta originale)
  - Futuro: se file futuro ha valuta non EUR → moltiplica per cambio → EUR
"""
import pandas as pd
import numpy as np
from typing import Optional
import warnings; warnings.filterwarnings('ignore')

# ── TEAM ─────────────────────────────────────────────────────────────────────
USERNAME_MAP = {
    "spepe":"Stefano Pepe","mpepe":"Stefano Pepe","s.pepe":"Stefano Pepe",
    "fdiclemente":"Francesco Di Clemente","f.diclemente":"Francesco Di Clemente",
    "lmonti":"Luca Monti","l.monti":"Luca Monti","LMonti":"Luca Monti",
    "sruotolo":"Silvana Ruotolo","s.ruotolo":"Silvana Ruotolo",
    "lveneruso":"Luisa Veneruso","l.veneruso":"Luisa Veneruso",
    "mpadricelli":"Marina Padricelli","m.padricelli":"Marina Padricelli",
    "fperazzetti":"Francesca Perazzetti","f.perazzetti":"Francesca Perazzetti",
    "kleonardi":"Katuscia Leonardi","k.leonardi":"Katuscia Leonardi",
    "lscialanga":"Loredana Scialanga","l.scialanga":"Loredana Scialanga",
    "mdimatteo":"Mariacarla Di Matteo","mdi_matteo":"Mariacarla Di Matteo","m.dimatteo":"Mariacarla Di Matteo",
    "mcatalano":"Matteo Catalano","m.catalano":"Matteo Catalano",
    "mnotarstefano":"Maria Dora Notarstefano","mdnotarstefano":"Maria Dora Notarstefano",
}
TEAM_UA = set(USERNAME_MAP.values())
AREA_MAP = {
    "Luca Monti":"RICERCA","Silvana Ruotolo":"RICERCA","Luisa Veneruso":"RICERCA",
    "Marina Padricelli":"RICERCA","Stefano Pepe":"RICERCA",
    "Francesca Perazzetti":"STRUTTURA","Katuscia Leonardi":"STRUTTURA","Francesco Di Clemente":"STRUTTURA",
}
MANAGER_OF = {
    "Luca Monti":"Stefano Pepe","Silvana Ruotolo":"Stefano Pepe",
    "Luisa Veneruso":"Stefano Pepe","Marina Padricelli":"Stefano Pepe",
    "Francesca Perazzetti":"Francesco Di Clemente","Katuscia Leonardi":"Francesco Di Clemente",
}

def normalize_username(raw) -> Optional[str]:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)): return None
    s = str(raw).strip()
    if s in USERNAME_MAP: return USERNAME_MAP[s]
    key = s.lower().replace('.','').replace(' ','').replace('_','')
    for k, v in USERNAME_MAP.items():
        if k.lower().replace('.','').replace(' ','').replace('_','') == key: return v
    return None

_NOISE = {'', 'nan', 'none', 'ordini diretti ricerca', 'ordini diretti ', 'ordini diretti'}
def normalize_buyer_name(raw) -> Optional[str]:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)): return None
    s = str(raw).strip()
    if s.lower() in _NOISE: return None
    if s in TEAM_UA: return s
    parts = s.split()
    if len(parts) == 2:
        inv = f"{parts[1]} {parts[0]}"
        if inv in TEAM_UA: return inv
    for m in TEAM_UA:
        if s.lower() == m.lower(): return m
    return s  # nome non UA — restituisce comunque

# ── CDC ───────────────────────────────────────────────────────────────────────
def derive_cdc(centro: str = '', desc: str = '') -> str:
    c, d = str(centro or '').upper(), str(desc or '').upper()
    if 'TIGEM' in d or 'TIGEM' in c: return 'TIGEM'
    if 'TIGET' in d or 'TIGET' in c: return 'TIGET'
    if 'GESTIONE DIRETTA' in d or c.startswith(('RCRIIR','RCREER')): return 'GD'
    if any(x in d for x in ('WELFARE','LOGISTIC')) or c.startswith('STR'): return 'STRUTTURA'
    return 'FT'

# ── CODICI DOCUMENTO ──────────────────────────────────────────────────────────
DOC_LABELS = {
    'ORN':'Ordine Ricerca','ORN01':'Ordine Ricerca',
    'ORD':'Ordine Diretto Ricerca','ORD01':'Ordine Diretto Ricerca','COR-ORD':'Correzione Ordine',
    'OPR':'Ordine Prev. Ricerca','OPR01':'Ordine Prev. Ricerca',
    'PS':'Procedura Straordinaria','PS006':'Procedura Straordinaria',
    'OS':'Ordine Struttura','OS001':'Ordine Struttura',
    'OSD':'Ordine Diretto Struttura','OSP':'Ordine Prev. Struttura','OSP01':'Ordine Prev. Struttura',
    'OSDP01':'Ordine Diretto Struttura','RA501':'Richiesta Acquisto','RAP501':'Richiesta Acquisto Prev.',
    'DTR01':'Documento Trasporto','DT000':'Documento Trasporto','DDT':'Doc. Trasporto','PV001':'Preventivo',
}
DOC_NEGOTIABLE = frozenset({'OS','OS001','OSP','OSP01','PS','PS006','OPR','OPR01','ORN','ORN01','ORD','ORD01','OSDP01'})
DOC_LOGISTICS  = frozenset({'DTR01','DT000','DDT'})
DOC_ORDER      = frozenset({'ORN','ORN01','ORD','ORD01','OPR','OPR01','PS','PS006','OS','OS001','OSD','OSP','OSP01','OSDP01','COR-ORD'})

def doc_label(code: str) -> str:
    return DOC_LABELS.get(str(code).strip(), str(code))

# ── SAVING NORMALIZER — REGOLE VERIFICATE ────────────────────────────────────
def normalize_saving(df: pd.DataFrame, anno: int) -> pd.DataFrame:
    """
    Normalizza file saving.
    
    FILE 2025 (colonne EUR Z-AC, CDC col AE):
      listino_eur  = Imp. Iniziale €   (col Z)
      impegnato_eur = Imp. Negoziato €  (col AA)
      saving_eur   = Saving.1          (col AB) ← saving reale EUR con cambio applicato
      perc_saving  = %saving           (col AC) * 100
      cdc          = CDC               (col AE) ← colonna diretta, NON derivata
      cambio già applicato in Saving.1

    FILE 2026 (no colonne EUR, tutti EURO):
      listino_eur  = Imp.iniziale  × cambio (default 1, tutti EURO)
      impegnato_eur= Imp.negoziato × cambio
      saving_eur   = Saving        × cambio (ricalcolato)
      cdc          = derive da Descrizione centro di costo

    VALORI IN VALUTA ORIGINALE (per esposizione valutaria):
      listino_val, impegnato_val, saving_val = colonne non-EUR
    """
    out = pd.DataFrame()
    out['anno'] = anno
    out['data_doc'] = pd.to_datetime(df.get('Data doc.'), errors='coerce')
    out['mese']      = out['data_doc'].dt.month
    out['mese_label']= out['data_doc'].dt.strftime('%Y-%m')

    out['alfa_documento']  = df.get('Alfa documento', pd.Series(dtype=str)).fillna('').str.strip()
    out['doc_label']       = out['alfa_documento'].apply(doc_label)
    out['is_negotiable']   = out['alfa_documento'].apply(lambda x: x in DOC_NEGOTIABLE)
    out['str_ric']         = df.get('Str./Ric.', pd.Series(dtype=str)).fillna('').str.strip().str.upper()
    out['fornitore']       = df.get('Ragione sociale fornitore', pd.Series(dtype=str)).fillna('').str.strip()
    out['codice_fornitore']= df.get('Codice fornitore', pd.Series(dtype=object))
    out['accred_albo']     = df.get('Accred.albo', pd.Series(dtype=str)).fillna('NO').str.strip().str.upper().eq('SI')
    out['valuta']          = df.get('Valuta', pd.Series(['EURO']*len(df))).fillna('EURO').str.strip()

    # ── IMPORTI EUR ────────────────────────────────────────────────
    has_eur = ('Imp. Iniziale €' in df.columns and 'Imp. Negoziato €' in df.columns
               and 'Saving.1' in df.columns)

    if has_eur:
        # 2025: usa direttamente le colonne EUR normalizzate (cambio già applicato)
        out['listino_eur']    = pd.to_numeric(df['Imp. Iniziale €'], errors='coerce').fillna(0)
        out['impegnato_eur']  = pd.to_numeric(df['Imp. Negoziato €'], errors='coerce').fillna(0)
        out['saving_eur']     = pd.to_numeric(df['Saving.1'], errors='coerce').fillna(0)
        pct = pd.to_numeric(df.get('%saving', pd.Series([0]*len(df))), errors='coerce').fillna(0)
        out['perc_saving']    = (pct * 100).where(pct <= 1, pct)  # gestisce sia 0.05 che 5.0
        # Valori in valuta originale
        out['listino_val']    = pd.to_numeric(df.get('Imp.iniziale', df['Imp. Iniziale €']), errors='coerce').fillna(0)
        out['impegnato_val']  = pd.to_numeric(df.get('Imp.negoziato', df['Imp. Negoziato €']), errors='coerce').fillna(0)
        out['saving_val']     = pd.to_numeric(df.get('Saving', df['Saving.1']), errors='coerce').fillna(0)
    else:
        # 2026 o altri: valuta locale * cambio → EUR
        cambio_raw = pd.to_numeric(df.get('cambio', pd.Series([1.0]*len(df))), errors='coerce').fillna(1.0)
        cambio = cambio_raw.where(cambio_raw > 0, 1.0)
        lst_v  = pd.to_numeric(df.get('Imp.iniziale', 0), errors='coerce').fillna(0)
        imp_v  = pd.to_numeric(df.get('Imp.negoziato', 0), errors='coerce').fillna(0)
        sav_v  = pd.to_numeric(df.get('Saving', 0), errors='coerce').fillna(0)
        out['listino_val']   = lst_v
        out['impegnato_val'] = imp_v
        out['saving_val']    = sav_v
        out['listino_eur']   = lst_v * cambio
        out['impegnato_eur'] = imp_v * cambio
        out['saving_eur']    = sav_v * cambio
        out['perc_saving']   = 0.0

    # Ricalcola saving dove anomalo (diff > €1) per garantire coerenza
    recalc = (out['listino_eur'] - out['impegnato_eur']).round(4)
    delta_abs = (out['saving_eur'] - recalc).abs()
    out['saving_eur'] = out['saving_eur'].where(delta_abs < 1.0, recalc)
    out['perc_saving'] = (out['saving_eur'] / out['listino_eur'].replace(0, np.nan) * 100).fillna(0).round(4)

    # ── CDC ────────────────────────────────────────────────────────
    # 2025: colonna diretta 'CDC ' (con spazio finale)
    # 2026: deriva da descrizione
    if 'CDC ' in df.columns:
        out['cdc'] = df['CDC '].fillna('').str.strip().replace('', 'FT')
    elif 'CDC' in df.columns:
        out['cdc'] = df['CDC'].fillna('').str.strip().replace('', 'FT')
    else:
        centro = df.get('Centro di costo', pd.Series([''] * len(df))).fillna('')
        desc   = df.get('Descrizione centro di costo', pd.Series([''] * len(df))).fillna('')
        out['cdc'] = [derive_cdc(c, d) for c, d in zip(centro, desc)]

    out['centro_costo'] = df.get('Centro di costo', pd.Series(dtype=str)).fillna('').str.strip()
    out['desc_cdc']     = df.get('Descrizione centro di costo', pd.Series(dtype=str)).fillna('').str.strip()

    # ── BUYER ──────────────────────────────────────────────────────
    if 'utente per presentazione ' in df.columns:
        raw_b = df['utente per presentazione '].fillna('').str.strip()
    elif 'Utente' in df.columns:
        raw_b = df['Utente'].fillna('').str.strip()
    else:
        raw_b = pd.Series([''] * len(df))
    out['buyer_raw']   = raw_b
    out['buyer']       = raw_b.apply(normalize_buyer_name)
    out['is_ua_buyer'] = out['buyer'].apply(lambda x: x in TEAM_UA if x else False)
    out['buyer_area']  = out['buyer'].map(AREA_MAP)

    out['negoziazione']    = df.get('Negoziazione', pd.Series(dtype=str)).fillna('NO').str.strip().str.upper().eq('SI')
    out['protoc_ordine']   = df.get('Protoc.ordine', pd.Series(dtype=object))
    out['protoc_commessa'] = df.get('Protoc.commessa', pd.Series(dtype=object))
    out['grp_merceol']     = df.get('Grp.merceol.', pd.Series(dtype=str)).fillna('').str.strip()
    out['desc_gruppo']     = df.get('Descrizione gruppo merceologic', pd.Series(dtype=str)).fillna('').str.strip()
    mc = df.get('macro categorie ', df.get('macro_categorie', pd.Series(dtype=str)))
    out['macro_cat'] = (mc if mc is not None else pd.Series([''] * len(df))).fillna('').str.strip()
    out['stato_dms']       = df.get('Stato DMS', pd.Series(dtype=str)).fillna('').str.strip()

    return out


# ── VIS DETTAGLIATA NORMALIZER ────────────────────────────────────────────────
def normalize_vis(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizza vis_dettagliata. REGOLA CRITICA: tot_documento NON si somma."""
    out = pd.DataFrame()
    out['cod_documento']   = df.get('Cod. documento', pd.Series(dtype=str)).fillna('').str.strip()
    out['doc_label']       = out['cod_documento'].apply(doc_label)
    out['is_order']        = out['cod_documento'].apply(lambda x: x in DOC_ORDER)
    out['is_logistics']    = out['cod_documento'].apply(lambda x: x in DOC_LOGISTICS)
    out['nr_doc']          = df.get('Nr.doc.', pd.Series(dtype=object))
    out['data_doc']        = pd.to_datetime(df.get('Data doc.'), errors='coerce')
    out['anno']            = out['data_doc'].dt.year
    out['mese_label']      = out['data_doc'].dt.strftime('%Y-%m')
    out['fornitore']       = df.get('Ragione sociale anagrafica', pd.Series(dtype=str)).fillna('').str.strip()
    out['codice_fornitore']= df.get('Cli./For.', pd.Series(dtype=object))
    # CRITICO: tot_documento NON si somma (replicato su ogni riga ordine)
    out['tot_documento']   = pd.to_numeric(df.get('Tot. documento', 0), errors='coerce').fillna(0)
    # importo_riga SI si somma
    out['importo_riga']    = pd.to_numeric(df.get('Importo riga', 0), errors='coerce').fillna(0)
    out['stato_doc']       = df.get('Stato doc.', pd.Series(dtype=str)).fillna('').str.strip()
    out['stato_evasione']  = df.get('Stato evasione doc.', pd.Series(dtype=str)).fillna('').str.strip()
    out['protoc_ordine']   = df.get('Protocollo Ordine', pd.Series(dtype=object))
    out['protoc_commessa'] = df.get('Protocollo Commessa', pd.Series(dtype=object))
    out['progetto']        = df.get('Progetto testata', pd.Series(dtype=str)).fillna('').str.strip()
    out['commessa']        = df.get('Commessa testata', pd.Series(dtype=str)).fillna('').str.strip()
    out['utente_ins_raw']  = df.get('Utente Ins.', pd.Series(dtype=str)).fillna('').str.strip()
    out['utente_ins']      = out['utente_ins_raw'].apply(normalize_username)
    out['is_ua']           = out['utente_ins'].apply(lambda x: x in TEAM_UA if x else False)
    out['categoria']       = df.get('Categoria Merceologica', pd.Series(dtype=str)).fillna('').str.strip()
    out['famiglia']        = df.get('Descrizione Fam.', pd.Series(dtype=str)).fillna('').str.strip().str.replace('\n',' ')
    out['cod_famiglia']    = df.get('Fam.', pd.Series(dtype=str)).fillna('').str.strip()
    out['sottofamiglia']   = df.get('Descrizione Sfam.', pd.Series(dtype=str)).fillna('').str.strip()
    out['cod_articolo']    = df.get('Cod. articolo', pd.Series(dtype=str)).fillna('').str.strip()
    out['desc_articolo']   = df.get('Descrizione articolo', pd.Series(dtype=str)).fillna('').str.strip()
    out['quantita']        = pd.to_numeric(df.get('Qta 1 doc.', 0), errors='coerce').fillna(0)
    out['prezzo_unitario'] = pd.to_numeric(df.get('Prezzo 1', 0), errors='coerce').fillna(0)
    out['filtro_cdc_raw']  = df.get('FILTRO PER CDC', pd.Series(dtype=str)).fillna('').str.strip()
    out['cdc']             = out['filtro_cdc_raw'].apply(lambda x: derive_cdc(x, ''))
    out['valuta']          = df.get('Valuta', pd.Series(['EURO']*len(df))).fillna('EURO').str.strip()
    return out


# ── KPI ENGINE ────────────────────────────────────────────────────────────────
def calc_kpi(df: pd.DataFrame) -> dict:
    """Unica fonte di verità per tutti i KPI."""
    if df is None or df.empty:
        return dict(listino=0,impegnato=0,saving=0,perc_saving=0,
                    n_righe=0,n_negotiable=0,n_negoziati=0,perc_negoziati=0,n_albo=0,perc_albo=0)
    def _s(c): return float(df[c].fillna(0).sum()) if c in df.columns else 0.0
    def _i(c): return int(df[c].fillna(False).sum()) if c in df.columns else 0
    def _p(a, b): return round(a/b*100, 2) if b else 0.0
    lst = _s('listino_eur'); imp = _s('impegnato_eur'); sav = _s('saving_eur'); n = len(df)
    neg = _i('is_negotiable'); nn = _i('negoziazione'); alb = _i('accred_albo')
    return dict(
        listino=round(lst,2), impegnato=round(imp,2), saving=round(sav,2),
        perc_saving=_p(sav,lst), n_righe=n, n_negotiable=neg,
        n_negoziati=nn, perc_negoziati=_p(nn,neg), n_albo=alb, perc_albo=_p(alb,n),
    )
