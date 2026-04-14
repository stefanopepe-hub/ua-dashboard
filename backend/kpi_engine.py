"""
kpi_engine.py — Motore KPI centrale e deterministico
Fondazione Telethon ETS — UA Dashboard Enterprise

REGOLE:
  listino    = imp_listino_eur   (Imp. Iniziale €)
  impegnato  = imp_impegnato_eur (Imp. Negoziato €)
  saving     = saving_eur        (Saving.1)
  % saving   = saving / listino * 100

Un solo posto dove i KPI sono definiti.
Nessuna pagina calcola KPI diversamente.
"""
from __future__ import annotations
import pandas as pd
from typing import Optional
from document_engine import NEGOTIABLE_ORDER_CODES, LOGISTICS_CODES
from spend_engine import classify_spend_label
from team_engine import normalize_name, get_manager

# ── Tipi KPI ──────────────────────────────────────────────────────────────────

def empty_kpi() -> dict:
    return dict(
        listino=0.0, impegnato=0.0, saving=0.0, perc_saving=0.0,
        n_righe=0, n_doc_neg=0, n_negoziati=0, perc_negoziati=0.0,
        n_albo=0, perc_albo=0.0,
    )


def safe_pct(num: float, den: float) -> float:
    try:
        return round(num / den * 100, 2) if den else 0.0
    except Exception:
        return 0.0


def calc_kpi(df: pd.DataFrame) -> dict:
    """
    KPI su DataFrame normalizzato. Unica fonte di verità.

    Inclusione:
      - Tutti i documenti (ordini + DDT) per n_righe
      - Solo documenti con saving > 0 per perc_saving
      - Documenti negoziabili (NEGOTIABLE_ORDER_CODES) per n_doc_neg
    Esclusione esplicita:
      - Nessuna esclusione automatica (caller filtra se necessario)
    """
    if df is None or df.empty:
        return empty_kpi()

    lst = float(df['imp_listino_eur'].fillna(0).sum())
    imp = float(df['imp_impegnato_eur'].fillna(0).sum())
    sav = float(df['saving_eur'].fillna(0).sum())
    n   = len(df)

    neg = int(df['alfa_documento'].isin(NEGOTIABLE_ORDER_CODES).sum()) \
        if 'alfa_documento' in df.columns else 0
    nn  = int(df['negoziazione'].fillna(False).sum()) \
        if 'negoziazione' in df.columns else 0
    alb = int(df['accred_albo'].fillna(False).sum()) \
        if 'accred_albo' in df.columns else 0

    return dict(
        listino       = round(lst, 2),
        impegnato     = round(imp, 2),
        saving        = round(sav, 2),
        perc_saving   = safe_pct(sav, lst),
        n_righe       = n,
        n_doc_neg     = neg,
        n_negoziati   = nn,
        perc_negoziati= safe_pct(nn, neg),
        n_albo        = alb,
        perc_albo     = safe_pct(alb, n),
    )


# ── Analisi per dimensione ────────────────────────────────────────────────────

def kpi_by_dimension(df: pd.DataFrame, dim_col: str, label_col: Optional[str] = None) -> list[dict]:
    """KPI aggregati per una dimensione generica."""
    if df is None or df.empty or dim_col not in df.columns:
        return []
    result = []
    for val, grp in df.groupby(dim_col):
        if not val or str(val).strip() in ('', 'nan', 'None'):
            continue
        k = calc_kpi(grp)
        k[dim_col] = val
        if label_col and label_col in grp.columns:
            k[label_col] = grp[label_col].dropna().mode().iloc[0] if not grp[label_col].dropna().empty else None
        result.append(k)
    return sorted(result, key=lambda x: x.get('saving', 0), reverse=True)


def kpi_by_buyer(df: pd.DataFrame) -> list[dict]:
    """KPI per buyer con normalizzazione team_engine."""
    if df is None or df.empty:
        return []
    df = df.copy()
    # usa utente_presentazione, fallback su utente
    df['_buyer_raw'] = df.get('utente_presentazione', pd.Series(dtype=str)).fillna(
        df.get('utente', pd.Series(dtype=str))
    )
    df['_buyer'] = df['_buyer_raw'].apply(normalize_name)
    df = df[df['_buyer'].notna()]
    result = []
    for buyer, grp in df.groupby('_buyer'):
        k = calc_kpi(grp)
        k['utente'] = buyer
        k['manager'] = get_manager(buyer)
        result.append(k)
    return sorted(result, key=lambda x: x.get('saving', 0), reverse=True)


def kpi_by_spend_bucket(df: pd.DataFrame) -> list[dict]:
    """KPI per bucket di spesa (MATERIALI, SERVIZI, STRUMENTAZIONE)."""
    if df is None or df.empty:
        return []
    df = df.copy()
    df['_bucket'] = df.apply(
        lambda r: classify_spend_label(
            r.get('macro_categoria'), r.get('desc_gruppo_merceol')
        ), axis=1
    )
    result = []
    for bucket, grp in df.groupby('_bucket'):
        k = calc_kpi(grp)
        k['bucket'] = bucket
        result.append(k)
    return sorted(result, key=lambda x: x.get('impegnato', 0), reverse=True)


def kpi_pareto(df: pd.DataFrame) -> list[dict]:
    """Curva Pareto fornitori per impegnato."""
    if df is None or df.empty or 'ragione_sociale' not in df.columns:
        return []
    grp = (
        df.groupby('ragione_sociale')['imp_impegnato_eur']
          .sum()
          .sort_values(ascending=False)
          .reset_index()
    )
    total = grp['imp_impegnato_eur'].sum()
    if total == 0:
        return []
    grp['cum_perc'] = (grp['imp_impegnato_eur'].cumsum() / total * 100).round(2)
    grp['rank'] = range(1, len(grp) + 1)
    return grp.to_dict(orient='records')


def kpi_top_suppliers(df: pd.DataFrame, by: str = 'saving', limit: int = 20) -> list[dict]:
    """Top N fornitori per saving o impegnato."""
    if df is None or df.empty:
        return []
    result = []
    for forn, grp in df.groupby('ragione_sociale'):
        if not forn or str(forn).strip() in ('', 'nan', 'None'):
            continue
        k = calc_kpi(grp)
        k['ragione_sociale'] = forn
        k['albo'] = bool(grp['accred_albo'].mode().iloc[0]) if 'accred_albo' in grp.columns and not grp.empty else False
        result.append(k)
    sort_key = by if by in ('saving', 'impegnato', 'listino') else 'saving'
    return sorted(result, key=lambda x: x.get(sort_key, 0), reverse=True)[:limit]


def kpi_concentration(df: pd.DataFrame) -> dict:
    """Indice di concentrazione HHI e quote cumulata."""
    if df is None or df.empty:
        return {}
    total = float(df['imp_impegnato_eur'].fillna(0).sum())
    if total == 0:
        return {}
    grp = (
        df.groupby('ragione_sociale')['imp_impegnato_eur']
          .sum()
          .sort_values(ascending=False)
          .reset_index()
    )
    grp['share'] = (grp['imp_impegnato_eur'] / total * 100).round(2)
    n = len(grp)
    def cumshare(k): return round(float(grp.head(k)['share'].sum()), 2) if k <= n else 100.0
    hhi = round(float((grp['share'] ** 2).sum()), 1)
    return {
        'n_fornitori_totali': n,
        'total_impegnato':    round(total, 2),
        'share_top_5':        cumshare(5),
        'share_top_10':       cumshare(10),
        'share_top_20':       cumshare(20),
        'hhi':                hhi,
        'hhi_interpretation': (
            'Mercato molto concentrato'        if hhi > 2500 else
            'Mercato concentrato'              if hhi > 1500 else
            'Mercato moderatamente concentrato'if hhi > 1000 else
            'Mercato non concentrato'
        ),
        'top_5': grp.head(5)[['ragione_sociale','imp_impegnato_eur','share']].to_dict(orient='records'),
    }
