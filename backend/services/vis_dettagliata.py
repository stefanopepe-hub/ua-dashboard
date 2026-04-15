"""
services/vis_dettagliata.py — Vis Dettagliata service layer
Processa il formato export Zucchetti "visualizzazione dettagliata" (71 colonne).
Nessuna logica HTTP qui: solo lettura dati e aggregazioni KPI.
"""
import logging
import io
from typing import Optional

import pandas as pd

log = logging.getLogger("ua.vis_dettagliata")

# ── Costanti colonne (indici 0-based) ─────────────────────────────
COL_COD_DOC       = 0   # Cod. documento  (DT000, …)
COL_DATA_DOC      = 3   # Data doc.
COL_CLI_FOR       = 4   # Cli./For.       (codice fornitore)
COL_RAG_SOC       = 5   # Ragione sociale anagrafica
COL_TOT_DOC       = 6   # Tot. documento  (EUR)
COL_STATO_DOC     = 7   # Stato doc.      (Fatturato, Stampato, …)
COL_VALUTA        = 17  # Valuta          (EURO, USD, GBP, …)
COL_TOT_DOC_VAL   = 19  # Tot. documento val. (valuta originale)
COL_IMP_RIGA      = 40  # Importo riga    (EUR)
COL_IMP_RIGA_VAL  = 47  # Importo riga val. (valuta originale)
COL_FILTRO_CDC    = 60  # FILTRO PER CDC  (codice breve, es. STSRSR000000501)
COL_DATA_INI      = 61  # Data inizio competenza
COL_DATA_FIN      = 62  # Data fine competenza
COL_CDC_LUNGO     = 15  # Centro di costo (codice lungo)

EXPECTED_NCOLS = 71
SHEET_NAME     = "Sheet1"


# ── Lettura raw ───────────────────────────────────────────────────

def read_vis_dettagliata(file_bytes: bytes) -> pd.DataFrame:
    """
    Legge il file Excel vis_dettagliata.
    Riga 0 = header, dati da riga 1 in poi.

    Raises:
        ValueError: sheet non trovato o numero colonne errato.
    """
    try:
        df = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=SHEET_NAME,
            header=0,
            dtype=str,
        )
    except Exception as exc:
        # Prova comunque con il primo sheet se "Sheet1" non esiste
        try:
            df = pd.read_excel(
                io.BytesIO(file_bytes),
                sheet_name=0,
                header=0,
                dtype=str,
            )
        except Exception:
            raise ValueError(
                f"Impossibile leggere il file Excel: {exc}"
            ) from exc

    if df.shape[1] < EXPECTED_NCOLS:
        raise ValueError(
            f"Formato non riconosciuto: attese {EXPECTED_NCOLS} colonne, "
            f"trovate {df.shape[1]}. "
            "Carica un export Zucchetti vis_dettagliata completo."
        )

    return df


# ── Normalizzazione ───────────────────────────────────────────────

def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converti tipi, pulisce valori nulli, rinomina con alias leggibili.
    Opera su una copia per non modificare il df originale.
    """
    df = df.copy()

    # Estrai colonne per indice e dai nomi semantici
    col_names = list(df.columns)

    def col(idx: int) -> str:
        return col_names[idx]

    df["_cod_doc"]      = df[col(COL_COD_DOC)].astype(str).str.strip()
    df["_data_doc"]     = pd.to_datetime(df[col(COL_DATA_DOC)], dayfirst=True, errors="coerce")
    df["_cli_for"]      = df[col(COL_CLI_FOR)].astype(str).str.strip()
    df["_rag_soc"]      = df[col(COL_RAG_SOC)].astype(str).str.strip()
    df["_tot_doc_eur"]  = pd.to_numeric(df[col(COL_TOT_DOC)], errors="coerce").fillna(0.0)
    df["_stato"]        = df[col(COL_STATO_DOC)].astype(str).str.strip()
    df["_valuta"]       = df[col(COL_VALUTA)].astype(str).str.strip()
    df["_tot_doc_val"]  = pd.to_numeric(df[col(COL_TOT_DOC_VAL)], errors="coerce").fillna(0.0)
    df["_imp_riga"]     = pd.to_numeric(df[col(COL_IMP_RIGA)], errors="coerce").fillna(0.0)
    df["_imp_riga_val"] = pd.to_numeric(df[col(COL_IMP_RIGA_VAL)], errors="coerce").fillna(0.0)
    df["_cdc"]          = df[col(COL_FILTRO_CDC)].astype(str).str.strip()
    df["_data_ini"]     = pd.to_datetime(df[col(COL_DATA_INI)], dayfirst=True, errors="coerce")
    df["_data_fin"]     = pd.to_datetime(df[col(COL_DATA_FIN)], dayfirst=True, errors="coerce")
    df["_cdc_lungo"]    = df[col(COL_CDC_LUNGO)].astype(str).str.strip()

    # Normalizza "EURO" → "EUR" per coerenza
    df["_valuta"] = df["_valuta"].replace("EURO", "EUR")

    # Scarta righe completamente vuote (artefatti export)
    df = df.dropna(subset=["_data_doc", "_rag_soc"], how="all")
    df = df[df["_rag_soc"].notna() & (df["_rag_soc"] != "") & (df["_rag_soc"] != "nan")]

    return df


# ── Aggregazioni ──────────────────────────────────────────────────

def _agg_per_valuta(df: pd.DataFrame) -> list:
    """Spesa aggregata per valuta: totale EUR e totale in valuta originale."""
    grp = (
        df.groupby("_valuta", sort=False)
        .agg(
            tot_eur=("_imp_riga", "sum"),
            tot_val=("_imp_riga_val", "sum"),
            n_docs=("_cod_doc", "nunique"),
        )
        .reset_index()
        .rename(columns={"_valuta": "valuta"})
        .sort_values("tot_eur", ascending=False)
    )
    return grp.to_dict(orient="records")


def _agg_per_stato(df: pd.DataFrame) -> list:
    grp = (
        df.groupby("_stato", sort=False)
        .agg(
            n_docs=("_cod_doc", "nunique"),
            tot_eur=("_imp_riga", "sum"),
        )
        .reset_index()
        .rename(columns={"_stato": "stato"})
        .sort_values("tot_eur", ascending=False)
    )
    return grp.to_dict(orient="records")


def _agg_per_cdc(df: pd.DataFrame) -> list:
    grp = (
        df.groupby("_cdc", sort=False)
        .agg(
            n_docs=("_cod_doc", "nunique"),
            tot_eur=("_imp_riga", "sum"),
        )
        .reset_index()
        .rename(columns={"_cdc": "cdc"})
        .sort_values("tot_eur", ascending=False)
    )
    return grp.to_dict(orient="records")


def _agg_mensile(df: pd.DataFrame) -> list:
    tmp = df.dropna(subset=["_data_doc"]).copy()
    tmp["_mese"] = tmp["_data_doc"].dt.to_period("M").astype(str)
    grp = (
        tmp.groupby("_mese", sort=True)
        .agg(
            n_docs=("_cod_doc", "nunique"),
            tot_eur=("_imp_riga", "sum"),
        )
        .reset_index()
        .rename(columns={"_mese": "mese"})
    )
    return grp.to_dict(orient="records")


def _agg_top_fornitori(df: pd.DataFrame, top_n: int = 20) -> list:
    grp = (
        df.groupby("_rag_soc", sort=False)
        .agg(
            n_docs=("_cod_doc", "nunique"),
            tot_eur=("_imp_riga", "sum"),
        )
        .reset_index()
        .rename(columns={"_rag_soc": "ragione_sociale"})
        .sort_values("tot_eur", ascending=False)
        .head(top_n)
    )
    return grp.to_dict(orient="records")


# ── Funzione principale ───────────────────────────────────────────

def process_vis_dettagliata(df: pd.DataFrame) -> dict:
    """
    Processa il DataFrame della vis_dettagliata e restituisce KPI aggregati.

    Returns:
        {
            "n_documenti": int,
            "n_righe": int,
            "totale_eur": float,
            "n_valute": int,
            "esposizione_estera_eur": float,
            "per_valuta": [{valuta, tot_eur, tot_val, n_docs}],
            "per_stato": [{stato, n_docs, tot_eur}],
            "per_cdc": [{cdc, n_docs, tot_eur}],
            "mensile": [{mese, n_docs, tot_eur}],
            "top_fornitori": [{ragione_sociale, n_docs, tot_eur}]
        }
    """
    norm = _normalise(df)

    totale_eur        = float(norm["_imp_riga"].sum())
    estera_eur        = float(norm.loc[norm["_valuta"] != "EUR", "_imp_riga"].sum())
    n_documenti       = int(norm["_cod_doc"].nunique())
    n_righe           = len(norm)
    n_valute          = int(norm["_valuta"].nunique())

    return {
        "n_documenti":         n_documenti,
        "n_righe":             n_righe,
        "totale_eur":          round(totale_eur, 2),
        "n_valute":            n_valute,
        "esposizione_estera_eur": round(estera_eur, 2),
        "per_valuta":          _agg_per_valuta(norm),
        "per_stato":           _agg_per_stato(norm),
        "per_cdc":             _agg_per_cdc(norm),
        "mensile":             _agg_mensile(norm),
        "top_fornitori":       _agg_top_fornitori(norm),
    }


# ── KPI da DB (futuro) ─────────────────────────────────────────────

def kpi_vis_dettagliata(client, anno: Optional[int] = None, str_ric: Optional[str] = None) -> dict:
    """
    Query dalla tabella vis_dettagliata se esiste nel DB.
    Placeholder per futura persistenza su Supabase.
    Attualmente ritorna i dati direttamente dalla tabella se disponibile,
    altrimenti segnala che i dati non sono ancora caricati.
    """
    try:
        q = client.table("vis_dettagliata").select("*")
        if anno:
            q = q.gte("data_doc", f"{anno}-01-01").lte("data_doc", f"{anno}-12-31")
        if str_ric:
            q = q.eq("str_ric", str_ric)
        rows = q.limit(1).execute().data
        if not rows:
            return {"disponibile": False, "messaggio": "Nessun dato vis_dettagliata nel DB."}
        return {"disponibile": True, "messaggio": "Dati presenti nel DB."}
    except Exception as exc:
        log.warning(f"kpi_vis_dettagliata: tabella non raggiungibile — {exc}")
        return {
            "disponibile": False,
            "messaggio": "Tabella vis_dettagliata non disponibile nel DB. Usa l'endpoint di upload per analisi in-memory.",
        }
