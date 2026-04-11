"""
upload_engine.py — Enterprise Upload Engine v1.0
Fondazione Telethon ETS — UA Dashboard

ARCHITETTURA:
  File Excel → WorkbookInspector → FamilyClassifier → ColumnMapper
  → Normalizer → Validator → CanonicalPersister → ReadinessMatrix

PRINCIPI:
  - Zero crash su file business supportati
  - Classificazione automatica della famiglia
  - Mapping adattivo senza dipendenza da nomi esatti
  - Supporto multi-anno per analisi YoY
  - Degradazione graceful: analisi parziali con motivazioni chiare
  - Unico punto di verità per preview + import + analytics
"""

import io
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from domain import (
    _b, _d, _f, _fn, _i, _s, clean, parse_commessa, safe_pct,
)
from ingestion_engine import (
    Confidence, FieldMapping, FileFamily, MappingResult,
    build_column_map, classify_file_family, detect_header_row,
    inspect_workbook, mapping_result_to_dict,
)

log = logging.getLogger("ua.upload")


# ══════════════════════════════════════════════════════════════════
# DOCUMENT TYPE LABELS — AUTORITATIVI (Fondazione Telethon ETS)
# ══════════════════════════════════════════════════════════════════

DOC_TYPE_LABELS = {
    "ORN":    "Ordine Ricerca",
    "ORD":    "Ordine Diretto Ricerca",
    "OPR":    "Ordine Previsionale Ricerca",
    "PS":     "Procedura Straordinaria",
    "OS":     "Ordine Struttura",
    "OSP":    "Ordine Previsionale Struttura",
    "OSD":    "Ordine Diretto Struttura",
    "OSDP01": "Ordine Diretto Struttura (variante)",
}

DOC_TYPE_AREA = {
    "ORN": "RICERCA", "ORD": "RICERCA", "OPR": "RICERCA", "PS": "RICERCA",
    "OS": "STRUTTURA", "OSP": "STRUTTURA", "OSD": "STRUTTURA", "OSDP01": "STRUTTURA",
}


# ══════════════════════════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════════════════════════

@dataclass
class UploadResult:
    """Risultato completo di un'operazione di upload."""
    status: str                          # "ok" | "partial" | "failed"
    upload_id: Optional[str]
    rows_inserted: int
    rows_skipped: int
    family: str
    family_label: str
    sheet_used: str
    header_row: int
    year_detected: Optional[int]
    years_found: List[int]
    mapping_confidence: str
    mapping_score: float
    mapped_fields: List[Dict]
    missing_critical: List[str]
    missing_optional: List[str]
    available_analyses: List[str]
    blocked_analyses: List[Dict]
    warnings: List[str]
    normalization_notes: List[str]
    yoy_ready: bool
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "upload_id": self.upload_id,
            "rows_inserted": self.rows_inserted,
            "rows_skipped": self.rows_skipped,
            "family": self.family,
            "family_label": self.family_label,
            "sheet_used": self.sheet_used,
            "header_row": self.header_row,
            "year_detected": self.year_detected,
            "years_found": self.years_found,
            "mapping_confidence": self.mapping_confidence,
            "mapping_score": self.mapping_score,
            "mapped_fields": self.mapped_fields,
            "missing_critical": self.missing_critical,
            "missing_optional": self.missing_optional,
            "available_analyses": self.available_analyses,
            "blocked_analyses": self.blocked_analyses,
            "warnings": self.warnings,
            "normalization_notes": self.normalization_notes,
            "yoy_ready": self.yoy_ready,
            "error": self.error,
        }


@dataclass
class WorkbookInspection:
    """Risultato dell'ispezione workbook."""
    sheet_name: str
    header_row: int
    df: pd.DataFrame
    mapping_result: MappingResult
    years_found: List[int] = field(default_factory=list)
    year_dominant: Optional[int] = None


# ══════════════════════════════════════════════════════════════════
# WORKBOOK INSPECTOR
# ══════════════════════════════════════════════════════════════════

def inspect_bytes(file_bytes: bytes, filename: str = "") -> MappingResult:
    """
    Ispeziona un file Excel dai bytes.
    Entry point per /upload/inspect.
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
        return inspect_workbook(xl)
    except Exception as e:
        log.error(f"inspect_bytes failed for {filename}: {e}")
        raise ValueError(f"Impossibile aprire il file Excel: {e}")


def inspect_and_load(file_bytes: bytes, filename: str = "") -> WorkbookInspection:
    """
    Ispeziona il workbook e carica il DataFrame completo per la normalizzazione.
    
    Performance:
    - inspect_workbook usa nrows=200 (veloce, per mapping)
    - qui facciamo UN SOLO full read (per la normalizzazione)
    - totale: 1 full read invece di 2-4
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Errore apertura file '{filename}': {e}")

    # Step 1: ispeziona struttura e mappa colonne (veloce — usa sample)
    mr = inspect_workbook(xl)

    # Step 2: UN SOLO full read per la normalizzazione
    try:
        df = pd.read_excel(xl, sheet_name=mr.sheet_name, header=mr.header_row)
    except Exception as e:
        log.warning(f"Header row {mr.header_row} failed for {filename}, trying 0: {e}")
        try:
            df = pd.read_excel(xl, sheet_name=mr.sheet_name, header=0)
        except Exception as e2:
            raise ValueError(f"Impossibile leggere il foglio '{mr.sheet_name}': {e2}")

    # Pulizia colonne
    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
    df.columns = [str(c).strip() for c in df.columns]

    # Aggiorna il mapping usando il df completo per valori più robusti
    # (il mapping sul sample è identico ma vogliamo consistenza)
    from ingestion_engine import build_column_map, classify_file_family
    full_col_map = build_column_map(df)
    # Usa il full map se ha più campi del sample map
    if len(full_col_map) >= len(mr.fields):
        mr.fields = full_col_map

    # Rilevamento anni
    years_found, year_dominant = _detect_years(df, mr.fields)

    return WorkbookInspection(
        sheet_name=mr.sheet_name,
        header_row=mr.header_row,
        df=df,
        mapping_result=mr,
        years_found=years_found,
        year_dominant=year_dominant,
    )


def _detect_years(df: pd.DataFrame, col_map: Dict[str, FieldMapping]) -> Tuple[List[int], Optional[int]]:
    """Rileva gli anni presenti nel file."""
    date_fm = col_map.get('data_doc') or col_map.get('data_origine') or col_map.get('year_month')
    if not date_fm:
        return [], None

    col = date_fm.source_column
    if col not in df.columns:
        return [], None

    try:
        series = df[col].dropna()
        if 'year_month' in (date_fm.canonical or ''):
            # Formato YYYY-MM
            years = series.astype(str).str[:4].astype(int, errors='ignore').dropna().unique().tolist()
            years = [y for y in years if 2000 <= y <= 2100]
        else:
            dates = pd.to_datetime(series, errors='coerce').dropna()
            years = sorted(dates.dt.year.unique().astype(int).tolist())

        if not years:
            return [], None

        # Anno dominante
        if len(years) == 1:
            return years, years[0]

        # Conta per anno
        if 'year_month' in (date_fm.canonical or ''):
            yr_counts = series.astype(str).str[:4].value_counts()
        else:
            dates2 = pd.to_datetime(df[col], errors='coerce').dropna()
            yr_counts = dates2.dt.year.value_counts()

        total = yr_counts.sum()
        dominant = int(yr_counts.index[0])
        dominant_pct = yr_counts.iloc[0] / total

        # Anno dominante solo se ≥80% (meno restrittivo di prima)
        return years, (dominant if dominant_pct >= 0.80 else None)
    except Exception as e:
        log.warning(f"Year detection failed: {e}")
        return [], None


# ══════════════════════════════════════════════════════════════════
# GCOL — lettore universale da col_map + row
# ══════════════════════════════════════════════════════════════════

def _gcol(col_map: Dict[str, FieldMapping], canonical: str, row: pd.Series) -> Any:
    """Legge valore da riga usando mapping canonico. Mai crasha."""
    fm = col_map.get(canonical)
    if not fm:
        return None
    return row.get(fm.source_column)


# ══════════════════════════════════════════════════════════════════
# NORMALIZERS — uno per famiglia
# ══════════════════════════════════════════════════════════════════

def normalize_saving_row(
    col_map: Dict[str, FieldMapping],
    row: pd.Series,
    upload_id: str,
    cdc_override: Optional[str] = None,
) -> Optional[dict]:
    """
    Normalizza una riga saving nel modello canonico DB.
    Ritorna None se data_doc non è valida (riga da saltare).
    """
    g = lambda k: _gcol(col_map, k, row)

    dv = _d(g('data_doc'))
    if not dv:
        return None

    cambio = _f(g('cambio'), 1.0) or 1.0
    valuta = _s(g('valuta')) or 'EURO'

    # Importi: EUR priorità assoluta, poi valuta convertita
    has_eur = 'listino_eur' in col_map and 'impegnato_eur' in col_map
    if has_eur:
        lst   = _f(g('listino_eur'))
        imp   = _f(g('impegnato_eur'))
        sav   = _f(g('saving_eur'))
        pct_s = _f(g('perc_saving_eur'))
    else:
        lst   = _f(g('listino_val')) * cambio
        imp   = _f(g('impegnato_val')) * cambio
        sav   = _f(g('saving_val')) * cambio
        pct_s = _f(g('perc_saving_val'))

    # Ricalcola saving se mancante
    if sav == 0 and lst > 0 and imp > 0:
        sav = lst - imp
    if pct_s == 0 and lst > 0:
        pct_s = safe_pct(sav, lst)

    # CDC
    from domain import derive_cdc
    if cdc_override:
        cdc_val = cdc_override
    elif 'cdc' in col_map:
        cdc_val = _s(g('cdc'))
    else:
        cdc_val = derive_cdc(
            _s(g('centro_costo')) or '',
            _s(g('desc_cdc')) or ''
        )

    # Commessa
    pc = _s(g('protoc_commessa'))
    pref, anno_comm = parse_commessa(pc)

    r = {
        "upload_id":            upload_id,
        "data_doc":             dv,
        "alfa_documento":       _s(g('alfa_documento')),
        "str_ric":              _s(g('str_ric')),
        "stato_dms":            _s(g('stato_dms')),
        "ragione_sociale":      _s(g('ragione_sociale')),
        "codice_fornitore":     _i(g('codice_fornitore')),
        "accred_albo":          _b(g('accred_albo')),
        "utente":               _s(g('utente')),
        "utente_presentazione": _s(g('utente_pres')),
        "cod_utente":           _i(g('cod_utente')),
        "num_doc":              _i(g('num_doc')),
        "protoc_ordine":        _fn(g('protoc_ordine')),
        "protoc_commessa":      pc,
        "prefisso_commessa":    pref,
        "anno_commessa":        anno_comm,
        "grp_merceol":          _s(g('grp_merceol')),
        "desc_gruppo_merceol":  _s(g('desc_merceol')),
        "macro_categoria":      _s(g('macro_cat')),
        "centro_di_costo":      _s(g('centro_costo')),
        "desc_cdc":             _s(g('desc_cdc')),
        "cdc":                  cdc_val,
        "valuta":               valuta,
        "cambio":               cambio,
        "imp_listino_eur":      lst,
        "imp_impegnato_eur":    imp,
        "saving_eur":           sav,
        "perc_saving_eur":      pct_s,
        "imp_iniziale":         _f(g('listino_val')),
        "imp_negoziato":        _f(g('impegnato_val')),
        "saving_val":           _f(g('saving_val')),
        "perc_saving":          _f(g('perc_saving_val')),
        "negoziazione":         _b(g('negoziazione')),
        "tail_spend":           _s(g('tail_spend')),
    }
    return {k: clean(v) for k, v in r.items()}


def normalize_risorse_row(
    col_map: Dict[str, FieldMapping],
    row: pd.Series,
    upload_id: str,
) -> dict:
    """Normalizza una riga risorse nel modello canonico."""
    g = lambda k: _gcol(col_map, k, row)

    mese_raw = _s(g('year_month')) or ''
    year = month = quarter = None
    try:
        # Supporta YYYY-MM, YYYY/MM, MMYYYY, ecc.
        clean_m = re.sub(r'[/\\]', '-', mese_raw)
        parts = clean_m.split('-')
        if len(parts) == 2:
            if len(parts[0]) == 4:  # YYYY-MM
                year, month = int(parts[0]), int(parts[1])
            else:                    # MM-YYYY
                month, year = int(parts[0]), int(parts[1])
            quarter = (month - 1) // 3 + 1
    except Exception:
        pass

    r = {
        "upload_id":             upload_id,
        "year":                  year,
        "month":                 month,
        "quarter":               quarter,
        "mese_label":            mese_raw,
        "risorsa":               _s(g('risorsa')) or _s(g('utente_pres')) or 'N/D',
        "struttura":             _s(g('str_ric')) or _s(g('cdc')),
        "business_unit":         _s(g('cdc')),
        "pratiche_gestite":      _i(g('pratiche_gestite')),
        "pratiche_aperte":       _i(g('pratiche_aperte')),
        "pratiche_chiuse":       _i(g('pratiche_chiuse')),
        "saving_generato":       _fn(g('saving_generato')),
        "negoziazioni_concluse": _i(g('negoziazioni_concluse')),
        "tempo_medio_giorni":    _fn(g('tempo_medio_risorsa')),
        "efficienza":            _fn(g('efficienza')),
        "backlog":               _i(g('pratiche_aperte')),
    }
    return {k: clean(v) for k, v in r.items()}


def normalize_nc_row(
    col_map: Dict[str, FieldMapping],
    row: pd.Series,
    upload_id: str,
) -> dict:
    """Normalizza una riga non conformità."""
    g = lambda k: _gcol(col_map, k, row)
    r = {
        "upload_id":             upload_id,
        "ragione_sociale":       _s(g('ragione_sociale')),
        "tipo_origine":          _s(g('tipo_origine')),
        "data_origine":          _d(g('data_origine')) or _d(g('data_doc')),
        "utente_origine":        _s(g('utente')) or _s(g('utente_pres')),
        "delta_giorni":          _fn(g('delta_giorni')),
        "non_conformita":        _b(g('non_conformita')),
    }
    return {k: clean(v) for k, v in r.items()}


def normalize_tempi_row(
    col_map: Dict[str, FieldMapping],
    row: pd.Series,
    upload_id: str,
) -> dict:
    """Normalizza una riga tempi attraversamento."""
    g = lambda k: _gcol(col_map, k, row)
    r = {
        "upload_id":        upload_id,
        "protocol":         _s(g('protoc_commessa')) or _s(g('protoc_ordine')),
        "year_month":       _s(g('year_month')),
        "days_purchasing":  _f(g('days_purchasing')),
        "days_auto":        _f(g('days_auto')),
        "days_other":       _f(g('days_other')),
        "total_days":       _f(g('total_days')),
        "bottleneck":       _s(g('bottleneck')),
    }
    return {k: clean(v) for k, v in r.items()}


# ══════════════════════════════════════════════════════════════════
# BATCH INSERTER — robusto con retry parziale
# ══════════════════════════════════════════════════════════════════

def batch_insert(client, table: str, records: List[dict], batch_size: int = 5000) -> Tuple[int, List[str]]:
    """
    Inserisce record in batch.
    Ritorna (inserted_count, errors).
    Non crasha se un batch fallisce — continua con gli altri.
    """
    inserted = 0
    errors = []

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            client.table(table).insert(batch).execute()
            inserted += len(batch)
            log.info(f"Inserted batch {i//batch_size + 1}: {inserted}/{len(records)}")
        except Exception as e:
            err_msg = str(e)[:400]
            log.error(f"Batch insert error at {i}: {err_msg}")
            errors.append(f"Batch {i//batch_size + 1}: {err_msg[:100]}")
            if i == 0 and not inserted:
                # Primo batch fallito → errore fatale
                raise RuntimeError(f"DB insert failed: {err_msg}")

    return inserted, errors


# ══════════════════════════════════════════════════════════════════
# READINESS MATRIX
# ══════════════════════════════════════════════════════════════════

def compute_readiness(mr: MappingResult, wbi: WorkbookInspection) -> dict:
    """
    Calcola la matrice di readiness analitica.
    """
    col_map = mr.fields
    years = wbi.years_found
    year_dom = wbi.year_dominant

    readiness = {
        "family": mr.family.value,
        "family_label": _family_label(mr.family),
        "year_detected": year_dom,
        "years_found": years,
        "yoy_ready": len(years) > 1 or (len(years) == 1),
        "yoy_note": _yoy_note(years, year_dom),
        "mapping_score": mr.overall_score,
        "mapping_confidence": mr.overall_confidence.value,
        "analytics_enabled": mr.available_analyses,
        "analytics_blocked": mr.blocked_analyses,
        "field_coverage": {
            "mapped": len(col_map),
            "missing_critical": mr.missing_critical,
            "missing_optional": mr.missing_optional,
        },
        "normalization_notes": _normalization_notes(mr, wbi),
    }
    return readiness


def _family_label(family: FileFamily) -> str:
    labels = {
        FileFamily.SAVINGS:         "File Saving / Ordini",
        FileFamily.ORDERS_DETAIL:   "Estrazione Ordini Dettagliati",
        FileFamily.NC:              "Non Conformità",
        FileFamily.TEMPI:           "Tempi Attraversamento",
        FileFamily.RISORSE:         "Analisi Risorse / Team",
        FileFamily.SUPPLIER_MASTER: "Anagrafica Fornitori",
        FileFamily.UNKNOWN:         "Tipo Non Riconosciuto",
    }
    return labels.get(family, family.value)


def _yoy_note(years: List[int], dominant: Optional[int]) -> str:
    if not years:
        return "Anno non rilevato automaticamente — verifica che il file contenga una colonna data."
    if len(years) == 1:
        return f"Anno {years[0]} rilevato. Carica file {years[0]+1} per abilitare confronto YoY."
    if dominant:
        others = [y for y in years if y != dominant]
        return (f"Anno dominante: {dominant} ({len(years)} anni totali: {', '.join(str(y) for y in sorted(years))}). "
                f"Confronto YoY abilitato tra {min(years)} e {max(years)}.")
    return f"Più anni trovati ({', '.join(str(y) for y in sorted(years))}). File multi-anno importato integralmente."


def _normalization_notes(mr: MappingResult, wbi: WorkbookInspection) -> List[str]:
    notes = []
    col_map = mr.fields

    # Note su campi inferiti da valori (non da nome colonna)
    for fm in col_map.values():
        if fm.method == 'value':
            notes.append(
                f"'{fm.source_column}' → {fm.canonical}: riconosciuto dal contenuto "
                f"(non dal nome colonna), confidenza {fm.confidence:.0%}"
            )
        elif fm.method in ('regex', 'synonym_partial'):
            notes.append(
                f"'{fm.source_column}' → {fm.canonical}: inferito da pattern/sinonimo, "
                f"confidenza {fm.confidence:.0%}"
            )

    # Note su anno
    if wbi.years_found:
        notes.append(f"Anni trovati nel file: {', '.join(str(y) for y in wbi.years_found)}")

    # Note su macro_categoria (spesso ha spazi finali)
    if 'macro_cat' in col_map:
        notes.append("macro_categoria normalizzata (spazi finali rimossi)")

    return notes


# ══════════════════════════════════════════════════════════════════
# MAIN UPLOAD HANDLERS — uno per famiglia
# ══════════════════════════════════════════════════════════════════

def _apply_year_filter(
    df: pd.DataFrame,
    col_map: Dict[str, FieldMapping],
    yoy_mode: bool,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Gestisce il filtro anno:
    - yoy_mode=False: prende solo l'anno dominante (≥80%)
    - yoy_mode=True: prende tutto (multi-anno per YoY)
    """
    notes = []
    date_fm = col_map.get('data_doc')
    if not date_fm or date_fm.source_column not in df.columns:
        return df, notes

    date_col = date_fm.source_column
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])

    if yoy_mode:
        notes.append("Modalità YoY: tutti gli anni importati")
        return df, notes

    yr = df[date_col].dt.year.value_counts()
    if len(yr) > 1 and yr.iloc[0] / len(df) >= 0.80:
        anno_dom = int(yr.index[0])
        before = len(df)
        df = df[df[date_col].dt.year == anno_dom]
        notes.append(f"Anno dominante {anno_dom} selezionato ({len(df)}/{before} righe)")
        log.info(f"Year filter: {anno_dom}, {len(df)} rows")
    elif len(yr) > 1:
        notes.append(f"File multi-anno senza dominante chiaro — importati tutti gli anni")

    return df, notes


def handle_saving_upload(
    wbi: WorkbookInspection,
    upload_id: str,
    client,
    cdc_override: Optional[str] = None,
    yoy_mode: bool = False,
) -> Tuple[int, int, List[str]]:
    """
    Normalizza e inserisce righe saving.
    Ritorna (inserted, skipped, notes).
    """
    col_map = wbi.mapping_result.fields
    df, notes = _apply_year_filter(wbi.df, col_map, yoy_mode)

    records = []
    skipped = 0
    for _, row in df.iterrows():
        rec = normalize_saving_row(col_map, row, upload_id, cdc_override)
        if rec:
            records.append(rec)
        else:
            skipped += 1

    if not records:
        return 0, skipped, notes + ["Nessuna riga valida trovata"]

    inserted, errs = batch_insert(client, "saving", records)
    if errs:
        notes.extend(errs)
    return inserted, skipped, notes


def handle_risorse_upload(
    wbi: WorkbookInspection,
    upload_id: str,
    client,
) -> Tuple[int, int, List[str]]:
    """Normalizza e inserisce righe risorse."""
    col_map = wbi.mapping_result.fields
    df = wbi.df
    notes = []

    records = [{
        k: v for k, v in normalize_risorse_row(col_map, row, upload_id).items()
    } for _, row in df.iterrows()]

    if not records:
        return 0, 0, ["Nessuna riga valida trovata"]

    inserted, errs = batch_insert(client, "resource_performance", records, batch_size=500)
    if errs:
        notes.extend(errs)
    return inserted, 0, notes


def handle_nc_upload(
    wbi: WorkbookInspection,
    upload_id: str,
    client,
) -> Tuple[int, int, List[str]]:
    """Normalizza e inserisce righe non conformità."""
    col_map = wbi.mapping_result.fields
    df = wbi.df
    records = [normalize_nc_row(col_map, row, upload_id) for _, row in df.iterrows()]
    inserted, errs = batch_insert(client, "non_conformita", records, batch_size=500)
    return inserted, 0, errs


def handle_tempi_upload(
    wbi: WorkbookInspection,
    upload_id: str,
    client,
) -> Tuple[int, int, List[str]]:
    """Normalizza e inserisce righe tempi."""
    col_map = wbi.mapping_result.fields
    df = wbi.df
    records = [normalize_tempi_row(col_map, row, upload_id) for _, row in df.iterrows()]
    inserted, errs = batch_insert(client, "tempo_attraversamento", records, batch_size=500)
    return inserted, 0, errs


# ══════════════════════════════════════════════════════════════════
# UNIFIED UPLOAD ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════

FAMILY_TABLE_MAP = {
    FileFamily.SAVINGS:       "saving",
    FileFamily.ORDERS_DETAIL: "saving",         # stessa tabella saving
    FileFamily.RISORSE:       "resource_performance",
    FileFamily.NC:            "non_conformita",
    FileFamily.TEMPI:         "tempo_attraversamento",
}

def process_upload(
    file_bytes: bytes,
    filename: str,
    client,
    cdc_override: Optional[str] = None,
    yoy_mode: bool = False,
    forced_family: Optional[str] = None,
) -> UploadResult:
    """
    Orchestratore principale upload.
    
    Entry point unico per tutti i tipi di file.
    Classifica → normalizza → persiste → ritorna readiness matrix.
    
    Parametri:
        file_bytes:     contenuto del file Excel
        filename:       nome originale del file
        client:         Supabase client
        cdc_override:   CDC fisso (opzionale)
        yoy_mode:       True = non filtrare per anno (multi-anno per YoY)
        forced_family:  forza la famiglia (opzionale, per override utente)
    """
    normalization_notes = []
    
    # ── Step 1: Ispeziona workbook ────────────────────────────────
    try:
        wbi = inspect_and_load(file_bytes, filename)
    except ValueError as e:
        return UploadResult(
            status="failed", upload_id=None, rows_inserted=0, rows_skipped=0,
            family="unknown", family_label="Sconosciuto", sheet_used="",
            header_row=0, year_detected=None, years_found=[], mapping_confidence="low",
            mapping_score=0.0, mapped_fields=[], missing_critical=[], missing_optional=[],
            available_analyses=[], blocked_analyses=[], warnings=[],
            normalization_notes=[], yoy_ready=False, error=str(e),
        )

    mr = wbi.mapping_result

    # ── Step 2: Override famiglia se richiesto dall'utente ────────
    if forced_family:
        try:
            mr = _force_family(mr, FileFamily(forced_family))
        except ValueError:
            normalization_notes.append(f"Family override '{forced_family}' non valido, ignorato")

    # ── Step 3: Valida se il file è processabile ──────────────────
    if mr.overall_score < 0.20:
        return UploadResult(
            status="failed", upload_id=None, rows_inserted=0, rows_skipped=0,
            family=mr.family.value, family_label=_family_label(mr.family),
            sheet_used=mr.sheet_name, header_row=mr.header_row,
            year_detected=wbi.year_dominant, years_found=wbi.years_found,
            mapping_confidence=mr.overall_confidence.value, mapping_score=mr.overall_score,
            mapped_fields=mapping_result_to_dict(mr)['mapped_fields'],
            missing_critical=mr.missing_critical, missing_optional=mr.missing_optional,
            available_analyses=[], blocked_analyses=[{
                'analysis': 'Tutte le analisi',
                'reason': f"Confidenza troppo bassa ({mr.overall_score:.0%}). "
                          f"Il file non sembra un file procurement riconoscibile. "
                          f"Colonne trovate: {mr.raw_columns[:8]}",
                'severity': 'critical'
            }],
            warnings=mr.warnings,
            normalization_notes=[f"File non riconoscibile. Tipo rilevato: {mr.family.value}"],
            yoy_ready=False,
            error=f"File non riconoscibile come file procurement (score: {mr.overall_score:.0%})",
        )

    # ── Step 4: Crea upload_log entry ────────────────────────────
    readiness = compute_readiness(mr, wbi)
    preview_dict = mapping_result_to_dict(mr)

    try:
        lr = client.table("upload_log").insert({
            "filename":           filename,
            "tipo":               mr.family.value,
            "cdc_filter":         cdc_override,
            "family_detected":    mr.family.value,
            "mapping_confidence": mr.overall_confidence.value,
            "mapping_score":      round(mr.overall_score, 4),
            "sheet_used":         mr.sheet_name,
            "header_row":         mr.header_row,
            "available_analyses": preview_dict.get('available_analyses', []),
            "blocked_analyses":   preview_dict.get('blocked_analyses', []),
            "warnings":           preview_dict.get('warnings', []),
        }).execute()
        upload_id = lr.data[0]["id"]
    except Exception as e:
        log.error(f"upload_log insert failed: {e}")
        return UploadResult(
            status="failed", upload_id=None, rows_inserted=0, rows_skipped=0,
            family=mr.family.value, family_label=_family_label(mr.family),
            sheet_used=mr.sheet_name, header_row=mr.header_row,
            year_detected=wbi.year_dominant, years_found=wbi.years_found,
            mapping_confidence=mr.overall_confidence.value, mapping_score=mr.overall_score,
            mapped_fields=preview_dict['mapped_fields'],
            missing_critical=mr.missing_critical, missing_optional=mr.missing_optional,
            available_analyses=mr.available_analyses,
            blocked_analyses=mr.blocked_analyses,
            warnings=mr.warnings, normalization_notes=[],
            yoy_ready=readiness['yoy_ready'],
            error=f"Errore DB log: {str(e)[:200]}",
        )

    # ── Step 5: Dispatch al handler corretto ──────────────────────
    inserted = skipped = 0
    handler_notes = []
    handler_error = None

    try:
        if mr.family in (FileFamily.SAVINGS, FileFamily.ORDERS_DETAIL):
            inserted, skipped, handler_notes = handle_saving_upload(
                wbi, upload_id, client, cdc_override, yoy_mode
            )
        elif mr.family == FileFamily.RISORSE:
            inserted, skipped, handler_notes = handle_risorse_upload(
                wbi, upload_id, client
            )
        elif mr.family == FileFamily.NC:
            inserted, skipped, handler_notes = handle_nc_upload(
                wbi, upload_id, client
            )
        elif mr.family == FileFamily.TEMPI:
            inserted, skipped, handler_notes = handle_tempi_upload(
                wbi, upload_id, client
            )
        else:
            # Family non supportata per import diretto
            handler_notes.append(
                f"Famiglia '{mr.family.value}' riconosciuta ma import non ancora supportato. "
                f"Famiglie supportate: savings, risorse, non_conformita, tempi."
            )
            handler_error = "Family not importable"

    except RuntimeError as e:
        handler_error = str(e)
        log.error(f"Handler error for {mr.family}: {e}")

    # ── Step 6: Aggiorna upload_log con risultati ─────────────────
    try:
        client.table("upload_log").update({"rows_inserted": inserted}).eq("id", upload_id).execute()
    except Exception:
        pass

    # ── Step 7: Costruisci risultato ──────────────────────────────
    normalization_notes.extend(handler_notes)
    normalization_notes.extend(readiness['normalization_notes'])

    status = "ok" if not handler_error else ("partial" if inserted > 0 else "failed")

    log.info(
        f"Upload complete: family={mr.family.value}, status={status}, "
        f"inserted={inserted}, skipped={skipped}, score={mr.overall_score:.0%}"
    )

    return UploadResult(
        status=status,
        upload_id=upload_id,
        rows_inserted=inserted,
        rows_skipped=skipped,
        family=mr.family.value,
        family_label=_family_label(mr.family),
        sheet_used=mr.sheet_name,
        header_row=mr.header_row,
        year_detected=wbi.year_dominant,
        years_found=wbi.years_found,
        mapping_confidence=mr.overall_confidence.value,
        mapping_score=mr.overall_score,
        mapped_fields=preview_dict['mapped_fields'],
        missing_critical=mr.missing_critical,
        missing_optional=mr.missing_optional,
        available_analyses=mr.available_analyses,
        blocked_analyses=mr.blocked_analyses,
        warnings=mr.warnings,
        normalization_notes=normalization_notes,
        yoy_ready=readiness['yoy_ready'],
        error=handler_error,
    )


def _force_family(mr: MappingResult, new_family: FileFamily) -> MappingResult:
    """Override della famiglia rilevata — per conferma utente."""
    mr.family = new_family
    mr.family_confidence = 0.70  # confidence ridotta per override
    return mr
