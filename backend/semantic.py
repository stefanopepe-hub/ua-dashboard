"""
Rilevamento semantico delle colonne Excel.
Analizza il CONTENUTO delle celle, non il nome della colonna.
Funziona con qualsiasi versione/variante del file Alyante.
"""
import re
import pandas as pd
from datetime import datetime

# ── Valori attesi per ogni tipo ────────────────────────────────
ALFA_VALIDI  = {'OPR','ORN','OS','OSP','ORD','OSD','OSDP01','PS','DDT'}
CDC_VALIDI   = {'GD','TIGEM','TIGET','FT','STRUTTURA','Terapie'}
SI_NO        = {'SI','SÌ','NO','SI ','NO ','Si','No'}
VALUTE       = {'EURO','USD','GBP','JPY','CHF','AUD','CAD','SEK','AED','DKK','EUR'}
STATI_DMS    = {'Ordine Fatturato','Ordine Consegnato','Inviato Fornitore',
                'Ordine Fatturato Parziale','Ordine Consegnato Parziale',
                'Inviato a sistema','In lavorazione'}
STR_RIC_VALS = {'RICERCA','STRUTTURA'}


def _str_sample(series, n=10):
    return [str(v).strip() for v in series.dropna().head(n).tolist() if pd.notna(v)]

def _num_sample(series, n=10):
    result = []
    for v in series.dropna().head(n).tolist():
        try: result.append(float(v))
        except: pass
    return result

def _subset_of(sample, valid_set, threshold=0.8):
    """Almeno threshold% dei valori è nel set atteso."""
    if not sample: return False
    matches = sum(1 for s in sample if s in valid_set or s.upper() in valid_set)
    return matches / len(sample) >= threshold


def detect_column_type(col_name: str, series: pd.Series) -> tuple:
    """
    Ritorna (tipo_interno, confidenza 0-100).
    Analizza prima il contenuto, poi il nome come hint.
    """
    name = col_name.strip().lower()
    ss = _str_sample(series)
    ns = _num_sample(series)

    if not ss and not ns:
        return None, 0

    # ── 1. DATE ────────────────────────────────────────────────
    raw = series.dropna().head(5).tolist()
    if raw and any(isinstance(v, (datetime, pd.Timestamp)) for v in raw):
        return 'data_doc', 98

    # ── 2. ALFA DOCUMENTO (OPR, ORN, OS, …) ───────────────────
    if ss and _subset_of(ss, ALFA_VALIDI, 0.9):
        return 'alfa_documento', 99

    # ── 3. STR/RIC (RICERCA / STRUTTURA) ───────────────────────
    if ss and _subset_of([s.upper() for s in ss], STR_RIC_VALS, 0.95):
        return 'str_ric', 99

    # ── 4. VALUTA ──────────────────────────────────────────────
    if ss and _subset_of([s.upper() for s in ss], VALUTE, 0.9):
        return 'valuta', 99

    # ── 5. CDC (GD, TIGEM, TIGET, FT, STRUTTURA) ──────────────
    if ss and _subset_of(ss, CDC_VALIDI, 0.85):
        return 'cdc', 96

    # ── 6. BOOLEANI SI/NO ─────────────────────────────────────
    if ss and _subset_of([s.upper().strip() for s in ss], {'SI','SÌ','NO'}, 0.95):
        if any(k in name for k in ['negoz','negozia']):
            return 'negoziazione', 94
        if any(k in name for k in ['albo','accred']):
            return 'accred_albo', 94
        return 'flag_generico', 55

    # ── 7. PROTOCOLLO COMMESSA (GMR24T.../00053) ───────────────
    if ss and sum(1 for s in ss if '/' in s and len(s) > 8) / len(ss) >= 0.8:
        if any(k in name for k in ['commessa','prot','protocol']):
            return 'protoc_commessa', 96
        return 'protoc_generico', 70

    # ── 8. CENTRO DI COSTO (RCRIIR000000026, STR…) ────────────
    if ss and all(re.match(r'^[A-Z]{3,8}\d{6,}$', s) for s in ss[:3] if s not in ('nan','None','')):
        return 'centro_di_costo', 96

    # ── 9. STATO DMS ────────────────────────────────────────────
    if ss and sum(1 for s in ss if any(d in s for d in ['Ordine','Fatturato','Consegnato','Inviato'])) > len(ss) * 0.7:
        return 'stato_dms', 90

    # ── 10. TESTO LIBERO — per nome ────────────────────────────
    if not ns:  # colonna di testo
        if any(k in name for k in ['ragione','fornitore','supplier','vendor']):
            return 'ragione_sociale', 92
        if any(k in name for k in ['descri']) and any(k in name for k in ['costo','cdc']):
            return 'desc_cdc', 88
        if any(k in name for k in ['descri']) and any(k in name for k in ['merceol','gruppo','categ']):
            return 'desc_gruppo_merceol', 88
        if any(k in name for k in ['pres','display','presentaz']) and 'utente' in name:
            return 'utente_presentazione', 88
        if 'utente' in name or 'buyer' in name or 'user' in name:
            return 'utente', 82
        if any(k in name for k in ['macro','categ']) and 'merceol' not in name:
            return 'macro_categoria', 82
        if 'tail' in name:
            return 'tail_spend', 80
        if any(k in name for k in ['stato','status','dms']):
            return 'stato_dms', 78
        if 'protoc' in name and 'ordine' in name:
            return 'protoc_ordine', 75
        if 'protoc' in name and 'origin' in name:
            return 'protoc_origine', 72
        if any(k in name for k in ['grp','gruppo']) and 'merceol' in name:
            return 'grp_merceol', 75
        return 'testo_generico', 20

    # ── 11. SAVING — priorità assoluta sul nome ───────────────
    # Colonne con "saving" nel nome e nessun simbolo % -> sono importi
    if 'saving' in name and '%' not in name and 'perc' not in name:
        if any(c.isdigit() for c in name) or '€' in name or 'eur' in name:
            return 'saving_eur', 94
        return 'saving_val', 90

    # ── 12. ESCLUDI PROTOCOLLI NUMERICI ───────────────────────
    # Protoc.ordine, Protoc.commessa numerici non sono importi
    if any(k in name for k in ['protoc','protocol','prot']):
        if 'commessa' in name:
            return 'protoc_commessa', 88
        if 'ordine' in name:
            return 'protoc_ordine', 88
        return 'protoc_generico', 70

    # ── 12. NUMERICI ───────────────────────────────────────────
    if not ns:
        return None, 0

    avg = sum(ns) / len(ns)
    max_v = max(ns)

    # Saving.1 — nome specifico
    if 'saving' in name and any(c.isdigit() for c in name):
        return 'saving_eur', 94

    # Codice fornitore: interi, range 100-99999, nome contiene 'codice'/'fornitore'
    if (any(k in name for k in ['codice','cod']) and
            any(k in name for k in ['fornitore','vendor','supplier']) and
            all(float(v) == int(float(v)) for v in ns[:5])):
        return 'codice_fornitore', 94

    # Cod utente: piccoli interi
    if 'cod' in name and 'utente' in name and max_v < 1000:
        return 'cod_utente', 90

    # Num doc: interi medi
    if any(k in name for k in ['num','doc','numero']) and all(float(v)==int(float(v)) for v in ns[:5]):
        return 'num_doc', 82

    # Cambio: vicino a 1
    if 0.3 <= avg <= 5.0 and max_v < 15 and any(k in name for k in ['cambio','exchange','tasso']):
        return 'cambio', 90

    # % saving in decimale (0-1)
    if 0 <= max_v <= 1.05 and any(k in name for k in ['%','perc','saving']):
        return 'perc_saving_eur', 85

    # % saving in percentuale (0-100)
    if 0 <= max_v <= 100 and any(k in name for k in ['%','perc','saving']):
        return 'perc_saving', 82

    # Grp merceol: piccoli interi o codici
    if any(k in name for k in ['grp','gruppo']) and avg < 1000:
        return 'grp_merceol', 80

    # Saving: qualsiasi colonna con "saving" nel nome e valori numerici >= 0
    if 'saving' in name and avg >= 0 and not any(k in name for k in ['%','perc']):
        if '€' in name or 'eur' in name or '.1' in name or '1' in name:
            return 'saving_eur', 90
        return 'saving_val', 88

    # Importi: colonne con "imp" o "importo" nel nome
    if any(k in name for k in ['imp','importo','amount']) and avg > 0:
        if '€' in name or 'eur' in name:
            if 'negoz' in name: return 'imp_negoziato_eur', 92
            return 'imp_iniziale_eur', 90
        if 'negoz' in name: return 'imp_negoziato', 88
        return 'imp_iniziale', 86

    # Importi grandi (fallback senza hint nel nome)
    if avg > 500:
        has_eur = any(k in name for k in ['€','eur','euro'])
        is_neg  = any(k in name for k in ['negoz','negoziato'])
        is_sav  = any(k in name for k in ['saving'])

        if has_eur and is_neg:   return 'imp_negoziato_eur', 92
        if has_eur and is_sav:   return 'saving_eur', 92
        if has_eur:              return 'imp_iniziale_eur', 90
        if is_neg:               return 'imp_negoziato', 86
        if is_sav:               return 'saving_val', 86
        # Senza hint nel nome: guarda se c'è già una colonna EUR più precisa
        return 'imp_iniziale', 72

    return 'numerico_generico', 15


def build_semantic_map(df: pd.DataFrame, min_confidence: int = 60) -> dict:
    """
    Costruisce la mappa tipo_interno -> nome_colonna_reale.
    In caso di conflitto, vince la confidenza più alta.
    Logga le colonne non mappate.
    """
    import logging
    log = logging.getLogger('ua-semantic')

    mapping    = {}   # tipo -> col_name
    confidence = {}   # tipo -> conf

    for col in df.columns:
        tipo, conf = detect_column_type(col, df[col])
        if tipo and conf >= min_confidence:
            if tipo not in mapping or conf > confidence[tipo]:
                mapping[tipo]    = col
                confidence[tipo] = conf

    # Report
    found   = set(mapping.keys())
    key_cols = {'data_doc','alfa_documento','str_ric','ragione_sociale',
                'imp_iniziale','valuta','negoziazione','accred_albo'}
    missing = key_cols - found
    if missing:
        log.warning(f"Colonne chiave non trovate: {missing}")

    log.info(f"Semantic mapping: {len(mapping)} colonne mappate, "
             f"{len(df.columns)-len(mapping)} non mappate")
    return mapping


def gcol(mapping: dict, key: str, row) -> any:
    """Legge valore da riga usando il mapping semantico."""
    col_name = mapping.get(key)
    if col_name is None:
        return None
    return row.get(col_name)
