"""
ingestion_engine.py — Enterprise Adaptive Ingestion Engine
Fondazione Telethon ETS — UA Dashboard v8

Architettura a 7 layer per riconoscimento colonne:
  L1: Exact match
  L2: Normalized match (lowercase, strip, punct)
  L3: Synonym dictionary (IT/EN)
  L4: Regex & token rules
  L5: Value-based inference (contenuto celle)
  L6: Sheet context inference
  L7: Aggregate confidence scoring

File family classification:
  SAVINGS  | ORDERS_DETAIL | NC | TEMPI | RISORSE | SUPPLIER_MASTER | UNKNOWN
"""

import re
import logging
import pandas as pd
from typing import Optional, Tuple, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

log = logging.getLogger('ua.ingestion')


# ══════════════════════════════════════════════════════════════════
# ENUMS & CONSTANTS
# ══════════════════════════════════════════════════════════════════

class FileFamily(str, Enum):
    SAVINGS         = "savings"
    ORDERS_DETAIL   = "orders_detail"
    NC              = "non_conformita"
    TEMPI           = "tempi"
    RISORSE         = "risorse"
    SUPPLIER_MASTER = "supplier_master"
    UNKNOWN         = "unknown"


class Confidence(str, Enum):
    HIGH   = "high"    # ≥ 0.85 → procede automaticamente
    MEDIUM = "medium"  # 0.60–0.84 → chiede conferma
    LOW    = "low"     # < 0.60 → blocca e spiega


# Valori noti nei file Telethon / ERP-Alyante
KNOWN_ALFA_DOC   = frozenset({'OPR','ORN','OS','OSP','ORD','OSD','OSDP01','PS','DDT'})
KNOWN_CDC        = frozenset({'GD','TIGEM','TIGET','FT','STRUTTURA','TERAPIE'})
KNOWN_STR_RIC    = frozenset({'RICERCA','STRUTTURA','SUPPORTO'})
KNOWN_SI_NO      = frozenset({'SI','SÌ','NO','SI ','NO '})
KNOWN_VALUTE     = frozenset({'EURO','EUR','USD','GBP','JPY','CHF','AUD','CAD','SEK','AED'})
KNOWN_DMS_STATES = frozenset({
    'ORDINE FATTURATO','ORDINE CONSEGNATO','INVIATO FORNITORE',
    'ORDINE FATTURATO PARZIALE','ORDINE CONSEGNATO PARZIALE',
    'INVIATO A SISTEMA','IN LAVORAZIONE'
})


# ══════════════════════════════════════════════════════════════════
# L3 — SYNONYM DICTIONARY
# Formato: campo_canonico → lista di sinonimi (tutti lowercase, stripped)
# ══════════════════════════════════════════════════════════════════

SYNONYMS: Dict[str, List[str]] = {
    # ── Importi ──────────────────────────────────────────────────
    'listino_eur': [
        'imp. iniziale €', 'imp.iniziale €', 'imp iniziale €',
        'imp. iniziale e', 'importo iniziale eur', 'importo listino eur',
        'list amount eur', 'initial amount eur', 'listino eur',
        'prezzo listino eur', 'valore iniziale eur', 'imp_listino_eur',
        # Varianti inglese
        'list price eur', 'price list eur', 'catalog price eur',
        'gross amount eur', 'original amount eur', 'budget amount eur',
        'list amount', 'list price', 'price list', 'catalog price',
        'gross amount', 'original amount', 'budget amount',
        'initial price', 'base price', 'standard price',
    ],
    'impegnato_eur': [
        'imp. negoziato €', 'imp.negoziato €', 'imp negoziato €',
        'imp. negoziato e', 'importo negoziato eur', 'importo impegnato eur',
        'committed amount eur', 'negotiated amount eur', 'impegnato eur',
        'importo effettivo eur', 'imp_impegnato_eur', 'importo finale eur',
        # Varianti inglese pre-elaborate da utenti
        'committed eur', 'committed_eur', 'total committed eur',
        'negotiated eur', 'net amount eur', 'actual amount eur',
        'spend eur', 'total spend eur', 'purchase amount eur',
        'committed amount', 'total committed', 'negotiated amount',
        'actual spend', 'net spend', 'final amount',
    ],
    'saving_eur': [
        'saving.1', 'saving eur', 'saving €', 'saving1', 'risparmio eur',
        'savings eur', 'saving_eur', 'saving amount eur', 'differenza eur',
    ],
    'perc_saving_eur': [
        '%saving', '% saving eur', 'perc saving eur', 'perc.saving',
        '% risparmio', 'saving %', 'savings percent', 'perc_saving_eur',
    ],
    'listino_val': [
        'imp.iniziale', 'imp. iniziale', 'importo iniziale', 'importo listino',
        'list amount', 'initial amount', 'listino', 'prezzo listino',
        'valore iniziale', 'imp_iniziale',
    ],
    'impegnato_val': [
        'imp.negoziato', 'imp. negoziato', 'importo negoziato', 'importo impegnato',
        'committed amount', 'negotiated amount', 'impegnato',
        'importo effettivo', 'imp_negoziato',
    ],
    'saving_val': [
        'saving', 'risparmio', 'savings', 'saving_val', 'saving amount',
        'differenza importo',
    ],
    'perc_saving_val': [
        '% saving', 'perc saving', '% risparmio', 'saving %',
        'savings percent', 'perc_saving',
    ],

    # ── Date ─────────────────────────────────────────────────────
    'data_doc': [
        'data doc.', 'data documento', 'data doc', 'document date', 'doc date',
        'order date', 'data ordine', 'data', 'data_doc', 'data doc.',
        'date', 'document_date', 'data emissione',
    ],
    'data_inizio_comp': [
        'data inizio competenza', 'start competence date', 'data inizio comp.',
        'inizio competenza', 'data_inizio_comp', 'comp start', 'start date',
        'data_inizio',
    ],
    'data_fine_comp': [
        'data fine competenza', 'end competence date', 'data fine comp.',
        'fine competenza', 'data_fine_comp', 'comp end', 'end date',
        'data scadenza', 'expiry date', 'data_fine',
    ],

    # ── Documento ────────────────────────────────────────────────
    'alfa_documento': [
        'alfa documento', 'tipo documento', 'tipo doc', 'doc type', 'document type',
        'alfa_documento', 'tipo_documento', 'doc_type', 'codice documento',
        'document_type', 'tipologia documento', 'tipo_doc',
    ],
    'num_doc': [
        'num.doc.', 'numero documento', 'num documento', 'doc number',
        'document number', 'num_doc', 'numero_doc', 'n. documento',
        'n.doc.', 'n_doc',
    ],
    'stato_dms': [
        'stato dms', 'stato documento', 'doc status', 'document status',
        'stato_dms', 'status', 'stato_doc',
    ],
    'protoc_ordine': [
        'protoc.ordine', 'protocollo ordine', 'order protocol', 'protocol order',
        'protoc_ordine', 'protocol_order', 'n. ordine', 'numero ordine',
    ],
    'protoc_commessa': [
        'protoc.commessa', 'protocollo commessa', 'commessa', 'project code',
        'protoc_commessa', 'protocol_commessa', 'codice commessa', 'project',
        'protocollo progetto', 'protocol', 'protocollo_commessa',
    ],

    # ── Fornitore ────────────────────────────────────────────────
    'ragione_sociale': [
        'ragione sociale fornitore', 'ragione sociale', 'fornitore', 'supplier',
        'vendor', 'supplier name', 'vendor name', 'nome fornitore',
        'ragione_sociale', 'supplier_name', 'vendor_name', 'ragsoc',
        'rag.soc.', 'rag. soc.',
    ],
    'codice_fornitore': [
        'codice fornitore', 'cod. fornitore', 'supplier code', 'vendor code',
        'codice_fornitore', 'supplier_code', 'vendor_code', 'cod_fornitore',
        'cod fornitore',
    ],
    'accred_albo': [
        'accred.albo', 'accreditamento albo', 'albo fornitori', 'accreditato',
        'accred_albo', 'qualified supplier', 'supplier accreditation',
        'albo', 'accreditamento', 'qualificato', 'approved supplier',
        'fornitore accreditato', 'accred. albo',
    ],

    # ── Struttura / CDC ──────────────────────────────────────────
    'str_ric': [
        'str./ric.', 'struttura/ricerca', 'struttura ricerca',
        'str_ric', 'area', 'structure', 'divisione',
        'str./ric', 'struttura e ricerca', 'ricerca struttura',
        'str ric', 'struttura/ric.',
    ],
    'cdc': [
        'cdc', 'centro di costo abbreviato', 'cost center code', 'bu code',
        'business unit code', 'cod cdc',
        # Varianti comuni
        'business unit', 'bu', 'cost_center_code', 'cost center',
        'organizational unit', 'org unit', 'profit center',
        'division code', 'department code', 'entity',
    ],
    'centro_costo': [
        'centro di costo', 'centro costo', 'cost center', 'cost_center',
        'centro_costo', 'cc', 'cdc esteso',
    ],
    'desc_cdc': [
        'descrizione centro di costo', 'desc cdc', 'cost center description',
        'desc_cdc', 'descrizione_cdc', 'nome centro costo',
    ],

    # ── Buyer / Utente ────────────────────────────────────────────
    'utente_pres': [
        'utente per presentazione', 'buyer', 'responsabile', 'purchaser',
        'utente_presentazione', 'buyer name', 'nome buyer', 'gestore',
        'utente_pres', 'presentation user',
        # Varianti inglese / HR
        'responsible', 'responsible person', 'account manager',
        'procurement officer', 'purchasing officer', 'purchase owner',
        'owner', 'assigned to', 'handler', 'case owner',
        'buyer name', 'buyer_name', 'acquirente', 'referente acquisti',
        'referente', 'referente ua', 'referente ufficio acquisti',
    ],
    'utente': [
        'utente', 'user', 'operatore', 'utente sistema', 'user name',
        'username', 'created by', 'operatore acquisti',
    ],
    'cod_utente': [
        'cod.utente', 'codice utente', 'user code', 'cod utente',
        'cod_utente', 'user_code',
    ],

    # ── Categorie ────────────────────────────────────────────────
    'macro_cat': [
        'macro categorie', 'macro categoria', 'macro category', 'categoria',
        'macro_cat', 'macro_categoria', 'categoria merceologica', 'commodity type',
        'category', 'spend category',
    ],
    'grp_merceol': [
        'grp.merceol.', 'gruppo merceologico', 'commodity group', 'commodity_group',
        'grp_merceol', 'gruppo_merceol', 'gruppo merceol.', 'material group',
    ],
    'desc_merceol': [
        'descrizione gruppo merceologic', 'descrizione gruppo merceologico',
        'desc merceol', 'commodity group description', 'desc_merceol',
        'descrizione_merceol', 'descrizione gruppo',
    ],

    # ── Negoziazione / Valuta ────────────────────────────────────
    'negoziazione': [
        'negoziazione', 'negoziato', 'negotiation', 'negotiated',
        'flag negoziazione', 'negoziazione_flag', 'is_negotiated',
    ],
    'valuta': [
        'valuta', 'currency', 'moneta', 'currency code', 'valuta_ordine',
    ],
    'cambio': [
        'cambio', 'exchange rate', 'tasso cambio', 'fx rate',
        'tasso_cambio', 'exchange_rate',
    ],
    'tail_spend': [
        'tail spend', 'tail_spend', 'coda acquisti', 'small spend',
    ],

    # ── NC specifico ──────────────────────────────────────────────
    'non_conformita': [
        'non conformità', 'non conformita', 'non_conformita', 'nc', 'nonconformity',
        'non-conformity', 'difformità', 'anomalia',
    ],
    'data_origine': [
        'data origine', 'origin date', 'data_origine', 'nc date', 'data nc',
        'data segnalazione',
    ],
    'delta_giorni': [
        'delta giorni (fattura - origine)', 'delta giorni (fattura origine)',
        'delta giorni', 'delta_giorni', 'days delta', 'giorni delta',
        'lead time', 'delta gg',
    ],
    'tipo_origine': [
        'tipo origine', 'origin type', 'tipo_origine', 'nc type', 'tipo nc',
        'category nc',
    ],

    # ── Tempi ─────────────────────────────────────────────────────
    'year_month': [
        'year_month', 'anno_mese', 'mese', 'month', 'periodo', 'period',
        'ym', 'anno mese',
    ],
    'days_purchasing': [
        'days_purchasing', 'giorni acquisti', 'days purchasing', 'days ua',
        'giorni_ua', 'purchasing days', 'phase_ua_days',
    ],
    'days_auto': [
        'days_auto', 'giorni automatici', 'days auto', 'auto days',
        'giorni_auto', 'automatic days',
    ],
    'days_other': [
        'days_other', 'altri giorni', 'days other', 'other days',
        'giorni_altri',
    ],
    'total_days': [
        'total_days', 'totale giorni', 'total days', 'giorni totali',
        'days total', 'throughput time',
    ],
    'bottleneck': [
        'bottleneck', 'collo di bottiglia', 'bottleneck phase',
        'fase critica', 'bottleneck_phase',
    ],

    # ── Risorse ──────────────────────────────────────────────────
    'risorsa': [
        'risorsa', 'resource', 'nome risorsa', 'risorsa_nome',
        'employee', 'operatore', 'nome operatore',
        # NB: 'buyer' e 'utente' sono in utente_pres per evitare ambiguità
        # In contesto risorse, l'L5 (value inference) disambigua dai valori
    ],
    'pratiche_gestite': [
        'pratiche gestite', 'pratiche', 'cases handled', 'documents managed',
        'ordini gestiti', 'pratiche_gestite', 'workload', 'numero pratiche',
        'orders handled', 'cases',
    ],
    'pratiche_aperte': [
        'pratiche aperte', 'open cases', 'backlog', 'pratiche_aperte',
        'open orders', 'pending cases', 'in lavorazione',
    ],
    'pratiche_chiuse': [
        'pratiche chiuse', 'closed cases', 'pratiche_chiuse',
        'closed orders', 'completate',
    ],
    'saving_generato': [
        'saving generato', 'saving risorsa', 'savings generated',
        'saving_generato', 'saving by buyer', 'risparmio generato',
    ],
    'negoziazioni_concluse': [
        'negoziazioni concluse', 'negoziazioni', 'negotiations completed',
        'negoziazioni_concluse', 'negotiated cases',
        # Varianti inglesi brevi
        'negotiations', 'deals closed', 'deals done',
        'trattative concluse', 'trattative', 'neg. concluse',
        'n. negoziazioni', 'num. negoziazioni',
    ],
    'tempo_medio_risorsa': [
        'tempo medio', 'avg cycle time', 'tempo_medio', 'average time',
        'media giorni', 'avg_days', 'giorni medi',
        # Varianti brevi inglesi
        'avg days', 'average days', 'avg time', 'mean days',
        'throughput', 'cycle time', 'elapsed days', 'gg medi',
        'tempo medio giorni', 'tempo medio (gg)', 'days avg',
    ],
    'efficienza': [
        'efficienza', 'efficiency', 'performance score', 'kpi score',
        'indice efficienza', 'efficiency_score',
    ],

    # ── Articoli (ordini dettagliati) ────────────────────────────
    'item_code': [
        'codice articolo', 'item code', 'article code', 'codice_articolo',
        'item_code', 'product code', 'sku', 'cod articolo', 'codice prodotto',
    ],
    'item_desc': [
        'descrizione articolo', 'item description', 'descrizione', 'desc articolo',
        'item_desc', 'product description', 'desc.articolo', 'articolo',
    ],
    'quantity': [
        'quantità', 'quantity', 'qty', 'quantita', 'qta', 'q.ta', 'q.tà',
    ],
    'unit_price': [
        'prezzo unitario', 'unit price', 'prezzo_unitario', 'unit_price',
        'price', 'costo unitario', 'up', 'p.unit.',
    ],
    'line_amount': [
        'importo riga', 'line amount', 'line_amount', 'imponibile riga',
        'riga importo', 'totale riga', 'row amount',
    ],
    'taxable_amount': [
        'imponibile', 'taxable amount', 'imponibile_totale', 'base imponibile',
        'taxable', 'net amount',
    ],
    'vat_amount': [
        'iva', 'vat', 'imposta', 'tax amount', 'iva_totale', 'vat_amount',
    ],
    'total_amount': [
        'totale documento', 'total document', 'total amount', 'importo totale',
        'totale', 'total', 'total_amount', 'grand total',
    ],
}

# Indice inverso: sinonimo → campo canonico
_SYN_INDEX: Dict[str, str] = {}
for _canon, _syns in SYNONYMS.items():
    for _s in _syns:
        _SYN_INDEX[_s] = _canon


# ══════════════════════════════════════════════════════════════════
# L4 — REGEX PATTERNS
# ══════════════════════════════════════════════════════════════════

REGEX_RULES: List[Tuple[re.Pattern, str, int]] = [
    # (pattern, canonical_field, base_confidence)

    # Tempo medio risorsa — varianti brevi
    (re.compile(r'avg[\s._]*days',                        re.I), 'tempo_medio_risorsa', 88),
    (re.compile(r'average[\s._]*days',                    re.I), 'tempo_medio_risorsa', 88),
    (re.compile(r'mean[\s._]*days',                       re.I), 'tempo_medio_risorsa', 86),
    (re.compile(r'cycle[\s._]*time',                      re.I), 'tempo_medio_risorsa', 84),
    # Negoziazioni concluse — varianti brevi
    (re.compile(r'^negotiation',                          re.I), 'negoziazioni_concluse', 82),

    # Committed / impegnato varianti inglesi
    (re.compile(r'commit+ed[\s._]*(?:eur|€|amount)?', re.I), 'impegnato_eur', 88),
    (re.compile(r'total[\s._]*commit+ed',              re.I), 'impegnato_eur', 86),
    (re.compile(r'net[\s._]*(?:spend|amount)[\s._]*eur', re.I), 'impegnato_eur', 84),
    (re.compile(r'actual[\s._]*(?:spend|amount)',       re.I), 'impegnato_eur', 82),
    (re.compile(r'purchase[\s._]*amount[\s._]*eur',     re.I), 'impegnato_eur', 84),
    # Responsible / buyer varianti
    (re.compile(r'respons[ai]b',                       re.I), 'utente_pres', 82),
    (re.compile(r'account[\s._]*manag',                re.I), 'utente_pres', 80),
    # Saving con cifra o suffisso EUR/€
    (re.compile(r'sav(ing)?[\s._]*(eur|€|\d)', re.I),   'saving_eur',     88),
    (re.compile(r'%\s*sav(ing)?',                re.I),  'perc_saving_eur',88),

    # Importi EUR
    (re.compile(r'imp[\s._]*iniz[\w\s]*[€e]',    re.I),  'listino_eur',    90),
    (re.compile(r'imp[\s._]*neg[\w\s]*[€e]',     re.I),  'impegnato_eur',  90),
    (re.compile(r'imp[\s._]*iniz',               re.I),  'listino_val',    82),
    (re.compile(r'imp[\s._]*neg',                re.I),  'impegnato_val',  82),

    # Date
    (re.compile(r'data[\s._]*doc',               re.I),  'data_doc',       90),
    (re.compile(r'data[\s._]*orig',              re.I),  'data_origine',   88),
    (re.compile(r'data[\s._]*iniz',              re.I),  'data_inizio_comp',88),
    (re.compile(r'data[\s._]*fine',              re.I),  'data_fine_comp', 88),
    (re.compile(r'data[\s._]*scad',              re.I),  'data_fine_comp', 86),

    # CDC / struttura
    (re.compile(r'\bcdc\b',                      re.I),  'cdc',            92),
    (re.compile(r'str[\s._]*/[\s._]*ric',        re.I),  'str_ric',        92),
    (re.compile(r'centro[\s._]*di[\s._]*costo',  re.I),  'centro_costo',   90),

    # Fornitore
    (re.compile(r'rag[\s._]*soc',                re.I),  'ragione_sociale',88),
    (re.compile(r'cod[\s._]*forn',               re.I),  'codice_fornitore',88),

    # Albo
    (re.compile(r'accred[\w\s]*albo',            re.I),  'accred_albo',    90),

    # Tempi
    (re.compile(r'days[\s._]*purch',             re.I),  'days_purchasing',92),
    (re.compile(r'days[\s._]*auto',              re.I),  'days_auto',      92),
    (re.compile(r'total[\s._]*days',             re.I),  'total_days',     92),
    (re.compile(r'year[\s._]*month',             re.I),  'year_month',     92),

    # Risorse
    (re.compile(r'pratich[\s._]*gest',           re.I),  'pratiche_gestite',90),
    (re.compile(r'pratich[\s._]*apert',          re.I),  'pratiche_aperte', 90),
    (re.compile(r'pratich[\s._]*chius',          re.I),  'pratiche_chiuse', 90),
    (re.compile(r'saving[\s._]*gen',             re.I),  'saving_generato', 88),
    (re.compile(r'negozi[\s._]*con',             re.I),  'negoziazioni_concluse',88),
]


# ══════════════════════════════════════════════════════════════════
# DATACLASSES
# ══════════════════════════════════════════════════════════════════

@dataclass
class FieldMapping:
    canonical: str
    source_column: str
    confidence: float          # 0.0 – 1.0
    method: str                # exact|normalized|synonym|regex|value|context
    is_critical: bool = False
    is_optional: bool = False

@dataclass
class MappingResult:
    family: FileFamily
    family_confidence: float
    family_candidate_scores: Dict[str, float]
    fields: Dict[str, FieldMapping]       # canonical → FieldMapping
    overall_confidence: Confidence
    overall_score: float                  # 0.0 – 1.0
    missing_critical: List[str]
    missing_optional: List[str]
    available_analyses: List[str]
    blocked_analyses: List[Dict]          # {analysis, reason}
    warnings: List[str]
    sheet_name: str
    header_row: int
    raw_columns: List[str]

    def get_col(self, canonical: str) -> Optional[str]:
        fm = self.fields.get(canonical)
        return fm.source_column if fm else None


# ══════════════════════════════════════════════════════════════════
# HEADER ROW DETECTION
# ══════════════════════════════════════════════════════════════════

def detect_header_row(df_raw: pd.DataFrame, max_scan: int = 10) -> int:
    """
    Trova la riga header scansionando le prime max_scan righe.
    Euristica: la riga con più celle stringa non-null e non-numero è l'header.
    """
    best_row, best_score = 0, 0
    for i in range(min(max_scan, len(df_raw))):
        row = df_raw.iloc[i]
        # Conta valori che sembrano etichette (stringa, non numero puro)
        score = 0
        for v in row:
            if pd.isna(v): continue
            s = str(v).strip()
            if s and not _is_pure_number(s) and len(s) > 1:
                score += 1
        # Bonus se alcune celle matchano sinonimi noti
        for v in row:
            if pd.isna(v): continue
            if _normalize(str(v)) in _SYN_INDEX:
                score += 3
        if score > best_score:
            best_score, best_row = score, i
    return best_row


def _is_pure_number(s: str) -> bool:
    try:
        float(s.replace(',', '.').replace(' ', ''))
        return True
    except ValueError:
        return False


# ══════════════════════════════════════════════════════════════════
# NORMALIZATION
# ══════════════════════════════════════════════════════════════════

def _normalize(s: str) -> str:
    """Normalizza: lowercase, strip accenti comuni, rimuovi punct multipla."""
    s = s.strip().lower()
    s = s.replace('à','a').replace('è','e').replace('é','e').replace('ì','i').replace('ò','o').replace('ù','u')
    s = re.sub(r'[_\-]+', ' ', s)          # underscore/dash → spazio
    s = re.sub(r'\s+', ' ', s)             # spazi multipli
    s = re.sub(r'[€£]', '€', s)            # normalizza simbolo euro
    return s.strip()


# ══════════════════════════════════════════════════════════════════
# VALUE-BASED INFERENCE (L5)
# ══════════════════════════════════════════════════════════════════

def _infer_from_values(col_name: str, series: pd.Series) -> Tuple[Optional[str], float]:
    """
    Inferisce il tipo canonico dal contenuto delle celle.
    Ritorna (campo_canonico, confidenza 0.0-1.0) o (None, 0.0).
    """
    name = _normalize(col_name)

    # Sample valori non-null
    sample_raw = series.dropna().head(20).tolist()
    if not sample_raw:
        return None, 0.0

    str_vals = [str(v).strip().upper() for v in sample_raw]
    str_vals_orig = [str(v).strip() for v in sample_raw]

    # Timestamp → data_doc
    if any(isinstance(v, (pd.Timestamp,)) for v in sample_raw[:5]):
        return 'data_doc', 0.97

    # OPR/ORN/OS → alfa_documento
    ratio_alfa = sum(1 for v in str_vals if v in KNOWN_ALFA_DOC) / len(str_vals)
    if ratio_alfa >= 0.85:
        return 'alfa_documento', min(0.99, 0.90 + ratio_alfa * 0.1)

    # RICERCA/STRUTTURA → str_ric
    ratio_strric = sum(1 for v in str_vals if v in KNOWN_STR_RIC) / len(str_vals)
    if ratio_strric >= 0.90:
        return 'str_ric', 0.98

    # GD/TIGEM/TIGET/FT → cdc
    ratio_cdc = sum(1 for v in str_vals if v in KNOWN_CDC) / len(str_vals)
    if ratio_cdc >= 0.85:
        return 'cdc', 0.97

    # EURO/USD → valuta
    ratio_val = sum(1 for v in str_vals if v in KNOWN_VALUTE) / len(str_vals)
    if ratio_val >= 0.90:
        return 'valuta', 0.98

    # SI/NO → flag (differenziamo con il nome)
    ratio_sino = sum(1 for v in str_vals if v in {'SI','SÌ','NO'}) / len(str_vals)
    if ratio_sino >= 0.90:
        if any(k in name for k in ['negoz']):       return 'negoziazione', 0.95
        if any(k in name for k in ['albo','accred']): return 'accred_albo', 0.95
        if any(k in name for k in ['tail']):         return 'tail_spend', 0.88
        return 'flag_generico', 0.65

    # Valori con / pattern (GMR24T.../00053) → commessa
    ratio_comm = sum(1 for v in str_vals_orig if '/' in v and len(v) > 8) / len(str_vals_orig)
    if ratio_comm >= 0.75:
        if any(k in name for k in ['commessa','prot','protocol']):
            return 'protoc_commessa', 0.95
        return 'protoc_generico', 0.72

    # Stato DMS
    ratio_dms = sum(1 for v in str_vals if v in KNOWN_DMS_STATES) / len(str_vals)
    if ratio_dms >= 0.70:
        return 'stato_dms', 0.92

    # Colonne numeriche
    nums = []
    for v in sample_raw:
        try: nums.append(float(v))
        except: pass

    if len(nums) >= len(sample_raw) * 0.8:
        avg = sum(nums) / len(nums)
        max_v = max(nums)
        min_v = min(nums)

        # Cambio: vicino a 1
        if 0.3 <= avg <= 8.0 and max_v < 20 and any(k in name for k in ['cambio','exchange','tasso']):
            return 'cambio', 0.92

        # Percentuale (0-1 decimale)
        if 0 <= max_v <= 1.01:
            if any(k in name for k in ['%','perc','saving','sav']):
                return 'perc_saving_eur', 0.88

        # Percentuale (0-100)
        if 0 <= max_v <= 100 and avg < 60:
            if any(k in name for k in ['%','perc','saving','sav']):
                return 'perc_saving_val', 0.84

        # Saving con nome
        if 'saving' in name and '€' in name:
            if avg >= 0: return 'saving_eur', 0.92

        # Grandi importi EUR
        if avg > 1000 and '€' in name:
            if 'neg' in name or 'impegn' in name: return 'impegnato_eur', 0.92
            if 'iniz' in name or 'list' in name:  return 'listino_eur', 0.92
            return 'listino_eur', 0.78

        # Grandi importi senza EUR
        if avg > 500 and any(k in name for k in ['imp','importo','amount']):
            if 'neg' in name: return 'impegnato_val', 0.85
            return 'listino_val', 0.80

        # Giorni (tempi)
        if 0 < avg < 200 and max_v < 500:
            if any(k in name for k in ['day','days','purch']): return 'days_purchasing', 0.88
            if any(k in name for k in ['auto','automatic']):   return 'days_auto', 0.88
            if any(k in name for k in ['total','tot']):        return 'total_days', 0.86

    # Nomi aziendali
    if len(str_vals_orig) >= 3:
        avg_len = sum(len(v) for v in str_vals_orig) / len(str_vals_orig)
        if avg_len > 8 and not nums:
            if any(k in name for k in ['fornitore','supplier','vendor','rag']):
                return 'ragione_sociale', 0.90
            if any(k in name for k in ['utente','buyer','risorsa','resource']):
                return 'utente_pres', 0.85

    return None, 0.0


# ══════════════════════════════════════════════════════════════════
# CORE MAPPER — 7 layer
# ══════════════════════════════════════════════════════════════════

def map_single_column(col_name: str, series: pd.Series) -> Optional[FieldMapping]:
    """
    Applica i 7 layer per mappare una singola colonna.
    Ritorna il FieldMapping con la confidenza più alta, o None.
    """
    norm = _normalize(col_name)
    best_canonical: Optional[str] = None
    best_conf: float = 0.0
    best_method: str = 'none'

    # ── L1: Exact match ──────────────────────────────────────────
    for canon, syns in SYNONYMS.items():
        if col_name.strip() in syns:
            if 1.0 > best_conf:
                best_canonical, best_conf, best_method = canon, 1.0, 'exact'

    # ── L2: Normalized match ─────────────────────────────────────
    if norm in _SYN_INDEX and best_conf < 0.98:
        c = _SYN_INDEX[norm]
        if 0.95 > best_conf:
            best_canonical, best_conf, best_method = c, 0.95, 'normalized'

    # ── L3: Synonym substring match ──────────────────────────────
    if best_conf < 0.92:
        for syn, canon in _SYN_INDEX.items():
            if norm == syn:
                if 0.92 > best_conf:
                    best_canonical, best_conf, best_method = canon, 0.92, 'synonym'
                break
            # Partial: colonna contiene il sinonimo
            if len(syn) >= 5 and syn in norm and len(norm) < len(syn) + 10:
                conf = 0.82
                if conf > best_conf:
                    best_canonical, best_conf, best_method = canon, conf, 'synonym_partial'

    # ── L4: Regex rules ──────────────────────────────────────────
    if best_conf < 0.88:
        for pattern, canon, base_conf in REGEX_RULES:
            if pattern.search(col_name):
                conf = base_conf / 100.0
                if conf > best_conf:
                    best_canonical, best_conf, best_method = canon, conf, 'regex'

    # ── L5: Value inference ──────────────────────────────────────
    val_canon, val_conf = _infer_from_values(col_name, series)
    if val_canon and val_conf > best_conf:
        best_canonical, best_conf, best_method = val_canon, val_conf, 'value'
    elif val_canon and val_conf >= best_conf - 0.05:
        # Valore coerente con nome → boost leggero
        if val_canon == best_canonical:
            best_conf = min(1.0, best_conf + 0.04)

    if not best_canonical or best_conf < 0.50:
        return None

    return FieldMapping(
        canonical=best_canonical,
        source_column=col_name,
        confidence=round(best_conf, 3),
        method=best_method,
    )


def build_column_map(df: pd.DataFrame) -> Dict[str, FieldMapping]:
    """
    Mappa tutte le colonne del DataFrame.
    In caso di conflitto sullo stesso canonical, vince la confidenza più alta.
    """
    result: Dict[str, FieldMapping] = {}
    for col in df.columns:
        fm = map_single_column(col, df[col])
        if fm:
            existing = result.get(fm.canonical)
            if not existing or fm.confidence > existing.confidence:
                result[fm.canonical] = fm
    return result


# ══════════════════════════════════════════════════════════════════
# FILE FAMILY CLASSIFIER
# ══════════════════════════════════════════════════════════════════

FAMILY_SIGNALS: Dict[FileFamily, Dict[str, float]] = {
    FileFamily.SAVINGS: {
        'alfa_documento':  3.0,  # OPR/ORN/OS → molto specifico
        'str_ric':         2.0,
        'listino_eur':     2.5,
        'impegnato_eur':   2.5,
        'saving_eur':      3.0,
        'negoziazione':    2.0,
        'accred_albo':     1.5,
        'cdc':             1.5,
        'data_doc':        1.0,
        'ragione_sociale': 1.0,
    },
    FileFamily.ORDERS_DETAIL: {
        'item_code':        3.0,
        'item_desc':        2.5,
        'quantity':         2.5,
        'unit_price':       2.0,
        'line_amount':      2.0,
        'taxable_amount':   1.5,
        'vat_amount':       1.5,
        'total_amount':     1.5,
        'data_inizio_comp': 2.0,
        'data_fine_comp':   2.0,
        'data_doc':         1.0,
    },
    FileFamily.NC: {
        'non_conformita':  4.0,
        'data_origine':    2.5,
        'delta_giorni':    2.5,
        'tipo_origine':    2.0,
        'ragione_sociale': 1.0,
    },
    FileFamily.TEMPI: {
        'year_month':      3.0,
        'days_purchasing': 3.5,
        'days_auto':       2.5,
        'total_days':      2.5,
        'bottleneck':      2.0,
    },
    FileFamily.RISORSE: {
        'risorsa':             4.0,
        'pratiche_gestite':    3.5,
        'pratiche_aperte':     2.5,
        'pratiche_chiuse':     2.5,
        'saving_generato':     2.0,
        'negoziazioni_concluse':2.0,
        'tempo_medio_risorsa': 2.0,
        'efficienza':          2.0,
    },
    FileFamily.SUPPLIER_MASTER: {
        'codice_fornitore': 3.0,
        'ragione_sociale':  2.0,
        'accred_albo':      2.5,
        'data_inizio_comp': 2.0,   # qualification_date
        'data_fine_comp':   2.0,   # expiry_date
    },
}

def classify_file_family(
    col_map: Dict[str, FieldMapping],
    sheet_name: str = '',
    df: Optional[pd.DataFrame] = None,
) -> Tuple[FileFamily, float, Dict[str, float]]:
    """
    Classifica la famiglia del file.
    Ritorna (family, confidence 0-1, {family: score_normalized}).
    """
    # Sheet name hints (bonus)
    sn = sheet_name.lower()
    sheet_bonus: Dict[FileFamily, float] = {
        FileFamily.SAVINGS:         3.0 if any(k in sn for k in ['saving','risparmio','negozia','ordini']) else 0,
        FileFamily.ORDERS_DETAIL:   3.0 if any(k in sn for k in ['ordini','order','dettagli','lines']) else 0,
        FileFamily.NC:              3.0 if any(k in sn for k in ['nc','conformit','nonconf']) else 0,
        FileFamily.TEMPI:           3.0 if any(k in sn for k in ['tempi','tempo','time','attraversa']) else 0,
        FileFamily.RISORSE:         3.0 if any(k in sn for k in ['risor','resource','buyer','team','workload']) else 0,
        FileFamily.SUPPLIER_MASTER: 3.0 if any(k in sn for k in ['supplier','fornitore','albo']) else 0,
    }

    scores: Dict[str, float] = {}
    max_scores: Dict[str, float] = {}

    for family, signals in FAMILY_SIGNALS.items():
        score = sheet_bonus.get(family, 0.0)
        max_score = sum(signals.values()) + 3.0  # +3 for sheet bonus

        for field, weight in signals.items():
            fm = col_map.get(field)
            if fm:
                score += weight * fm.confidence

        scores[family.value] = score
        max_scores[family.value] = max_score

    # Normalizza
    norm_scores = {}
    for fam, s in scores.items():
        ms = max_scores.get(fam, 1)
        norm_scores[fam] = round(s / ms, 3) if ms else 0

    best_fam_str = max(norm_scores, key=norm_scores.get)
    best_fam = FileFamily(best_fam_str)
    best_conf = norm_scores[best_fam_str]

    # Se il punteggio massimo è troppo basso, è UNKNOWN
    if best_conf < 0.15:
        return FileFamily.UNKNOWN, best_conf, norm_scores

    return best_fam, best_conf, norm_scores


# ══════════════════════════════════════════════════════════════════
# ANALYSIS AVAILABILITY
# ══════════════════════════════════════════════════════════════════

ANALYSES_BY_FAMILY: Dict[FileFamily, Dict[str, List[str]]] = {
    FileFamily.SAVINGS: {
        'available': [
            ('KPI Riepilogo', ['listino_eur','impegnato_eur','saving_eur']),
            ('Saving YoY', ['data_doc','saving_eur']),
            ('Per Tipo Documento', ['alfa_documento','listino_eur']),
        ],
        'optional': [
            ('Saving per CDC', ['cdc']),
            ('Saving per Buyer', ['utente_pres']),
            ('% Negoziati', ['negoziazione']),
            ('Analisi Albo Fornitori', ['accred_albo']),
            ('Saving per Macro Categoria', ['macro_cat']),
            ('Analisi Valutaria', ['valuta','cambio']),
        ],
    },
    FileFamily.NC: {
        'available': [
            ('KPI Non Conformità', ['non_conformita']),
            ('Andamento mensile NC', ['data_origine','non_conformita']),
        ],
        'optional': [
            ('Top fornitori NC', ['ragione_sociale','non_conformita']),
            ('NC per tipo', ['tipo_origine','non_conformita']),
            ('Delta giorni', ['delta_giorni']),
        ],
    },
    FileFamily.TEMPI: {
        'available': [
            ('KPI Tempi', ['total_days']),
            ('Trend mensile tempi', ['year_month','total_days']),
        ],
        'optional': [
            ('Fase Acquisti', ['days_purchasing']),
            ('Fase Automatica', ['days_auto']),
            ('Bottleneck analysis', ['bottleneck']),
        ],
    },
    FileFamily.RISORSE: {
        'available': [
            ('Overview team', ['risorsa','pratiche_gestite']),
        ],
        'optional': [
            ('Saving per risorsa', ['saving_generato']),
            ('Negoziazioni per risorsa', ['negoziazioni_concluse']),
            ('Tempo medio per risorsa', ['tempo_medio_risorsa']),
            ('Efficienza risorsa', ['efficienza']),
        ],
    },
}

def compute_available_analyses(
    family: FileFamily,
    col_map: Dict[str, FieldMapping],
) -> Tuple[List[str], List[Dict]]:
    """
    Ritorna (analisi_disponibili, analisi_bloccate_con_motivo).
    """
    available = []
    blocked = []

    family_def = ANALYSES_BY_FAMILY.get(family, {})

    for label, required_fields in family_def.get('available', []):
        missing = [f for f in required_fields if f not in col_map]
        if not missing:
            available.append(label)
        else:
            missing_labels = [SYNONYMS.get(f, [f])[0] if f in SYNONYMS else f for f in missing]
            blocked.append({
                'analysis': label,
                'reason': f"Campi mancanti: {', '.join(missing_labels)}",
                'missing_fields': missing,
                'severity': 'critical',
            })

    for label, opt_fields in family_def.get('optional', []):
        missing = [f for f in opt_fields if f not in col_map]
        if not missing:
            available.append(label)
        else:
            missing_labels = [SYNONYMS.get(f, [f])[0] if f in SYNONYMS else f for f in missing]
            blocked.append({
                'analysis': label,
                'reason': f"Campo opzionale non trovato: {', '.join(missing_labels)}",
                'missing_fields': missing,
                'severity': 'optional',
            })

    return available, blocked


# ══════════════════════════════════════════════════════════════════
# CRITICAL FIELDS PER FAMILY
# ══════════════════════════════════════════════════════════════════

CRITICAL_FIELDS: Dict[FileFamily, List[str]] = {
    FileFamily.SAVINGS:  ['data_doc', 'ragione_sociale'],
    FileFamily.NC:       ['non_conformita'],
    FileFamily.TEMPI:    ['total_days', 'year_month'],
    FileFamily.RISORSE:  ['risorsa', 'pratiche_gestite'],
    FileFamily.ORDERS_DETAIL: ['data_doc', 'ragione_sociale'],
    FileFamily.SUPPLIER_MASTER: ['ragione_sociale'],
}

OPTIONAL_FIELDS: Dict[FileFamily, List[str]] = {
    FileFamily.SAVINGS: [
        'cdc', 'negoziazione', 'accred_albo', 'macro_cat',
        'utente_pres', 'protoc_commessa', 'valuta', 'cambio',
        'alfa_documento', 'str_ric',
    ],
    FileFamily.NC:       ['ragione_sociale', 'tipo_origine', 'delta_giorni'],
    FileFamily.TEMPI:    ['days_purchasing', 'days_auto', 'days_other', 'bottleneck'],
    FileFamily.RISORSE:  ['saving_generato', 'negoziazioni_concluse', 'tempo_medio_risorsa', 'efficienza'],
}


# ══════════════════════════════════════════════════════════════════
# MAIN INSPECTOR — entry point pubblico
# ══════════════════════════════════════════════════════════════════

def inspect_workbook(
    xl: pd.ExcelFile,
    user_sheet: Optional[str] = None,
) -> MappingResult:
    """
    Ispeziona un workbook Excel e produce un MappingResult completo.
    
    Parametri:
        xl:          pd.ExcelFile già aperto
        user_sheet:  nome foglio specificato dall'utente (opzionale)
    """
    # 1. Seleziona il foglio migliore
    sheet = _select_best_sheet(xl, user_sheet)

    # 2. Leggi raw per header detection
    df_raw = pd.read_excel(xl, sheet_name=sheet, header=None, nrows=15)
    header_row = detect_header_row(df_raw)

    # 3. Leggi SAMPLE per il mapping (nrows=200 è abbastanza per 7-layer inference)
    # Il full read viene fatto UNA SOLA VOLTA da upload_engine durante la normalizzazione
    INSPECT_ROWS = 200
    df = pd.read_excel(xl, sheet_name=sheet, header=header_row, nrows=INSPECT_ROWS)
    df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
    df.columns = [str(c).strip() for c in df.columns]

    # 4. Mappa colonne (7 layer) — sul sample, identico al full df
    col_map = build_column_map(df)

    # 5. Classifica famiglia
    family, fam_conf, fam_scores = classify_file_family(col_map, sheet, df)

    # 6. Marca campi critici e opzionali
    for canon, fm in col_map.items():
        fm.is_critical = canon in CRITICAL_FIELDS.get(family, [])
        fm.is_optional = canon in OPTIONAL_FIELDS.get(family, [])

    # 7. Campi mancanti
    missing_critical = [f for f in CRITICAL_FIELDS.get(family, []) if f not in col_map]
    missing_optional = [f for f in OPTIONAL_FIELDS.get(family, []) if f not in col_map]

    # 8. Calcola confidenza complessiva
    if not col_map:
        overall_score = 0.0
    else:
        mapped_conf = sum(fm.confidence for fm in col_map.values()) / len(col_map)
        coverage_crit = 1.0 - (len(missing_critical) / max(len(CRITICAL_FIELDS.get(family, [])), 1))
        overall_score = round(fam_conf * 0.4 + mapped_conf * 0.3 + coverage_crit * 0.3, 3)

    if overall_score >= 0.85 and not missing_critical:
        overall_confidence = Confidence.HIGH
    elif overall_score >= 0.60:
        overall_confidence = Confidence.MEDIUM
    else:
        overall_confidence = Confidence.LOW

    # 9. Analisi disponibili / bloccate
    available, blocked = compute_available_analyses(family, col_map)

    # 10. Warnings
    warnings = []
    if missing_critical:
        labels = [SYNONYMS.get(f, [f])[0] for f in missing_critical]
        warnings.append(f"Campi critici non trovati: {', '.join(labels)}")
    for mf in missing_optional[:3]:
        label = SYNONYMS.get(mf, [mf])[0]
        warnings.append(f"Campo opzionale '{label}' non trovato — alcune analisi non disponibili")
    if family == FileFamily.UNKNOWN:
        warnings.append("Tipo file non riconosciuto. Usare il template standard per il massimo della compatibilità.")

    return MappingResult(
        family=family,
        family_confidence=round(fam_conf, 3),
        family_candidate_scores=fam_scores,
        fields=col_map,
        overall_confidence=overall_confidence,
        overall_score=overall_score,
        missing_critical=missing_critical,
        missing_optional=missing_optional,
        available_analyses=available,
        blocked_analyses=blocked,
        warnings=warnings,
        sheet_name=sheet,
        header_row=header_row,
        raw_columns=list(df.columns),
    )


def _select_best_sheet(xl: pd.ExcelFile, preferred: Optional[str] = None) -> str:
    """
    Seleziona il foglio con più colonne riconoscibili.
    USA nrows=100 per ispezione veloce — il full read avviene dopo.
    Su file con 10K righe: da ~4000ms a ~200ms.
    """
    if preferred and preferred in xl.sheet_names:
        return preferred

    if len(xl.sheet_names) == 1:
        return xl.sheet_names[0]

    best, best_score = xl.sheet_names[0], -1
    for s in xl.sheet_names:
        try:
            # nrows=100: abbastanza per il mapping, molto più veloce del full read
            df = pd.read_excel(xl, sheet_name=s, nrows=100)
            df.columns = [str(c).strip() for c in df.columns]
            # Filtra colonne Unnamed
            df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
            col = build_column_map(df)
            # Score = colonne riconosciute * 100 + stima righe (da metadati, non full read)
            # Stima righe: usa la dimensione del campione come proxy
            score = len(col) * 100 + len(df)
            if score > best_score:
                best_score, best = score, s
        except Exception:
            pass
    return best


# ══════════════════════════════════════════════════════════════════
# SERIALIZER — per risposta API
# ══════════════════════════════════════════════════════════════════

def mapping_result_to_dict(mr: MappingResult) -> dict:
    """Serializza MappingResult per la risposta JSON."""
    return {
        'family':                   mr.family.value,
        'family_label':             _FAMILY_LABELS.get(mr.family, mr.family.value),
        'family_confidence':        mr.family_confidence,
        'family_candidate_scores':  mr.family_candidate_scores,
        'overall_confidence':       mr.overall_confidence.value,
        'overall_score':            mr.overall_score,
        'sheet_name':               mr.sheet_name,
        'header_row':               mr.header_row,
        'raw_columns':              mr.raw_columns,
        'mapped_fields': [
            {
                'canonical':     fm.canonical,
                'source_column': fm.source_column,
                'confidence':    fm.confidence,
                'method':        fm.method,
                'is_critical':   fm.is_critical,
                'is_optional':   fm.is_optional,
            }
            for fm in sorted(mr.fields.values(), key=lambda x: -x.confidence)
        ],
        'missing_critical':    mr.missing_critical,
        'missing_optional':    mr.missing_optional,
        'available_analyses':  mr.available_analyses,
        'blocked_analyses':    mr.blocked_analyses,
        'warnings':            mr.warnings,
        'can_proceed':         mr.overall_confidence in (Confidence.HIGH, Confidence.MEDIUM)
                               and not mr.missing_critical,
        'needs_confirmation':  mr.overall_confidence == Confidence.MEDIUM,
        'is_blocked':          mr.overall_confidence == Confidence.LOW or bool(mr.missing_critical),
    }


_FAMILY_LABELS = {
    FileFamily.SAVINGS:         'File Saving / Ordini',
    FileFamily.ORDERS_DETAIL:   'Estrazione Ordini Dettagliati',
    FileFamily.NC:              'Non Conformità',
    FileFamily.TEMPI:           'Tempi Attraversamento',
    FileFamily.RISORSE:         'Analisi Risorse / Team',
    FileFamily.SUPPLIER_MASTER: 'Anagrafica Fornitori',
    FileFamily.UNKNOWN:         'Tipo Non Riconosciuto',
}
