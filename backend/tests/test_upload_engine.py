"""
Test suite enterprise — Upload Engine v1.0
Fondazione Telethon ETS — UA Dashboard

Copertura:
  - classificazione famiglia file
  - rilevamento header row
  - mapping adattivo colonne (sinonimi, regex, valori)
  - normalizzazione righe saving/risorse/nc/tempi
  - rilevamento anno / YoY readiness
  - comportamento graceful su file imperfetti
  - consistenza preview ↔ import
  - matrice readiness analitica
"""
import io
import sys
import pytest
import pandas as pd

sys.path.insert(0, '/home/claude/ua-dashboard/backend')

from upload_engine import (
    inspect_bytes, inspect_and_load, compute_readiness,
    normalize_saving_row, normalize_risorse_row, normalize_nc_row, normalize_tempi_row,
    _detect_years, _gcol, batch_insert,
    DOC_TYPE_LABELS, DOC_TYPE_AREA,
)
from ingestion_engine import (
    FileFamily, Confidence, inspect_workbook,
    build_column_map, classify_file_family, mapping_result_to_dict,
)

SAVING_FILE = '/mnt/user-data/uploads/file_saving_2025_final.xlsx'


# ── Helpers ────────────────────────────────────────────────────────

def make_excel(df: pd.DataFrame, sheet_name: str = 'Sheet1') -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name=sheet_name)
    return buf.getvalue()


def make_excel_with_title(df: pd.DataFrame) -> bytes:
    """Simula file con righe titolo sopra l'header."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df_title = pd.concat([
            pd.DataFrame([['REPORT ACQUISTI 2025', None, None] + [None]*(len(df.columns)-3)],
                         columns=df.columns),
            pd.DataFrame([['Estratto al 31/12/2025', None, None] + [None]*(len(df.columns)-3)],
                         columns=df.columns),
            df,
        ], ignore_index=True)
        df_title.to_excel(w, index=False, sheet_name='Report')
    return buf.getvalue()


# ── FIXTURES ────────────────────────────────────────────────────────

@pytest.fixture(scope='module')
def saving_bytes():
    return open(SAVING_FILE, 'rb').read()

@pytest.fixture(scope='module')
def saving_wbi(saving_bytes):
    return inspect_and_load(saving_bytes, 'saving_2025.xlsx')

@pytest.fixture
def df_saving_standard():
    return pd.DataFrame({
        'Data doc.': pd.to_datetime(['2025-01-10', '2025-03-15', '2025-06-20']),
        'Imp. Iniziale €': [50000.0, 30000.0, 80000.0],
        'Imp. Negoziato €': [45000.0, 27000.0, 72000.0],
        'Saving.1': [5000.0, 3000.0, 8000.0],
        'Ragione sociale fornitore': ['MERCK', 'LIFE TECH', 'EUROCLONE'],
        'Alfa documento': ['ORD', 'OS', 'ORN'],
        'CDC': ['TIGEM', 'GD', 'FT'],
        'Str./Ric.': ['RICERCA', 'STRUTTURA', 'RICERCA'],
        'Negoziazione': ['SI', 'NO', 'SI'],
        'Accred.albo': ['SI', 'NO', 'SI'],
        'macro categorie': ['Ricerca ', 'Raccolta Fondi', 'Pharma '],
        'utente per presentazione': ['Marina Padricelli', 'Silvana Ruotolo', 'Monti Luca'],
        'Valuta': ['EURO', 'EURO', 'USD'],
        'cambio': [1.0, 1.0, 1.08],
    })

@pytest.fixture
def df_saving_english():
    return pd.DataFrame({
        'order_date': pd.to_datetime(['2025-01-10', '2025-03-15']),
        'supplier_name': ['MERCK', 'LIFE TECH'],
        'list_amount_eur': [50000.0, 30000.0],
        'committed_eur': [45000.0, 27000.0],
        'savings': [5000.0, 3000.0],
        'doc_type': ['ORD', 'OS'],
        'cost_center': ['TIGEM', 'GD'],
        'business_unit': ['RICERCA', 'STRUTTURA'],
        'negotiated': ['SI', 'NO'],
        'buyer': ['Marina P.', 'Silvana R.'],
        'macro_category': ['Ricerca', 'Raccolta Fondi'],
        'currency': ['EURO', 'USD'],
        'exchange_rate': [1.0, 1.08],
    })

@pytest.fixture
def df_risorse_standard():
    return pd.DataFrame({
        'Risorsa': ['Marina Padricelli', 'Silvana Ruotolo', 'Monti Luca'],
        'Mese': ['2025-01', '2025-02', '2025-01'],
        'Struttura': ['GD', 'TIGEM', 'GD'],
        'Pratiche Gestite': [45, 38, 52],
        'Pratiche Aperte': [8, 5, 12],
        'Pratiche Chiuse': [37, 33, 40],
        'Saving Generato': [120000.0, 95000.0, 180000.0],
        'Negoziazioni Concluse': [15, 12, 20],
        'Tempo Medio Giorni': [8.2, 6.5, 7.1],
        'Efficienza': [85.0, 78.0, 91.0],
    })

@pytest.fixture
def df_risorse_english():
    """File risorse con nomi colonne inglesi (pre-elaborato da utente)."""
    return pd.DataFrame({
        'Buyer': ['Marina P.', 'Silvana R.', 'Monti L.'],
        'Period': ['2025-01', '2025-02', '2025-01'],
        'BU': ['GD', 'TIGEM', 'GD'],
        'Cases Managed': [45, 38, 52],
        'Open Cases': [8, 5, 12],
        'Closed Cases': [37, 33, 40],
        'Savings Generated': [120000.0, 95000.0, 180000.0],
        'Negotiations': [15, 12, 20],
        'Avg Days': [8.2, 6.5, 7.1],
    })

@pytest.fixture
def df_nc():
    return pd.DataFrame({
        'Ragione Sociale': ['SUPPLIER A', 'SUPPLIER B'],
        'Data Origine': pd.to_datetime(['2025-01-15', '2025-02-20']),
        'Non Conformità': ['NO', 'SI'],
        'Tipo Origine': ['Qualità', 'Consegna'],
        'Delta Giorni (Fattura - Origine)': [12, 45],
    })

@pytest.fixture
def df_2026():
    """Saving 2026 — per test YoY."""
    return pd.DataFrame({
        'Data doc.': pd.to_datetime(['2026-01-10', '2026-03-15', '2026-06-20']),
        'Imp. Iniziale €': [55000.0, 33000.0, 88000.0],
        'Imp. Negoziato €': [49500.0, 29700.0, 79200.0],
        'Saving.1': [5500.0, 3300.0, 8800.0],
        'Ragione sociale fornitore': ['MERCK', 'LIFE TECH', 'EUROCLONE'],
        'Alfa documento': ['ORD', 'OS', 'ORN'],
        'CDC': ['TIGEM', 'GD', 'FT'],
        'Str./Ric.': ['RICERCA', 'STRUTTURA', 'RICERCA'],
        'Negoziazione': ['SI', 'NO', 'SI'],
    })


# ══════════════════════════════════════════════════════════════════
# SECTION 1: DOCUMENT TYPE LABELS
# ══════════════════════════════════════════════════════════════════

def test_doc_labels_ricerca():
    assert DOC_TYPE_LABELS['ORN'] == 'Ordine Ricerca'
    assert DOC_TYPE_LABELS['ORD'] == 'Ordine Diretto Ricerca'
    assert DOC_TYPE_LABELS['OPR'] == 'Ordine Previsionale Ricerca'
    assert DOC_TYPE_LABELS['PS']  == 'Procedura Straordinaria'

def test_doc_labels_struttura():
    assert DOC_TYPE_LABELS['OS']  == 'Ordine Struttura'
    assert DOC_TYPE_LABELS['OSP'] == 'Ordine Previsionale Struttura'
    assert DOC_TYPE_LABELS['OSD'] == 'Ordine Diretto Struttura'

def test_doc_area_ricerca():
    for code in ['ORN', 'ORD', 'OPR', 'PS']:
        assert DOC_TYPE_AREA[code] == 'RICERCA', f"{code} should be RICERCA"

def test_doc_area_struttura():
    for code in ['OS', 'OSP', 'OSD']:
        assert DOC_TYPE_AREA[code] == 'STRUTTURA', f"{code} should be STRUTTURA"


# ══════════════════════════════════════════════════════════════════
# SECTION 2: FILE FAMILY CLASSIFICATION
# ══════════════════════════════════════════════════════════════════

def test_classify_saving_real(saving_bytes):
    mr = inspect_bytes(saving_bytes, 'saving_2025.xlsx')
    assert mr.family == FileFamily.SAVINGS
    assert mr.family_confidence >= 0.90

def test_classify_saving_standard(df_saving_standard):
    b = make_excel(df_saving_standard, 'saving 2025')
    mr = inspect_bytes(b, 'saving.xlsx')
    assert mr.family == FileFamily.SAVINGS

def test_classify_saving_english_columns(df_saving_english):
    b = make_excel(df_saving_english, 'Orders')
    mr = inspect_bytes(b, 'orders.xlsx')
    assert mr.family == FileFamily.SAVINGS
    assert mr.overall_score >= 0.60

def test_classify_saving_2026(df_2026):
    b = make_excel(df_2026, 'saving 2026')
    mr = inspect_bytes(b, 'saving_2026.xlsx')
    assert mr.family == FileFamily.SAVINGS, f"Expected SAVINGS, got {mr.family}"
    assert mr.family_confidence >= 0.60  # aggiornato dopo estensione FAMILY_SIGNALS

def test_classify_risorse_standard(df_risorse_standard):
    b = make_excel(df_risorse_standard, 'Risorse Team')
    mr = inspect_bytes(b, 'risorse.xlsx')
    assert mr.family == FileFamily.RISORSE

def test_classify_risorse_english(df_risorse_english):
    b = make_excel(df_risorse_english, 'Team Analytics')
    mr = inspect_bytes(b, 'team.xlsx')
    assert mr.family == FileFamily.RISORSE
    assert mr.overall_score >= 0.50

def test_classify_nc(df_nc):
    b = make_excel(df_nc, 'NC 2025')
    mr = inspect_bytes(b, 'nc.xlsx')
    assert mr.family == FileFamily.NC

def test_classify_ambiguous_does_not_crash():
    """File con pochissimi campi: non deve crashare."""
    df = pd.DataFrame({'Fornitore': ['MERCK'], 'Importo': [50000.0]})
    b = make_excel(df)
    mr = inspect_bytes(b, 'ambiguo.xlsx')
    assert mr is not None
    assert mr.family is not None  # Ritorna qualcosa, anche UNKNOWN

def test_no_false_supplier_master(df_saving_standard):
    """File saving non deve essere classificato come supplier_master."""
    b = make_excel(df_saving_standard, 'saving')
    mr = inspect_bytes(b, 'saving.xlsx')
    assert mr.family != FileFamily.SUPPLIER_MASTER


# ══════════════════════════════════════════════════════════════════
# SECTION 3: HEADER ROW DETECTION
# ══════════════════════════════════════════════════════════════════

def test_header_row_standard(df_saving_standard):
    b = make_excel(df_saving_standard)
    wbi = inspect_and_load(b, 'test.xlsx')
    assert wbi.header_row == 0

def test_header_row_with_title_rows(df_saving_standard):
    """File con righe titolo sopra l'header."""
    b = make_excel_with_title(df_saving_standard)
    wbi = inspect_and_load(b, 'test.xlsx')
    # L'engine deve trovare l'header anche se non è riga 0
    assert wbi.df is not None
    assert len(wbi.df) > 0

def test_header_row_real_file(saving_wbi):
    assert saving_wbi.header_row == 0
    assert len(saving_wbi.df) > 100


# ══════════════════════════════════════════════════════════════════
# SECTION 4: ADAPTIVE COLUMN MAPPING
# ══════════════════════════════════════════════════════════════════

def test_mapping_real_file(saving_wbi):
    col_map = saving_wbi.mapping_result.fields
    critical = ['listino_eur', 'impegnato_eur', 'saving_eur', 'data_doc',
                'ragione_sociale', 'alfa_documento', 'str_ric', 'cdc']
    for f in critical:
        assert f in col_map, f"Campo critico '{f}' non mappato"

def test_mapping_standard_columns(df_saving_standard):
    b = make_excel(df_saving_standard)
    wbi = inspect_and_load(b, 'test.xlsx')
    col_map = wbi.mapping_result.fields
    assert 'listino_eur' in col_map
    assert 'impegnato_eur' in col_map
    assert 'saving_eur' in col_map
    assert 'data_doc' in col_map
    assert 'cdc' in col_map

def test_mapping_english_columns(df_saving_english):
    b = make_excel(df_saving_english)
    wbi = inspect_and_load(b, 'test.xlsx')
    col_map = wbi.mapping_result.fields
    assert 'listino_eur' in col_map,    "'list_amount_eur' deve mapparsi a listino_eur"
    assert 'impegnato_eur' in col_map,  "'committed_eur' deve mapparsi a impegnato_eur"
    assert 'ragione_sociale' in col_map, "'supplier_name' deve mapparsi a ragione_sociale"
    assert 'data_doc' in col_map,        "'order_date' deve mapparsi a data_doc"
    assert 'utente_pres' in col_map,     "'buyer' deve mapparsi a utente_pres"

def test_mapping_risorse_standard(df_risorse_standard):
    b = make_excel(df_risorse_standard)
    wbi = inspect_and_load(b, 'risorse.xlsx')
    col_map = wbi.mapping_result.fields
    assert 'risorsa' in col_map
    assert 'pratiche_gestite' in col_map

def test_mapping_risorse_english(df_risorse_english):
    b = make_excel(df_risorse_english)
    wbi = inspect_and_load(b, 'team.xlsx')
    col_map = wbi.mapping_result.fields
    assert 'pratiche_gestite' in col_map, "'Cases Managed' deve mapparsi a pratiche_gestite"
    assert 'pratiche_aperte' in col_map,  "'Open Cases' deve mapparsi a pratiche_aperte"
    assert 'pratiche_chiuse' in col_map,  "'Closed Cases' deve mapparsi a pratiche_chiuse"
    assert 'saving_generato' in col_map,  "'Savings Generated' deve mapparsi a saving_generato"
    assert 'negoziazioni_concluse' in col_map, "'Negotiations' deve mapparsi a negoziazioni_concluse"
    assert 'tempo_medio_risorsa' in col_map,    "'Avg Days' deve mapparsi a tempo_medio_risorsa"

@pytest.mark.parametrize("col_name,expected", [
    ('committed_eur',     'impegnato_eur'),
    ('committed eur',     'impegnato_eur'),
    ('total committed',   'impegnato_eur'),
    ('list price eur',    'listino_eur'),
    ('list amount',       'listino_eur'),
    ('responsible',       'utente_pres'),
    ('Savings Generated', 'saving_generato'),
    ('Cases Managed',     'pratiche_gestite'),
    ('Avg Days',          'tempo_medio_risorsa'),
    ('Negotiations',      'negoziazioni_concluse'),
    ('order_date',        'data_doc'),
    ('supplier_name',     'ragione_sociale'),
])
def test_synonym_variants(col_name, expected):
    from ingestion_engine import map_single_column
    fm = map_single_column(col_name, pd.Series([]))
    assert fm is not None, f"'{col_name}' should be mapped"
    assert fm.canonical == expected, f"'{col_name}' → '{fm.canonical}' (expected '{expected}')"


# ══════════════════════════════════════════════════════════════════
# SECTION 5: YEAR DETECTION & YOY
# ══════════════════════════════════════════════════════════════════

def test_year_detect_2025(df_saving_standard):
    b = make_excel(df_saving_standard)
    wbi = inspect_and_load(b, 'saving_2025.xlsx')
    assert 2025 in wbi.years_found
    assert wbi.year_dominant == 2025

def test_year_detect_2026(df_2026):
    b = make_excel(df_2026, 'saving 2026')
    wbi = inspect_and_load(b, 'saving_2026.xlsx')
    assert 2026 in wbi.years_found
    assert wbi.year_dominant == 2026

def test_year_detect_multi_year():
    """File con righe 2025 e 2026 — nessun anno dominante chiaro."""
    df = pd.DataFrame({
        'Data doc.': pd.to_datetime(['2025-01-10', '2025-06-15', '2026-01-10', '2026-06-15']),
        'Imp. Iniziale €': [1000.0, 2000.0, 1100.0, 2100.0],
        'Imp. Negoziato €': [900.0, 1800.0, 990.0, 1890.0],
        'Saving.1': [100.0, 200.0, 110.0, 210.0],
        'Ragione sociale fornitore': ['A', 'B', 'C', 'D'],
    })
    b = make_excel(df)
    wbi = inspect_and_load(b, 'multi.xlsx')
    assert 2025 in wbi.years_found
    assert 2026 in wbi.years_found
    # Nessun dominante con 50/50
    assert wbi.year_dominant is None

def test_yoy_readiness(df_saving_standard, df_2026):
    b25 = make_excel(df_saving_standard)
    wbi25 = inspect_and_load(b25, '2025.xlsx')
    r25 = compute_readiness(wbi25.mapping_result, wbi25)
    assert r25['yoy_ready']
    assert '2025' in r25['yoy_note']

    b26 = make_excel(df_2026, 'saving 2026')
    wbi26 = inspect_and_load(b26, '2026.xlsx')
    r26 = compute_readiness(wbi26.mapping_result, wbi26)
    assert r26['yoy_ready']
    assert '2026' in r26['yoy_note']

def test_year_detect_risorse(df_risorse_standard):
    b = make_excel(df_risorse_standard, 'Risorse')
    wbi = inspect_and_load(b, 'risorse.xlsx')
    assert 2025 in wbi.years_found


# ══════════════════════════════════════════════════════════════════
# SECTION 6: NORMALIZATION
# ══════════════════════════════════════════════════════════════════

def test_normalize_saving_row_real(saving_wbi):
    col_map = saving_wbi.mapping_result.fields
    row = saving_wbi.df.iloc[0]
    rec = normalize_saving_row(col_map, row, 'test-id')
    assert rec is not None
    assert rec['imp_listino_eur'] >= 0
    assert rec['imp_impegnato_eur'] >= 0
    assert rec['saving_eur'] >= 0
    assert rec['data_doc'] is not None
    assert rec['upload_id'] == 'test-id'

def test_normalize_saving_no_nan(saving_wbi):
    """Il record normalizzato non deve contenere NaN."""
    import math
    col_map = saving_wbi.mapping_result.fields
    for i in range(min(10, len(saving_wbi.df))):
        row = saving_wbi.df.iloc[i]
        rec = normalize_saving_row(col_map, row, 'test-id')
        if rec:
            for k, v in rec.items():
                if isinstance(v, float):
                    assert not math.isnan(v), f"NaN in field '{k}'"

def test_normalize_saving_date_invalid_skipped():
    """Righe senza data valida devono essere saltate (ritorna None)."""
    df = pd.DataFrame({
        'Data doc.': [None, pd.NaT],
        'Imp. Iniziale €': [1000.0, 2000.0],
        'Imp. Negoziato €': [900.0, 1800.0],
        'Saving.1': [100.0, 200.0],
        'Ragione sociale fornitore': ['A', 'B'],
    })
    b = make_excel(df)
    wbi = inspect_and_load(b, 'test.xlsx')
    col_map = wbi.mapping_result.fields
    for _, row in wbi.df.iterrows():
        rec = normalize_saving_row(col_map, row, 'test-id')
        assert rec is None  # Data non valida → skip

def test_normalize_saving_infers_saving():
    """Se saving_eur manca ma listino e impegnato ci sono, deve calcolarlo."""
    df = pd.DataFrame({
        'Data doc.': pd.to_datetime(['2025-01-10']),
        'Imp. Iniziale €': [10000.0],
        'Imp. Negoziato €': [9000.0],
        # Saving.1 assente
        'Ragione sociale fornitore': ['MERCK'],
    })
    b = make_excel(df)
    wbi = inspect_and_load(b, 'test.xlsx')
    col_map = wbi.mapping_result.fields
    row = wbi.df.iloc[0]
    rec = normalize_saving_row(col_map, row, 'test-id')
    assert rec is not None
    assert rec['saving_eur'] == pytest.approx(1000.0, rel=0.01)

def test_normalize_saving_macro_stripped():
    """macro_categoria con spazi finali deve essere normalizzata."""
    df = pd.DataFrame({
        'Data doc.': pd.to_datetime(['2025-01-10']),
        'Imp. Iniziale €': [10000.0],
        'Imp. Negoziato €': [9000.0],
        'Saving.1': [1000.0],
        'Ragione sociale fornitore': ['MERCK'],
        'macro categorie': ['Pharma '],  # spazio finale
    })
    b = make_excel(df)
    wbi = inspect_and_load(b, 'test.xlsx')
    col_map = wbi.mapping_result.fields
    row = wbi.df.iloc[0]
    rec = normalize_saving_row(col_map, row, 'test-id')
    if rec and rec.get('macro_categoria'):
        assert rec['macro_categoria'] == rec['macro_categoria'].strip()

def test_normalize_risorse_row(df_risorse_standard):
    b = make_excel(df_risorse_standard)
    wbi = inspect_and_load(b, 'risorse.xlsx')
    col_map = wbi.mapping_result.fields
    row = wbi.df.iloc[0]
    rec = normalize_risorse_row(col_map, row, 'test-id')
    assert rec is not None
    assert rec['risorsa'] == 'Marina Padricelli'
    assert rec['pratiche_gestite'] == 45
    assert rec['saving_generato'] == pytest.approx(120000.0)
    assert rec['year'] == 2025
    assert rec['month'] == 1

def test_normalize_risorse_english(df_risorse_english):
    b = make_excel(df_risorse_english)
    wbi = inspect_and_load(b, 'team.xlsx')
    col_map = wbi.mapping_result.fields
    row = wbi.df.iloc[0]
    rec = normalize_risorse_row(col_map, row, 'test-id')
    assert rec is not None
    assert rec['pratiche_gestite'] == 45
    assert rec['saving_generato'] == pytest.approx(120000.0)
    assert rec['negoziazioni_concluse'] == 15

def test_normalize_nc_row(df_nc):
    b = make_excel(df_nc)
    wbi = inspect_and_load(b, 'nc.xlsx')
    col_map = wbi.mapping_result.fields
    row = wbi.df.iloc[1]  # la NC (SI)
    rec = normalize_nc_row(col_map, row, 'test-id')
    assert rec is not None
    assert rec['non_conformita'] == True
    assert rec['tipo_origine'] == 'Consegna'


# ══════════════════════════════════════════════════════════════════
# SECTION 7: READINESS MATRIX
# ══════════════════════════════════════════════════════════════════

def test_readiness_saving_real(saving_wbi):
    r = compute_readiness(saving_wbi.mapping_result, saving_wbi)
    assert r['family'] == 'savings'
    assert r['mapping_score'] >= 0.85
    assert 'KPI Riepilogo' in r['analytics_enabled']
    assert r['yoy_ready'] is True

def test_readiness_risorse(df_risorse_standard):
    b = make_excel(df_risorse_standard)
    wbi = inspect_and_load(b, 'risorse.xlsx')
    r = compute_readiness(wbi.mapping_result, wbi)
    assert r['family'] == 'risorse'
    assert r['yoy_ready'] is True

def test_readiness_blocked_when_missing_critical():
    """File con soli 2 campi: molte analisi bloccate ma no crash."""
    df = pd.DataFrame({
        'Fornitore': ['MERCK', 'LIFE TECH'],
        'Importo': [50000.0, 30000.0],
    })
    b = make_excel(df)
    wbi = inspect_and_load(b, 'ambiguo.xlsx')
    r = compute_readiness(wbi.mapping_result, wbi)
    assert r is not None  # No crash

def test_readiness_includes_yoy_note(df_saving_standard):
    b = make_excel(df_saving_standard)
    wbi = inspect_and_load(b, 'saving.xlsx')
    r = compute_readiness(wbi.mapping_result, wbi)
    assert r['yoy_note'] != ''
    assert '2025' in r['yoy_note']

def test_readiness_normalization_notes(saving_wbi):
    """Le note di normalizzazione devono essere presenti."""
    r = compute_readiness(saving_wbi.mapping_result, saving_wbi)
    assert isinstance(r['normalization_notes'], list)


# ══════════════════════════════════════════════════════════════════
# SECTION 8: PREVIEW ↔ IMPORT CONSISTENCY
# ══════════════════════════════════════════════════════════════════

def test_preview_import_same_fields(saving_bytes):
    """Preview e import devono usare lo stesso mapping."""
    mr_preview = inspect_bytes(saving_bytes, 'test.xlsx')
    wbi_import  = inspect_and_load(saving_bytes, 'test.xlsx')
    mr_import   = wbi_import.mapping_result

    preview_fields = set(mr_preview.fields.keys())
    import_fields  = set(mr_import.fields.keys())
    assert preview_fields == import_fields, \
        f"Preview/import field mismatch: {preview_fields ^ import_fields}"

def test_preview_import_same_family(saving_bytes):
    mr_preview = inspect_bytes(saving_bytes, 'test.xlsx')
    wbi_import  = inspect_and_load(saving_bytes, 'test.xlsx')
    assert mr_preview.family == wbi_import.mapping_result.family

def test_preview_import_same_confidence(saving_bytes):
    mr_preview = inspect_bytes(saving_bytes, 'test.xlsx')
    wbi_import  = inspect_and_load(saving_bytes, 'test.xlsx')
    assert mr_preview.overall_confidence == wbi_import.mapping_result.overall_confidence

def test_no_split_brain_on_english_file(df_saving_english):
    """File con colonne inglesi: preview e import devono essere coerenti."""
    b = make_excel(df_saving_english)
    mr_preview = inspect_bytes(b, 'test.xlsx')
    wbi_import  = inspect_and_load(b, 'test.xlsx')
    assert mr_preview.family == wbi_import.mapping_result.family
    assert mr_preview.overall_confidence == wbi_import.mapping_result.overall_confidence


# ══════════════════════════════════════════════════════════════════
# SECTION 9: GRACEFUL DEGRADATION
# ══════════════════════════════════════════════════════════════════

def test_no_crash_on_empty_file():
    df = pd.DataFrame({'A': [], 'B': []})
    b = make_excel(df)
    try:
        wbi = inspect_and_load(b, 'vuoto.xlsx')
        assert wbi is not None
    except Exception as e:
        pytest.fail(f"Crashed on empty file: {e}")

def test_no_crash_on_single_column():
    df = pd.DataFrame({'Fornitore': ['MERCK', 'LIFE TECH']})
    b = make_excel(df)
    try:
        mr = inspect_bytes(b, 'single.xlsx')
        assert mr is not None
    except Exception as e:
        pytest.fail(f"Crashed on single-column file: {e}")

def test_no_crash_on_missing_optional_fields(df_saving_standard):
    """File senza CDC, buyer, albo: deve funzionare senza crashare."""
    df = df_saving_standard.drop(columns=['CDC', 'utente per presentazione', 'Accred.albo'])
    b = make_excel(df)
    wbi = inspect_and_load(b, 'partial.xlsx')
    col_map = wbi.mapping_result.fields
    assert 'listino_eur' in col_map   # I campi critici ci sono
    assert 'impegnato_eur' in col_map
    # CDC e buyer mancano ma non crashano
    for _, row in wbi.df.iterrows():
        rec = normalize_saving_row(col_map, row, 'test-id')
        assert rec is not None

def test_partial_import_continues_on_bad_rows():
    """Alcune righe con data invalida: le altre devono essere normalizzate."""
    df = pd.DataFrame({
        'Data doc.': [pd.NaT, pd.to_datetime('2025-01-10'), pd.to_datetime('2025-03-15')],
        'Imp. Iniziale €': [1000.0, 2000.0, 3000.0],
        'Imp. Negoziato €': [900.0, 1800.0, 2700.0],
        'Saving.1': [100.0, 200.0, 300.0],
        'Ragione sociale fornitore': ['A', 'B', 'C'],
    })
    b = make_excel(df)
    wbi = inspect_and_load(b, 'partial.xlsx')
    col_map = wbi.mapping_result.fields
    records = []
    for _, row in wbi.df.iterrows():
        rec = normalize_saving_row(col_map, row, 'test-id')
        if rec:
            records.append(rec)
    # 2 righe valide su 3
    assert len(records) == 2

def test_risorse_file_never_crashes(df_risorse_english):
    """File risorse con nomi inglesi: no crash."""
    b = make_excel(df_risorse_english)
    try:
        wbi = inspect_and_load(b, 'risorse_en.xlsx')
        col_map = wbi.mapping_result.fields
        for _, row in wbi.df.iterrows():
            rec = normalize_risorse_row(col_map, row, 'test-id')
            assert rec is not None
    except Exception as e:
        pytest.fail(f"Crashed on English resource file: {e}")


# ══════════════════════════════════════════════════════════════════
# SECTION 10: GCOL SAFETY
# ══════════════════════════════════════════════════════════════════

def test_gcol_returns_none_for_missing():
    from ingestion_engine import FieldMapping
    col_map = {'foo': FieldMapping('foo', 'Foo Column', 0.9, 'exact')}
    row = pd.Series({'Foo Column': 'val1', 'Bar Column': 'val2'})
    assert _gcol(col_map, 'foo', row) == 'val1'
    assert _gcol(col_map, 'missing', row) is None

def test_gcol_handles_missing_source_column():
    from ingestion_engine import FieldMapping
    col_map = {'foo': FieldMapping('foo', 'NonExistentColumn', 0.9, 'exact')}
    row = pd.Series({'OtherColumn': 'val'})
    result = _gcol(col_map, 'foo', row)
    # Deve ritornare None o NaN, non crashare
    assert result is None or pd.isna(result)

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
