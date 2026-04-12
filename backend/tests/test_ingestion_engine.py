"""
Test suite per ingestion_engine.py
Testa: file family classification, column mapping 7-layer, confidence scoring.
"""
import pytest
import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, '/home/claude/ua-dashboard/backend')

from ingestion_engine import (
    inspect_workbook, map_single_column, build_column_map,
    classify_file_family, detect_header_row,
    _normalize, _infer_from_values,
    FileFamily, Confidence, FieldMapping,
    mapping_result_to_dict, SYNONYMS, _SYN_INDEX,
)

SAVING_FILE = '/mnt/user-data/uploads/file_saving_2025_final.xlsx'


def _require_sample_file(path: str) -> Path:
    sample = Path(path)
    if not sample.exists():
        pytest.skip(f"Sample file not available in this environment: {path}")
    return sample

# ══════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def saving_xl():
    sample = _require_sample_file(SAVING_FILE)
    return pd.ExcelFile(sample)

@pytest.fixture(scope="module")
def saving_mr(saving_xl):
    return inspect_workbook(saving_xl)

@pytest.fixture(scope="module")
def saving_df(saving_xl):
    df = pd.read_excel(saving_xl, sheet_name='Final saving 2025 (3)')
    df.columns = [c.strip() for c in df.columns]
    return df

# Fixture per un DataFrame fittizio di risorse
@pytest.fixture
def risorse_df():
    return pd.DataFrame({
        'Risorsa':             ['Marina Padricelli', 'Silvana Ruotolo', 'Monti Luca'],
        'Mese':                ['2025-01', '2025-01', '2025-01'],
        'Struttura':           ['GD', 'TIGEM', 'GD'],
        'Pratiche Gestite':    [45, 38, 52],
        'Pratiche Aperte':     [8, 5, 12],
        'Pratiche Chiuse':     [37, 33, 40],
        'Saving Generato':     [120000.0, 95000.0, 180000.0],
        'Negoziazioni Concluse': [15, 12, 20],
        'Tempo Medio':         [8.2, 6.5, 7.1],
    })

@pytest.fixture
def risorse_buyer_period_df():
    """Formato risorse frequente in export utente: Buyer + Period."""
    return pd.DataFrame({
        'Buyer': ['Marina P.', 'Silvana R.'],
        'Period': ['2025-01', '2025-02'],
        'Cases Managed': [22, 18],
        'Open Cases': [4, 2],
        'Closed Cases': [18, 16],
        'Savings Generated': [35000.0, 29000.0],
        'Negotiations': [7, 5],
        'Avg Days': [6.2, 7.1],
    })

@pytest.fixture
def nc_df():
    return pd.DataFrame({
        'Ragione Sociale':   ['SUPPLIER A', 'SUPPLIER B'],
        'Data Origine':      pd.to_datetime(['2025-01-15', '2025-02-20']),
        'Non Conformita':    ['NO', 'SI'],
        'Tipo Origine':      ['Qualità', 'Consegna'],
        'Delta Giorni (Fattura - Origine)': [12, 45],
    })

@pytest.fixture
def tempi_df():
    return pd.DataFrame({
        'Year_Month':       ['2025-01', '2025-02'],
        'Total_Days':       [15.2, 18.4],
        'Days_Purchasing':  [5.1, 7.2],
        'Days_Auto':        [8.3, 9.1],
        'Days_Other':       [1.8, 2.1],
        'Bottleneck':       ['PURCHASING', 'AUTO'],
    })


# ══════════════════════════════════════════════════════════════════
# NORMALIZE
# ══════════════════════════════════════════════════════════════════

def test_normalize_lowercase():
    assert _normalize('Ragione Sociale') == 'ragione sociale'

def test_normalize_strips():
    assert _normalize('  data doc.  ') == 'data doc.'

def test_normalize_accents():
    assert _normalize('Non Conformità') == 'non conformita'

def test_normalize_underscore_to_space():
    assert _normalize('data_doc') == 'data doc'

def test_normalize_multi_space():
    assert _normalize('imp.  iniziale') == 'imp. iniziale'  # doppi spazi collassati


# ══════════════════════════════════════════════════════════════════
# SYNONYM INDEX
# ══════════════════════════════════════════════════════════════════

def test_synonym_index_listino():
    assert _SYN_INDEX.get('imp. iniziale €') == 'listino_eur'

def test_synonym_index_impegnato():
    assert _SYN_INDEX.get('imp. negoziato €') == 'impegnato_eur'

def test_synonym_index_saving1():
    assert _SYN_INDEX.get('saving.1') == 'saving_eur'

def test_synonym_index_ragione_sociale():
    assert _SYN_INDEX.get('ragione sociale fornitore') == 'ragione_sociale'

def test_synonym_index_risorsa():
    assert _SYN_INDEX.get('risorsa') == 'risorsa'

def test_synonym_index_pratiche_gestite():
    assert _SYN_INDEX.get('pratiche gestite') == 'pratiche_gestite'

def test_synonym_index_nc():
    # Non conformità con accento
    found = any(_SYN_INDEX.get(s) == 'non_conformita' for s in _SYN_INDEX)
    assert found


# ══════════════════════════════════════════════════════════════════
# VALUE INFERENCE (L5)
# ══════════════════════════════════════════════════════════════════

def test_value_infer_alfa_documento():
    s = pd.Series(['OPR','ORN','OS','OSP','ORD','OPR','ORN','OS'])
    canon, conf = _infer_from_values('Alfa documento', s)
    assert canon == 'alfa_documento'
    assert conf >= 0.95

def test_value_infer_str_ric():
    s = pd.Series(['RICERCA','STRUTTURA','RICERCA','RICERCA','STRUTTURA'])
    canon, conf = _infer_from_values('Str./Ric.', s)
    assert canon == 'str_ric'
    assert conf >= 0.95

def test_value_infer_cdc():
    s = pd.Series(['GD','TIGEM','GD','TIGET','FT'])
    canon, conf = _infer_from_values('CDC', s)
    assert canon == 'cdc'
    assert conf >= 0.90

def test_value_infer_valuta():
    s = pd.Series(['EURO','EURO','USD','EURO','GBP'])
    canon, conf = _infer_from_values('Valuta', s)
    assert canon == 'valuta'
    assert conf >= 0.95

def test_value_infer_data():
    s = pd.Series(pd.to_datetime(['2025-01-15','2025-03-20','2025-06-01']))
    canon, conf = _infer_from_values('Data doc.', s)
    assert canon == 'data_doc'
    assert conf >= 0.95

def test_value_infer_negoziazione():
    s = pd.Series(['SI','NO','SI','NO','NO','SI'])
    canon, conf = _infer_from_values('Negoziazione', s)
    assert canon == 'negoziazione'
    assert conf >= 0.90

def test_value_infer_large_eur():
    s = pd.Series([5068352.9, 2205817.09, 1749173.0, 3200000.0])
    canon, conf = _infer_from_values('Imp. Iniziale €', s)
    assert canon == 'listino_eur'
    assert conf >= 0.85


# ══════════════════════════════════════════════════════════════════
# MAP SINGLE COLUMN
# ══════════════════════════════════════════════════════════════════

def test_map_column_listino_eur():
    s = pd.Series([5068352.9, 2205817.09])
    fm = map_single_column('Imp. Iniziale €', s)
    assert fm is not None
    assert fm.canonical == 'listino_eur'
    assert fm.confidence >= 0.90

def test_map_column_impegnato_eur():
    s = pd.Series([5068352.9, 2173171.0])
    fm = map_single_column('Imp. Negoziato €', s)
    assert fm is not None
    assert fm.canonical == 'impegnato_eur'

def test_map_column_saving_eur():
    s = pd.Series([0.0, 32646.09, 108081.1])
    fm = map_single_column('Saving.1', s)
    assert fm is not None
    assert fm.canonical == 'saving_eur'

def test_map_column_alfa_documento():
    s = pd.Series(['OPR','ORN','OS','OSP','ORD'])
    fm = map_single_column('Alfa documento', s)
    assert fm is not None
    assert fm.canonical == 'alfa_documento'
    assert fm.confidence >= 0.95

def test_map_column_ragione_sociale():
    s = pd.Series(['LIFE TECHNOLOGIES','MERCK LIFE SCIENCE','EUROCLONE'])
    fm = map_single_column('Ragione sociale fornitore', s)
    assert fm is not None
    assert fm.canonical == 'ragione_sociale'

def test_map_column_risorsa():
    s = pd.Series(['Marina Padricelli', 'Silvana Ruotolo', 'Monti Luca'])
    fm = map_single_column('Risorsa', s)
    assert fm is not None
    assert fm.canonical == 'risorsa'

def test_map_column_pratiche_gestite():
    s = pd.Series([45, 38, 52, 41, 60])
    fm = map_single_column('Pratiche Gestite', s)
    assert fm is not None
    assert fm.canonical == 'pratiche_gestite'

def test_map_column_non_conformita():
    s = pd.Series(['SI','NO','SI','NO'])
    fm = map_single_column('Non Conformità', s)
    assert fm is not None
    assert fm.canonical in ('non_conformita', 'accred_albo', 'negoziazione')  # SI/NO flag

def test_map_column_days_purchasing():
    s = pd.Series([5.1, 7.2, 4.8, 6.3])
    fm = map_single_column('Days_Purchasing', s)
    assert fm is not None
    assert fm.canonical == 'days_purchasing'

def test_map_column_year_month():
    s = pd.Series(['2025-01', '2025-02', '2025-03'])
    fm = map_single_column('Year_Month', s)
    assert fm is not None
    assert fm.canonical == 'year_month'

def test_map_column_none_for_garbage():
    s = pd.Series(['xyz','abc','zzz'])
    fm = map_single_column('zzz_unknown_col_xyz', s)
    # Potrebbe ritornare None o un campo a bassa confidenza
    if fm:
        assert fm.confidence < 0.70


# ══════════════════════════════════════════════════════════════════
# BUILD COLUMN MAP
# ══════════════════════════════════════════════════════════════════

def test_build_column_map_saving(saving_df):
    col_map = build_column_map(saving_df)
    for required in ['listino_eur','impegnato_eur','saving_eur','data_doc',
                     'ragione_sociale','alfa_documento','str_ric','cdc']:
        assert required in col_map, f"'{required}' not in column map"

def test_build_column_map_no_duplicates(saving_df):
    col_map = build_column_map(saving_df)
    # Ogni source_column appare al massimo una volta
    source_cols = [fm.source_column for fm in col_map.values()]
    assert len(source_cols) == len(set(source_cols))

def test_build_column_map_risorse(risorse_df):
    col_map = build_column_map(risorse_df)
    assert 'risorsa' in col_map
    assert 'pratiche_gestite' in col_map

def test_build_column_map_tempi(tempi_df):
    col_map = build_column_map(tempi_df)
    assert 'year_month' in col_map
    assert 'total_days' in col_map
    assert 'days_purchasing' in col_map

def test_build_column_map_nc(nc_df):
    col_map = build_column_map(nc_df)
    # 'Data Origine' può essere mappata come data_doc o data_origine (entrambi accettabili)
    has_date = 'data_origine' in col_map or 'data_doc' in col_map
    assert has_date, 'Data Origine deve essere mappata a un campo data'
    assert 'delta_giorni' in col_map


# ══════════════════════════════════════════════════════════════════
# FILE FAMILY CLASSIFICATION
# ══════════════════════════════════════════════════════════════════

def test_classify_savings(saving_df):
    col_map = build_column_map(saving_df)
    family, conf, scores = classify_file_family(col_map, 'Final saving 2025 (3)')
    assert family == FileFamily.SAVINGS
    assert conf >= 0.90

def test_classify_risorse(risorse_df):
    col_map = build_column_map(risorse_df)
    family, conf, scores = classify_file_family(col_map, 'Risorse')
    assert family == FileFamily.RISORSE
    assert conf >= 0.60

def test_classify_risorse_buyer_period(risorse_buyer_period_df):
    col_map = build_column_map(risorse_buyer_period_df)
    family, conf, _ = classify_file_family(col_map, 'Team Workload')
    assert family == FileFamily.RISORSE
    assert conf >= 0.50

def test_classify_tempi(tempi_df):
    col_map = build_column_map(tempi_df)
    family, conf, scores = classify_file_family(col_map, 'Tempi')
    assert family == FileFamily.TEMPI
    assert conf >= 0.70

def test_classify_nc(nc_df):
    col_map = build_column_map(nc_df)
    family, conf, scores = classify_file_family(col_map, 'NonConformita')
    assert family == FileFamily.NC
    assert conf >= 0.50

def test_classify_sheet_name_bonus():
    """Sheet name 'saving' deve dare un boost alla family SAVINGS."""
    minimal_df = pd.DataFrame({
        'Data doc.': pd.to_datetime(['2025-01-01']),
        'Ragione sociale fornitore': ['TEST SRL'],
    })
    col_map = build_column_map(minimal_df)
    _, conf_with, _ = classify_file_family(col_map, 'saving 2025')
    _, conf_without, _ = classify_file_family(col_map, 'Foglio1')
    assert conf_with >= conf_without


# ══════════════════════════════════════════════════════════════════
# HEADER ROW DETECTION
# ══════════════════════════════════════════════════════════════════

def test_header_row_standard():
    """Header nella prima riga → 0."""
    df = pd.DataFrame([
        ['Fornitore', 'Data', 'Importo'],
        ['MERCK', '2025-01-01', 1000],
        ['LIFE TECH', '2025-02-01', 2000],
    ])
    assert detect_header_row(df) == 0

def test_header_row_with_title():
    """Header alla riga 2 se ci sono righe di titolo sopra."""
    df = pd.DataFrame([
        ['REPORT ACQUISTI 2025', None, None],
        ['Estratto al 31/12/2025', None, None],
        ['Fornitore', 'Data', 'Importo'],
        ['MERCK', '2025-01-01', 1000],
    ])
    detected = detect_header_row(df)
    assert detected >= 2  # deve trovare la riga con i label


# ══════════════════════════════════════════════════════════════════
# FULL INSPECT — file reale
# ══════════════════════════════════════════════════════════════════

def test_inspect_saving_file_family(saving_mr):
    assert saving_mr.family == FileFamily.SAVINGS

def test_inspect_saving_high_confidence(saving_mr):
    assert saving_mr.overall_confidence == Confidence.HIGH

def test_inspect_saving_no_missing_critical(saving_mr):
    assert saving_mr.missing_critical == []

def test_inspect_saving_can_proceed(saving_mr):
    r = mapping_result_to_dict(saving_mr)
    assert r['can_proceed'] is True
    assert r['is_blocked'] is False

def test_inspect_saving_has_all_analyses(saving_mr):
    available = saving_mr.available_analyses
    assert 'KPI Riepilogo' in available
    assert 'Saving YoY' in available
    assert 'Per Tipo Documento' in available

def test_inspect_saving_maps_all_critical(saving_mr):
    for f in ['listino_eur','impegnato_eur','saving_eur','data_doc','ragione_sociale']:
        assert f in saving_mr.fields, f"'{f}' not in mapped fields"

def test_inspect_saving_family_confidence_high(saving_mr):
    assert saving_mr.family_confidence >= 0.90

def test_inspect_saving_sheet_correct(saving_mr):
    assert saving_mr.sheet_name == 'Final saving 2025 (3)'

def test_inspect_saving_header_row_zero(saving_mr):
    assert saving_mr.header_row == 0

def test_inspect_serializable(saving_mr):
    """Il risultato deve essere serializzabile in JSON."""
    import json
    r = mapping_result_to_dict(saving_mr)
    s = json.dumps(r)
    assert len(s) > 100


# ══════════════════════════════════════════════════════════════════
# VARIANT FILE NAMES — L2/L3 robustness
# ══════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("col_name,expected_canon", [
    ('Imp. Iniziale €',        'listino_eur'),
    ('Importo Listino EUR',    'listino_eur'),
    ('List Amount EUR',        'listino_eur'),
    ('Imp. Negoziato €',       'impegnato_eur'),
    ('Committed Amount EUR',   'impegnato_eur'),
    ('Importo Impegnato Eur',  'impegnato_eur'),
    ('Saving.1',               'saving_eur'),
    ('Savings EUR',            'saving_eur'),
    ('Ragione Sociale Fornitore', 'ragione_sociale'),
    ('Supplier Name',          'ragione_sociale'),
    ('Vendor',                 'ragione_sociale'),
    ('Accred.Albo',            'accred_albo'),
    ('Albo Fornitori',         'accred_albo'),
    ('Days_Purchasing',        'days_purchasing'),
    ('Pratiche Gestite',       'pratiche_gestite'),
    ('Saving Generato',        'saving_generato'),
    ('Centro di Costo',        'centro_costo'),
    ('Cost Center',            'centro_costo'),
    ('Macro Categoria',        'macro_cat'),
    ('Commodity Type',         'macro_cat'),
])
def test_variant_column_names(col_name, expected_canon):
    """Verifica che varianti comuni vengano riconosciute."""
    s = pd.Series([])
    fm = map_single_column(col_name, s)
    assert fm is not None, f"'{col_name}' should be mapped"
    assert fm.canonical == expected_canon, \
        f"'{col_name}' mapped to '{fm.canonical}' instead of '{expected_canon}'"

if __name__ == '__main__':
    pytest.main([__file__, '-v'])


# ══════════════════════════════════════════════════════════════════
# NUOVI TEST v10 — synonym gaps risolti
# ══════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("col_name,expected_canon", [
    # Impegnato — varianti inglesi pre-elaborate da utenti
    ('committed_eur',     'impegnato_eur'),
    ('committed eur',     'impegnato_eur'),
    ('total committed',   'impegnato_eur'),
    ('negotiated amount', 'impegnato_eur'),
    ('net amount eur',    'impegnato_eur'),
    # Listino — varianti inglesi
    ('list price',        'listino_eur'),
    ('catalog price',     'listino_eur'),
    ('list price eur',    'listino_eur'),
    ('initial price',     'listino_eur'),
    # Buyer/utente
    ('responsible',       'utente_pres'),
    ('account manager',   'utente_pres'),
    ('buyer name',        'utente_pres'),
    # Protocollo
    ('protocol order',    'protoc_ordine'),
    ('protocol_order',    'protoc_ordine'),
])
def test_new_synonym_variants(col_name, expected_canon):
    """Varianti colonne che utenti pre-elaboratori usano frequentemente."""
    s = pd.Series([])
    fm = map_single_column(col_name, s)
    assert fm is not None, f"'{col_name}' should be mapped (got None)"
    assert fm.canonical == expected_canon, \
        f"'{col_name}' → '{fm.canonical}' instead of '{expected_canon}'"


def test_resource_file_recognition(risorse_df):
    """File risorse deve essere riconosciuto anche senza exact field names."""
    col_map = build_column_map(risorse_df)
    family, conf, scores = classify_file_family(col_map, 'Risorse Team')
    assert family == FileFamily.RISORSE, f"Expected RISORSE, got {family}"
    assert conf >= 0.50


def test_saving_variant_english_columns():
    """File saving con colonne inglesi pre-elaborate deve essere mappato."""
    df_en = pd.DataFrame({
        'supplier_name':   ['MERCK', 'LIFE TECH', 'EUROCLONE'],
        'order_date':      pd.to_datetime(['2025-01-15', '2025-02-20', '2025-03-10']),
        'list_price_eur':  [10000.0, 5000.0, 8000.0],
        'committed_eur':   [9000.0, 4500.0, 7200.0],
        'savings':         [1000.0, 500.0, 800.0],
        'business_unit':   ['RICERCA', 'STRUTTURA', 'RICERCA'],
        'doc_type':        ['ORD', 'OS', 'ORN'],
        'negotiated':      ['SI', 'NO', 'SI'],
        'macro_category':  ['Ricerca', 'Raccolta Fondi', 'Ricerca'],
        'buyer name':      ['Marina P.', 'Silvana R.', 'Monti L.'],
        'currency':        ['EURO', 'USD', 'EURO'],
    })
    col_map = build_column_map(df_en)
    assert 'ragione_sociale' in col_map, "supplier_name deve essere mappato"
    assert 'data_doc'        in col_map, "order_date deve essere mappato"
    assert 'listino_eur'     in col_map, "list_price_eur deve essere mappato"
    assert 'impegnato_eur'   in col_map, "committed_eur deve essere mappato"
    assert 'utente_pres'     in col_map, "buyer name deve essere mappato"


def test_multi_year_file():
    """File con anno 2026 deve essere riconosciuto correttamente."""
    df_2026 = pd.DataFrame({
        'Data doc.':               pd.to_datetime(['2026-01-10', '2026-03-15', '2026-06-20']),
        'Imp. Iniziale €':         [5000.0, 3000.0, 8000.0],
        'Imp. Negoziato €':        [4500.0, 2700.0, 7200.0],
        'Saving.1':                [500.0, 300.0, 800.0],
        'Ragione sociale fornitore': ['MERCK', 'LIFE TECH', 'EUROCLONE'],
        'Alfa documento':          ['ORD', 'OS', 'ORN'],
        'CDC':                     ['TIGEM', 'GD', 'FT'],
        'Str./Ric.':               ['RICERCA', 'STRUTTURA', 'RICERCA'],
    })
    col_map = build_column_map(df_2026)
    assert 'data_doc'     in col_map
    assert 'listino_eur'  in col_map
    assert 'impegnato_eur' in col_map
    family, conf, _ = classify_file_family(col_map, 'saving 2026')
    assert family == FileFamily.SAVINGS
    assert conf >= 0.60  # valore aggiornato dopo estensione FAMILY_SIGNALS
