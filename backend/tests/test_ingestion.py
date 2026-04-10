"""
Test suite: file ingestion, column mapping, KPI calculations.
Testano il modulo domain.py — funzioni pure, zero dipendenze FastAPI.
"""
import pytest
import math
import numpy as np
import pandas as pd
import sys
sys.path.insert(0, '/home/claude/ua-dashboard/backend')

from domain import (
    map_cols, gcol, best_sheet, derive_cdc, parse_commessa,
    validate_mapping, calc_kpi, build_record,
    DOC_NEG, _f, _fn, _i, _s, _b, _d, clean, safe_pct,
)

SAVING_FILE = '/mnt/user-data/uploads/file_saving_2025_final.xlsx'

# ── Fixtures ──────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def df2025():
    xl = pd.ExcelFile(SAVING_FILE)
    df = pd.read_excel(xl, sheet_name='Final saving 2025 (3)')
    df.columns = [c.strip() for c in df.columns]
    df['Data doc.'] = pd.to_datetime(df['Data doc.'], errors='coerce')
    return df[df['Data doc.'].dt.year == 2025].copy()

@pytest.fixture(scope="module")
def col_map(df2025):
    return map_cols(df2025.columns)

@pytest.fixture(scope="module")
def kpi_df(df2025, col_map):
    """DataFrame normalizzato con le colonne KPI."""
    d = df2025.copy()
    d['imp_listino_eur']    = pd.to_numeric(d[col_map['listino_eur']], errors='coerce').fillna(0)
    d['imp_impegnato_eur']  = pd.to_numeric(d[col_map['impegnato_eur']], errors='coerce').fillna(0)
    d['saving_eur']         = pd.to_numeric(d[col_map['saving_eur']], errors='coerce').fillna(0)
    d['alfa_documento']     = d[col_map['alfa_documento']]
    d['negoziazione']       = d[col_map['negoziazione']].apply(_b)
    d['accred_albo']        = d[col_map['accred_albo']].apply(_b)
    return d

# ══════════════════════════════════════════════════════════════════
# SHEET DETECTION
# ══════════════════════════════════════════════════════════════════

def test_best_sheet_finds_correct_sheet():
    xl = pd.ExcelFile(SAVING_FILE)
    sheet = best_sheet(xl)
    assert sheet == 'Final saving 2025 (3)', f"Got: {sheet}"

# ══════════════════════════════════════════════════════════════════
# COLUMN MAPPING
# ══════════════════════════════════════════════════════════════════

def test_mapping_finds_listino():
    col = map_cols(['Imp. Iniziale €', 'Data doc.', 'Alfa documento'])
    assert col.get('listino_eur') == 'Imp. Iniziale €'

def test_mapping_finds_impegnato():
    col = map_cols(['Imp. Negoziato €', 'Data doc.'])
    assert col.get('impegnato_eur') == 'Imp. Negoziato €'

def test_mapping_finds_saving1():
    col = map_cols(['Saving.1', 'Data doc.'])
    assert col.get('saving_eur') == 'Saving.1'

def test_mapping_case_insensitive():
    col = map_cols(['IMP. INIZIALE €', 'Data doc.'])
    assert 'listino_eur' in col

def test_mapping_first_wins():
    # Se ci sono due colonne che matchano, la prima vince
    col = map_cols(['Imp. Iniziale €', 'imp iniziale €', 'Data doc.'])
    assert col.get('listino_eur') == 'Imp. Iniziale €'

def test_mapping_all_critical_fields(col_map):
    CRITICAL = ['data_doc','listino_eur','impegnato_eur','saving_eur',
                'alfa_documento','str_ric','cdc','ragione_sociale',
                'negoziazione','accred_albo']
    for f in CRITICAL:
        assert f in col_map, f"Critical field '{f}' not mapped"

def test_mapping_listino_column(col_map):
    assert col_map['listino_eur'] == 'Imp. Iniziale €'

def test_mapping_impegnato_column(col_map):
    assert col_map['impegnato_eur'] == 'Imp. Negoziato €'

def test_mapping_saving_column(col_map):
    assert col_map['saving_eur'] == 'Saving.1'

# ══════════════════════════════════════════════════════════════════
# VALIDATE MAPPING
# ══════════════════════════════════════════════════════════════════

def test_validate_mapping_high_confidence(col_map):
    result = validate_mapping(col_map)
    assert result['valid'] is True
    assert result['confidence'] in ('high', 'medium')
    assert result['missing_critical'] == []

def test_validate_mapping_fails_without_date():
    col = {'listino_eur': 'x', 'impegnato_eur': 'y', 'saving_eur': 'z'}
    result = validate_mapping(col)
    assert result['valid'] is False
    assert 'data_doc' in result['missing_critical']

def test_validate_mapping_low_confidence_empty():
    result = validate_mapping({})
    assert result['valid'] is False
    assert result['confidence'] == 'low'

# ══════════════════════════════════════════════════════════════════
# KPI TOTALS — numeri di riferimento assoluti
# ══════════════════════════════════════════════════════════════════

def test_kpi_listino_2025(kpi_df):
    k = calc_kpi(kpi_df)
    assert abs(k['listino'] - 77_465_963.18) < 1.0, f"Listino: {k['listino']:,.2f}"

def test_kpi_impegnato_2025(kpi_df):
    k = calc_kpi(kpi_df)
    assert abs(k['impegnato'] - 69_676_501.89) < 1.0, f"Impegnato: {k['impegnato']:,.2f}"

def test_kpi_saving_2025(kpi_df):
    k = calc_kpi(kpi_df)
    assert abs(k['saving'] - 7_789_461.29) < 1.0, f"Saving: {k['saving']:,.2f}"

def test_kpi_perc_saving_2025(kpi_df):
    k = calc_kpi(kpi_df)
    assert abs(k['perc_saving'] - 10.06) < 0.05, f"% Saving: {k['perc_saving']:.2f}"

def test_kpi_n_righe_2025(kpi_df):
    k = calc_kpi(kpi_df)
    assert k['n_righe'] == 10413

def test_kpi_negoziati_2025(kpi_df):
    k = calc_kpi(kpi_df)
    assert k['n_negoziati'] == 4725, f"Negoziati: {k['n_negoziati']}"

def test_kpi_albo_2025(kpi_df):
    k = calc_kpi(kpi_df)
    # 6566 SI + 11 Si = 6577 albo
    assert k['n_albo'] >= 6566, f"Albo: {k['n_albo']}"

def test_kpi_empty_df():
    k = calc_kpi(pd.DataFrame())
    assert k['listino'] == 0
    assert k['saving'] == 0
    assert k['n_righe'] == 0

def test_kpi_none_df():
    k = calc_kpi(None)
    assert k['listino'] == 0

# ══════════════════════════════════════════════════════════════════
# SAVING = LISTINO - IMPEGNATO
# ══════════════════════════════════════════════════════════════════

def test_saving_equals_listino_minus_impegnato(kpi_df):
    k = calc_kpi(kpi_df)
    computed = k['listino'] - k['impegnato']
    assert abs(computed - k['saving']) < 1.0, \
        f"Incoerenza: {k['listino']:,.0f} - {k['impegnato']:,.0f} = {computed:,.0f} ≠ {k['saving']:,.0f}"

# ══════════════════════════════════════════════════════════════════
# CDC
# ══════════════════════════════════════════════════════════════════

def test_cdc_tigem(df2025, col_map):
    counts = df2025[col_map['cdc']].value_counts()
    assert counts.get('TIGEM') == 3612

def test_cdc_gd(df2025, col_map):
    counts = df2025[col_map['cdc']].value_counts()
    assert counts.get('GD') == 3317

def test_cdc_struttura(df2025, col_map):
    counts = df2025[col_map['cdc']].value_counts()
    assert counts.get('STRUTTURA') == 1930

def test_derive_cdc_tigem():
    assert derive_cdc('', 'Ricerca Interna - Tigem') == 'TIGEM'

def test_derive_cdc_tiget():
    assert derive_cdc('', 'Ricerca Interna - Tiget') == 'TIGET'

def test_derive_cdc_gd_rcriir():
    assert derive_cdc('RCRIIR000000026', '') == 'GD'

def test_derive_cdc_gd_rcreer():
    assert derive_cdc('RCREER000000001', '') == 'GD'

def test_derive_cdc_struttura():
    assert derive_cdc('STRFRF000000206', '') == 'STRUTTURA'

def test_derive_cdc_default_ft():
    assert derive_cdc('UNKNOWN999', 'Sconosciuto') == 'FT'

# ══════════════════════════════════════════════════════════════════
# PARSE COMMESSA
# ══════════════════════════════════════════════════════════════════

def test_parse_commessa_gmr():
    pref, anno = parse_commessa('GMR24T2072/00053')
    assert pref == 'GMR' and anno == '24'

def test_parse_commessa_tff():
    pref, anno = parse_commessa('TFF25XYZW/00001')
    assert pref == 'TFF' and anno == '25'

def test_parse_commessa_none():
    pref, anno = parse_commessa(None)
    assert pref is None and anno is None

def test_parse_commessa_too_short():
    pref, anno = parse_commessa('AB')
    assert pref is None

def test_parse_commessa_no_digit_anno():
    pref, anno = parse_commessa('ABCXXYYY/001')
    assert pref == 'ABC'
    assert anno is None  # XX non sono cifre

# ══════════════════════════════════════════════════════════════════
# TYPE CONVERTERS
# ══════════════════════════════════════════════════════════════════

def test_f_float():
    assert _f(3.14) == pytest.approx(3.14)

def test_f_none_default():
    assert _f(None) == 0.0

def test_f_none_custom_default():
    assert _f(None, 99.0) == 99.0

def test_f_nan():
    assert _f(np.nan) == 0.0

def test_f_string_number():
    assert _f('42.5') == pytest.approx(42.5)

def test_fn_none():
    assert _fn(None) is None

def test_fn_value():
    assert _fn(5.0) == 5.0

def test_i_int():
    assert _i('38') == 38

def test_i_none():
    assert _i(None) is None

def test_s_strips():
    assert _s('  Ricerca  ') == 'Ricerca'

def test_s_none():
    assert _s(None) is None

def test_s_nan_string():
    assert _s('nan') is None

def test_s_empty():
    assert _s('') is None

def test_b_si():
    assert _b('SI') is True

def test_b_si_lower():
    assert _b('si') is True

def test_b_si_accent():
    assert _b('SÌ') is True

def test_b_no():
    assert _b('NO') is False

def test_b_none():
    assert _b(None) is False

def test_d_valid():
    result = _d('2025-05-28')
    assert result == '2025-05-28'

def test_d_none():
    assert _d(None) is None

def test_d_invalid():
    assert _d('not-a-date') is None

def test_clean_nan():
    assert clean(float('nan')) is None

def test_clean_bool_true():
    assert clean(True) is True

def test_clean_bool_false():
    assert clean(False) is False

def test_clean_none():
    assert clean(None) is None

def test_clean_int():
    assert clean(42) == 42

def test_clean_float():
    result = clean(3.14159)
    assert isinstance(result, float)

def test_clean_string():
    assert clean('  hello  ') == 'hello'

def test_clean_empty_string():
    assert clean('') is None

def test_safe_pct_normal():
    assert safe_pct(10, 100) == 10.0

def test_safe_pct_zero_den():
    assert safe_pct(10, 0) == 0.0

def test_safe_pct_zero_num():
    assert safe_pct(0, 100) == 0.0

# ══════════════════════════════════════════════════════════════════
# DOC_NEG
# ══════════════════════════════════════════════════════════════════

def test_doc_neg_contains_negoziabili():
    for doc in ('OS', 'OSP', 'PS', 'OPR', 'ORN', 'ORD'):
        assert doc in DOC_NEG

def test_doc_neg_excludes_osd():
    assert 'OSD' not in DOC_NEG

def test_doc_neg_excludes_osdp01():
    assert 'OSDP01' not in DOC_NEG

# ══════════════════════════════════════════════════════════════════
# ALFA DOCUMENTO DISTRIBUTION
# ══════════════════════════════════════════════════════════════════

def test_alfa_doc_ord(df2025, col_map):
    counts = df2025[col_map['alfa_documento']].value_counts()
    assert counts.get('ORD') == 4697

def test_alfa_doc_orn(df2025, col_map):
    counts = df2025[col_map['alfa_documento']].value_counts()
    assert counts.get('ORN') == 3437

def test_alfa_doc_osd(df2025, col_map):
    counts = df2025[col_map['alfa_documento']].value_counts()
    assert counts.get('OSD') == 820

# ══════════════════════════════════════════════════════════════════
# BUILD RECORD
# ══════════════════════════════════════════════════════════════════

def test_build_record_returns_dict(df2025, col_map):
    row = df2025.iloc[0]
    rec = build_record(col_map, row, 'test-upload-id')
    assert isinstance(rec, dict)

def test_build_record_has_required_keys(df2025, col_map):
    row = df2025.iloc[0]
    rec = build_record(col_map, row, 'test-upload-id')
    for key in ('data_doc', 'imp_listino_eur', 'imp_impegnato_eur', 'saving_eur',
                'cdc', 'alfa_documento', 'ragione_sociale', 'negoziazione'):
        assert key in rec, f"Missing key: {key}"

def test_build_record_no_nan_values(df2025, col_map):
    row = df2025.iloc[0]
    rec = build_record(col_map, row, 'test-upload-id')
    for k, v in rec.items():
        assert not (isinstance(v, float) and math.isnan(v)), f"NaN in record key '{k}'"

def test_build_record_listino_positive(df2025, col_map):
    row = df2025.iloc[0]
    rec = build_record(col_map, row, 'test-upload-id')
    assert rec['imp_listino_eur'] >= 0

def test_build_record_cdc_override(df2025, col_map):
    row = df2025.iloc[0]
    rec = build_record(col_map, row, 'test-id', cdc_override='TIGEM')
    assert rec['cdc'] == 'TIGEM'

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
