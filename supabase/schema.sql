-- ============================================================
-- SCHEMA: Dashboard Ufficio Acquisti - Fondazione Telethon
-- ============================================================

-- Tabella upload log
CREATE TABLE IF NOT EXISTS upload_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filename TEXT NOT NULL,
  upload_date TIMESTAMPTZ DEFAULT NOW(),
  rows_inserted INTEGER,
  tipo TEXT CHECK (tipo IN ('saving', 'tempi', 'nc')),
  status TEXT DEFAULT 'ok'
);

-- ============================================================
-- SAVING / ORDINI
-- ============================================================
CREATE TABLE IF NOT EXISTS saving (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cod_utente INTEGER,
  utente TEXT,
  num_doc INTEGER,
  data_doc DATE,
  alfa_documento TEXT,
  str_ric TEXT,
  stato_dms TEXT,
  codice_fornitore INTEGER,
  ragione_sociale TEXT,
  accred_albo BOOLEAN,
  protoc_ordine NUMERIC,
  protoc_commessa TEXT,
  grp_merceol NUMERIC,
  desc_gruppo_merceol TEXT,
  centro_di_costo TEXT,
  desc_cdc TEXT,
  valuta TEXT,
  imp_iniziale NUMERIC,
  imp_negoziato NUMERIC,
  saving_val NUMERIC,
  perc_saving NUMERIC,
  negoziazione BOOLEAN,
  imp_iniziale_eur NUMERIC,
  imp_negoziato_eur NUMERIC,
  saving_eur NUMERIC,
  perc_saving_eur NUMERIC,
  cdc TEXT,
  cambio NUMERIC,
  upload_id UUID REFERENCES upload_log(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_saving_data ON saving(data_doc);
CREATE INDEX IF NOT EXISTS idx_saving_cdc ON saving(cdc);
CREATE INDEX IF NOT EXISTS idx_saving_utente ON saving(utente);
CREATE INDEX IF NOT EXISTS idx_saving_fornitore ON saving(ragione_sociale);
CREATE INDEX IF NOT EXISTS idx_saving_str_ric ON saving(str_ric);

-- ============================================================
-- TEMPO ATTRAVERSAMENTO ORDINI
-- ============================================================
CREATE TABLE IF NOT EXISTS tempo_attraversamento (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  protocol TEXT,
  year_month TEXT,
  days_purchasing NUMERIC,
  days_auto NUMERIC,
  days_other NUMERIC,
  total_days NUMERIC,
  perc_purchasing NUMERIC,
  perc_auto NUMERIC,
  perc_other NUMERIC,
  bottleneck TEXT,
  upload_id UUID REFERENCES upload_log(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tempo_ym ON tempo_attraversamento(year_month);
CREATE INDEX IF NOT EXISTS idx_tempo_bottleneck ON tempo_attraversamento(bottleneck);

-- ============================================================
-- NON CONFORMITÀ
-- ============================================================
CREATE TABLE IF NOT EXISTS non_conformita (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  protocollo_commessa TEXT,
  ragione_sociale TEXT,
  tipo_origine TEXT,
  data_origine DATE,
  utente_origine TEXT,
  codice_prima_fattura TEXT,
  data_prima_fattura DATE,
  importo_prima_fattura NUMERIC,
  delta_giorni NUMERIC,
  non_conformita BOOLEAN,
  upload_id UUID REFERENCES upload_log(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_nc_origine ON non_conformita(data_origine);
CREATE INDEX IF NOT EXISTS idx_nc_fornitore ON non_conformita(ragione_sociale);

-- ============================================================
-- VIEWS UTILI
-- ============================================================

-- KPI mensile saving
CREATE OR REPLACE VIEW v_saving_mensile AS
SELECT
  TO_CHAR(data_doc, 'YYYY-MM') AS anno_mese,
  cdc,
  str_ric,
  COUNT(*) AS n_ordini,
  COUNT(*) FILTER (WHERE negoziazione = TRUE) AS n_negoziati,
  SUM(imp_iniziale_eur) AS impegnato_eur,
  SUM(saving_eur) AS saving_eur,
  CASE WHEN SUM(imp_iniziale_eur) > 0
    THEN ROUND(SUM(saving_eur) / SUM(imp_iniziale_eur) * 100, 2)
    ELSE 0 END AS perc_saving
FROM saving
WHERE alfa_documento IN ('OS', 'OSP', 'PS', 'OPR', 'ORN', 'ORD')
GROUP BY 1, 2, 3;

-- KPI mensile tempi
CREATE OR REPLACE VIEW v_tempi_mensile AS
SELECT
  year_month,
  COUNT(*) AS n_ordini,
  ROUND(AVG(total_days)::NUMERIC, 1) AS avg_total_days,
  ROUND(AVG(days_purchasing)::NUMERIC, 1) AS avg_purchasing,
  ROUND(AVG(days_auto)::NUMERIC, 1) AS avg_auto,
  ROUND(AVG(days_other)::NUMERIC, 1) AS avg_other,
  COUNT(*) FILTER (WHERE bottleneck = 'PURCHASING') AS n_bottleneck_purchasing,
  COUNT(*) FILTER (WHERE bottleneck = 'AUTO') AS n_bottleneck_auto,
  COUNT(*) FILTER (WHERE bottleneck = 'OTHER') AS n_bottleneck_other
FROM tempo_attraversamento
GROUP BY 1;

-- KPI mensile NC
CREATE OR REPLACE VIEW v_nc_mensile AS
SELECT
  TO_CHAR(data_origine, 'YYYY-MM') AS anno_mese,
  tipo_origine,
  COUNT(*) AS n_totale,
  COUNT(*) FILTER (WHERE non_conformita = TRUE) AS n_nc,
  ROUND(COUNT(*) FILTER (WHERE non_conformita = TRUE)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS perc_nc,
  ROUND(AVG(delta_giorni)::NUMERIC, 1) AS avg_delta_giorni
FROM non_conformita
GROUP BY 1, 2;
