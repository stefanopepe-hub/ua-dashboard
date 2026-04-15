-- ============================================================
-- SCHEMA: Dashboard Ufficio Acquisti — Fondazione Telethon ETS
-- Versione: 3.0 — aggiornata per corrispondere al backend v10
-- ============================================================

-- Tabella upload log
CREATE TABLE IF NOT EXISTS upload_log (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filename            TEXT NOT NULL,
  upload_date         TIMESTAMPTZ DEFAULT NOW(),
  rows_inserted       INTEGER,
  rows_skipped        INTEGER DEFAULT 0,
  tipo                TEXT CHECK (tipo IN ('saving', 'tempi', 'nc', 'risorse')),
  family_detected     TEXT,
  mapping_confidence  TEXT CHECK (mapping_confidence IN ('high', 'medium', 'low')),
  mapping_score       NUMERIC,
  status              TEXT DEFAULT 'ok'
);

-- ============================================================
-- SAVING / ORDINI
-- Nomi colonne allineati con backend v10 (domain.py build_record)
-- ============================================================
CREATE TABLE IF NOT EXISTS saving (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Riferimento upload
  upload_id             UUID REFERENCES upload_log(id) ON DELETE CASCADE,

  -- Data e documento
  data_doc              DATE,
  alfa_documento        TEXT,
  num_doc               INTEGER,
  stato_dms             TEXT,
  str_ric               TEXT,

  -- Fornitore
  ragione_sociale       TEXT,
  codice_fornitore      INTEGER,
  accred_albo           BOOLEAN DEFAULT FALSE,

  -- Utente / Buyer
  cod_utente            INTEGER,
  utente                TEXT,
  utente_presentazione  TEXT,   -- "utente per presentazione" — buyer che firma

  -- Protocolli
  protoc_ordine         NUMERIC,
  protoc_commessa       TEXT,
  prefisso_commessa     TEXT,   -- es. "GMR", "TGM"
  anno_commessa         TEXT,   -- es. "24", "25"
  desc_commessa         TEXT,

  -- Categorizzazione
  grp_merceol           TEXT,
  desc_gruppo_merceol   TEXT,
  macro_categoria       TEXT,
  centro_di_costo       TEXT,
  desc_cdc              TEXT,
  cdc                   TEXT,   -- GD | TIGEM | TIGET | FT | STRUTTURA

  -- Valuta
  valuta                TEXT DEFAULT 'EURO',
  cambio                NUMERIC DEFAULT 1.0,

  -- Importi in EUR (usati per tutte le analytics)
  imp_listino_eur       NUMERIC,   -- Imp. Iniziale € — prezzo di partenza
  imp_impegnato_eur     NUMERIC,   -- Imp. Negoziato € — quanto si paga
  saving_eur            NUMERIC,   -- Saving reale in EUR (con cambio applicato)
  perc_saving_eur       NUMERIC,   -- % saving su imp_listino_eur

  -- Importi in valuta originale (per analisi esposizione valutaria)
  imp_iniziale          NUMERIC,
  imp_negoziato         NUMERIC,
  saving_val            NUMERIC,
  perc_saving           NUMERIC,

  -- Flags
  negoziazione          BOOLEAN DEFAULT FALSE,
  tail_spend            TEXT    -- SI/NO flag per ordini sotto soglia
);

CREATE INDEX IF NOT EXISTS idx_saving_data       ON saving(data_doc);
CREATE INDEX IF NOT EXISTS idx_saving_cdc        ON saving(cdc);
CREATE INDEX IF NOT EXISTS idx_saving_str_ric    ON saving(str_ric);
CREATE INDEX IF NOT EXISTS idx_saving_utente     ON saving(utente_presentazione);
CREATE INDEX IF NOT EXISTS idx_saving_fornitore  ON saving(ragione_sociale);
CREATE INDEX IF NOT EXISTS idx_saving_alfa       ON saving(alfa_documento);
CREATE INDEX IF NOT EXISTS idx_saving_macro      ON saving(macro_categoria);
CREATE INDEX IF NOT EXISTS idx_saving_commessa   ON saving(prefisso_commessa);
CREATE INDEX IF NOT EXISTS idx_saving_upload     ON saving(upload_id);

-- ============================================================
-- TEMPO ATTRAVERSAMENTO ORDINI
-- ============================================================
CREATE TABLE IF NOT EXISTS tempo_attraversamento (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  upload_id        UUID REFERENCES upload_log(id) ON DELETE CASCADE,
  protocol         TEXT,
  year_month       TEXT,
  days_purchasing  NUMERIC,
  days_auto        NUMERIC,
  days_other       NUMERIC,
  total_days       NUMERIC,
  perc_purchasing  NUMERIC,
  perc_auto        NUMERIC,
  perc_other       NUMERIC,
  bottleneck       TEXT
);

CREATE INDEX IF NOT EXISTS idx_tempo_ym         ON tempo_attraversamento(year_month);
CREATE INDEX IF NOT EXISTS idx_tempo_bottleneck ON tempo_attraversamento(bottleneck);

-- ============================================================
-- NON CONFORMITÀ
-- ============================================================
CREATE TABLE IF NOT EXISTS non_conformita (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  upload_id               UUID REFERENCES upload_log(id) ON DELETE CASCADE,
  protocollo_commessa     TEXT,
  ragione_sociale         TEXT,
  tipo_origine            TEXT,
  data_origine            DATE,
  utente_origine          TEXT,
  codice_prima_fattura    TEXT,
  data_prima_fattura      DATE,
  importo_prima_fattura   NUMERIC,
  delta_giorni            NUMERIC,
  non_conformita          BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_nc_origine   ON non_conformita(data_origine);
CREATE INDEX IF NOT EXISTS idx_nc_fornitore ON non_conformita(ragione_sociale);

-- ============================================================
-- PERFORMANCE RISORSE / TEAM (tabella dedicata, opzionale)
-- Se non presente, le analytics Risorse derivano da saving.
-- ============================================================
CREATE TABLE IF NOT EXISTS resource_performance (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  upload_id             UUID REFERENCES upload_log(id) ON DELETE CASCADE,
  year                  INTEGER,
  month                 INTEGER,
  quarter               INTEGER,
  mese_label            TEXT,
  responsabile          TEXT,
  risorsa               TEXT,
  struttura             TEXT,
  business_unit         TEXT,
  pratiche_gestite      INTEGER,
  pratiche_aperte       INTEGER,
  pratiche_chiuse       INTEGER,
  saving_generato       NUMERIC,
  negoziazioni_concluse INTEGER,
  tempo_medio_giorni    NUMERIC,
  efficienza            NUMERIC,
  backlog               INTEGER
);

CREATE INDEX IF NOT EXISTS idx_rp_year    ON resource_performance(year);
CREATE INDEX IF NOT EXISTS idx_rp_risorsa ON resource_performance(risorsa);

-- ============================================================
-- VIEWS UTILI (per reporting SQL diretto — le analytics usano pandas)
-- ============================================================

-- KPI mensile saving
CREATE OR REPLACE VIEW v_saving_mensile AS
SELECT
  TO_CHAR(data_doc, 'YYYY-MM') AS anno_mese,
  cdc,
  str_ric,
  COUNT(*) AS n_ordini,
  COUNT(*) FILTER (WHERE negoziazione = TRUE) AS n_negoziati,
  SUM(imp_listino_eur)    AS listino_eur,
  SUM(imp_impegnato_eur)  AS impegnato_eur,
  SUM(saving_eur)         AS saving_eur,
  CASE WHEN SUM(imp_listino_eur) > 0
    THEN ROUND(SUM(saving_eur) / SUM(imp_listino_eur) * 100, 2)
    ELSE 0 END AS perc_saving
FROM saving
WHERE alfa_documento IN ('OS','OSP','PS','OPR','ORN','ORD','OS001','OSP01','ORN01','ORD01')
GROUP BY 1, 2, 3;

-- KPI mensile tempi
CREATE OR REPLACE VIEW v_tempi_mensile AS
SELECT
  year_month,
  COUNT(*) AS n_ordini,
  ROUND(AVG(total_days)::NUMERIC, 1)      AS avg_total_days,
  ROUND(AVG(days_purchasing)::NUMERIC, 1) AS avg_purchasing,
  ROUND(AVG(days_auto)::NUMERIC, 1)       AS avg_auto,
  ROUND(AVG(days_other)::NUMERIC, 1)      AS avg_other,
  COUNT(*) FILTER (WHERE bottleneck = 'PURCHASING') AS n_bottleneck_purchasing,
  COUNT(*) FILTER (WHERE bottleneck = 'AUTO')       AS n_bottleneck_auto,
  COUNT(*) FILTER (WHERE bottleneck = 'OTHER')      AS n_bottleneck_other
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
