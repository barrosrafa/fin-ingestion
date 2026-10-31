-- Expand only. Existing legacy rows remain intact and retain their provenance.
CREATE SCHEMA IF NOT EXISTS ingestion;
CREATE SCHEMA IF NOT EXISTS canonical;
CREATE SCHEMA IF NOT EXISTS market;
CREATE TABLE IF NOT EXISTS market.sgs_observation (
    series_code INTEGER NOT NULL, reference_date DATE NOT NULL,
    series_name TEXT NOT NULL, value NUMERIC NOT NULL,
    PRIMARY KEY(series_code,reference_date)
);
CREATE TABLE IF NOT EXISTS canonical.provider_record (
    source TEXT NOT NULL, dataset TEXT NOT NULL, record_key TEXT NOT NULL,
    payload JSONB NOT NULL, captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(source, dataset, record_key)
);
CREATE TABLE IF NOT EXISTS market.nominal_bar (
    ticker TEXT NOT NULL, source TEXT NOT NULL, interval TEXT NOT NULL,
    reference_date DATE NOT NULL, timestamp_utc TIMESTAMPTZ,
    open NUMERIC NOT NULL CHECK(open > 0), high NUMERIC NOT NULL CHECK(high > 0),
    low NUMERIC NOT NULL CHECK(low > 0), close NUMERIC NOT NULL CHECK(close > 0),
    volume NUMERIC CHECK(volume >= 0), currency TEXT NOT NULL,
    bar_key TEXT NOT NULL, captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(ticker,source,interval,bar_key),
    CHECK(low <= open AND open <= high AND low <= close AND close <= high),
    CHECK(interval = 'day' OR timestamp_utc IS NOT NULL)
);
CREATE TABLE IF NOT EXISTS market.ptax_observation (
    currency TEXT NOT NULL, quoted_at TIMESTAMPTZ NOT NULL, bulletin TEXT NOT NULL,
    buy NUMERIC NOT NULL CHECK(buy > 0), sell NUMERIC NOT NULL CHECK(sell > 0),
    PRIMARY KEY(currency,quoted_at,bulletin)
);
CREATE TABLE IF NOT EXISTS market.treasury_quote (
    title TEXT NOT NULL, maturity DATE NOT NULL, reference_date DATE NOT NULL,
    buy_rate NUMERIC, sell_rate NUMERIC, buy_price NUMERIC CHECK(buy_price >= 0),
    sell_price NUMERIC CHECK(sell_price >= 0), base_price NUMERIC CHECK(base_price >= 0),
    PRIMARY KEY(title,maturity,reference_date)
);
CREATE TABLE IF NOT EXISTS market.focus_expectation (
    indicator TEXT NOT NULL, published_on DATE NOT NULL, reference TEXT NOT NULL,
    detail TEXT NOT NULL, base_calculation INTEGER NOT NULL, frequency TEXT NOT NULL, payload JSONB NOT NULL,
    PRIMARY KEY(indicator,detail,published_on,reference,base_calculation,frequency)
);
CREATE TABLE IF NOT EXISTS market.debenture_quote (
    code TEXT NOT NULL, reference_date DATE NOT NULL, source TEXT NOT NULL,
    payload JSONB NOT NULL, PRIMARY KEY(code,reference_date,source)
);
CREATE TABLE IF NOT EXISTS canonical.cvm_statement (
    cnpj TEXT NOT NULL, filing TEXT NOT NULL, statement TEXT NOT NULL, scope TEXT NOT NULL,
    reference_date DATE NOT NULL, version INTEGER NOT NULL, account TEXT NOT NULL,
    period_start TEXT NOT NULL, period_end DATE NOT NULL, exercise_order TEXT NOT NULL,
    currency TEXT NOT NULL, scale TEXT NOT NULL, amount NUMERIC NOT NULL,
    description TEXT, payload JSONB NOT NULL, captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(cnpj,filing,statement,scope,reference_date,version,account,
                period_start,period_end,exercise_order,currency,scale)
);
CREATE OR REPLACE VIEW canonical.cvm_statement_latest AS
SELECT * FROM (
    SELECT f.*, max(version) OVER (
        PARTITION BY cnpj,filing,statement,scope,reference_date
    ) AS latest_document_version FROM canonical.cvm_statement f
) ranked WHERE version=latest_document_version;
CREATE TABLE IF NOT EXISTS canonical.corporate_action_revision (
    source TEXT NOT NULL, event_id TEXT NOT NULL, revision_hash TEXT NOT NULL,
    ticker TEXT NOT NULL, event_type TEXT NOT NULL, payload JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(source,event_id,revision_hash)
);
CREATE OR REPLACE VIEW canonical.corporate_action_latest AS
SELECT DISTINCT ON (source,event_id) * FROM canonical.corporate_action_revision
ORDER BY source,event_id,observed_at DESC,revision_hash;

-- New writers carry source_id; null legacy provenance stays outside the new identity.
ALTER TABLE market.cotacao_intraday ADD COLUMN IF NOT EXISTS source_id TEXT;
ALTER TABLE market.cotacao_intraday ADD COLUMN IF NOT EXISTS price_kind TEXT NOT NULL DEFAULT 'intraday_snapshot';
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM market.cotacao_intraday WHERE source_id IS NOT NULL
             GROUP BY id_ativo,timestamp,source_id,price_kind HAVING count(*)>1) THEN
    RAISE EXCEPTION 'Known-source intraday duplicates require review; no rows were deleted';
  END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS uq_intraday_source_snapshot
    ON market.cotacao_intraday(id_ativo,timestamp,source_id,price_kind)
    WHERE source_id IS NOT NULL;
