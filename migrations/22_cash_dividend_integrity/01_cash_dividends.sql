-- Preserve the precision published by B3, including small per-share amounts.
ALTER TABLE market.provento ALTER COLUMN valor_por_acao TYPE numeric(24,11);
-- Equal installments with the same ex-date are separate payment events.
DROP INDEX IF EXISTS market.uq_provento_ativo_tipo_ex_valor;
ALTER TABLE market.provento ADD COLUMN IF NOT EXISTS fonte_provento text;
ALTER TABLE market.provento ADD COLUMN IF NOT EXISTS payload_fonte jsonb;

CREATE TABLE IF NOT EXISTS ingestion.provento_quarentena (
    id_provento bigint PRIMARY KEY,
    registro_original jsonb NOT NULL,
    motivo text NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ingestion.provento_snapshot (
    sha256 text PRIMARY KEY,
    emissor text NOT NULL,
    fonte text NOT NULL,
    payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now()
);
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='fin_ingestion_rw') THEN
        GRANT SELECT,INSERT ON ingestion.provento_quarentena,
            ingestion.provento_snapshot TO fin_ingestion_rw;
    END IF;
END $grants$;
