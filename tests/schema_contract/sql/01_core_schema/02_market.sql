CREATE TABLE market.cotacao_diaria (
    data_pregao        DATE NOT NULL,
    id_ativo           INT  NOT NULL REFERENCES core.ativo(id_ativo) ON DELETE RESTRICT,
    preco_abertura     NUMERIC(10,2),
    preco_maximo       NUMERIC(10,2),
    preco_minimo       NUMERIC(10,2),
    preco_fechamento   NUMERIC(10,2),
    preco_medio        NUMERIC(10,2),
    volume_total       NUMERIC(18,2),
    quantidade_negocios INT,
    preco_ajustado     NUMERIC(10,2),
    variacao_percentual NUMERIC(8,4),
    preco_ajuste_anterior NUMERIC(10,2),
    preco_fechamento_anterior NUMERIC(10,2),
    quantidade_teorica BIGINT,
    ajuste             NUMERIC(10,2),
    PRIMARY KEY (id_ativo, data_pregao)
);

CREATE TABLE market.cotacao_intraday (
    id_tick      BIGSERIAL,
    id_ativo     INT NOT NULL REFERENCES core.ativo(id_ativo) ON DELETE RESTRICT,
    timestamp    TIMESTAMPTZ NOT NULL,
    preco_atual  NUMERIC(12,4),
    variacao     NUMERIC(8,4),
    volume       NUMERIC(18,2)
);