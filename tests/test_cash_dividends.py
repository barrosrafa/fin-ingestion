from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest

from fin_ingestion.pipelines.cash_dividends import (
    archive_synthetic,
    parse_b3,
    parse_statusinvest,
    persist,
)


def event(**changes):
    return {"isinCode": "BRPETRACNPR6", "label": "JRS CAP PROPRIO",
            "lastDatePrior": "01/06/2026", "paymentDate": "20/08/2026",
            "approvedOn": "11/05/2026", "rate": "0,35048636000", **changes}


def test_equal_installments_and_share_classes_are_preserved():
    rows = parse_b3([{"cashDividends": [event(), event(paymentDate="21/09/2026"),
                    event(isinCode="BRPETRACNOR9")]}], "BRPETRACNPR6")
    assert len(rows) == 2
    assert rows[0]["valor_por_acao"] == Decimal("0.35048636000")
    assert rows[0]["data_ex"] is None  # B3 does not supply the ex-date here.
    assert rows[0]["tipo_provento"] == "JCP"


def test_capital_returns_do_not_suppress_dividend_income():
    rows = parse_b3([{"cashDividends": [event(label="REST CAP DIN"),
                     event(label="AMORTIZACAO RF"), event()]}], "BRPETRACNPR6")
    assert len(rows) == 1
    assert rows[0]["tipo_provento"] == "JCP"


def test_invalid_or_incomplete_events_are_not_invented():
    with pytest.raises(ValueError):
        parse_b3([{"cashDividends": [event(label="UNKNOWN")]}], "BRPETRACNPR6")
    assert parse_b3([{"cashDividends": [event(paymentDate="")]}], "BRPETRACNPR6") == []


def test_statusinvest_taxed_income_keeps_precision_and_type():
    rows = parse_statusinvest('''<table><thead><tr><th>Tipo</th><th>DATA COM</th>
        <th>Pagamento</th><th>Valor</th></tr></thead><tbody><tr>
        <td>Rend. Tributado</td><td>22/04/2026</td><td>22/06/2026</td>
        <td>0,01314955</td></tr></tbody></table>''')
    assert rows[0]["valor_por_acao"] == Decimal("0.01314955")
    assert rows[0]["tipo_provento"] == "RENDIMENTO_TRIBUTADO"


@pytest.mark.asyncio
async def test_real_postgres_installments_replay_quarantine_and_source_precedence(tmp_path):
    pgserver = pytest.importorskip("pgserver")
    server = pgserver.get_server(tmp_path / "pg", cleanup_mode="stop")
    conn = await asyncpg.connect(server.get_uri())
    try:
        await conn.execute("""
            CREATE SCHEMA market; CREATE SCHEMA ingestion; CREATE SCHEMA analytics;
            CREATE TYPE market.tipo_provento_enum AS ENUM('DIVIDENDO','JCP','RENDIMENTO');
            CREATE TABLE market.provento(id_provento bigserial PRIMARY KEY,id_ativo int,
                tipo_provento market.tipo_provento_enum,data_com date,data_ex date,
                data_pagamento date,data_anuncio date,valor_por_acao numeric(12,6),
                mes_referencia text,observacao text,
                UNIQUE(id_ativo,tipo_provento,data_com,data_pagamento));
            CREATE UNIQUE INDEX uq_provento_ativo_tipo_ex_valor
                ON market.provento(id_ativo,tipo_provento,data_ex,valor_por_acao);
            CREATE VIEW analytics.payment_values WITH(security_barrier=true) AS SELECT valor_por_acao FROM market.provento;
            CREATE MATERIALIZED VIEW analytics.payment_sum AS SELECT sum(valor_por_acao) AS total FROM analytics.payment_values WITH NO DATA;
            CREATE UNIQUE INDEX payment_sum_total ON analytics.payment_sum(total);
            COMMENT ON VIEW analytics.payment_values IS 'retained comment';
            CREATE TABLE market.calendario_proventos(id_provento bigint REFERENCES market.provento);
            INSERT INTO market.provento(id_ativo,tipo_provento,data_com,data_ex,data_pagamento,
                valor_por_acao,mes_referencia)
            VALUES(1,'DIVIDENDO','2026-08-23','2026-08-24','2026-08-24',1.348143,'2026-08'),
                  (2,'DIVIDENDO','2024-08-21','2024-08-22','2024-11-20',1.15,'08/2024');
        """)
        for path in sorted((Path(__file__).parents[1] / "migrations/22_cash_dividend_integrity").glob("*.sql")):
            await conn.execute(path.read_text())
        assert await conn.fetchval("SELECT obj_description('analytics.payment_values'::regclass)") == 'retained comment'
        assert await conn.fetchval("SELECT reloptions FROM pg_class WHERE oid='analytics.payment_values'::regclass") == ['security_barrier=true']
        await conn.execute("REFRESH MATERIALIZED VIEW analytics.payment_sum")
        async with conn.transaction():
            assert await archive_synthetic(conn) == 2
            assert await archive_synthetic(conn) == 0
        assert await conn.fetchval("SELECT count(*) FROM ingestion.provento_quarentena") == 2
        assert {r["motivo"] for r in await conn.fetch("SELECT motivo FROM ingestion.provento_quarentena")} == {
            "YAHOO_AGGREGATE_WITH_INVENTED_PAYMENT", "LEGACY_WITHOUT_SOURCE_EVIDENCE"}
        rows = parse_b3([{"cashDividends": [event(), event(paymentDate="21/09/2026")]}], "BRPETRACNPR6")
        for _ in range(2):
            await persist(conn, 1, rows, "B3")
        await conn.execute("UPDATE market.provento SET data_ex='2026-06-02'")
        assert await conn.fetchval("SELECT count(*) FROM market.provento") == 2
        assert await conn.fetchval("SELECT sum(valor_por_acao) FROM market.provento") == Decimal("0.70097272000")
        income = parse_b3([{"cashDividends": [event(label="RENDIMENTO", rate="0,02038398")]}], "BRPETRACNPR6")
        await persist(conn, 1, income, "B3")
        corrected = [dict(income[0], tipo_provento="RENDIMENTO_TRIBUTADO", valor_por_acao=Decimal("0.01314955"))]
        await persist(conn, 1, corrected, "STATUSINVEST_USER_HTML")
        await persist(conn, 1, income, "B3")
        assert await archive_synthetic(conn) == 0
        assert await conn.fetchval("SELECT count(*) FROM market.provento") == 3
        assert await conn.fetchval("SELECT valor_por_acao FROM market.provento WHERE tipo_provento='RENDIMENTO_TRIBUTADO'") == Decimal("0.01314955")
    finally:
        await conn.close()
        server.cleanup()


@pytest.mark.asyncio
async def test_seasonality_rebuild_removes_old_income_and_preserves_payment_months(tmp_path):
    from fin_ingestion.pipelines.dividend_seasonality_pipeline import DividendSeasonalityAggregator

    pgserver = pytest.importorskip("pgserver")
    server = pgserver.get_server(tmp_path / "pg", cleanup_mode="stop")
    conn = await asyncpg.connect(server.get_uri())
    try:
        await conn.execute("""
            CREATE SCHEMA market; CREATE SCHEMA analytics;
            CREATE TABLE market.provento(id_ativo int,data_com date,data_pagamento date,valor_por_acao numeric);
            CREATE TABLE market.cotacao_diaria(id_ativo int,data_pregao date,preco_fechamento numeric);
            CREATE TABLE analytics.mapa_sazonalidade_dividendo(id_ativo int,mes_calendario smallint,
                tipo_evento text,ocorrencias_historicas int DEFAULT 0,anos_analisados int DEFAULT 1,
                valor_medio_provento numeric,yield_medio_mensal numeric,ultimo_ano_pago int,
                data_atualizacao timestamptz DEFAULT now(),PRIMARY KEY(id_ativo,mes_calendario,tipo_evento));
            INSERT INTO analytics.mapa_sazonalidade_dividendo(id_ativo,mes_calendario,tipo_evento)
                VALUES(999,1,'PAGAMENTO');
            INSERT INTO market.provento VALUES
                (1,'2026-08-21','2026-11-23',0.67407131),
                (1,'2026-08-21','2026-12-21',0.67407131);
        """)
        aggregator = DividendSeasonalityAggregator()
        for _ in range(2):
            result = await aggregator.aggregate_seasonality(conn=conn)
            assert result["total_records"] == 3
        assert await conn.fetchval("SELECT count(*) FROM analytics.mapa_sazonalidade_dividendo WHERE id_ativo=999") == 0
        months = await conn.fetch("SELECT mes_calendario FROM analytics.mapa_sazonalidade_dividendo WHERE tipo_evento='PAGAMENTO' ORDER BY 1")
        assert [r["mes_calendario"] for r in months] == [11, 12]
    finally:
        await conn.close()
        server.cleanup()
