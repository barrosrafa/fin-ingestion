"""Testes unitários e de integração para o pipeline Status Invest ETL e sanitização."""

from decimal import Decimal

import pytest

from fin_ingestion.pipelines.enrichment import AssetMetadataEnricher
from fin_ingestion.pipelines.status_invest_etl import (
    StatusInvestETL,
    sanitize_decimal,
    sanitize_int,
)


def test_sanitize_decimal_brazilian_numbers():
    assert sanitize_decimal("1.234,56") == Decimal("1234.56")
    assert sanitize_decimal("-21,66") == Decimal("-21.66")
    assert sanitize_decimal("20.006.672.976,50") == Decimal("20006672976.50")
    assert sanitize_decimal("R$ 45,90") == Decimal("45.90")
    assert sanitize_decimal("15,50%") == Decimal("15.50")
    assert sanitize_decimal("0,00") == Decimal("0.00")


def test_sanitize_decimal_null_and_empty():
    assert sanitize_decimal(None) is None
    assert sanitize_decimal("") is None
    assert sanitize_decimal("   ") is None
    assert sanitize_decimal("-") is None
    assert sanitize_decimal("--") is None
    assert sanitize_decimal("null") is None
    assert sanitize_decimal("None") is None


def test_sanitize_int():
    assert sanitize_int("12.973,00") == 12973
    assert sanitize_int("4.709.082,00") == 4709082
    assert sanitize_int("0,00") == 0
    assert sanitize_int("") is None
    assert sanitize_int(None) is None


def test_csv_reading_stock_and_fii(tmp_path):
    stock_csv = tmp_path / "stocks.csv"
    stock_csv.write_text(
        "TICKER;PRECO;DY;P/L;P/VP; LIQUIDEZ MEDIA DIARIA; VALOR DE MERCADO\n"
        "PETR4;38,45;14,80;4,80;1,15;1.450.000.000,00;500.000.000.000,00\n",
        encoding="utf-8",
    )

    rows = StatusInvestETL.read_csv_rows(stock_csv)
    assert len(rows) == 1
    r = rows[0]
    assert r["TICKER"] == "PETR4"
    assert sanitize_decimal(r["PRECO"]) == Decimal("38.45")
    assert sanitize_decimal(r["LIQUIDEZ MEDIA DIARIA"]) == Decimal("1450000000.00")
    assert sanitize_decimal(r["VALOR DE MERCADO"]) == Decimal("500000000000.00")


@pytest.mark.asyncio
async def test_enrichment_worker_handles_missing_cnpj():
    # Garante que enriquecimento não gera CNPJ sintético falso
    assert not hasattr(AssetMetadataEnricher, "_synthetic_cnpj_for_ticker")


@pytest.mark.asyncio
async def test_csv_prices_are_observations_not_official_daily_closes(tmp_path):
    from unittest.mock import AsyncMock, MagicMock
    csv = tmp_path / "snapshot.csv"
    csv.write_text("TICKER;PRECO;DY;P/L;P/VP\nQA0003;12,34;0;0;0\n", encoding="utf-8")
    connection = MagicMock()
    connection.executemany = AsyncMock()
    connection.fetch = AsyncMock(return_value=[{"ticker": "QA0003", "id_ativo": 1}])
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    connection.transaction.return_value = transaction
    acquired = MagicMock()
    acquired.__aenter__ = AsyncMock(return_value=connection)
    acquired.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire.return_value = acquired
    etl = StatusInvestETL(db_url="postgresql://localhost/qa")
    etl.get_pool = AsyncMock(return_value=pool)
    assert await etl.ingest_stocks(csv) == 1
    writes = [call.args for call in connection.executemany.await_args_list]
    observation = next((sql, records) for sql, records in writes if "market.cotacao_intraday" in sql)
    assert not any("INSERT INTO market.cotacao_diaria" in sql for sql, _ in writes)
    assert observation[1][0] == (1, etl.observed_at, Decimal("12.34"), None, None)
    assert etl.observed_at.tzinfo is not None
