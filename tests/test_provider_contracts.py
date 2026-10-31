"""Offline provider contract tests using the documented payload shapes."""

import io
import zipfile
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

import pytest

from fin_ingestion.analytics.adjustments import adjusted_prices
from fin_ingestion.collectors.commercial.brapi import BrapiCollector
from fin_ingestion.collectors.commercial.international import MassiveCollector, TwelveDataCollector
from fin_ingestion.collectors.official.cvm import CVMCollector
from fin_ingestion.collectors.official.olinda import OlindaCollector
from fin_ingestion.collectors.official.tesouro import TesouroCollector
from fin_ingestion.config import settings
from fin_ingestion.pipelines.market_gateway import MarketGateway


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(settings, "distributed_source_controls", False)
    monkeypatch.setattr(settings, "intraday_enabled", False)


@pytest.mark.asyncio
async def test_brapi_batches_and_checks_coverage(monkeypatch):
    monkeypatch.setattr(settings, "brapi_batch_size", 1)
    c = BrapiCollector()
    c.request_json = AsyncMock(
        side_effect=[
            {"results": [{"requestedSymbol": "PETR4", "symbol": "PETR4", "data": {}}]},
            {"results": []},
        ]
    )
    with pytest.raises(ValueError, match="coverage"):
        await c.extract(tickers=["PETR4", "VALE3"])
    assert c.request_json.await_count == 2
    assert c.request_json.call_args_list[0].kwargs["params"]["symbols"] == "PETR4"


def test_brapi_v2_snapshot_preserves_quote_time():
    records = BrapiCollector().parse(
        {
            "endpoint": "quote",
            "results": [
                {
                    "symbol": "PETR4",
                    "data": {
                        "regularMarketTime": "2026-10-30T15:00:00Z",
                        "regularMarketPrice": 37,
                        "currency": "BRL",
                    },
                }
            ],
        }
    )
    assert records[0]["timestamp"] == datetime(2026, 10, 30, 15, tzinfo=timezone.utc)
    assert records[0]["price_kind"] == "intraday_snapshot"


def test_adjusted_brapi_history_is_never_nominal():
    with pytest.raises(ValueError, match="raw OHLC"):
        BrapiCollector().parse(
            {
                "endpoint": "historical",
                "results": [
                    {
                        "symbol": "PETR4",
                        "data": {"historicalDataPrice": [{"date": 1, "open": 1, "close": 2}]},
                    }
                ],
            }
        )


@pytest.mark.asyncio
async def test_missing_commercial_credentials_do_not_open_redis(monkeypatch):
    monkeypatch.setattr(settings, "massive_api_key", None)
    collector = MassiveCollector()
    with pytest.raises(ValueError, match="credentials"):
        collector.authorize()
    assert collector.budget_redis is None


@pytest.mark.asyncio
async def test_massive_requires_unadjusted_and_trusted_pagination():
    collector = MassiveCollector()
    collector.request_json = AsyncMock(return_value={"adjusted": True, "results": []})
    with pytest.raises(ValueError, match="adjusted=false"):
        await collector.extract(ticker="AAPL", start_date="2026-10-01", end_date="2026-10-02")
    collector.request_json.return_value = {
        "adjusted": False,
        "results": [],
        "next_url": "https://attacker.test/?apiKey=secret",
    }
    with pytest.raises(ValueError, match="pagination"):
        await collector.extract(ticker="AAPL", start_date="2026-10-01", end_date="2026-10-02")


def test_twelve_daily_dates_are_not_converted_to_fake_utc_times():
    rows = TwelveDataCollector().parse(
        {
            "ticker": "AAPL",
            "interval": "1day",
            "meta": {"currency": "USD"},
            "values": [
                {"datetime": "2026-10-30", "open": "1", "high": "2", "low": "1", "close": "2"}
            ],
        }
    )
    assert rows[0]["reference_date"] == date(2026, 10, 30)
    assert "timestamp" not in rows[0]


def test_treasury_brazilian_decimal_and_absent_values():
    text = "Tipo Titulo;Data Vencimento;Data Base;Taxa Compra Manha;Taxa Venda Manha;PU Compra Manha;PU Venda Manha;PU Base Manha\nTesouro IPCA+;15/05/2035;30/10/2026;6,35;;1.234,56;;1.200,00\n"
    row = TesouroCollector().parse(text.encode())[0]
    assert str(row["buy_price"]) == "1234.56" and row["sell_price"] is None


def test_ptax_bulletins_are_distinct_and_use_brazilian_timezone():
    records = OlindaCollector().parse(
        {
            "endpoint": "ptax",
            "currency": "USD",
            "data": [
                {
                    "tipoBoletim": "Abertura",
                    "dataHoraCotacao": "2026-10-30 10:00:00.000",
                    "cotacaoCompra": 5,
                    "cotacaoVenda": 5.1,
                },
                {
                    "tipoBoletim": "Fechamento",
                    "dataHoraCotacao": "2026-10-30 13:00:00.000",
                    "cotacaoCompra": 5,
                    "cotacaoVenda": 5.1,
                },
            ],
        }
    )
    assert records[0]["quoted_at"].hour == 13
    assert records[0]["bulletin"] != records[1]["bulletin"]


@pytest.mark.asyncio
async def test_odata_filter_uses_percent_encoded_spaces():
    import httpx

    c = OlindaCollector()
    c.http_client.get = AsyncMock(
        side_effect=[
            httpx.Response(
                200,
                json={"value": [{"Data": "2026-10-23"}]},
                request=httpx.Request("GET", "https://example.test"),
            ),
            httpx.Response(
                200, json={"value": []}, request=httpx.Request("GET", "https://example.test")
            ),
        ]
    )
    c.raw_sink.save_payload = AsyncMock()
    await c.extract(endpoint="ExpectativasMercadoAnuais", reference_date="2026-10-23")
    url = c.http_client.get.call_args.args[0]
    assert "%20eq%20" in url and "+eq+" not in url


def test_cvm_scope_and_versions_are_never_collapsed():
    buffer = io.BytesIO()
    header = "CNPJ_CIA;DT_REFER;VERSAO;CD_CONTA;DT_FIM_EXERC;ORDEM_EXERC;MOEDA;ESCALA_MOEDA;VL_CONTA;DS_CONTA\n"
    with zipfile.ZipFile(buffer, "w") as archive:
        for scope in ("con", "ind"):
            archive.writestr(
                f"dfp_cia_aberta_BPA_{scope}_2026.csv",
                header
                + "00.000.000/0001-00;2026-12-31;1;1;2026-12-31;ÚLTIMO;REAL;MIL;100;Ativo\n".encode(
                    "latin1"
                ).decode("latin1"),
            )
    # zipfile encodes str as UTF-8; use ASCII order label here to isolate identity.
    records = CVMCollector().parse(
        {"endpoint": "financials", "filing": "dfp", "content": buffer.getvalue()}
    )
    assert {r["scope"] for r in records} == {"con", "ind"}
    assert len(records) == 2


@pytest.mark.asyncio
async def test_intraday_staleness_is_rejected_before_writing(monkeypatch):
    monkeypatch.setattr(settings, "intraday_enabled", True)
    g = MarketGateway()
    g.brapi.extract = AsyncMock(return_value={})
    g.brapi.parse = lambda _: [
        {"ticker": "PETR4", "timestamp": datetime(2026, 10, 29, tzinfo=timezone.utc)}
    ]
    g.writer.write_price_observation = AsyncMock()
    with pytest.raises(ValueError, match="Stale"):
        await g.intraday(["PETR4"], now=datetime(2026, 10, 30, tzinfo=timezone.utc))
    g.writer.write_price_observation.assert_not_called()


def test_dynamic_adjustments_are_point_in_time_and_do_not_mutate_nominal():
    prices = [{"ticker": "A", "reference_date": "2026-01-01", "close": 100}]
    events = [
        {
            "source": "s",
            "event_id": "split",
            "ticker": "A",
            "event_type": "split",
            "factor": 10,
            "observed_at": "2026-02-01T00:00:00Z",
            "published_at": "2026-02-01T00:00:00Z",
            "ex_date": "2026-02-02",
        }
    ]
    assert adjusted_prices(prices, events, "2026-01-31T00:00:00Z")[0]["adjusted_close"] == 100
    assert adjusted_prices(prices, events, "2026-02-03T00:00:00Z")[0]["adjusted_close"] == 10
    assert prices[0]["close"] == 100
