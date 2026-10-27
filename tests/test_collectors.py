"""Testes unitários dos coletores oficiais e plataformas de análise."""

import pytest

from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.collectors.official.bcb import BCBCollector
from fin_ingestion.collectors.sites.brick360 import Brick360Collector
from fin_ingestion.collectors.sites.etfs_brasil import ETFsBrasilCollector
from fin_ingestion.collectors.sites.investidor10 import Investidor10Collector


@pytest.mark.asyncio
async def test_b3_cothist_line_parsing():
    collector = B3Collector()
    # Padrão posicional exato:
    # 00..02: "01" (2)
    # 02..10: "20261026" (8)
    # 10..12: "02" (2)
    # 12..24: "PETR4       " (12)
    # 24..27: "010" (3)
    # 27..39: "PETROBRAS   " (12)
    # 39..49: "PN        " (10)
    # 49..52: "   " (3)
    # 52..56: "R$  " (4)
    # 56..69: "0000000003750" (13) -> 37.50
    # 69..82: "0000000003820" (13) -> 38.20
    # 82..95: "0000000003710" (13) -> 37.10
    # 95..108: "0000000003780" (13) -> 37.80
    # 108..121: "0000000003785" (13) -> 37.85
    # 121..134: "0000000003784" (13)
    # 134..147: "0000000003786" (13)
    # 147..152: "01245" (5)
    # 152..170: "000000000000025400" (18)
    # 170..188: "000000000000961390" (18)
    # 188..201: "0000000000000" (13)
    # 201..202: "0" (1)
    # 202..210: "99991231" (8)
    # 210..217: "0000001" (7)
    # 217..230: "             " (13)
    # 230..242: "BRPETRACNPR6" (12)
    # 242..245: "100" (3)
    sample_line = (
        "01"
        "20261026"
        "02"
        "PETR4       "
        "010"
        "PETROBRAS   "
        "PN        "
        "   "
        "R$  "
        "0000000003750"
        "0000000003820"
        "0000000003710"
        "0000000003780"
        "0000000003785"
        "0000000003784"
        "0000000003786"
        "01245"
        "000000000000025400"
        "000000000000961390"
        "0000000000000"
        "0"
        "99991231"
        "0000001"
        "             "
        "BRPETRACNPR6"
        "100"
    )
    record = collector.parse_cothist_line(sample_line)

    assert record["ticker"] == "PETR4"
    assert record["data_pregao"] == "2026-10-26"
    assert record["preco_abertura"] == 37.50
    assert record["preco_maximo"] == 38.20
    assert record["preco_minimo"] == 37.10
    assert record["preco_fechamento"] == 37.85
    assert record["codigo_isin"] == "BRPETRACNPR6"
    await collector.close()


@pytest.mark.asyncio
async def test_bcb_collector_parse():
    collector = BCBCollector()
    mock_payload = {
        "serie_name": "selic_over",
        "serie_id": 11,
        "data": [
            {"data": "25/10/2026", "valor": "0.040156"},
            {"data": "26/10/2026", "valor": "0.040156"},
        ],
    }
    records = collector.parse(mock_payload)
    assert len(records) == 2
    assert records[0]["serie_nome"] == "selic_over"
    assert records[0]["valor"] == 0.040156
    await collector.close()


@pytest.mark.asyncio
async def test_investidor10_collector_parse():
    collector = Investidor10Collector()
    mock_payload = {
        "ticker": "PETR4",
        "endpoint": "indicadores",
        "data": {"pl": 4.5, "pvp": 1.1, "roe": 24.5, "dy": 14.8},
    }
    records = collector.parse(mock_payload)
    assert len(records) == 1
    assert records[0]["ticker"] == "PETR4"
    assert records[0]["preco_lucro"] == 4.5
    assert records[0]["dividend_yield"] == 14.8
    await collector.close()


@pytest.mark.asyncio
async def test_brick360_collector_parse():
    collector = Brick360Collector()
    mock_payload = {
        "ticker": "HGLG11",
        "data": {
            "brick_score": {
                "global_score": 88.5,
                "eixo_imobiliario": 85.0,
            },
            "walt_years": 4.5,
        },
    }
    records = collector.parse(mock_payload)
    assert len(records) == 1
    assert records[0]["ticker"] == "HGLG11"
    assert records[0]["score_global"] == 88.5
    assert records[0]["walt_anos"] == 4.5
    await collector.close()


@pytest.mark.asyncio
async def test_etfs_brasil_collector_parse():
    collector = ETFsBrasilCollector()
    mock_payload = {
        "ticker": "BOVA11",
        "data": {
            "taxa_total_ter": 0.30,
            "nav": 124.50,
            "spread_nav_pct": 0.04,
        },
    }
    records = collector.parse(mock_payload)
    assert len(records) == 1
    assert records[0]["ticker"] == "BOVA11"
    assert records[0]["custo_total_ter"] == 0.30
    assert records[0]["spread_nav_pct"] == 0.04
    await collector.close()
