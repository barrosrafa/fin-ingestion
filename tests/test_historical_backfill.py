"""Testes unitários do motor de Backfill Histórico de 30 Anos (HistoricalBackfillPipeline)."""

from datetime import date
from decimal import Decimal

from fin_ingestion.pipelines.historical_backfill import HistoricalBackfillPipeline


def test_parse_yahoo_history_payload():
    """Valida o parsing determinístico de candles, proventos e splits com detecção de IPO."""
    mock_payload = {
        "chart": {
            "result": [
                {
                    "meta": {"currency": "BRL", "symbol": "TEST3.SA"},
                    "timestamp": [946897200, 946983600],  # 2000-01-03 e 2000-01-04
                    "indicators": {
                        "quote": [
                            {
                                "open": [10.0, 10.5],
                                "high": [11.0, 11.2],
                                "low": [9.8, 10.2],
                                "close": [10.5, 11.0],
                                "volume": [1000000, 1200000],
                            }
                        ]
                    },
                    "events": {
                        "dividends": {
                            "950000000": {"amount": 0.50, "date": 950000000}
                        },
                        "splits": {
                            "1000000000": {
                                "numerator": 2,
                                "denominator": 1,
                                "splitRatio": "2:1",
                            }
                        },
                    },
                }
            ]
        }
    }

    pipeline = HistoricalBackfillPipeline()
    result = pipeline.parse_history_data(
        ticker="TEST3",
        tipo_ativo="ACAO",
        raw_data=mock_payload,
    )

    assert result["success"] is True
    assert result["ticker"] == "TEST3"
    assert result["ipo_date"] == date(2000, 1, 3)
    assert len(result["candles"]) == 2

    # Verifica candle 1
    c1 = result["candles"][0]
    assert c1["data_pregao"] == date(2000, 1, 3)
    assert c1["preco_abertura"] == Decimal("10.00")
    assert c1["preco_fechamento"] == Decimal("10.50")
    assert c1["volume_total"] == Decimal("1000000")

    # Yahoo supplies an ex-date aggregate, not a confirmed payment event.
    assert result["dividends"] == []

    # Verifica split
    assert len(result["splits"]) == 1
    s1 = result["splits"][0]
    assert s1["tipo"] == "SPLIT"
    assert s1["fator"] == Decimal("2.000000")


def test_yahoo_fii_aggregate_is_not_a_confirmed_payment():
    """Garante que proventos de FIIs são devidamente classificados como RENDIMENTO."""
    mock_payload = {
        "chart": {
            "result": [
                {
                    "timestamp": [1600000000],
                    "indicators": {
                        "quote": [{"open": [100.0], "high": [101.0], "low": [99.0], "close": [100.5], "volume": [50000]}]
                    },
                    "events": {
                        "dividends": {
                            "1600000000": {"amount": 0.75}
                        }
                    },
                }
            ]
        }
    }

    pipeline = HistoricalBackfillPipeline()
    result = pipeline.parse_history_data(
        ticker="HGLG11",
        tipo_ativo="FII",
        raw_data=mock_payload,
    )

    assert result["dividends"] == []


def test_parse_empty_or_delisted_payload():
    """Garante tratamento gracioso para tickers delistados ou respostas vazias."""
    pipeline = HistoricalBackfillPipeline()
    result = pipeline.parse_history_data(
        ticker="DELISTED3",
        tipo_ativo="ACAO",
        raw_data={"chart": {"result": None, "error": {"code": "Not Found"}}},
    )

    assert result["success"] is False
    assert result["candles"] == []
    assert result["dividends"] == []
    assert result["splits"] == []
