from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from fin_ingestion.pipelines.analise_de_acoes_pipeline import (
    PtaxExchangeRatePipeline,
    USAssetsIngestionPipeline,
)
from fin_ingestion.pipelines.dividend_seasonality_pipeline import DividendSeasonalityAggregator


@pytest.mark.asyncio
async def test_dividend_seasonality_aggregator():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = "INSERT 0 12"
    mock_conn.fetchval.return_value = 41

    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    aggregator = DividendSeasonalityAggregator(db_pool=mock_pool)
    result = await aggregator.aggregate_seasonality()

    assert result["status"] == "success"
    assert result["total_records"] == 41
    mock_conn.execute.assert_called_once()


@pytest.mark.asyncio
async def test_ptax_pipeline():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = "INSERT 0 1"
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    ptax_pipeline = PtaxExchangeRatePipeline(db_pool=mock_pool)
    res = await ptax_pipeline.ingest_daily_ptax(
        target_date=date(2026, 10, 27),
        moeda_origem="USD",
        taxa_compra=5.352,
        taxa_venda=5.3526,
    )
    assert res["currency"] == "USD"
    assert res["ptax_venda"] == 5.3526
    mock_conn.execute.assert_called_once()


@pytest.mark.asyncio
async def test_us_assets_reit_sync():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetchval.return_value = 99
    mock_conn.execute.return_value = "INSERT 0 1"
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    reit_pipeline = USAssetsIngestionPipeline(db_pool=mock_pool)
    res = await reit_pipeline.sync_reit_metrics(
        ticker="O",
        subtipo="Retail Net Lease",
        ffo_12m=3450000000.0,
        affo_12m=3120000000.0,
        ffo_per_share=4.12,
        affo_per_share=3.75,
        p_ffo=12.8,
        p_affo=14.1,
        payout_ratio=76.5,
        occupancy_rate=98.7,
        properties_count=15450,
        leverage_ev=32.5,
    )
    assert res["ticker"] == "O"
    assert res["status"] == "synced"
    assert res["id_ativo"] == 99
