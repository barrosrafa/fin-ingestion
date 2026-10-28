import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import date
from fin_ingestion.collectors.sites.etfs_brasil import ETFsBrasilCollector
from fin_ingestion.pipelines.etfs_brasil_pipeline import ETFsBrasilIngestionPipeline


@pytest.mark.asyncio
async def test_etfs_brasil_collector_detail_and_composition():
    collector = ETFsBrasilCollector()

    # Test extract detail
    detail = await collector.extract(ticker="BOVA11", mode="detail")
    assert detail["ticker"] == "BOVA11"
    parsed_detail = collector.parse(detail)
    assert len(parsed_detail) == 1
    assert parsed_detail[0]["ticker"] == "BOVA11"
    assert parsed_detail[0]["custo_total_ter"] == 0.10
    assert parsed_detail[0]["indice_referencia"] == "IBOVESPA"

    # Test extract composition
    comp = await collector.extract(ticker="BOVA11", mode="composition")
    assert comp["mode"] == "composition"
    parsed_comp = collector.parse(comp)
    assert len(parsed_comp) >= 3
    assert parsed_comp[0]["codigo_ativo"] == "VALE3"
    assert parsed_comp[0]["peso_pct"] == 13.5


@pytest.mark.asyncio
async def test_etfs_brasil_pipeline_sync_holdings():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = "INSERT 0 1"

    # Transaction mock
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=mock_tx)
    mock_tx.__aexit__ = AsyncMock(return_value=None)
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    pipeline = ETFsBrasilIngestionPipeline(db_pool=mock_pool)
    holdings = [
        {"codigo_ativo": "VALE3", "descricao_ativo": "Vale", "setor_economico": "Materiais", "peso_percentual": 12.0},
        {"codigo_ativo": "PETR4", "descricao_ativo": "Petrobras", "setor_economico": "Petróleo", "peso_percentual": 10.0},
    ]

    inserted = await pipeline.sync_etf_holdings(
        id_ativo_fundo=1,
        data_referencia=date(2026, 10, 27),
        holdings=holdings,
    )
    assert inserted == 2
    assert mock_conn.execute.call_count == 3  # 1 delete + 2 inserts


@pytest.mark.asyncio
async def test_etfs_brasil_pipeline_sync_daily_metrics():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = "INSERT 0 1"
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    pipeline = ETFsBrasilIngestionPipeline(db_pool=mock_pool)
    res = await pipeline.sync_etf_daily_metrics(
        id_ativo=1,
        data_pregao=date(2026, 10, 27),
        preco_fechamento_mercado=172.42,
        nav_cota_patrimonial=172.10,
        patrimonio_liquido_diario=18947800000.0,
        numero_cotistas=104623,
        aluguel_acoes_pl_pct=40.0,
        tracking_difference_diario=0.02,
    )

    assert res["id_ativo"] == 1
    assert res["preco"] == 172.42
    assert res["nav"] == 172.10
    assert res["spread_nav_pct"] > 0
    mock_conn.execute.assert_called_once()


def test_calculate_holdings_overlap():
    holdings_a = [
        {"codigo_ativo": "VALE3", "peso_percentual": 15.0},
        {"codigo_ativo": "PETR4", "peso_percentual": 10.0},
        {"codigo_ativo": "ITUB4", "peso_percentual": 8.0},
    ]
    holdings_b = [
        {"codigo_ativo": "VALE3", "peso_percentual": 12.0},  # min: 12.0
        {"codigo_ativo": "PETR4", "peso_percentual": 14.0},  # min: 10.0
        {"codigo_ativo": "BBAS3", "peso_percentual": 5.0},   # no match
    ]

    result = ETFsBrasilIngestionPipeline.calculate_holdings_overlap(holdings_a, holdings_b)
    # min(15, 12) + min(10, 14) = 12 + 10 = 22.0
    assert result["total_overlap_pct"] == 22.0
    assert result["common_holdings_count"] == 2
    assert len(result["top_shared_holdings"]) == 2
    assert result["top_shared_holdings"][0]["codigo"] == "VALE3"
    assert result["top_shared_holdings"][0]["overlap_min"] == 12.0
