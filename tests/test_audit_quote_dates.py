from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.pipelines.market_gateway import MarketGateway


@pytest.mark.asyncio
async def test_future_daily_quote_is_rejected_before_opening_a_connection():
    writer = DWWriter()
    writer.get_pool = AsyncMock()
    tomorrow = datetime.now(ZoneInfo('America/Sao_Paulo')).date() + timedelta(days=1)
    with pytest.raises(ValueError, match='future market sessions'):
        await writer.write_daily_quotes([{'ticker': 'ANY11', 'data_pregao': tomorrow, 'preco_fechamento': 10}])
    writer.get_pool.assert_not_awaited()


def test_quotation_factor_is_applied_once_without_an_absolute_price_cap():
    day = datetime.now(ZoneInfo('America/Sao_Paulo')).date()
    raw = {'ticker': 'QA11B', 'fator_cotacao': 1000, 'data_pregao': day.isoformat(),
           'preco_abertura': 3100000, 'preco_maximo': 3300000, 'preco_minimo': 3000000,
           'preco_fechamento': 3221000, 'quantidade_titulos': 1}
    normalized = MarketGateway.normalize_official(raw, day)
    assert normalized['close'] == 3221
    assert raw['preco_fechamento'] == 3221000


def test_official_replay_rejects_future_and_duplicate_sessions():
    from fin_ingestion.operations.replay_official_quotes import official_records
    day = datetime.now(ZoneInfo('America/Sao_Paulo')).date()
    row = {'ticker': 'ANY11', 'tipo_mercado': 10, 'data_pregao': day.isoformat(),
           'fator_cotacao': 1, 'preco_abertura': 10, 'preco_maximo': 10,
           'preco_minimo': 10, 'preco_fechamento': 10, 'preco_medio': 10, 'quantidade_titulos': 1}
    assert official_records([row], day.year, day)[0]['close'] == 10
    with pytest.raises(ValueError, match='Duplicate'):
        official_records([row, row], day.year, day)
    with pytest.raises(ValueError, match='future'):
        official_records([{**row, 'data_pregao': (day+timedelta(days=1)).isoformat()}], day.year, day)
