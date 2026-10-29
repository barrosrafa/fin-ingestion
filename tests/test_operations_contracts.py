import subprocess
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from fin_ingestion.cli import app
from fin_ingestion.core.base_collector import CollectorResult
from fin_ingestion.pipelines.monthly_fii_funds import MonthlyFIIFundsPipeline
from fin_ingestion.pipelines.oceans14_macro_market_pipeline import Oceans14MacroMarketPipeline


def test_dbt_failure_is_not_success():
    with patch('fin_ingestion.cli.subprocess.run', side_effect=subprocess.CalledProcessError(1, 'dbt')):
        assert CliRunner().invoke(app, ['dbt-run']).exit_code == 1


@pytest.mark.asyncio
async def test_monthly_accepts_cli_tickers():
    pipeline = MonthlyFIIFundsPipeline()
    collectors = [pipeline.clube_fii, pipeline.funds_explorer, pipeline.fiis_com_br, pipeline.brick360, pipeline.mais_retorno, pipeline.etfs_brasil]
    for collector in collectors:
        collector.run = AsyncMock(return_value=CollectorResult(source='test',success=True,records_count=1))
        collector.close = AsyncMock()
    result = await pipeline.run(tickers=['HGLG11'])
    assert result['total_records'] > 0
    pipeline.clube_fii.run.assert_awaited_once_with(ticker='HGLG11')


@pytest.mark.asyncio
async def test_macro_has_executable_entrypoint():
    pipeline = Oceans14MacroMarketPipeline()
    pipeline.bcb.run = AsyncMock(return_value=CollectorResult(source='bcb',success=True,records_count=1,data=[{'data':'2026-01-01','valor':1}]))
    pipeline.bcb.close = AsyncMock()
    pipeline.b3.close = AsyncMock()
    result = await pipeline.run(tickers=None)
    assert result['total_records'] > 0
    pipeline.bcb.close.assert_awaited_once()

@pytest.mark.asyncio
async def test_b3_portfolio_uses_encoded_request_and_never_fabricates():
    import base64
    import json

    import httpx

    from fin_ingestion.collectors.official.b3 import B3Collector
    c = B3Collector()
    c.http_client.get = AsyncMock(return_value=httpx.Response(200,request=httpx.Request('GET','https://example.test'),json={'results':[{'cod':'PETR4','asset':'PETROBRAS','type':'PN','theoricalQty':'1.000','part':'10,2'}]}))
    payload = await c.extract(index='IFIX')
    endpoint = c.http_client.get.call_args.args[0]
    assert json.loads(base64.b64decode(endpoint.rsplit('/',1)[1]))['index'] == 'IFIX'
    assert c.parse(payload)[0]['ticker'] == 'PETR4'
    c.http_client.get.side_effect = RuntimeError('upstream unavailable')
    with pytest.raises(RuntimeError):
        await c.extract()


def test_no_data_is_failure():
    from fin_ingestion.pipelines.result import summarize_results
    with pytest.raises(RuntimeError,match='nenhuma linha'):
        summarize_results('test', {'broken':CollectorResult(source='test',success=False,records_count=0,errors=['db unavailable'])})

@pytest.mark.asyncio
async def test_daily_cannot_succeed_without_quotes():
    from fin_ingestion.pipelines.daily_market import DailyMarketPipeline
    p = DailyMarketPipeline()
    p.get_active_universe = AsyncMock(return_value=[])
    with pytest.raises(RuntimeError, match='ticker'):
        await p.run()


def test_dbt_installs_dependencies_before_running():
    with patch('fin_ingestion.cli.subprocess.run') as run:
        assert CliRunner().invoke(app, ['dbt-run']).exit_code == 0
        assert run.call_args_list[0].args[0][1] == 'deps'
        assert run.call_args_list[1].args[0][1] == 'run'


@pytest.mark.asyncio
async def test_raw_sink_concurrent_initialization_creates_one_pool():
    import asyncio

    from fin_ingestion.core.raw_sink import RawSink
    sink = RawSink(db_url='postgresql://unused')
    pool = AsyncMock()
    async def create_pool(**kwargs):
        await asyncio.sleep(0)
        return pool
    with patch('fin_ingestion.core.raw_sink.asyncpg.create_pool', side_effect=create_pool) as create:
        pools = await asyncio.gather(*(sink.get_pool() for _ in range(30)))
    assert all(p is pool for p in pools)
    assert create.call_count == 1


@pytest.mark.asyncio
async def test_active_daily_universe_requires_official_trading_list():
    from fin_ingestion.pipelines.daily_market import DailyMarketPipeline
    p = DailyMarketPipeline()
    p.b3.get_recent_traded_tickers = AsyncMock(return_value=['PETR4'])
    conn = AsyncMock()
    conn.fetch.return_value = [{'ticker':'PETR4'}]
    pool = AsyncMock()
    pool.acquire = lambda: __import__('unittest.mock',fromlist=['MagicMock']).MagicMock(__aenter__=AsyncMock(return_value=conn),__aexit__=AsyncMock(return_value=None))
    p.dw_writer.get_pool = AsyncMock(return_value=pool)
    assert await p.get_active_universe() == ['PETR4']
    assert ['PETR4'] in conn.fetch.call_args.args
