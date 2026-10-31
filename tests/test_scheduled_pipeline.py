"""Offline acceptance tests for scheduled, replayable and validated ingestion."""

import io
import json
import zipfile
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pandas as pd
import pandera.errors
import pytest
from tenacity import wait_none
from typer.testing import CliRunner

from fin_ingestion.cli import app
from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.collectors.official.bcb import BCBCollector
from fin_ingestion.core.circuit_breaker import CircuitBreaker, CircuitState
from fin_ingestion.core.distributed_controls import DistributedSourceControls
from fin_ingestion.core.http_client import RateLimitExceededError, ResilientHttpClient
from fin_ingestion.core.local_archive import LocalRawArchive
from fin_ingestion.core.rate_limiter import AsyncRateLimiter
from fin_ingestion.core.raw_sink import RawSink
from fin_ingestion.dispatcher.task_dispatcher import TaskDispatcher
from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.pipelines.result import PipelineExecutionError, summarize_results
from fin_ingestion.quality.market_schema import validate_complete_candles
from fin_ingestion.quality.temporal import forward_fill_prices, timestamp_utc
from fin_ingestion.scheduler.market_calendar import MarketCalendar
from fin_ingestion.scheduler.scheduler_service import IngestionSchedulerService
from fin_ingestion.workers.base_worker import IngestionWorker


@pytest.fixture(autouse=True)
def isolate_external_services(monkeypatch):
    from fin_ingestion.config import settings

    monkeypatch.setattr(settings, "distributed_source_controls", False)


def mock_pool():
    pool, conn = MagicMock(), AsyncMock()
    pool.acquire.return_value.__aenter__.return_value = conn
    conn.transaction = MagicMock()
    return pool, conn


def candle(**overrides):
    return {
        "ticker": "PETR4",
        "data_pregao": "2026-10-30",
        "preco_abertura": 35,
        "preco_maximo": 37,
        "preco_minimo": 34,
        "preco_fechamento": 36,
        "volume_total": 0,
        **overrides,
    }


@pytest.mark.asyncio
async def test_scheduler_uses_due_occurrence_and_reuses_key():
    pool, conn = mock_pool()
    conn.fetch.return_value = [
        {
            "policy_id": "daily",
            "dataset_name": "cotacao_diaria",
            "calendar_type": "B3_TRADING_DAYS",
            "priority": 2,
            "cron_expression": "0 9,19 * * 1-5",
        }
    ]
    service = IngestionSchedulerService(pool)
    seen = set()

    async def enqueue(**kwargs):
        if kwargs["task_key"] in seen:
            return None
        seen.add(kwargs["task_key"])
        return "task"

    service.queue.enqueue_task = AsyncMock(side_effect=enqueue)
    service.reconciler.recover_zombie_tasks = AsyncMock()
    # BRT 08:59 is before first occurrence; BRT 09:00 is exactly due.
    assert (
        await service.evaluate_and_dispatch(datetime(2026, 10, 30, 11, 59, tzinfo=timezone.utc))
        == 0
    )
    assert (
        await service.evaluate_and_dispatch(datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)) == 1
    )
    assert (
        await service.evaluate_and_dispatch(datetime(2026, 10, 30, 15, 0, tzinfo=timezone.utc)) == 0
    )
    assert (
        await service.evaluate_and_dispatch(datetime(2026, 10, 30, 22, 0, tzinfo=timezone.utc)) == 1
    )
    assert len(seen) == 2
    assert "daily:2026-10-30T22:00:00+00:00" in seen


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "calendar,cron,day,dataset",
    [
        ("BUSINESS_DAYS", "0 0 * * *", 31, "serie_macro"),
        ("CALENDAR_DAYS", "bad cron", 30, "serie_macro"),
        ("UNKNOWN", "* * * * *", 30, "serie_macro"),
        ("CALENDAR_DAYS", "* * * * * *", 30, "serie_macro"),
        ("B3_TRADING_DAYS", "* * * * *", 30, "cotacao_intraday"),
    ],
)
async def test_scheduler_skips_ineligible_policy(calendar, cron, day, dataset):
    pool, conn = mock_pool()
    conn.fetch.return_value = [
        {
            "policy_id": "test",
            "dataset_name": dataset,
            "calendar_type": calendar,
            "priority": 2,
            "cron_expression": cron,
        }
    ]
    service = IngestionSchedulerService(pool)
    service.queue.enqueue_task = AsyncMock()
    service.reconciler.recover_zombie_tasks = AsyncMock()
    assert (
        await service.evaluate_and_dispatch(datetime(2026, 10, day, 22, tzinfo=timezone.utc)) == 0
    )
    service.queue.enqueue_task.assert_not_called()


def test_market_session_converts_utc_before_comparing_hours():
    assert MarketCalendar.is_b3_market_open(datetime(2026, 10, 30, 13, tzinfo=timezone.utc))
    assert not MarketCalendar.is_b3_market_open(datetime(2026, 10, 30, 12, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        MarketCalendar.is_b3_market_open(datetime(2026, 10, 30, 13))


@pytest.mark.asyncio
async def test_weighted_bucket_charges_each_credit():
    limiter = AsyncRateLimiter(100, burst=100)
    await limiter.acquire(cost=100)
    assert limiter.tokens == 0
    await limiter.acquire(cost=10)
    assert limiter.tokens < 5


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", [0, -1, 6, float("nan"), float("inf")])
async def test_invalid_credit_cost_never_reaches_redis(cost):
    limiter = AsyncRateLimiter(1, burst=5)
    with pytest.raises(ValueError):
        await limiter.acquire(cost)
    redis = AsyncMock()
    controls = DistributedSourceControls(redis, "test", 1, 5)
    with pytest.raises(ValueError):
        await controls.before_request(cost)
    redis.eval.assert_not_called()


@pytest.mark.asyncio
async def test_distributed_bucket_receives_weight():
    redis = AsyncMock()
    redis.eval.side_effect = [0, 7]
    controls = DistributedSourceControls(redis, "test", 100, 100)
    assert await controls.before_request(cost=75) == 7
    assert redis.eval.call_args_list[0].args[-1] == 75


@pytest.mark.asyncio
async def test_get_retries_5xx_but_not_429(monkeypatch):
    monkeypatch.setattr(ResilientHttpClient.get.retry, "wait", wait_none())
    statuses = iter([503, 200, 429])
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(next(statuses), request=request, headers={"Retry-After": "120"})

    client = ResilientHttpClient("test")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    try:
        assert (await client.get("https://example.test")).status_code == 200
        assert client.circuit_breaker.failure_count == 0
        with pytest.raises(RateLimitExceededError) as error:
            await client.get("https://example.test")
        assert error.value.retry_after == 120
        assert len(requests) == 3
    finally:
        await client.close()


def test_retry_after_http_date():
    from email.utils import format_datetime

    retry_at = datetime.now(timezone.utc) + timedelta(seconds=120)
    response = httpx.Response(429, headers={"Retry-After": format_datetime(retry_at)})
    with pytest.raises(RateLimitExceededError) as error:
        ResilientHttpClient("test")._handle_response_status(response)
    assert 118 <= error.value.retry_after <= 120


def test_local_breaker_only_allows_one_recovery_probe():
    breaker = CircuitBreaker("test", recovery_timeout_seconds=0)
    breaker.open()
    breaker.last_state_change -= 1
    assert breaker.can_execute()
    assert not breaker.can_execute()
    breaker.record_failure()
    assert breaker.state == CircuitState.OPEN


def test_sgs_windows_cover_leap_years_without_gaps():
    first, last = date(1999, 1, 1), date(2026, 10, 30)
    windows = list(BCBCollector.date_windows(first, last))
    assert windows[0][0] == first and windows[-1][1] == last
    assert all((end - start).days < 3 * 365 for start, end in windows)
    assert all(a[1] + timedelta(days=1) == b[0] for a, b in zip(windows, windows[1:]))


@pytest.mark.asyncio
async def test_sgs_history_merges_sorts_and_deduplicates():
    collector = BCBCollector()
    calls = []
    collector.raw_sink.save_payload = AsyncMock()

    async def get(url, params):
        calls.append(params)
        day = params["dataInicial"]
        return httpx.Response(
            200,
            request=httpx.Request("GET", "https://example.test"),
            json=[{"data": day, "valor": "1"}, {"data": day, "valor": "2"}],
        )

    collector.http_client.get = AsyncMock(side_effect=get)
    payload = await collector.extract(serie="ipca", start_date="2000-02-29", end_date="2026-10-30")
    records = collector.parse(payload)
    assert len(calls) > 8 and len(records) == len(calls)
    assert records == sorted(records, key=lambda row: row["data"])
    assert all(row["valor"] == 2 for row in records)
    assert calls[-1]["dataFinal"] == "30/10/2026"
    assert collector.raw_sink.save_payload.await_count == len(calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"serie": "unknown"},
        {"start_date": "2020-01-01"},
        {"start_date": "2021-01-01", "end_date": "2020-01-01"},
        {"ultimos": 0},
    ],
)
async def test_sgs_bad_parameters_do_not_call_api(params):
    collector = BCBCollector()
    collector.http_client.get = AsyncMock()
    with pytest.raises(ValueError):
        await collector.extract(**params)
    collector.http_client.get.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"preco_fechamento": float("inf")},
        {"preco_fechamento": float("nan")},
        {"preco_fechamento": -1},
        {"preco_abertura": -1},
        {"preco_maximo": 35},
        {"volume_total": -1},
        {"data_pregao": "bad-date"},
        {"data_pregao": None},
        {"quantidade_negocios": 1.5},
        {"variacao_percentual": "NaN"},
    ],
)
async def test_bad_candle_batch_is_rejected_before_database(overrides):
    writer = DWWriter()
    writer.get_pool = AsyncMock()
    with pytest.raises((ValueError, pandera.errors.SchemaErrors)):
        await writer.write_daily_quotes([candle(), candle(**overrides)])
    writer.get_pool.assert_not_called()


def test_pandera_reports_multiple_invalid_rows():
    with pytest.raises(pandera.errors.SchemaErrors) as error:
        validate_complete_candles(
            [candle(preco_fechamento=-1), candle(preco_abertura=float("inf"))]
        )
    assert len(error.value.failure_cases) >= 2


@pytest.mark.asyncio
async def test_daily_writer_preserves_zero_and_absent_counts():
    writer = DWWriter()
    pool, conn = mock_pool()
    writer.get_pool = AsyncMock(return_value=pool)
    writer.get_ticker_id_map = AsyncMock(return_value={"PETR4": 1})
    assert await writer.write_daily_quotes([candle(volume_financeiro=999)]) == 1
    query, values = conn.executemany.call_args.args
    assert "ON CONFLICT (id_ativo, data_pregao)" in query
    assert values[0][7] == 0
    assert values[0][8] is None


def test_utc_normalization_and_dst_rejection():
    assert timestamp_utc("2026-10-30T10:00:00", "America/Sao_Paulo").hour == 13
    assert timestamp_utc("2026-10-30T10:00:00-03:00").hour == 13
    for value in ("2026-10-30", "bad-date", None, "2026-10-30T10:00:00"):
        with pytest.raises(ValueError):
            timestamp_utc(value)
    for value in ("2026-03-08T02:30:00", "2026-11-01T01:30:00"):
        with pytest.raises(Exception):
            timestamp_utc(value, "America/New_York")


def test_fill_is_causal_per_ticker_and_limited_to_three():
    frame = pd.DataFrame(
        {
            "ticker": ["A"] * 6 + ["B"],
            "timestamp": pd.date_range("2026-10-01", periods=7, tz="UTC"),
            "close": [None, 10, None, None, None, None, None],
        }
    )
    filled = forward_fill_prices(frame.iloc[::-1], ["close"])
    assert pd.isna(filled.iloc[0]["close"])
    assert filled.iloc[4]["close"] == 10
    assert pd.isna(filled.iloc[5]["close"]) and pd.isna(filled.iloc[6]["close"])
    assert frame["close"].notna().sum() == 1


@pytest.mark.asyncio
async def test_bronze_local_preserves_bytes_deduplicates_and_detects_corruption(
    tmp_path, monkeypatch
):
    from fin_ingestion.config import settings

    monkeypatch.setattr(settings, "raw_landing_backend", "local")
    monkeypatch.setattr(settings, "raw_landing_dir", str(tmp_path))
    sink = RawSink()
    sink.get_pool = AsyncMock()
    metadata = {"request": "original"}
    path = await sink.save_payload("b3", "cotahist", b"PK\x00\xff", metadata=metadata)
    again = await sink.save_payload("b3", "cotahist", b"PK\x00\xff")
    assert path == again
    assert LocalRawArchive.read(path) == b"PK\x00\xff"
    assert len(list((tmp_path / "captures").glob("*.json"))) == 2
    assert metadata == {"request": "original"}
    sink.get_pool.assert_not_called()
    from pathlib import Path

    Path(path).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum"):
        LocalRawArchive.read(path)


def test_cli_replays_bcb_without_network_and_prevents_overwrite(tmp_path):
    payload = {
        "serie_id": 11,
        "serie_name": "selic_over",
        "data": [{"data": "30/10/2026", "valor": "1"}],
    }
    path = LocalRawArchive(tmp_path).save(json.dumps(payload).encode(), {})
    output = tmp_path / "replayed.json"
    args = ["raw-replay", path, "--source", "bcb", "--output", str(output)]
    assert CliRunner().invoke(app, args).exit_code == 0
    assert json.loads(output.read_text())[0]["valor"] == 1
    assert CliRunner().invoke(app, args).exit_code != 0


@pytest.mark.asyncio
async def test_collector_propagates_retry_after_to_queue():
    collector = BCBCollector()
    collector.extract = AsyncMock(side_effect=RateLimitExceededError("429", retry_after=300))
    result = await collector.run()
    assert result.retry_after_seconds == 300
    with pytest.raises(PipelineExecutionError) as error:
        summarize_results("bcb", {"sgs": result})
    assert error.value.retry_after == 300


@pytest.mark.asyncio
async def test_partial_result_never_completes_or_updates_checkpoint():
    pool, conn = mock_pool()
    dispatcher = TaskDispatcher(pool)
    dispatcher.queue.claim_task = AsyncMock(
        return_value={
            "task_id": "task",
            "lock_token": "token",
            "policy_id": "p",
            "dataset_name": "serie_macro",
        }
    )
    dispatcher.queue.complete_task = AsyncMock()
    dispatcher.queue.fail_task = AsyncMock()
    dispatcher.execute_task_payload = AsyncMock(
        return_value={"status": "PARTIAL", "total_records": 10, "retry_after_seconds": 300}
    )
    assert (await dispatcher.dispatch_one())["status"] == "FAILED"
    dispatcher.queue.complete_task.assert_not_called()
    assert dispatcher.queue.fail_task.call_args.kwargs["retry_delay_seconds"] == 300
    assert not any("ingestion_checkpoint" in call.args[0] for call in conn.execute.call_args_list)


@pytest.mark.asyncio
async def test_worker_reuses_dispatcher_and_rejects_daily_as_intraday():
    worker = IngestionWorker(MagicMock())
    with pytest.raises(ValueError, match="explicit ticker"):
        await worker.execute_task_payload({"dataset_name": "cotacao_intraday", "task_id": "test"})
    worker.dispatch_one = AsyncMock(return_value={"status": "FAILED"})
    assert not await worker.process_one_task()


def test_cotahist_zip_replay_has_distinct_average_and_last_price():
    line = list("0" * 245)
    line[0:2], line[2:10], line[12:24] = "01", "20261030", "PETR4       "
    line[56:69], line[69:82], line[82:95] = "0000000003500", "0000000003700", "0000000003400"
    line[95:108], line[108:121] = "0000000003550", "0000000003600"
    line[210:217] = "0000001"
    content = "".join(line)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("COTAHIST.TXT", "00header\n" + content + "\n99trailer\n")
    records = B3Collector().parse(output.getvalue())
    assert records[0]["preco_medio"] == 35.5
    assert records[0]["preco_fechamento"] == 36
    with pytest.raises(ValueError, match="245"):
        B3Collector.parse_cothist_line(content[:-1])


@pytest.mark.asyncio
async def test_official_anbima_never_defaults_to_sample_prices():
    from fin_ingestion.collectors.official.anbima import ANBIMACollector

    with pytest.raises(NotImplementedError, match="autenticada"):
        await ANBIMACollector().extract()


@pytest.mark.asyncio
async def test_ptax_needs_explicit_official_rates_before_database():
    from fin_ingestion.pipelines.analise_de_acoes_pipeline import PtaxExchangeRatePipeline

    pool = MagicMock()
    pipeline = PtaxExchangeRatePipeline(pool)
    with pytest.raises(ValueError, match="explicit"):
        await pipeline.ingest_daily_ptax(date(2026, 10, 30))
    pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_empty_collector_does_not_report_success():
    collector = BCBCollector()
    collector.extract = AsyncMock(return_value={"data": []})
    collector.raw_sink.save_payload = AsyncMock()
    result = await collector.run()
    assert not result.success
    assert result.records_count == 0


@pytest.mark.asyncio
async def test_daily_closes_every_client_even_when_universe_fails():
    from fin_ingestion.pipelines.daily_market import DailyMarketPipeline

    pipeline = DailyMarketPipeline()
    pipeline.get_active_universe = AsyncMock(side_effect=RuntimeError("no universe"))
    clients = [
        pipeline.b3,
        pipeline.bcb,
        pipeline.status_invest,
        pipeline.advfn,
        pipeline.dw_writer,
    ]
    for client in clients:
        client.close = AsyncMock()
    with pytest.raises(RuntimeError, match="no universe"):
        await pipeline.run()
    for client in clients:
        client.close.assert_awaited_once()


def test_replay_native_sgs_requires_series_identity(tmp_path):
    path = LocalRawArchive(tmp_path).save(b'[{"data":"30/10/2026","valor":"1"}]', {})
    output = tmp_path / "sgs.json"
    args = ["raw-replay", path, "--source", "bcb", "--output", str(output)]
    assert CliRunner().invoke(app, args).exit_code != 0
    assert CliRunner().invoke(app, args + ["--serie-id", "11"]).exit_code == 0
    assert json.loads(output.read_text())[0]["serie_id"] == 11


@pytest.mark.asyncio
async def test_b3_archives_zip_before_parsing_and_fetches_current_session():
    from unittest.mock import patch

    collector = B3Collector()
    response = httpx.Response(
        200, request=httpx.Request("GET", "https://example.test"), content=b"PK-invalid-zip"
    )
    collector.http_client.get = AsyncMock(return_value=response)
    collector.raw_sink.save_payload = AsyncMock()
    with patch("fin_ingestion.collectors.official.b3.datetime") as clock:
        clock.now.return_value = datetime(2026, 10, 30, 20, tzinfo=timezone.utc)
        with pytest.raises(zipfile.BadZipFile):
            await collector.get_recent_traded_tickers()
    collector.raw_sink.save_payload.assert_awaited_once()
    assert collector.raw_sink.save_payload.call_args.kwargs["payload"] == response.content
    assert "COTAHIST_D30102026.ZIP" in collector.http_client.get.call_args.args[0]
