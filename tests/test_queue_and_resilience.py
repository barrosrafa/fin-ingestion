"""Testes unitários para fila persistente, reconciliador, validador de qualidade e calendário."""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from fin_ingestion.quality.validator import DataQualityValidator
from fin_ingestion.queue.reconciler import IngestionReconciler
from fin_ingestion.queue.task_queue import PostgresTaskQueue
from fin_ingestion.scheduler.market_calendar import MarketCalendar


def test_market_calendar_trading_days():
    # Segunda-feira comum de 2026 (ex: 2026-10-26)
    mon = date(2026, 10, 26)
    assert MarketCalendar.is_b3_trading_day(mon) is True

    # Sábado
    sat = date(2026, 10, 24)
    assert MarketCalendar.is_b3_trading_day(sat) is False

    # Domingo
    sun = date(2026, 10, 25)
    assert MarketCalendar.is_b3_trading_day(sun) is False

    # Feriado: Natal (2026-12-25)
    natal = date(2026, 12, 25)
    assert MarketCalendar.is_b3_trading_day(natal) is False


def test_data_quality_validator_ohlc_consistency():
    # Candle válido: Low <= Open <= High e Low <= Close <= High
    valid_candle = {
        "preco_abertura": Decimal("35.00"),
        "preco_maximo": Decimal("36.00"),
        "preco_minimo": Decimal("34.50"),
        "preco_fechamento": Decimal("35.80"),
        "variacao_percentual": Decimal("2.28"),
    }
    is_valid, reason, needs_check = DataQualityValidator.validate_daily_candle(valid_candle)
    assert is_valid is True
    assert reason is None
    assert needs_check is False

    # Candle inconsistente: Close > High
    invalid_candle = {
        "preco_abertura": Decimal("35.00"),
        "preco_maximo": Decimal("36.00"),
        "preco_minimo": Decimal("34.50"),
        "preco_fechamento": Decimal("37.00"),  # Inválido!
    }
    is_valid, reason, needs_check = DataQualityValidator.validate_daily_candle(invalid_candle)
    assert is_valid is False
    assert "Inconsistência geométrica" in reason
    assert needs_check is False

    # Variação extrema (> 50%): quarentena para verificação de split/provento
    extreme_candle = {
        "preco_abertura": Decimal("10.00"),
        "preco_maximo": Decimal("20.00"),
        "preco_minimo": Decimal("9.50"),
        "preco_fechamento": Decimal("18.00"),
        "variacao_percentual": Decimal("80.00"),
    }
    is_valid, reason, needs_check = DataQualityValidator.validate_daily_candle(extreme_candle)
    assert is_valid is False
    assert needs_check is True
    assert "Suspeita de provento ou split" in reason


@pytest.mark.asyncio
async def test_task_queue_enqueue_and_claim():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    queue = PostgresTaskQueue(pool=mock_pool, worker_id="test-worker")

    # Mock enqueue
    mock_conn.fetchval.return_value = "00000000-0000-0000-0000-000000000001"
    task_id = await queue.enqueue_task(
        policy_id="b3_daily_quotes",
        task_key="b3_daily:2026-10-28:PETR4",
        dataset_name="cotacao_diaria",
        target_entity="PETR4",
    )
    assert task_id == "00000000-0000-0000-0000-000000000001"

    # Mock claim task
    mock_conn.fetchrow.return_value = {
        "task_id": "00000000-0000-0000-0000-000000000001",
        "policy_id": "b3_daily_quotes",
        "dataset_name": "cotacao_diaria",
        "target_entity": "PETR4",
        "reference_date": date(2026, 10, 28),
        "attempts": 1,
        "lock_token": "token-123",
    }
    task = await queue.claim_task()
    assert task is not None
    assert task["target_entity"] == "PETR4"
    assert task["attempts"] == 1


@pytest.mark.asyncio
async def test_reconciler_recover_zombies():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    reconciler = IngestionReconciler(pool=mock_pool)
    mock_conn.execute.side_effect = ["UPDATE 3", "UPDATE 0"]

    recovered = await reconciler.recover_zombie_tasks()
    assert recovered == 3
