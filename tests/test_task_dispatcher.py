"""Testes unitários e de integração para o TaskDispatcher do fin-ingestion."""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fin_ingestion.dispatcher.task_dispatcher import TaskDispatcher


@pytest.mark.asyncio
async def test_task_dispatcher_initialization():
    mock_pool = MagicMock()
    dispatcher = TaskDispatcher(pool=mock_pool, worker_id="test-dispatcher-1")

    assert dispatcher.worker_id == "test-dispatcher-1"
    assert dispatcher.queue is not None
    assert dispatcher.is_running is False


@pytest.mark.asyncio
async def test_task_dispatcher_dispatch_one_no_task():
    mock_pool = MagicMock()
    dispatcher = TaskDispatcher(pool=mock_pool)
    dispatcher.queue.claim_task = AsyncMock(return_value=None)

    result = await dispatcher.dispatch_one()
    assert result is None


@pytest.mark.asyncio
async def test_task_dispatcher_dispatch_one_success():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_conn
    mock_ctx.__aexit__.return_value = None
    mock_pool.acquire.return_value = mock_ctx

    dispatcher = TaskDispatcher(pool=mock_pool, worker_id="test-dispatcher-2")

    fake_task_id = uuid.uuid4()
    fake_token = uuid.uuid4()
    mock_task = {
        "task_id": fake_task_id,
        "lock_token": fake_token,
        "policy_id": "b3_daily_quotes",
        "dataset_name": "cotacao_diaria",
        "target_entity": "PETR4",
    }

    dispatcher.queue.claim_task = AsyncMock(return_value=mock_task)
    dispatcher.queue.complete_task = AsyncMock(return_value=True)

    with patch.object(dispatcher, "execute_task_payload", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = {
            "pipeline": "daily_market",
            "total_records": 10,
            "quotes_written": 1,
        }

        result = await dispatcher.dispatch_one()

        assert result is not None
        assert result["status"] == "SUCCEEDED"
        assert result["task_id"] == str(fake_task_id)
        assert result["dataset"] == "cotacao_diaria"
        assert result["target"] == "PETR4"

        # Verifica chamada de finalização com lock token
        dispatcher.queue.complete_task.assert_awaited_once_with(
            task_id=fake_task_id,
            lock_token=fake_token,
        )


@pytest.mark.asyncio
async def test_task_dispatcher_dispatch_one_failure():
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_conn
    mock_ctx.__aexit__.return_value = None
    mock_pool.acquire.return_value = mock_ctx

    dispatcher = TaskDispatcher(pool=mock_pool, worker_id="test-dispatcher-3")

    fake_task_id = uuid.uuid4()
    fake_token = uuid.uuid4()
    mock_task = {
        "task_id": fake_task_id,
        "lock_token": fake_token,
        "policy_id": "b3_daily_quotes",
        "dataset_name": "cotacao_diaria",
        "target_entity": "VALE3",
    }

    dispatcher.queue.claim_task = AsyncMock(return_value=mock_task)
    dispatcher.queue.fail_task = AsyncMock(return_value=True)

    with patch.object(dispatcher, "execute_task_payload", side_effect=Exception("Database timeout")):
        result = await dispatcher.dispatch_one()

        assert result is not None
        assert result["status"] == "FAILED"
        assert "Database timeout" in result["error"]

        # Verifica registro de falha na fila com lock token
        dispatcher.queue.fail_task.assert_awaited_once_with(
            task_id=fake_task_id,
            lock_token=fake_token,
            error_message="Database timeout",
        )


@pytest.mark.asyncio
async def test_task_dispatcher_dispatch_batch():
    mock_pool = MagicMock()
    dispatcher = TaskDispatcher(pool=mock_pool)

    # Simula 2 tarefas com sucesso e a 3ª retornando None (fila vazia)
    dispatcher.dispatch_one = AsyncMock(
        side_effect=[
            {"task_id": "1", "status": "SUCCEEDED"},
            {"task_id": "2", "status": "SUCCEEDED"},
            None,
        ]
    )

    batch_res = await dispatcher.dispatch_batch(max_tasks=5)
    assert len(batch_res) == 2
    assert batch_res[0]["task_id"] == "1"
    assert batch_res[1]["task_id"] == "2"
