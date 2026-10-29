"""Testes unitários dos pipelines e orquestrador de execução."""

from unittest.mock import AsyncMock

import pytest

from fin_ingestion.pipelines.orchestrator import PipelineOrchestrator


@pytest.mark.asyncio
async def test_orchestrator_daily_pipeline():
    orchestrator = PipelineOrchestrator()
    orchestrator.daily.run = AsyncMock(return_value={"pipeline": "daily_market", "sources_executed": ["public_quotes"], "total_records": 1})
    res = await orchestrator.execute_pipeline("daily", tickers=["PETR4"])
    orchestrator.daily.run.assert_awaited_once_with(tickers=["PETR4"])

    assert res["pipeline"] == "daily_market"
    assert "sources_executed" in res
    assert res["total_records"] >= 0


@pytest.mark.asyncio
async def test_orchestrator_invalid_pipeline():
    orchestrator = PipelineOrchestrator()
    with pytest.raises(ValueError, match="Pipeline desconhecido"):
        await orchestrator.execute_pipeline("non_existent_pipeline")
