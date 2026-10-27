"""Testes unitários dos pipelines e orquestrador de execução."""

import pytest

from fin_ingestion.pipelines.orchestrator import PipelineOrchestrator


@pytest.mark.asyncio
async def test_orchestrator_daily_pipeline():
    orchestrator = PipelineOrchestrator()
    res = await orchestrator.execute_pipeline("daily", tickers=["PETR4"])

    assert res["pipeline"] == "daily_market"
    assert "sources_executed" in res
    assert res["total_records"] >= 0


@pytest.mark.asyncio
async def test_orchestrator_invalid_pipeline():
    orchestrator = PipelineOrchestrator()
    with pytest.raises(ValueError, match="Pipeline desconhecido"):
        await orchestrator.execute_pipeline("non_existent_pipeline")
