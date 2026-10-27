"""Orquestrador mestre de execução dos pipelines de dados."""

from typing import Any

import structlog

from fin_ingestion.pipelines.daily_market import DailyMarketPipeline
from fin_ingestion.pipelines.monthly_fii_funds import MonthlyFIIFundsPipeline
from fin_ingestion.pipelines.quarterly_financials import QuarterlyFinancialsPipeline

logger = structlog.get_logger(__name__)


class PipelineOrchestrator:
    """Orquestrador unificado para execução periódica ou sob demanda."""

    def __init__(self) -> None:
        self.daily = DailyMarketPipeline()
        self.quarterly = QuarterlyFinancialsPipeline()
        self.monthly = MonthlyFIIFundsPipeline()

    async def execute_pipeline(self, pipeline_name: str, **kwargs: Any) -> dict[str, Any]:
        """Dispara o pipeline solicitado com validação de parâmetros."""
        logger.info("Solicitada execução de pipeline", pipeline=pipeline_name)

        if pipeline_name in ("daily", "daily_market"):
            return await self.daily.run(**kwargs)
        elif pipeline_name in ("quarterly", "quarterly_financials"):
            return await self.quarterly.run(**kwargs)
        elif pipeline_name in ("monthly", "monthly_fii_funds"):
            return await self.monthly.run(**kwargs)
        elif pipeline_name == "all":
            res_daily = await self.daily.run(**kwargs)
            res_quarterly = await self.quarterly.run(**kwargs)
            res_monthly = await self.monthly.run(**kwargs)
            return {
                "pipeline": "all",
                "daily": res_daily,
                "quarterly": res_quarterly,
                "monthly": res_monthly,
            }
        else:
            raise ValueError(
                f"Pipeline desconhecido: '{pipeline_name}'. Válidos: daily, quarterly, monthly, all"
            )
