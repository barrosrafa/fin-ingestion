"""Orquestrador mestre unificado de execução de todos os pipelines de dados."""

from typing import Any

import structlog

from fin_ingestion.pipelines.analise_de_acoes_pipeline import (
    PtaxExchangeRatePipeline,
    USAssetsIngestionPipeline,
)
from fin_ingestion.pipelines.daily_market import DailyMarketPipeline
from fin_ingestion.pipelines.dividend_seasonality_pipeline import DividendSeasonalityAggregator
from fin_ingestion.pipelines.etfs_brasil_pipeline import ETFsBrasilIngestionPipeline
from fin_ingestion.pipelines.financial_matrix_pipeline import FinancialMatrixPipeline
from fin_ingestion.pipelines.monthly_fii_funds import MonthlyFIIFundsPipeline
from fin_ingestion.pipelines.oceans14_macro_market_pipeline import Oceans14MacroMarketPipeline
from fin_ingestion.pipelines.quarterly_financials import QuarterlyFinancialsPipeline

logger = structlog.get_logger(__name__)


class PipelineOrchestrator:
    """Orquestrador unificado para execução periódica ou sob demanda de todos os pipelines do FIN."""

    def __init__(self) -> None:
        self.daily = DailyMarketPipeline()
        self.quarterly = QuarterlyFinancialsPipeline()
        self.monthly = MonthlyFIIFundsPipeline()
        self.etfs = ETFsBrasilIngestionPipeline()
        self.financial_matrix = FinancialMatrixPipeline()
        self.macro = Oceans14MacroMarketPipeline()
        self.seasonality = DividendSeasonalityAggregator()
        self.ptax = PtaxExchangeRatePipeline()
        self.us_assets = USAssetsIngestionPipeline()

        self.pipelines: dict[str, Any] = {
            "daily": self.daily,
            "daily_market": self.daily,
            "quarterly": self.quarterly,
            "quarterly_financials": self.quarterly,
            "monthly": self.monthly,
            "monthly_fii_funds": self.monthly,
            "etfs": self.etfs,
            "etfs_brasil": self.etfs,
            "financial_matrix": self.financial_matrix,
            "macro": self.macro,
            "macro_market": self.macro,
            "seasonality": self.seasonality,
            "dividend_seasonality": self.seasonality,
            "ptax": self.ptax,
            "us_assets": self.us_assets,
        }

    async def execute_pipeline(self, pipeline_name: str, **kwargs: Any) -> dict[str, Any]:
        """Dispara o pipeline solicitado com validação de parâmetros e rastreabilidade."""
        logger.info("Solicitada execução de pipeline", pipeline=pipeline_name)

        if pipeline_name == "all":
            results = {}
            unique_pipes = {
                "daily_market": self.daily,
                "quarterly_financials": self.quarterly,
                "monthly_fii_funds": self.monthly,
                "etfs_brasil": self.etfs,
                "financial_matrix": self.financial_matrix,
                "macro_market": self.macro,
                "ptax_rates": self.ptax,
                "us_assets": self.us_assets,
            }
            for name, pipe in unique_pipes.items():
                try:
                    logger.info(f"Executando etapa do lote: {name}")
                    results[name] = await pipe.run(**kwargs)
                except Exception as e:
                    logger.error(f"Falha na execução do pipeline {name}: {e}")
                    results[name] = {"status": "FAILED", "error": str(e)}

            return {"pipeline": "all", "results": results}

        if pipeline_name not in self.pipelines:
            valid_names = sorted(list(set(self.pipelines.keys())) + ["all"])
            raise ValueError(
                f"Pipeline desconhecido: '{pipeline_name}'. Válidos: {valid_names}"
            )

        pipe = self.pipelines[pipeline_name]
        return await pipe.run(**kwargs)
