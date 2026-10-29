"""Pipeline de ingestão de demonstrações contábeis trimestrais (CVM, Investidor10, Fundamentei)."""

import asyncio
from typing import Any

import structlog

from fin_ingestion.collectors.official.cvm import CVMCollector
from fin_ingestion.collectors.sites.fundamentei import FundamenteiCollector
from fin_ingestion.collectors.sites.fundamentus import FundamentusCollector
from fin_ingestion.collectors.sites.investidor10 import Investidor10Collector
from fin_ingestion.core.base_collector import CollectorResult
from fin_ingestion.pipelines.result import summarize_results

logger = structlog.get_logger(__name__)


class QuarterlyFinancialsPipeline:
    """Executa a captura de balanços, DFP/ITR e indicadores fundamentalistas trimestrais."""

    def __init__(self) -> None:
        self.cvm = CVMCollector()
        self.investidor10 = Investidor10Collector()
        self.fundamentei = FundamenteiCollector()
        self.fundamentus = FundamentusCollector()

    async def run(self, tickers: list[str] | None = None) -> dict[str, Any]:
        target_tickers = tickers or ["PETR4", "VALE3", "WEGE3"]
        logger.info("Iniciando pipeline trimestral de demonstrativos", tickers=target_tickers)

        results: dict[str, CollectorResult] = {}

        # 1. Cadastro CVM
        results["cvm_cad"] = await self.cvm.run(endpoint="cad_cia_aberta")

        # 2. Investidor10, Fundamentei e Fundamentus em paralelo
        inv_tasks = [self.investidor10.run(ticker=t, endpoint="indicadores") for t in target_tickers]
        fundamentei_tasks = [self.fundamentei.run(ticker=t) for t in target_tickers]
        fundamentus_tasks = [self.fundamentus.run(ticker=t, route="detalhes") for t in target_tickers]

        inv_res = await asyncio.gather(*inv_tasks, return_exceptions=True)
        fnd_res = await asyncio.gather(*fundamentei_tasks, return_exceptions=True)
        fds_res = await asyncio.gather(*fundamentus_tasks, return_exceptions=True)

        for t, r in zip(target_tickers, inv_res):
            results[f"investidor10_{t}"] = r

        for t, r in zip(target_tickers, fnd_res):
            results[f"fundamentei_{t}"] = r

        for t, r in zip(target_tickers, fds_res):
            results[f"fundamentus_{t}"] = r

        await self.cvm.close()
        await self.investidor10.close()
        await self.fundamentei.close()
        await self.fundamentus.close()

        return summarize_results("quarterly_financials", results)
