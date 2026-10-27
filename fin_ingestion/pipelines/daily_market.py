"""Pipeline de ingestão diária de mercado (B3, BCB, Status Invest, ADVFN)."""

import asyncio
from typing import Any

import structlog

from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.collectors.official.bcb import BCBCollector
from fin_ingestion.collectors.sites.advfn import ADVFNCollector
from fin_ingestion.collectors.sites.status_invest import StatusInvestCollector
from fin_ingestion.core.base_collector import CollectorResult
from fin_ingestion.dw.dw_writer import DWWriter

logger = structlog.get_logger(__name__)


class DailyMarketPipeline:
    """Executa o ciclo diário pós-fechamento do mercado financeiro."""

    def __init__(self) -> None:
        self.b3 = B3Collector()
        self.bcb = BCBCollector()
        self.status_invest = StatusInvestCollector()
        self.advfn = ADVFNCollector()
        self.dw_writer = DWWriter()

    async def run(self, tickers: list[str] | None = None) -> dict[str, Any]:
        target_tickers = tickers or ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3"]
        logger.info("Iniciando pipeline diário de mercado", tickers=target_tickers)

        results: dict[str, CollectorResult] = {}

        # 1. Indicadores Macroeconômicos BCB (Selic, IPCA, CDI)
        results["bcb_selic"] = await self.bcb.run(serie="selic_over", ultimos=5)
        results["bcb_ipca"] = await self.bcb.run(serie="ipca", ultimos=3)

        # 2. Carteira B3 IBOV
        results["b3_ibov"] = await self.b3.run(endpoint="index_portfolio", index="IBOV")

        # 3. Status Invest & ADVFN para os tickers alvo
        status_tasks = [self.status_invest.run(ticker=t, category="acao") for t in target_tickers]
        advfn_tasks = [self.advfn.run(ticker=t, section="tecnica") for t in target_tickers]

        status_results = await asyncio.gather(*status_tasks, return_exceptions=True)
        advfn_results = await asyncio.gather(*advfn_tasks, return_exceptions=True)

        for t, res in zip(target_tickers, status_results):
            if isinstance(res, CollectorResult):
                results[f"status_invest_{t}"] = res

        for t, res in zip(target_tickers, advfn_results):
            if isinstance(res, CollectorResult):
                results[f"advfn_{t}"] = res

        # Fechamento de clientes
        await self.b3.close()
        await self.bcb.close()
        await self.status_invest.close()
        await self.advfn.close()
        await self.dw_writer.close()

        total_records = sum(r.records_count for r in results.values())
        logger.info("Pipeline diário finalizado com sucesso", total_records=total_records)

        return {
            "pipeline": "daily_market",
            "total_records": total_records,
            "sources_executed": list(results.keys()),
        }
