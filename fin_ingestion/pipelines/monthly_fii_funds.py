"""Pipeline mensal de FIIs, Fundos CVM 175 e ETFs (Clube FII, Brick360, Mais Retorno, ETFs Brasil)."""

import asyncio
from typing import Any

import structlog

from fin_ingestion.collectors.official.cvm import CVMCollector
from fin_ingestion.collectors.sites.brick360 import Brick360Collector
from fin_ingestion.collectors.sites.clube_fii import ClubeFIICollector
from fin_ingestion.collectors.sites.etfs_brasil import ETFsBrasilCollector
from fin_ingestion.collectors.sites.fiis_com_br import FIISComBrCollector
from fin_ingestion.collectors.sites.funds_explorer import FundsExplorerCollector
from fin_ingestion.collectors.sites.mais_retorno import MaisRetornoCollector
from fin_ingestion.core.base_collector import CollectorResult
from fin_ingestion.pipelines.result import summarize_results

logger = structlog.get_logger(__name__)


class MonthlyFIIFundsPipeline:
    """Executa a coleta mensal de vacância, carteiras CDA, rankings de FIIs e taxas de ETFs."""

    def __init__(self) -> None:
        self.cvm = CVMCollector()
        self.clube_fii = ClubeFIICollector()
        self.funds_explorer = FundsExplorerCollector()
        self.fiis_com_br = FIISComBrCollector()
        self.brick360 = Brick360Collector()
        self.mais_retorno = MaisRetornoCollector()
        self.etfs_brasil = ETFsBrasilCollector()

    async def run(
        self,
        fii_tickers: list[str] | None = None,
        etf_tickers: list[str] | None = None,
        tickers: list[str] | None = None,
        include_secondary: bool = False,
    ) -> dict[str, Any]:
        fiis = fii_tickers or tickers or ["HGLG11", "KNIP11", "XPML11"]
        etfs = etf_tickers or ["BOVA11", "IVVB11", "SMAL11"]

        logger.info("Iniciando pipeline mensal de FIIs, Fundos e ETFs", fiis=fiis, etfs=etfs)
        results: dict[str, CollectorResult] = {}
        try:
            results["cvm_informe_mensal"] = await self.cvm.run(endpoint="fii_informe_mensal")
        finally:
            await self.cvm.close()
        if not include_secondary:
            return summarize_results("monthly_fii_funds", results)

        # Tarefas assíncronas em lote
        tasks = []
        for f in fiis:
            tasks.append(("clube_fii", f, self.clube_fii.run(ticker=f)))
            tasks.append(("funds_exp", f, self.funds_explorer.run(ticker=f, action="income")))
            tasks.append(("fiis_com_br", f, self.fiis_com_br.run(ticker=f)))
            tasks.append(("brick360", f, self.brick360.run(ticker=f)))

        for e in etfs:
            tasks.append(("etfs_brasil", e, self.etfs_brasil.run(ticker=e)))

        tasks.append(("mais_retorno_idx", "indexes", self.mais_retorno.run(category="indexes")))

        gathered_results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

        for meta, res in zip(tasks, gathered_results):
            key = f"{meta[0]}_{meta[1]}"
            results[key] = res

        await self.clube_fii.close()
        await self.funds_explorer.close()
        await self.fiis_com_br.close()
        await self.brick360.close()
        await self.mais_retorno.close()
        await self.etfs_brasil.close()

        return summarize_results("monthly_fii_funds", results)
