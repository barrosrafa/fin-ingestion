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

    async def get_active_universe(self, limit: int | None = None, offset: int = 0) -> list[str]:
        """Consulta o universo ativo cadastrado no banco, com ordenação por liquidez e paginação."""
        try:
            pool = await self.dw_writer.get_pool()
            async with pool.acquire() as conn:
                if limit is None or limit <= 0:
                    rows = await conn.fetch(
                        """
                        SELECT ticker FROM core.ativo
                        WHERE ticker IS NOT NULL
                          AND tipo_ativo IN ('ACAO', 'FII', 'ETF', 'BDR')
                        ORDER BY volume_medio DESC NULLS LAST, ticker ASC;
                        """
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT ticker FROM core.ativo
                        WHERE ticker IS NOT NULL
                          AND tipo_ativo IN ('ACAO', 'FII', 'ETF', 'BDR')
                        ORDER BY volume_medio DESC NULLS LAST, ticker ASC
                        LIMIT $1 OFFSET $2;
                        """,
                        limit,
                        offset,
                    )
                if rows:
                    return [r["ticker"] for r in rows]
        except Exception as e:
            logger.warning("Falha ao carregar universo ativo do banco de dados", error=str(e))
        return []

    async def run(
        self,
        tickers: list[str] | None = None,
        limit: int | None = None,
        offset: int = 0,
        concurrency: int = 10,
    ) -> dict[str, Any]:
        target_tickers = tickers or await self.get_active_universe(limit=limit, offset=offset)
        if not target_tickers:
            logger.warning("Nenhum ticker ativo disponível para processamento no pipeline diário.")
            return {
                "pipeline": "daily_market",
                "total_records": 0,
                "quotes_written": 0,
                "intraday_observations": 0,
                "sources_executed": [],
            }
        logger.info("Iniciando pipeline diário de mercado", tickers_count=len(target_tickers))

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

        quotes_to_persist = []
        intraday_observations_written = 0
        for t, res in zip(target_tickers, status_results):
            if isinstance(res, CollectorResult):
                results[f"status_invest_{t}"] = res
                for r in res.data:
                    dt = r.get("data_referencia")
                    val = r.get("valor")
                    if dt and val is not None:
                        # Grava observação pontual no mercado sem forçar OHLC artificial
                        try:
                            saved = await self.dw_writer.write_price_observation(
                                ticker=t,
                                price=val,
                                observed_at=dt,
                                source_name="status_invest",
                            )
                            if saved:
                                intraday_observations_written += 1
                        except Exception as e:
                            logger.debug("Falha ao gravar cotação intraday", ticker=t, error=str(e))

                        # Para market.cotacao_diaria, grava somente campos reais fornecidos;
                        # NÃO replica valor pontual em abertura/máxima/mínima nem inventa volume zero
                        quotes_to_persist.append({
                            "ticker": t,
                            "data_pregao": dt,
                            "preco_fechamento": val,
                            "preco_abertura": None,
                            "preco_maximo": None,
                            "preco_minimo": None,
                            "preco_medio": None,
                            "volume_total": None,
                            "quantidade_negocios": None,
                        })

        for t, res in zip(target_tickers, advfn_results):
            if isinstance(res, CollectorResult):
                results[f"advfn_{t}"] = res

        # 4. Gravação idempotente das cotações no DW (market.cotacao_diaria)
        written_quotes = 0
        if quotes_to_persist:
            written_quotes = await self.dw_writer.write_daily_quotes(quotes_to_persist)
            logger.info("Cotações persistidas com sucesso pelo DailyMarketPipeline", written=written_quotes)

        # Fechamento de clientes
        await self.b3.close()
        await self.bcb.close()
        await self.status_invest.close()
        await self.advfn.close()
        await self.dw_writer.close()

        total_records = sum(r.records_count for r in results.values()) + written_quotes
        logger.info("Pipeline diário finalizado com sucesso", total_records=total_records, quotes_written=written_quotes)

        return {
            "pipeline": "daily_market",
            "total_records": total_records,
            "quotes_written": written_quotes,
            "intraday_observations": intraday_observations_written,
            "sources_executed": list(results.keys()),
        }
