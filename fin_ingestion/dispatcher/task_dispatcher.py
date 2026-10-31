"""Despachante Autônomo de Tarefas de Ingestão (Task Dispatcher).

Governa a execução concorrente e atômica de tarefas da fila PostgreSQL
(schema `ingestion`), utilizando reserva transacional com `SKIP LOCKED`,
auditoria em `ingestion.ingestion_run` e checkpoints em `ingestion.ingestion_checkpoint`.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from fin_ingestion.collectors.official.anbima import ANBIMACollector
from fin_ingestion.collectors.official.cvm import CVMCollector
from fin_ingestion.collectors.sites.etfs_brasil import ETFsBrasilCollector
from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.pipelines.daily_market import DailyMarketPipeline
from fin_ingestion.pipelines.enrichment import AssetMetadataEnricher
from fin_ingestion.pipelines.market_gateway import MarketPipeline
from fin_ingestion.pipelines.monthly_fii_funds import MonthlyFIIFundsPipeline
from fin_ingestion.pipelines.provider_pipeline import ProviderPipeline
from fin_ingestion.pipelines.quarterly_financials import QuarterlyFinancialsPipeline
from fin_ingestion.pipelines.result import require_complete_result, summarize_results
from fin_ingestion.queue.task_queue import PostgresTaskQueue

logger = structlog.get_logger(__name__)


class TaskDispatcher:
    """Motor central de despacho de tarefas de ingestão baseadas no schema `ingestion`."""

    def __init__(self, pool: asyncpg.Pool, worker_id: str | None = None) -> None:
        self.pool = pool
        self.worker_id = worker_id or f"dispatcher-{uuid.uuid4().hex[:8]}"
        self.queue = PostgresTaskQueue(pool=pool, worker_id=self.worker_id)
        self.dw_writer = DWWriter()
        self.daily_pipeline = DailyMarketPipeline()
        self.quarterly_pipeline = QuarterlyFinancialsPipeline()
        self.monthly_pipeline = MonthlyFIIFundsPipeline()
        self.enricher = AssetMetadataEnricher(db_pool=self.pool)
        self.is_running = False

    async def execute_task_payload(self, task: dict[str, Any]) -> dict[str, Any]:
        """Roteia e executa a tarefa no pipeline ou coletor correspondente."""
        dataset = task["dataset_name"]
        target = task.get("target_entity")
        tickers = [target] if target else None

        logger.info(
            "Despachando tarefa de ingestão",
            worker_id=self.worker_id,
            task_id=str(task["task_id"]),
            policy_id=task.get("policy_id"),
            dataset=dataset,
            target=target,
        )

        # 1. Cotações e Mercado Diário/Intraday
        if dataset == "cotacao_intraday":
            return await MarketPipeline(intraday=True, pool=self.pool).run(tickers=tickers)
        if dataset in {"ptax", "focus", "tesouro", "anbima_feed", "cvm_dfp", "cvm_itr", "massive", "fmp", "twelve_data", "sgs", "b3_history"}:
            return await ProviderPipeline(dataset, pool=self.pool).run(tickers=tickers, reference_date=task.get("reference_date"))
        if dataset == "market_eod":
            return await MarketPipeline(pool=self.pool).run(tickers=tickers, reference_date=task.get("reference_date"))
        if dataset in ("cotacao_diaria", "b3_daily_quotes"):
            return await self.daily_pipeline.run(tickers=tickers)

        # 2. Demonstrações Financeiras e Balanços CVM (ITR/DFP)
        elif dataset in ("demonstrativo_financeiro", "dfp", "itr", "cvm_quarterly_dfp"):
            return await self.quarterly_pipeline.run(tickers=tickers)

        # 3. Informes e Rendimentos Mensais de FIIs
        elif dataset in ("informe_mensal_fii", "fii_rendimentos", "cvm_fii_monthly"):
            return await self.monthly_pipeline.run(fii_tickers=tickers)

        # 4. Séries Macroeconômicas (BCB SGS / PTAX)
        elif dataset in ("serie_macro", "bcb_macro_series"):
            from fin_ingestion.pipelines.oceans14_macro_market_pipeline import (
                Oceans14MacroMarketPipeline,
            )
            return await Oceans14MacroMarketPipeline().run()

        # 5. Renda Fixa e Indicativos ANBIMA
        elif dataset in ("taxa_anbima", "anbima_indicativos"):
            anbima = ANBIMACollector()
            try:
                res = await anbima.run()
                await anbima.close()
                return summarize_results("anbima_indicativos", {"anbima": res})
            except Exception:
                await anbima.close()
                raise

        # 6. ETFs Brasil
        elif dataset in ("etf_taxas_nav", "etfs_brasil_taxas"):
            etfs = ETFsBrasilCollector()
            try:
                res = await etfs.run(ticker=target or "BOVA11")
                await etfs.close()
                return summarize_results("etfs_brasil", {"etfs_brasil": res})
            except Exception:
                await etfs.close()
                raise

        # 7. Cotas de Fundos de Investimento (CVM)
        elif dataset in ("cota_fundo_investimento", "cvm_funds_daily"):
            cvm = CVMCollector()
            try:
                res = await cvm.run(dataset_type="informe_diario_fi")
                await cvm.close()
                return summarize_results("cvm_funds_daily", {"cvm": res})
            except Exception:
                await cvm.close()
                raise

        # 8. Cadastro e Enriquecimento de Instrumentos
        elif dataset in ("cadastro_instrumento", "b3_instrument_cad"):
            res = await self.enricher.enrich_missing_assets(limit=50)
            return {
                "pipeline": "asset_enrichment",
                "total_records": res.get("enriched_count", 0),
                "quotes_written": 0,
                "sources_executed": ["enricher"],
            }

        # 9. Fallback genérico para pipeline diário de mercado
        else:
            raise ValueError(f"Dataset sem rota de ingestão: {dataset}")

    async def dispatch_one(self, lease_duration_sec: int = 300) -> dict[str, Any] | None:
        """Reserva atomicamente e processa uma única tarefa da fila."""
        task = await self.queue.claim_task(lease_duration_sec=lease_duration_sec)
        if not task:
            return None

        task_id = task["task_id"]
        lock_token = task["lock_token"]
        policy_id = task["policy_id"]
        dataset_name = task["dataset_name"]
        target_entity = task.get("target_entity")
        start_time = datetime.now(timezone.utc)

        try:
            result = await self.execute_task_payload(task)
            require_complete_result(result)
            duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

            await self.queue.finish_success(task,result,start_time,duration_ms)
            logger.info("Tarefa de ingestão concluída com sucesso", task_id=str(task_id), duration_ms=duration_ms)

            return {
                "task_id": str(task_id),
                "status": "SUCCEEDED",
                "dataset": dataset_name,
                "target": target_entity,
                "duration_ms": duration_ms,
                "result": result,
            }

        except Exception as e:
            duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
            logger.error("Falha ao executar tarefa de ingestão", task_id=str(task_id), error=str(e))

            # Registra run com falha
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO ingestion.ingestion_run (
                            task_id, policy_id, worker_id, started_at, finished_at,
                            status, error_stack, duration_ms
                        )
                        VALUES ($1, $2, $3, $4, NOW(), 'FAILED', $5, $6);
                        """,
                        task_id,
                        policy_id,
                        self.worker_id,
                        start_time,
                        str(e),
                        duration_ms,
                    )
                    # Registra issue na quarentena
                    await conn.execute(
                        """
                        INSERT INTO ingestion.ingestion_issue (
                            task_id, dataset_name, source_name, entity_id,
                            issue_type, severity, details
                        )
                        VALUES ($1, $2, 'task-dispatcher', $3, 'DISPATCHER_EXECUTION_ERROR', 'WARNING', $4::jsonb);
                        """,
                        task_id,
                        dataset_name,
                        target_entity,
                        json.dumps({"error": str(e)[:200]}),
                    )
            except Exception as log_err:
                logger.warning("Falha ao registrar auditoria de erro no banco", error=str(log_err))

            # Falha a tarefa para retentativa com backoff
            retry_options = {}
            if getattr(e, "retry_after", None) is not None:
                retry_options["retry_delay_seconds"] = max(60, e.retry_after)
            await self.queue.fail_task(task_id=task_id, lock_token=lock_token, error_message=str(e), **retry_options)
            return {
                "task_id": str(task_id),
                "status": "FAILED",
                "dataset": dataset_name,
                "target": target_entity,
                "duration_ms": duration_ms,
                "error": str(e),
            }

    async def dispatch_batch(self, max_tasks: int = 10, lease_duration_sec: int = 300) -> list[dict[str, Any]]:
        """Processa um lote de tarefas pendentes até atingir max_tasks ou esvaziar a fila."""
        processed_tasks = []
        for _ in range(max_tasks):
            result = await self.dispatch_one(lease_duration_sec=lease_duration_sec)
            if result is None:
                break
            processed_tasks.append(result)
        return processed_tasks

    async def run_forever(self, poll_interval_sec: float = 5.0) -> None:
        """Loop contínuo de processamento do despachante."""
        self.is_running = True
        logger.info("TaskDispatcher iniciado em loop contínuo", worker_id=self.worker_id)
        while self.is_running:
            res = await self.dispatch_one()
            if res is None:
                await asyncio.sleep(poll_interval_sec)

    async def close(self) -> None:
        """Libera conexões e recursos dos pipelines."""
        self.is_running = False
        await self.dw_writer.close()
        await self.daily_pipeline.close()
        await self.quarterly_pipeline.close()
        await self.monthly_pipeline.close()
