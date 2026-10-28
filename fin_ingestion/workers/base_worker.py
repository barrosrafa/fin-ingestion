"""Worker assíncrono para execução concorrente de tarefas da fila PostgreSQL."""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.pipelines.daily_market import DailyMarketPipeline
from fin_ingestion.pipelines.monthly_fii_funds import MonthlyFIIFundsPipeline
from fin_ingestion.pipelines.quarterly_financials import QuarterlyFinancialsPipeline
from fin_ingestion.queue.task_queue import PostgresTaskQueue

logger = structlog.get_logger(__name__)


class IngestionWorker:
    """Consumidor da fila com reserva atômica (lease token) e auditoria de execução."""

    def __init__(self, pool: asyncpg.Pool, worker_id: str | None = None) -> None:
        self.pool = pool
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queue = PostgresTaskQueue(pool=pool, worker_id=self.worker_id)
        self.dw_writer = DWWriter()
        self.daily_pipeline = DailyMarketPipeline()
        self.quarterly_pipeline = QuarterlyFinancialsPipeline()
        self.monthly_pipeline = MonthlyFIIFundsPipeline()
        self.is_running = False

    async def execute_task_payload(self, task: dict[str, Any]) -> dict[str, Any]:
        """Despacha a tarefa para o pipeline correspondente com base no dataset."""
        dataset = task["dataset_name"]
        target = task.get("target_entity")
        tickers = [target] if target else None

        logger.info(
            "Executando tarefa de ingestão",
            worker_id=self.worker_id,
            task_id=str(task["task_id"]),
            dataset=dataset,
            target=target,
        )

        if dataset in ("cotacao_diaria", "cotacao_intraday"):
            return await self.daily_pipeline.run(tickers=tickers)
        elif dataset in ("demonstrativo_financeiro", "dfp", "itr"):
            return await self.quarterly_pipeline.run(tickers=tickers)
        elif dataset in ("informe_mensal_fii", "fii_rendimentos"):
            return await self.monthly_pipeline.run(fii_tickers=tickers)
        else:
            # Pipeline genérico de coleta
            return await self.daily_pipeline.run(tickers=tickers)

    async def process_one_task(self, lease_duration_sec: int = 300) -> bool:
        """Tenta reservar e processar uma única tarefa da fila."""
        task = await self.queue.claim_task(lease_duration_sec=lease_duration_sec)
        if not task:
            return False

        task_id = task["task_id"]
        lock_token = task["lock_token"]
        policy_id = task["policy_id"]
        dataset_name = task["dataset_name"]
        target_entity = task.get("target_entity")
        start_time = datetime.now(timezone.utc)

        try:
            result = await self.execute_task_payload(task)
            duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

            # Grava auditoria de execução (ingestion.ingestion_run)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO ingestion.ingestion_run (
                        task_id, policy_id, worker_id, started_at, finished_at,
                        status, records_extracted, records_loaded, duration_ms
                    )
                    VALUES ($1, $2, $3, $4, NOW(), 'SUCCEEDED', $5, $6, $7);
                    """,
                    task_id,
                    policy_id,
                    self.worker_id,
                    start_time,
                    result.get("total_records", 0),
                    result.get("quotes_written", 0),
                    duration_ms,
                )

                # Atualiza checkpoint de progresso
                partition = target_entity or "ALL"
                checkpoint_key = f"{dataset_name}:{partition}"
                await conn.execute(
                    """
                    INSERT INTO ingestion.ingestion_checkpoint (
                        checkpoint_key, dataset_name, source_name, partition_id,
                        watermark_date, watermark_timestamp, last_confirmed_at
                    )
                    VALUES ($1, $2, 'fin-worker', $3, CURRENT_DATE, NOW(), NOW())
                    ON CONFLICT (checkpoint_key) DO UPDATE SET
                        watermark_date = EXCLUDED.watermark_date,
                        watermark_timestamp = EXCLUDED.watermark_timestamp,
                        last_confirmed_at = EXCLUDED.last_confirmed_at;
                    """,
                    checkpoint_key,
                    dataset_name,
                    partition,
                )

            # Baixa a tarefa confirmando com o lease token
            await self.queue.complete_task(task_id=task_id, lock_token=lock_token)
            logger.info("Tarefa de ingestão concluída com sucesso", task_id=str(task_id), duration_ms=duration_ms)
            return True

        except Exception as e:
            duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)
            logger.error("Falha na execução da tarefa de ingestão", task_id=str(task_id), error=str(e))

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
            except Exception:
                pass

            # Registra falha na fila para retentativa com backoff
            await self.queue.fail_task(task_id=task_id, lock_token=lock_token, error_message=str(e))
            return False

    async def run_forever(self, poll_interval_sec: float = 5.0) -> None:
        """Loop contínuo de processamento do worker."""
        self.is_running = True
        logger.info("Worker iniciado consumindo fila PostgreSQL (SKIP LOCKED)", worker_id=self.worker_id)

        while self.is_running:
            processed = await self.process_one_task()
            if not processed:
                # Se não houver tarefas prontas, aguarda antes da próxima tentativa
                await asyncio.sleep(poll_interval_sec)

    async def close(self) -> None:
        self.is_running = False
        await self.dw_writer.close()
        await self.daily_pipeline.dw_writer.close()
