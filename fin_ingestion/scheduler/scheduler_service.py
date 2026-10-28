"""Serviço de agendamento autônomo baseado em políticas e calendários de mercado."""

import asyncio
from datetime import datetime

import asyncpg
import structlog

from fin_ingestion.queue.reconciler import IngestionReconciler
from fin_ingestion.queue.task_queue import PostgresTaskQueue
from fin_ingestion.scheduler.market_calendar import TZ_SP, MarketCalendar

logger = structlog.get_logger(__name__)


class IngestionSchedulerService:
    """Monitora políticas de ingestão e gera tarefas na fila persistente."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self.queue = PostgresTaskQueue(pool=pool, worker_id="scheduler-engine")
        self.reconciler = IngestionReconciler(pool=pool)
        self.is_running = False

    async def evaluate_and_dispatch(self) -> int:
        """Avalia políticas ativas e gera tarefas pendentes necessárias."""
        now_sp = datetime.now(TZ_SP)
        today = now_sp.date()
        tasks_created = 0

        # 1. Recupera políticas ativas
        query_policies = """
        SELECT policy_id, dataset_name, source_name, calendar_type, priority
        FROM ingestion.ingestion_policy
        WHERE is_active = TRUE;
        """
        async with self.pool.acquire() as conn:
            policies = await conn.fetch(query_policies)

        for p in policies:
            policy_id = p["policy_id"]
            dataset = p["dataset_name"]
            cal_type = p["calendar_type"]
            priority = p["priority"]

            # Valida calendário
            if cal_type == "B3_TRADING_DAYS" and not MarketCalendar.is_b3_trading_day(today):
                continue

            # Chave de idempotência do dia para tarefas diárias
            task_key = f"{policy_id}:{today.isoformat()}"

            # Enfileira se ainda não existir
            task_id = await self.queue.enqueue_task(
                policy_id=policy_id,
                task_key=task_key,
                dataset_name=dataset,
                reference_date=today,
                priority=priority,
            )
            if task_id:
                tasks_created += 1
                logger.info(
                    "Nova tarefa de ingestão enfileirada pelo agendador",
                    policy_id=policy_id,
                    task_key=task_key,
                    task_id=str(task_id),
                )

        # 2. Executa recuperação de tarefas zumbis
        await self.reconciler.recover_zombie_tasks()

        return tasks_created

    async def run_forever(self, interval_seconds: int = 60) -> None:
        """Loop contínuo do serviço de agendamento em container Docker."""
        self.is_running = True
        logger.info("Iniciando serviço de agendamento autônomo do fin-ingestion", interval_sec=interval_seconds)

        while self.is_running:
            try:
                created = await self.evaluate_and_dispatch()
                if created > 0:
                    logger.info("Ciclo do agendador concluído", tasks_created=created)
            except Exception as e:
                logger.error("Erro no ciclo de agendamento", error=str(e))

            await asyncio.sleep(interval_seconds)

    def stop(self) -> None:
        self.is_running = False
