"""Serviço de agendamento autônomo baseado em políticas e calendários de mercado."""

import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
import structlog
from croniter import croniter

from fin_ingestion.config import settings
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

    async def evaluate_and_dispatch(self, now: datetime | None = None) -> int:
        """Avalia políticas ativas e gera tarefas pendentes necessárias."""
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Scheduler requires a timezone-aware instant")
        now_sp = now.astimezone(TZ_SP)
        today = now_sp.date()
        tasks_created = 0

        # 1. Recupera políticas ativas
        query_policies = """
        SELECT policy_id, dataset_name, source_name, calendar_type, priority, cron_expression
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

            # The current daily route cannot supply genuine intraday observations.
            if dataset == "cotacao_intraday":
                if (not settings.intraday_enabled or not settings.intraday_tickers
                    or p["source_name"] != "brapi" or not MarketCalendar.is_b3_market_open(now_sp)):
                    continue

            # Valida calendário
            if cal_type == "B3_TRADING_DAYS" and not MarketCalendar.is_b3_trading_day(today):
                continue
            if cal_type == "BUSINESS_DAYS" and today.weekday() >= 5:
                continue
            if cal_type not in {"B3_TRADING_DAYS", "BUSINESS_DAYS", "CALENDAR_DAYS"}:
                logger.error("Calendário de política desconhecido", policy_id=policy_id)
                continue

            # Most recent due occurrence today; no unbounded catch-up storm after downtime.
            # DB policies use local BRT cron, independently of GitHub Actions UTC cron.
            try:
                expression = p["cron_expression"]
                if len(expression.split()) != 5:
                    raise ValueError("Only five-field cron expressions are supported")
                occurrence = croniter(
                    expression, now_sp.replace(second=0, microsecond=0) + timedelta(microseconds=1)
                ).get_prev(datetime)
            except (ValueError, KeyError, TypeError) as exc:
                logger.error("Cron inválido", policy_id=policy_id, error=str(exc))
                continue
            if occurrence.date() != today:
                continue
            task_key = f"{policy_id}:{occurrence.astimezone(timezone.utc).isoformat()}"

            # Enfileira se ainda não existir
            targets = settings.intraday_tickers if dataset == "cotacao_intraday" else [None]
            for target in targets:
                key = f"{task_key}:{target}" if target else task_key
                task_id = await self.queue.enqueue_task(
                    policy_id=policy_id, task_key=key, dataset_name=dataset,
                    reference_date=today, priority=priority, target_entity=target,
                )
                if task_id:
                    tasks_created += 1
                    logger.info("Nova tarefa de ingestão enfileirada pelo agendador",
                                policy_id=policy_id, task_key=key, task_id=str(task_id))

        # 2. Executa recuperação de tarefas zumbis
        await self.reconciler.recover_zombie_tasks()

        return tasks_created

    async def run_forever(self, interval_seconds: int = 60) -> None:
        """Loop contínuo do serviço de agendamento em container Docker."""
        if interval_seconds <= 0:
            raise ValueError("Scheduler interval must be positive")
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
