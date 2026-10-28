"""Reconciliador autônomo para recuperação de tarefas zumbis e detecção de lacunas de mercado."""

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


class IngestionReconciler:
    """Detecta falhas operacionais, leases vencidos e reconcilia o estado da ingestão."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def recover_zombie_tasks(self) -> int:
        """Recupera tarefas cujo lease expirou sem confirmação (worker morreu ou crashou)."""
        query_reopen = """
        UPDATE ingestion.ingestion_task
        SET status = 'PENDING',
            locked_until = NULL,
            lock_token = NULL,
            updated_at = NOW()
        WHERE status = 'RUNNING'
          AND locked_until < NOW()
          AND attempts < max_attempts;
        """
        query_fail_dead = """
        UPDATE ingestion.ingestion_task
        SET status = 'FAILED',
            locked_until = NULL,
            lock_token = NULL,
            error_message = 'Max attempts exceeded after lease timeout',
            updated_at = NOW()
        WHERE status = 'RUNNING'
          AND locked_until < NOW()
          AND attempts >= max_attempts;
        """
        async with self.pool.acquire() as conn:
            res_reopen = await conn.execute(query_reopen)
            await conn.execute(query_fail_dead)

        try:
            count = int(res_reopen.split(" ")[1])
        except Exception:
            count = 0

        if count > 0:
            logger.warning("Tarefas zumbis recuperadas com sucesso pelo reconciliador", recovered_count=count)
        return count

    async def detect_stale_policies(self) -> list[dict]:
        """Identifica datasets cujo atraso superou o SLA aceitável."""
        query = """
        SELECT dataset_name, source_name, minutes_since_last_success, expected_delay_min
        FROM ingestion.v_pipeline_health_summary
        WHERE sla_status = 'ATRASADO';
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(r) for r in rows]
