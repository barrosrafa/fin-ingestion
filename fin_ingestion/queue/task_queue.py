"""Fila persistente de tarefas no PostgreSQL utilizando FOR UPDATE SKIP LOCKED."""

import uuid
from typing import Any

import asyncpg
import structlog

logger = structlog.get_logger(__name__)


class PostgresTaskQueue:
    """Fila transacional durável no schema ingestion do PostgreSQL."""

    def __init__(self, pool: asyncpg.Pool, worker_id: str | None = None) -> None:
        self.pool = pool
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

    async def enqueue_task(
        self,
        policy_id: str,
        task_key: str,
        dataset_name: str,
        target_entity: str | None = None,
        reference_date: Any = None,
        priority: int = 5,
    ) -> uuid.UUID | None:
        """Enfileira tarefa garantindo idempotência via task_key único."""
        query = """
        INSERT INTO ingestion.ingestion_task (
            policy_id, task_key, dataset_name, target_entity, reference_date, priority
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (task_key) DO NOTHING
        RETURNING task_id;
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                query,
                policy_id,
                task_key,
                dataset_name,
                target_entity,
                reference_date,
                priority,
            )

    async def claim_task(self, lease_duration_sec: int = 300) -> dict[str, Any] | None:
        """Reserva a próxima tarefa pronta usando SKIP LOCKED via função PL/pgSQL."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM ingestion.claim_next_task($1, $2);",
                self.worker_id,
                lease_duration_sec,
            )
            return dict(row) if row else None

    async def complete_task(
        self,
        task_id: uuid.UUID,
        lock_token: uuid.UUID,
        payload_id: uuid.UUID | None = None,
    ) -> bool:
        """Finaliza a tarefa se o worker detiver o token de lease válido."""
        query = """
        UPDATE ingestion.ingestion_task
        SET status = 'COMPLETED',
            payload_id = $3,
            locked_until = NULL,
            lock_token = NULL,
            updated_at = NOW()
        WHERE task_id = $1 AND lock_token = $2;
        """
        async with self.pool.acquire() as conn:
            res = await conn.execute(query, task_id, lock_token, payload_id)
            return "UPDATE 1" in res

    async def fail_task(
        self,
        task_id: uuid.UUID,
        lock_token: uuid.UUID,
        error_message: str,
        retry_delay_seconds: int = 60,
    ) -> bool:
        """Registra falha na tentativa e reagenda com backoff ou marca FAILED se atingiu o teto."""
        query = """
        UPDATE ingestion.ingestion_task
        SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
            next_run_at = NOW() + ($3 || ' seconds')::INTERVAL,
            locked_until = NULL,
            lock_token = NULL,
            error_message = $4,
            updated_at = NOW()
        WHERE task_id = $1 AND lock_token = $2;
        """
        async with self.pool.acquire() as conn:
            res = await conn.execute(query, task_id, lock_token, retry_delay_seconds, error_message)
            return "UPDATE 1" in res

    async def get_queue_stats(self) -> dict[str, int]:
        """Retorna contadores de tarefas por status."""
        query = """
        SELECT status, COUNT(*) AS total
        FROM ingestion.ingestion_task
        GROUP BY status;
        """
        stats = {"PENDING": 0, "RUNNING": 0, "COMPLETED": 0, "FAILED": 0, "CANCELLED": 0}
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            for r in rows:
                stats[r["status"]] = r["total"]
        return stats
