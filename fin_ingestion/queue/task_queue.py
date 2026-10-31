"""Fila persistente de tarefas no PostgreSQL utilizando FOR UPDATE SKIP LOCKED."""

import uuid
from datetime import date
from typing import Any

import asyncpg
import structlog

from fin_ingestion.config import settings
from fin_ingestion.quality.temporal import timestamp_utc

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
            if settings.worker_datasets:
                row = await conn.fetchrow(
                    "SELECT * FROM ingestion.claim_next_task_filtered($1,$2,$3)",
                    self.worker_id,
                    lease_duration_sec,
                    settings.worker_datasets,
                )
                return dict(row) if row else None
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

    async def finish_success(self, task, result, started_at, duration_ms):
        """Audit, actual watermark and task completion commit under the same live lease."""
        day = result.get("watermark_date")
        day = date.fromisoformat(day) if isinstance(day, str) else day
        instant = result.get("watermark_timestamp")
        instant = timestamp_utc(instant) if instant is not None else None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                valid = await conn.fetchval(
                    "SELECT 1 FROM ingestion.ingestion_task WHERE task_id=$1 AND lock_token=$2 "
                    "AND status='RUNNING' AND locked_until>now() FOR UPDATE",
                    task["task_id"],
                    task["lock_token"],
                )
                if not valid:
                    raise RuntimeError("Task lease lost before audit/checkpoint commit")
                await conn.execute(
                    "INSERT INTO ingestion.ingestion_run(task_id,policy_id,worker_id,started_at,finished_at,status,records_extracted,records_loaded,duration_ms) "
                    "VALUES($1,$2,$3,$4,now(),'SUCCEEDED',$5,$6,$7)",
                    task["task_id"],
                    task["policy_id"],
                    self.worker_id,
                    started_at,
                    result.get("total_records", 0),
                    result.get("records_loaded", result.get("quotes_written", 0)),
                    duration_ms,
                )
                partition = task.get("target_entity") or "ALL"
                await conn.execute(
                    "INSERT INTO ingestion.ingestion_checkpoint(checkpoint_key,dataset_name,source_name,partition_id,watermark_date,watermark_timestamp,last_confirmed_at) "
                    "VALUES($1,$2,'fin-dispatcher',$3,$4,$5,now()) ON CONFLICT(checkpoint_key) DO UPDATE SET "
                    "watermark_date=COALESCE(EXCLUDED.watermark_date,ingestion.ingestion_checkpoint.watermark_date),"
                    "watermark_timestamp=COALESCE(EXCLUDED.watermark_timestamp,ingestion.ingestion_checkpoint.watermark_timestamp),last_confirmed_at=now()",
                    f"{task['dataset_name']}:{partition}",
                    task["dataset_name"],
                    partition,
                    day,
                    instant,
                )
                await conn.execute(
                    "UPDATE ingestion.ingestion_task SET status='COMPLETED',locked_until=NULL,lock_token=NULL,updated_at=now() WHERE task_id=$1 AND lock_token=$2",
                    task["task_id"],
                    task["lock_token"],
                )

    async def fail_task(
        self,
        task_id: uuid.UUID,
        lock_token: uuid.UUID,
        error_message: str,
        retry_delay_seconds: int = 60,
    ) -> bool:
        """Registra falha na tentativa e reagenda com backoff ou marca FAILED se atingiu o teto."""
        if (
            not isinstance(retry_delay_seconds, int)
            or isinstance(retry_delay_seconds, bool)
            or retry_delay_seconds < 1
        ):
            raise ValueError("Retry delay must be a positive integer")
        query = """
        UPDATE ingestion.ingestion_task
        SET status = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
            next_run_at = NOW() + make_interval(secs =>
                GREATEST($3::double precision,
                    LEAST(3600.0,$3::double precision * power(2.0,GREATEST(0,LEAST(attempts-1,10)))))
                + random() * $3::double precision * 0.1),
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
