"""Mecanismo de persistência raw (Bronze Layer) no PostgreSQL/TimescaleDB.
Totalmente aderente ao DDL canônico de raw.ingestion_batch e raw.payload."""

import asyncio
import hashlib
import json
import uuid
from typing import Any

import asyncpg
import structlog

from fin_ingestion.config import settings

logger = structlog.get_logger(__name__)


class RawSink:
    """Grava payloads brutos em raw.payload vinculado a um lote em raw.ingestion_batch com hash SHA-256."""

    def __init__(self, db_url: str | None = None) -> None:
        self.db_url = db_url or settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(dsn=self.db_url, min_size=1, max_size=5)
        return self._pool

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def create_batch(
        self,
        collector_name: str,
        collector_version: str = "1.0.0",
        source_id: uuid.UUID | None = None,
        notes: str | None = None,
    ) -> uuid.UUID:
        """Cria um lote de ingestão em raw.ingestion_batch com status RUNNING."""
        batch_id = uuid.uuid4()
        query = """
        INSERT INTO raw.ingestion_batch (
            batch_id, source_id, collector_name, collector_version,
            started_at, status, request_count, error_count, notes
        )
        VALUES ($1, $2, $3, $4, NOW(), 'RUNNING', 0, 0, $5)
        RETURNING batch_id;
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(query, batch_id, source_id, collector_name, collector_version, notes)
        return batch_id

    async def finish_batch(
        self,
        batch_id: uuid.UUID,
        status: str = "SUCCEEDED",
        request_count: int = 0,
        error_count: int = 0,
        notes: str | None = None,
    ) -> None:
        """Finaliza o lote de ingestão com contadores e status final."""
        query = """
        UPDATE raw.ingestion_batch
        SET status = $2,
            finished_at = NOW(),
            request_count = $3,
            error_count = $4,
            notes = COALESCE($5, notes)
        WHERE batch_id = $1;
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(query, batch_id, status, request_count, error_count, notes)

    async def save_payload(
        self,
        source: str,
        endpoint: str,
        payload: Any,
        status_code: int = 200,
        metadata: dict[str, Any] | None = None,
        batch_id: uuid.UUID | None = None,
        mime_type: str = "application/json",
        encoding: str = "utf-8",
    ) -> str:
        """Salva um payload bruto no schema raw. Aderente ao DDL oficial de raw.payload."""
        if not settings.enable_raw_landing:
            return "raw_landing_disabled"

        if isinstance(payload, bytes):
            payload_bytes = payload
        elif isinstance(payload, str):
            payload_bytes = payload.encode(encoding)
        else:
            payload_bytes = json.dumps(payload, default=str, sort_keys=True).encode(encoding)

        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        source_url = f"{source}://{endpoint}"
        meta = metadata or {}
        meta["source"] = source
        meta["endpoint"] = endpoint

        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Garante que um lote existe para satisfazer a FK payload_batch_id_fkey
            effective_batch_id = batch_id
            if effective_batch_id is None:
                effective_batch_id = await conn.fetchval(
                    """
                    INSERT INTO raw.ingestion_batch (
                        batch_id, collector_name, collector_version, started_at, status, request_count, error_count, notes
                    )
                    VALUES (gen_random_uuid(), $1, '1.0.0', NOW(), 'SUCCEEDED', 1, 0, 'Auto-generated single payload batch')
                    RETURNING batch_id;
                    """,
                    source,
                )

            query = """
            INSERT INTO raw.payload (
                payload_id, batch_id, source_url, captured_at,
                http_status, mime_type, content_sha256, encoding, body, metadata
            )
            VALUES (gen_random_uuid(), $1, $2, NOW(), $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (batch_id, content_sha256) DO NOTHING
            RETURNING payload_id;
            """
            row = await conn.fetchrow(
                query,
                effective_batch_id,
                source_url,
                status_code,
                mime_type,
                payload_hash,
                encoding,
                payload_bytes,
                json.dumps(meta, default=str),
            )
            saved_id = str(row["payload_id"]) if row else "duplicate_skipped"
            logger.debug(
                "Raw payload persistido no DDL canônico",
                source=source,
                endpoint=endpoint,
                id=saved_id,
                sha256=payload_hash,
            )
            return saved_id

