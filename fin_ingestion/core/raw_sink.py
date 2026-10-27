"""Mecanismo de persistência raw (Bronze Layer) no PostgreSQL/TimescaleDB."""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from fin_ingestion.config import settings

logger = structlog.get_logger(__name__)


class RawSink:
    """Grava payloads brutos na tabela raw.payload com idempotência via hash SHA-256."""

    def __init__(self, db_url: str | None = None) -> None:
        self.db_url = db_url or settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: asyncpg.Pool | None = None

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(dsn=self.db_url, min_size=1, max_size=5)
        return self._pool

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def save_payload(
        self,
        source: str,
        endpoint: str,
        payload: Any,
        status_code: int = 200,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Salva um payload bruto no schema raw. Retorna o ID gerado ou o hash de idempotência."""
        if not settings.enable_raw_landing:
            return "raw_landing_disabled"

        payload_bytes = json.dumps(payload, default=str, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        now = datetime.now(timezone.utc)
        record_id = str(uuid.uuid4())

        query = """
        INSERT INTO raw.payload (
            id, source, endpoint, payload, payload_hash, http_status, metadata, captured_at
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::jsonb, $8)
        ON CONFLICT (source, endpoint, payload_hash) DO UPDATE
        SET captured_at = EXCLUDED.captured_at,
            http_status = EXCLUDED.http_status
        RETURNING id;
        """

        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    query,
                    record_id,
                    source,
                    endpoint,
                    json.dumps(payload, default=str),
                    payload_hash,
                    status_code,
                    json.dumps(metadata or {}, default=str),
                    now,
                )
                saved_id = str(row["id"]) if row else record_id
                logger.debug(
                    "Raw payload persistido com sucesso",
                    source=source,
                    endpoint=endpoint,
                    id=saved_id,
                )
                return saved_id
        except Exception as e:
            # Em desenvolvimento local sem banco ativo, loga e continua sem quebrar o pipeline
            logger.warning(
                "Falha ao persistir em raw.payload (prosseguindo em memória)",
                source=source,
                error=str(e),
            )
            return record_id
