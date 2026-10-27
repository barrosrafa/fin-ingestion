"""Escritor de dados de alta performance para o Data Warehouse (PostgreSQL 18 / TimescaleDB)."""

from typing import Any

import asyncpg
import structlog

from fin_ingestion.config import settings

logger = structlog.get_logger(__name__)


class DWWriter:
    """Carrega dados harmonizados e métricas analíticas calculadas nas tabelas de fatos e dimensões."""

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

    async def write_daily_quotes(self, quotes: list[dict[str, Any]]) -> int:
        """Grava cotações diárias normalizadas na hypertable market.cotacao_diaria."""
        if not quotes:
            return 0

        query = """
        INSERT INTO market.cotacao_diaria (
            data_pregao, ticker, preco_abertura, preco_maximo, preco_minimo,
            preco_fechamento, preco_medio, volume_financeiro, quantidade_negociada
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (data_pregao, ticker) DO UPDATE
        SET preco_fechamento = EXCLUDED.preco_fechamento,
            volume_financeiro = EXCLUDED.volume_financeiro;
        """
        written = 0
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                for q in quotes:
                    await conn.execute(
                        query,
                        q.get("data_pregao"),
                        q.get("ticker"),
                        q.get("preco_abertura"),
                        q.get("preco_maximo"),
                        q.get("preco_minimo"),
                        q.get("preco_fechamento"),
                        q.get("preco_medio"),
                        q.get("volume_financeiro"),
                        q.get("quantidade_titulos", 0),
                    )
                    written += 1
            logger.info("Cotações diárias gravadas no DW", total=written)
        except Exception as e:
            logger.warning("Falha na gravação em lote no DW (modo offline)", error=str(e))
        return written
