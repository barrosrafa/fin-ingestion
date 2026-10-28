"""Escritor de dados de alta performance para o Data Warehouse (PostgreSQL 18 / TimescaleDB).
Aderente estritamente ao DDL de market.cotacao_diaria e canonical.price_observation."""

from datetime import date
from decimal import Decimal
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
        self._ticker_map: dict[str, int] = {}

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(dsn=self.db_url, min_size=1, max_size=5)
        return self._pool

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def get_ticker_id_map(self) -> dict[str, int]:
        """Carrega e armazena em cache o mapa ticker -> id_ativo de core.ativo."""
        if not self._ticker_map:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT ticker, id_ativo FROM core.ativo WHERE ticker IS NOT NULL;")
                self._ticker_map = {r["ticker"]: r["id_ativo"] for r in rows}
        return self._ticker_map

    async def write_daily_quotes(self, quotes: list[dict[str, Any]]) -> int:
        """Grava cotações diárias normalizadas na hypertable market.cotacao_diaria de forma idempotente."""
        if not quotes:
            return 0

        ticker_map = await self.get_ticker_id_map()

        query = """
        INSERT INTO market.cotacao_diaria (
            id_ativo, data_pregao, preco_abertura, preco_maximo, preco_minimo,
            preco_fechamento, preco_medio, volume_total, quantidade_negocios, variacao_percentual
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id_ativo, data_pregao) DO UPDATE SET
            preco_abertura = EXCLUDED.preco_abertura,
            preco_maximo = EXCLUDED.preco_maximo,
            preco_minimo = EXCLUDED.preco_minimo,
            preco_fechamento = EXCLUDED.preco_fechamento,
            preco_medio = EXCLUDED.preco_medio,
            volume_total = EXCLUDED.volume_total,
            quantidade_negocios = EXCLUDED.quantidade_negocios,
            variacao_percentual = EXCLUDED.variacao_percentual;
        """

        records = []
        for q in quotes:
            id_ativo = q.get("id_ativo")
            if not id_ativo and "ticker" in q:
                id_ativo = ticker_map.get(q["ticker"])

            if not id_ativo:
                continue

            dt = q.get("data_pregao")
            if isinstance(dt, str):
                dt = date.fromisoformat(dt)

            def to_dec(val):
                return Decimal(str(val)) if val is not None else None

            preco_fechamento = to_dec(q.get("preco_fechamento"))
            if preco_fechamento is None:
                continue

            records.append((
                id_ativo,
                dt,
                to_dec(q.get("preco_abertura")),
                to_dec(q.get("preco_maximo")),
                to_dec(q.get("preco_minimo")),
                preco_fechamento,
                to_dec(q.get("preco_medio")),
                to_dec(q.get("volume_total") or q.get("volume_financeiro")),
                q.get("quantidade_negocios") or 0,
                to_dec(q.get("variacao_percentual")),
            ))

        if not records:
            return 0

        pool = await self.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, records)

        logger.info("Cotações diárias gravadas no DW com sucesso", total=len(records))
        return len(records)

