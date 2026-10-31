"""Escritor de dados de alta performance para o Data Warehouse (PostgreSQL 18 / TimescaleDB).
Aderente estritamente ao DDL de market.cotacao_diaria e canonical.price_observation."""

import asyncio
import re
from datetime import date
from decimal import Decimal
from typing import Any

import asyncpg
import structlog

from fin_ingestion.config import settings
from fin_ingestion.core.database import create_pool_with_retry
from fin_ingestion.quality.market_schema import validate_complete_candles
from fin_ingestion.quality.temporal import timestamp_utc
from fin_ingestion.quality.validator import DataQualityValidator

logger = structlog.get_logger(__name__)


class DWWriter:
    """Carrega dados harmonizados e métricas analíticas calculadas nas tabelas de fatos e dimensões."""

    def __init__(self, db_url: str | None = None) -> None:
        self.db_url = db_url or settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()
        self._ticker_map: dict[str, int] = {}

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await create_pool_with_retry(self.db_url)
        return self._pool

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def write_instrument_identifiers(self, quotes: list[dict[str, Any]]) -> None:
        """Completa ISINs ausentes com identificadores oficiais da B3."""
        values = list({
            (q["ticker"], q["codigo_isin"])
            for q in quotes if q.get("ticker") and re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", q.get("codigo_isin", ""))
        })
        if not values:
            return
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(
                "UPDATE core.ativo SET isin = $2 WHERE ticker = $1 AND isin IS NULL", values,
            )

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

        # Fail the whole batch before opening a connection; Bronze remains replayable.
        errors = []
        for index, quote in enumerate(quotes):
            valid, reason, _ = DataQualityValidator.validate_daily_candle(quote)
            try:
                day = quote.get("data_pregao")
                day = date.fromisoformat(day) if isinstance(day, str) else day
                if type(day) is not date:
                    raise ValueError("data_pregao must be a date")
            except (TypeError, ValueError) as exc:
                valid, reason = False, str(exc)
            if not valid:
                errors.append(f"row {index}: {reason}")
        if errors:
            raise ValueError("Invalid daily quote batch: " + "; ".join(errors))
        validate_complete_candles(quotes)

        ticker_map = await self.get_ticker_id_map()

        query = """
        INSERT INTO market.cotacao_diaria (
            id_ativo, data_pregao, preco_abertura, preco_maximo, preco_minimo,
            preco_fechamento, preco_medio, volume_total, quantidade_negocios, variacao_percentual
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id_ativo, data_pregao) DO UPDATE SET
            preco_abertura = COALESCE(EXCLUDED.preco_abertura, market.cotacao_diaria.preco_abertura),
            preco_maximo = COALESCE(EXCLUDED.preco_maximo, market.cotacao_diaria.preco_maximo),
            preco_minimo = COALESCE(EXCLUDED.preco_minimo, market.cotacao_diaria.preco_minimo),
            preco_fechamento = COALESCE(EXCLUDED.preco_fechamento, market.cotacao_diaria.preco_fechamento),
            preco_medio = COALESCE(EXCLUDED.preco_medio, market.cotacao_diaria.preco_medio),
            volume_total = COALESCE(EXCLUDED.volume_total, market.cotacao_diaria.volume_total),
            quantidade_negocios = COALESCE(EXCLUDED.quantidade_negocios, market.cotacao_diaria.quantidade_negocios),
            variacao_percentual = COALESCE(EXCLUDED.variacao_percentual, market.cotacao_diaria.variacao_percentual);
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
                to_dec(q.get("volume_total") if q.get("volume_total") is not None else q.get("volume_financeiro")),
                q.get("quantidade_negocios", q.get("numero_negocios")),
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

    async def write_price_observation(
        self,
        ticker: str,
        price: float | Decimal,
        observed_at: Any = None,
        source_name: str = "status_invest",
        variation: float | Decimal | None = None,
        volume: float | Decimal | None = None,
        source_timezone: str | None = None,
    ) -> bool:
        """Grava observação pontual em market.cotacao_intraday sem criar OHLC artificial."""
        ts = timestamp_utc(observed_at, source_timezone=source_timezone)
        price_value = Decimal(str(price))
        if not price_value.is_finite() or price_value <= 0:
            raise ValueError("Observation price must be finite and positive")
        for name, value in (("variation", variation), ("volume", volume)):
            if value is not None:
                number = Decimal(str(value))
                if not number.is_finite() or (name == "volume" and number < 0):
                    raise ValueError(f"Invalid observation {name}")
        ticker_map = await self.get_ticker_id_map()
        id_ativo = ticker_map.get(ticker)
        if not id_ativo:
            return False

        pool = await self.get_pool()
        query = """
        INSERT INTO market.cotacao_intraday (id_ativo, timestamp, preco_atual, variacao, volume, source_id, price_kind)
        VALUES ($1, $2, $3, $4, $5, $6, 'intraday_snapshot')
        ON CONFLICT (id_ativo,timestamp,source_id,price_kind) WHERE source_id IS NOT NULL
        DO UPDATE SET preco_atual=EXCLUDED.preco_atual, variacao=EXCLUDED.variacao, volume=EXCLUDED.volume;
        """
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                id_ativo,
                ts,
                Decimal(str(price)),
                Decimal(str(variation)) if variation is not None else None,
                Decimal(str(volume)) if volume is not None else None,
                source_name,
            )
        return True

