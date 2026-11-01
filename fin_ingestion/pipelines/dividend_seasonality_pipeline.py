"""
fin-ingestion: Dividend Seasonality & Map Aggregation Pipeline
Aggregates historical corporate action events (data-com and payment dates)
into the pre-aggregated heatmap matrix `analytics.mapa_sazonalidade_dividendo`.
"""

import logging
from typing import Any

import asyncpg

logger = logging.getLogger("fin_ingestion.pipelines.dividend_seasonality")


class DividendSeasonalityAggregator:
    def __init__(self, db_pool: asyncpg.Pool | None = None):
        self.db_pool = db_pool

    async def aggregate_seasonality(self, conn=None) -> dict[str, Any]:
        """
        Scans market.provento and populates analytics.mapa_sazonalidade_dividendo
        for all 12 calendar months with occurrence count, analyzed years, average payout
        and computed probability.
        """
        if not self.db_pool and conn is None:
            raise RuntimeError("Database connection pool not initialized")

        query = """
        INSERT INTO analytics.mapa_sazonalidade_dividendo (
            id_ativo, mes_calendario, tipo_evento, ocorrencias_historicas,
            anos_analisados, valor_medio_provento, yield_medio_mensal, ultimo_ano_pago
        )
        SELECT
            p.id_ativo,
            EXTRACT(MONTH FROM event.event_date)::smallint AS mes_calendario,
            event.event_kind AS tipo_evento,
            COUNT(DISTINCT EXTRACT(YEAR FROM event.event_date)) AS ocorrencias_historicas,
            (GREATEST(EXTRACT(YEAR FROM MAX(event.event_date)) - EXTRACT(YEAR FROM MIN(event.event_date)) + 1, 1))::int AS anos_analisados,
            COALESCE(AVG(p.valor_por_acao), 0.0) AS valor_medio_provento,
            COALESCE(AVG(p.valor_por_acao / NULLIF(cd.preco_fechamento, 0)), 0.0) AS yield_medio_mensal,
            MAX(EXTRACT(YEAR FROM event.event_date))::int AS ultimo_ano_pago
        FROM market.provento p
        CROSS JOIN LATERAL (VALUES ('DATA_COM',p.data_com), ('PAGAMENTO',p.data_pagamento))
            AS event(event_kind,event_date)
        LEFT JOIN LATERAL (
            SELECT preco_fechamento
            FROM market.cotacao_diaria
            WHERE id_ativo = p.id_ativo AND data_pregao <= event.event_date
            ORDER BY data_pregao DESC LIMIT 1
        ) cd ON TRUE
        WHERE event.event_date IS NOT NULL
        GROUP BY p.id_ativo, event.event_kind, EXTRACT(MONTH FROM event.event_date)
        ON CONFLICT (id_ativo, mes_calendario, tipo_evento) DO UPDATE
        SET
            ocorrencias_historicas = EXCLUDED.ocorrencias_historicas,
            anos_analisados = EXCLUDED.anos_analisados,
            valor_medio_provento = EXCLUDED.valor_medio_provento,
            yield_medio_mensal = EXCLUDED.yield_medio_mensal,
            ultimo_ano_pago = EXCLUDED.ultimo_ano_pago,
            data_atualizacao = NOW();
        """

        async def refresh(connection):
            async with connection.transaction():
                # Derived cache: remove obsolete months/assets in the same transaction.
                await connection.execute("DELETE FROM analytics.mapa_sazonalidade_dividendo")
                result = await connection.execute(query)
                count_val = await connection.fetchval("SELECT count(*) FROM analytics.mapa_sazonalidade_dividendo")
            logger.info(f"Dividend seasonality aggregation completed. Result: {result}, Total entries: {count_val}")
            return {"status": "success", "result": result, "total_records": count_val}

        if conn is not None:
            return await refresh(conn)
        async with self.db_pool.acquire() as connection:
            return await refresh(connection)
