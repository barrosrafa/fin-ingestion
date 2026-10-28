"""
fin-ingestion: Análise de Ações International Assets & FX Pipeline
Ingests BCB PTAX closing exchange rates, normalizes USD financial indicators,
and reconciles US Stocks/REITs with withholding tax parameters.
"""

import logging
from datetime import date
from typing import Any

import asyncpg

logger = logging.getLogger("fin_ingestion.pipelines.analise_de_acoes")


class PtaxExchangeRatePipeline:
    def __init__(self, db_pool: asyncpg.Pool | None = None):
        self.db_pool = db_pool

    async def ingest_daily_ptax(
        self,
        target_date: date,
        moeda_origem: str = "USD",
        taxa_compra: float = 5.3520,
        taxa_venda: float = 5.3526,
    ) -> dict[str, Any]:
        """
        Stores daily official PTAX exchange rate for foreign asset valuation.
        """
        if not self.db_pool:
            raise RuntimeError("Database pool not initialized")

        stmt = """
        INSERT INTO market.cotacao_cambio_diaria (
            data_cambio, moeda_origem, moeda_destino, taxa_compra, taxa_venda, boletim
        ) VALUES ($1, $2, 'BRL', $3, $4, 'FECHAMENTO')
        ON CONFLICT (data_cambio, moeda_origem, moeda_destino) DO UPDATE
        SET taxa_compra = EXCLUDED.taxa_compra,
            taxa_venda = EXCLUDED.taxa_venda;
        """
        async with self.db_pool.acquire() as conn:
            await conn.execute(stmt, target_date, moeda_origem, taxa_compra, taxa_venda)
            return {
                "date": target_date.isoformat(),
                "currency": moeda_origem,
                "ptax_venda": taxa_venda,
            }


class USAssetsIngestionPipeline:
    def __init__(self, db_pool: asyncpg.Pool | None = None):
        self.db_pool = db_pool

    async def sync_reit_metrics(
        self,
        ticker: str,
        subtipo: str,
        ffo_12m: float,
        affo_12m: float,
        ffo_per_share: float,
        affo_per_share: float,
        p_ffo: float,
        p_affo: float,
        payout_ratio: float,
        occupancy_rate: float,
        properties_count: int,
        leverage_ev: float,
    ) -> dict[str, Any]:
        """
        Syncs Nareit accounting metrics (FFO, AFFO, P/FFO, P/AFFO) for REITs.
        """
        if not self.db_pool:
            raise RuntimeError("Database pool not initialized")

        async with self.db_pool.acquire() as conn:
            # 1. Ensure asset exists in core.ativo
            await conn.execute(
                """
                INSERT INTO core.ativo (ticker, tipo_ativo, moeda)
                VALUES ($1, 'REIT', 'USD')
                ON CONFLICT (ticker) DO UPDATE SET moeda = 'USD';
                """,
                ticker,
            )
            ativo_id = await conn.fetchval("SELECT id_ativo FROM core.ativo WHERE ticker = $1", ticker)

            # 2. Upsert asset_details.detalhe_reit
            stmt = """
            INSERT INTO asset_details.detalhe_reit (
                id_ativo, subtipo_reit, ffo_12m, affo_12m, ffo_por_acao,
                affo_por_acao, p_ffo, p_affo, ffo_payout_ratio, taxa_ocupacao_pct,
                total_propriedades, alavancagem_divida_ev, withholding_tax_pct, data_atualizacao
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 30.00, NOW()
            ) ON CONFLICT (id_ativo) DO UPDATE SET
                subtipo_reit = EXCLUDED.subtipo_reit,
                ffo_12m = EXCLUDED.ffo_12m,
                affo_12m = EXCLUDED.affo_12m,
                ffo_por_acao = EXCLUDED.ffo_por_acao,
                affo_por_acao = EXCLUDED.affo_por_acao,
                p_ffo = EXCLUDED.p_ffo,
                p_affo = EXCLUDED.p_affo,
                ffo_payout_ratio = EXCLUDED.ffo_payout_ratio,
                taxa_ocupacao_pct = EXCLUDED.taxa_ocupacao_pct,
                total_propriedades = EXCLUDED.total_propriedades,
                alavancagem_divida_ev = EXCLUDED.alavancagem_divida_ev,
                data_atualizacao = NOW();
            """
            await conn.execute(
                stmt,
                ativo_id,
                subtipo,
                ffo_12m,
                affo_12m,
                ffo_per_share,
                affo_per_share,
                p_ffo,
                p_affo,
                payout_ratio,
                occupancy_rate,
                properties_count,
                leverage_ev,
            )
            return {"ticker": ticker, "status": "synced", "id_ativo": ativo_id}
