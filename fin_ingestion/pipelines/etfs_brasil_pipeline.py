"""
fin-ingestion: ETFs Brasil Pipeline
Synchronizes ETF market data, NAV spread, tracking metrics, holdings compositions,
and calculates analytical portfolio overlap for canonical pairs.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import date
import asyncpg

logger = logging.getLogger("fin_ingestion.pipelines.etfs_brasil")


class ETFsBrasilIngestionPipeline:
    def __init__(self, db_pool: Optional[asyncpg.Pool] = None):
        self.db_pool = db_pool

    async def sync_etf_holdings(
        self,
        id_ativo_fundo: int,
        data_referencia: date,
        holdings: List[Dict[str, Any]],
    ) -> int:
        """
        Inserts or replaces the open holdings composition for an ETF/Fund on a given cut date.
        """
        if not self.db_pool:
            raise RuntimeError("Database pool not initialized")

        delete_stmt = """
        DELETE FROM market.etf_composicao_carteira 
        WHERE id_ativo_fundo = $1 AND data_referencia = $2;
        """
        insert_stmt = """
        INSERT INTO market.etf_composicao_carteira (
            id_ativo_fundo, data_referencia, codigo_ativo, descricao_ativo, 
            setor_economico, pais_origem, peso_percentual
        ) VALUES ($1, $2, $3, $4, $5, $6, $7);
        """

        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(delete_stmt, id_ativo_fundo, data_referencia)
                inserted = 0
                for h in holdings:
                    codigo = h.get("codigo_ativo") or h.get("ticker") or ""
                    desc = h.get("descricao_ativo") or h.get("name") or codigo
                    setor = h.get("setor_economico") or h.get("sector") or "Geral"
                    pais = h.get("pais_origem") or h.get("country") or "Brasil"
                    peso = float(h.get("peso_percentual") or h.get("weight_pct") or 0.0)

                    await conn.execute(
                        insert_stmt,
                        id_ativo_fundo,
                        data_referencia,
                        codigo.upper(),
                        desc,
                        setor,
                        pais,
                        peso,
                    )
                    inserted += 1
                return inserted

    async def sync_etf_daily_metrics(
        self,
        id_ativo: int,
        data_pregao: date,
        preco_fechamento_mercado: float,
        nav_cota_patrimonial: float,
        patrimonio_liquido_diario: float,
        numero_cotistas: int,
        aluguel_acoes_pl_pct: float = 0.0,
        tracking_difference_diario: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Stores daily ETF pricing, accounting NAV, and tracking difference in market.etf_metricas_diarias.
        """
        if not self.db_pool:
            raise RuntimeError("Database pool not initialized")

        stmt = """
        INSERT INTO market.etf_metricas_diarias (
            id_ativo, data_pregao, preco_fechamento_mercado, nav_cota_patrimonial,
            patrimonio_liquido_diario, numero_cotistas, aluguel_acoes_pl_pct,
            tracking_difference_diario
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id_ativo, data_pregao) DO UPDATE
        SET preco_fechamento_mercado = EXCLUDED.preco_fechamento_mercado,
            nav_cota_patrimonial = EXCLUDED.nav_cota_patrimonial,
            patrimonio_liquido_diario = EXCLUDED.patrimonio_liquido_diario,
            numero_cotistas = EXCLUDED.numero_cotistas,
            aluguel_acoes_pl_pct = EXCLUDED.aluguel_acoes_pl_pct,
            tracking_difference_diario = EXCLUDED.tracking_difference_diario;
        """

        spread_pct = 0.0
        if nav_cota_patrimonial > 0:
            spread_pct = ((preco_fechamento_mercado - nav_cota_patrimonial) / nav_cota_patrimonial) * 100.0

        async with self.db_pool.acquire() as conn:
            await conn.execute(
                stmt,
                id_ativo,
                data_pregao,
                preco_fechamento_mercado,
                nav_cota_patrimonial,
                patrimonio_liquido_diario,
                numero_cotistas,
                aluguel_acoes_pl_pct,
                tracking_difference_diario,
            )
            return {
                "id_ativo": id_ativo,
                "data": data_pregao.isoformat(),
                "preco": preco_fechamento_mercado,
                "nav": nav_cota_patrimonial,
                "spread_nav_pct": round(spread_pct, 4),
            }

    @staticmethod
    def calculate_holdings_overlap(
        holdings_a: List[Dict[str, Any]],
        holdings_b: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Calculates the portfolio holdings overlap sum(min(w_A_i, w_B_i)).
        """
        map_a = {h["codigo_ativo"].upper(): float(h["peso_percentual"]) for h in holdings_a}
        map_b = {h["codigo_ativo"].upper(): float(h["peso_percentual"]) for h in holdings_b}

        common_keys = set(map_a.keys()).intersection(set(map_b.keys()))
        overlap_sum = 0.0
        details = []

        for k in common_keys:
            w_a = map_a[k]
            w_b = map_b[k]
            overlap_weight = min(w_a, w_b)
            overlap_sum += overlap_weight
            details.append({
                "codigo": k,
                "peso_a": w_a,
                "peso_b": w_b,
                "overlap_min": overlap_weight,
            })

        details.sort(key=lambda x: x["overlap_min"], reverse=True)
        return {
            "total_overlap_pct": round(overlap_sum, 2),
            "common_holdings_count": len(common_keys),
            "top_shared_holdings": details[:10],
        }
