"""Consolidação periódica de snapshots de patrimônio, rankings e indicadores."""

from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class SnapshotConsolidator:
    """Gera snapshots dimensionais diários e mensais no schema dw."""

    @staticmethod
    def build_screener_snapshot(
        tickers_data: list[dict[str, Any]],
        category: str = "acoes",
    ) -> list[dict[str, Any]]:
        """Gera registros agregados para a tabela dw.ranking."""
        now = datetime.now(timezone.utc).date().isoformat()
        ranked = []

        # Ordena por dividend yield ou pontuação composta
        sorted_data = sorted(
            tickers_data,
            key=lambda x: float(x.get("dividend_yield", 0) or 0),
            reverse=True,
        )

        for rank_pos, item in enumerate(sorted_data, start=1):
            ranked.append({
                "data_snapshot": now,
                "categoria": category,
                "posicao": rank_pos,
                "ticker": item.get("ticker"),
                "pontuacao": float(item.get("dividend_yield", 0) or 0),
                "detalhes": item,
            })

        logger.info("Snapshot gerado com sucesso", categoria=category, total_ativos=len(ranked))
        return ranked
