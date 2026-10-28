"""Validador de qualidade pré-publicação e quarentena de anomalias financeiras."""

from decimal import Decimal
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class DataQualityValidator:
    """Aplica regras financeiras e geométricas rigorosas antes da persistência canônica."""

    @staticmethod
    def validate_daily_candle(quote: dict[str, Any]) -> tuple[bool, str | None, bool]:
        """Valida sanidade geométrica OHLC e consistência de valores.

        Retorna:
          (is_valid, error_reason, needs_corporate_action_investigation)
        """
        c = quote.get("preco_fechamento")
        if c is None:
            return False, "Preço de fechamento ausente", False

        try:
            close_val = Decimal(str(c))
            if close_val <= 0:
                return False, f"Preço de fechamento não positivo: {close_val}", False
        except Exception:
            return False, f"Preço de fechamento inválido: {c}", False

        open_raw = quote.get("preco_abertura")
        high_raw = quote.get("preco_maximo")
        low_raw = quote.get("preco_minimo")

        # Se houver candle OHLC completo, valida consistência geométrica
        if open_raw is not None and high_raw is not None and low_raw is not None:
            try:
                open_val = Decimal(str(open_raw))
                high_val = Decimal(str(high_raw))
                low_val = Decimal(str(low_raw))

                if not (low_val <= open_val <= high_val and low_val <= close_val <= high_val):
                    return (
                        False,
                        f"Inconsistência geométrica OHLC: Low={low_val}, High={high_val}, Open={open_val}, Close={close_val}",
                        False,
                    )
            except Exception as e:
                return False, f"Erro ao converter valores OHLC: {e}", False

        # Verifica variações atípicas (> 50% em um pregão)
        var = quote.get("variacao_percentual")
        if var is not None:
            try:
                var_val = Decimal(str(var))
                if abs(var_val) > 50.0:
                    return (
                        False,
                        f"Variação atípica de {var_val}%. Suspeita de provento ou split.",
                        True,
                    )
            except Exception:
                pass

        return True, None, False
