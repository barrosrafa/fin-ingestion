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
            if not close_val.is_finite() or close_val <= 0:
                return False, f"Preço de fechamento não positivo: {close_val}", False
        except Exception:
            return False, f"Preço de fechamento inválido: {c}", False

        open_raw = quote.get("preco_abertura")
        high_raw = quote.get("preco_maximo")
        low_raw = quote.get("preco_minimo")

        # Validate even partial candles, including NaN/Infinity and negative volumes.
        for field in ("preco_abertura", "preco_maximo", "preco_minimo", "preco_medio",
                      "volume_total", "volume_financeiro", "quantidade_negocios", "numero_negocios"):
            value = quote.get(field)
            if value is None:
                continue
            try:
                number = Decimal(str(value))
                if not number.is_finite() or number < 0 or (field.startswith("preco_") and number == 0):
                    return False, f"Valor inválido em {field}: {value}", False
                if field in {"quantidade_negocios", "numero_negocios"} and number != number.to_integral_value():
                    return False, f"Contagem fracionária em {field}: {value}", False
            except Exception:
                return False, f"Valor inválido em {field}: {value}", False

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
                if not var_val.is_finite():
                    return False, "Variação não finita", False
                if abs(var_val) > 50.0:
                    return (
                        False,
                        f"Variação atípica de {var_val}%. Suspeita de provento ou split.",
                        True,
                    )
            except Exception:
                return False, "Variação inválida", False

        return True, None, False


DIVIDEND_YIELD_FIELDS = frozenset({
    "dividend_yield", "dividend_yield_pct", "dividend_yield_12m",
    "dividend_yield_12m_pct", "dy_pct", "dividendYield",
})


def validate_snapshot_indicators(data: Any) -> None:
    """Fail publication before any writer runs; preserve the last known good snapshot.

    Yields use percentage points. Out-of-policy values need provenance review,
    never an automatic division by 100. Missing values are allowed.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key in DIVIDEND_YIELD_FIELDS and value is not None:
                try:
                    number = Decimal(str(value))
                    valid = number.is_finite() and 0 <= number <= 100
                except Exception:
                    valid = False
                if not valid:
                    raise ValueError(f"Snapshot blocked: invalid {key}")
            validate_snapshot_indicators(value)
    elif isinstance(data, (list, tuple)):
        for item in data:
            validate_snapshot_indicators(item)
