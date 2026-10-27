"""Motor de cálculo de modelos de Valuation (Graham, Bazin, DCF, Gordon e Múltiplos)."""

import math
from typing import Any


class ValuationEngine:
    """Implementa fórmulas financeiras rigorosas de precificação e valor intrínseco."""

    @staticmethod
    def graham_fair_price(lpa: float, vpa: float) -> float | None:
        """Fórmula de Benjamin Graham: V = sqrt(22.5 * LPA * VPA)."""
        if lpa <= 0 or vpa <= 0:
            return None
        return math.sqrt(22.5 * lpa * vpa)

    @staticmethod
    def bazin_ceiling_price(dpa_medio: float, target_yield: float = 0.06) -> float | None:
        """Método de Décio Bazin: Preço Teto = DPA Médio / Yield Alvo (default 6%)."""
        if target_yield <= 0 or dpa_medio <= 0:
            return None
        return dpa_medio / target_yield

    @staticmethod
    def gordon_growth_price(
        next_dividend: float,
        cost_of_equity: float,
        growth_rate: float,
    ) -> float | None:
        """Modelo de Desconto de Dividendos de Gordon: P0 = D1 / (r - g)."""
        if cost_of_equity <= growth_rate or next_dividend <= 0:
            return None
        return next_dividend / (cost_of_equity - growth_rate)

    @staticmethod
    def discounted_cash_flow(
        free_cash_flow_0: float,
        growth_rate_5y: float,
        terminal_growth: float,
        wacc: float,
        shares_outstanding: int,
        net_debt: float = 0.0,
    ) -> dict[str, Any] | None:
        """DCF de 2 estágios simplificado com valor terminal na perpetuidade de Gordon."""
        if wacc <= terminal_growth or shares_outstanding <= 0 or free_cash_flow_0 <= 0:
            return None

        pv_cash_flows = 0.0
        current_fcf = free_cash_flow_0

        for year in range(1, 6):
            current_fcf *= (1.0 + growth_rate_5y)
            pv_cash_flows += current_fcf / ((1.0 + wacc) ** year)

        terminal_fcf = current_fcf * (1.0 + terminal_growth)
        terminal_value = terminal_fcf / (wacc - terminal_growth)
        pv_terminal_value = terminal_value / ((1.0 + wacc) ** 5)

        enterprise_value = pv_cash_flows + pv_terminal_value
        equity_value = enterprise_value - net_debt
        price_per_share = equity_value / shares_outstanding

        return {
            "enterprise_value": enterprise_value,
            "equity_value": equity_value,
            "fair_price_per_share": price_per_share,
            "pv_explicit_period": pv_cash_flows,
            "pv_terminal_value": pv_terminal_value,
        }
