"""Métricas de risco e performance estatística de ativos e carteiras."""

import math
from typing import Any

import numpy as np


class RiskAnalyticsEngine:
    """Calcula métricas quantitativas de risco de mercado em conformidade com o Mais Retorno."""

    @staticmethod
    def calculate_returns(prices: list[float]) -> np.ndarray:
        """Converte série de preços em retornos percentuais simples."""
        p = np.array(prices, dtype=float)
        return (p[1:] - p[:-1]) / p[:-1]

    @staticmethod
    def sharpe_ratio(
        returns: np.ndarray,
        risk_free_rate_annual: float = 0.105,
        periods_per_year: int = 252,
    ) -> float | None:
        """Índice de Sharpe: (Retorno Médio Anualizado - Rf) / Volatilidade Anualizada."""
        if len(returns) < 2:
            return None
        rf_daily = (1.0 + risk_free_rate_annual) ** (1.0 / periods_per_year) - 1.0
        excess_returns = returns - rf_daily
        volatility = np.std(returns, ddof=1)
        if volatility == 0:
            return None
        return float(np.mean(excess_returns) / volatility * math.sqrt(periods_per_year))

    @staticmethod
    def sortino_ratio(
        returns: np.ndarray,
        risk_free_rate_annual: float = 0.105,
        periods_per_year: int = 252,
    ) -> float | None:
        """Índice de Sortino: focado no downside risk (desvios negativos)."""
        if len(returns) < 2:
            return None
        rf_daily = (1.0 + risk_free_rate_annual) ** (1.0 / periods_per_year) - 1.0
        excess_returns = returns - rf_daily
        negative_returns = returns[returns < 0]
        if len(negative_returns) == 0:
            return None
        downside_deviation = np.std(negative_returns, ddof=1)
        if downside_deviation == 0:
            return None
        return float(np.mean(excess_returns) / downside_deviation * math.sqrt(periods_per_year))

    @staticmethod
    def max_drawdown(prices: list[float]) -> dict[str, Any]:
        """Calcula o drawdown máximo acumulado e pico histórico da série."""
        if not prices:
            return {"max_drawdown_pct": 0.0, "peak": 0.0, "trough": 0.0}

        p = np.array(prices, dtype=float)
        cumulative_max = np.maximum.accumulate(p)
        drawdowns = (p - cumulative_max) / cumulative_max
        max_dd = float(np.min(drawdowns))

        return {
            "max_drawdown_pct": round(max_dd * 100.0, 2),
            "peak": float(np.max(p)),
            "current_drawdown_pct": round(float(drawdowns[-1]) * 100.0, 2),
        }

    @staticmethod
    def value_at_risk(returns: np.ndarray, confidence: float = 0.95) -> dict[str, float]:
        """Calcula VaR Histórico e Conditional VaR (CVaR / Expected Shortfall)."""
        if len(returns) < 10:
            return {"var": 0.0, "cvar": 0.0}

        sorted_returns = np.sort(returns)
        cutoff_index = int((1.0 - confidence) * len(sorted_returns))
        var = float(-sorted_returns[cutoff_index])
        cvar = float(-np.mean(sorted_returns[:cutoff_index])) if cutoff_index > 0 else var

        return {
            "var_pct": round(var * 100.0, 2),
            "cvar_pct": round(cvar * 100.0, 2),
        }
