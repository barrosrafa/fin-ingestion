"""Motor de cálculo de indicadores técnicos clássicos e níveis de suporte e resistência."""

import numpy as np


class TechnicalAnalysisEngine:
    """Implementa indicadores de análise técnica em concordância com a ADVFN."""

    @staticmethod
    def pivot_points_4s_3r(high: float, low: float, close: float) -> dict[str, float]:
        """Calcula Pivot Point e 4 Suportes / 3 Resistências clássicos."""
        pp = (high + low + close) / 3.0

        r1 = 2 * pp - low
        s1 = 2 * pp - high

        r2 = pp + (high - low)
        s2 = pp - (high - low)

        r3 = high + 2 * (pp - low)
        s3 = low - 2 * (high - pp)

        s4 = s3 - (r1 - s1)

        return {
            "pivot_point": round(pp, 2),
            "suporte_1": round(s1, 2),
            "suporte_2": round(s2, 2),
            "suporte_3": round(s3, 2),
            "suporte_4": round(s4, 2),
            "resistencia_1": round(r1, 2),
            "resistencia_2": round(r2, 2),
            "resistencia_3": round(r3, 2),
        }

    @staticmethod
    def relative_strength_index(prices: list[float], period: int = 14) -> float | None:
        """Índice de Força Relativa (IFR / RSI de 14 períodos)."""
        if len(prices) <= period:
            return None

        p = np.array(prices, dtype=float)
        deltas = np.diff(p)

        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return round(float(rsi), 2)

    @staticmethod
    def bollinger_bands(prices: list[float], window: int = 20, num_std: float = 2.0) -> dict[str, float] | None:
        """Bandas de Bollinger: Média Móvel Central, Banda Superior e Banda Inferior."""
        if len(prices) < window:
            return None

        recent = np.array(prices[-window:], dtype=float)
        mean = float(np.mean(recent))
        std = float(np.std(recent, ddof=1))

        return {
            "media_central": round(mean, 2),
            "banda_superior": round(mean + num_std * std, 2),
            "banda_inferior": round(mean - num_std * std, 2),
            "largura_banda_pct": round(((2 * num_std * std) / mean) * 100.0, 2) if mean > 0 else 0.0,
        }
