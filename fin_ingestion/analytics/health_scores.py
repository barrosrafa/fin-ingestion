"""Scores de saúde financeira e qualidade: Piotroski F-Score, Altman Z-Score e Beneish M-Score."""

from typing import Any


class HealthScoreEngine:
    """Calcula índices consagrados de solidez contábil e detecção de riscos."""

    @staticmethod
    def piotroski_f_score(
        net_income: float,
        operating_cash_flow: float,
        roa_current: float,
        roa_previous: float,
        cf_vs_net_income: bool,
        leverage_current: float,
        leverage_previous: float,
        current_ratio_current: float,
        current_ratio_previous: float,
        shares_current: int,
        shares_previous: int,
        gross_margin_current: float,
        gross_margin_previous: float,
        asset_turnover_current: float,
        asset_turnover_previous: float,
    ) -> dict[str, Any]:
        """Calcula o Piotroski F-Score (0 a 9 pontos em 3 categorias)."""
        criteria = {
            # Rentabilidade
            "positive_net_income": 1 if net_income > 0 else 0,
            "positive_operating_cf": 1 if operating_cash_flow > 0 else 0,
            "higher_roa": 1 if roa_current > roa_previous else 0,
            "quality_of_earnings": 1 if operating_cash_flow > net_income else 0,
            # Alavancagem e Liquidez
            "lower_leverage": 1 if leverage_current < leverage_previous else 0,
            "higher_current_ratio": 1 if current_ratio_current > current_ratio_previous else 0,
            "no_dilution": 1 if shares_current <= shares_previous else 0,
            # Eficiência Operacional
            "higher_gross_margin": 1 if gross_margin_current > gross_margin_previous else 0,
            "higher_asset_turnover": 1 if asset_turnover_current > asset_turnover_previous else 0,
        }

        total_score = sum(criteria.values())
        return {
            "f_score": total_score,
            "max_score": 9,
            "health_level": "Forte" if total_score >= 7 else "Moderado" if total_score >= 4 else "Fraco",
            "criteria_breakdown": criteria,
        }

    @staticmethod
    def altman_z_score_emerging(
        working_capital: float,
        total_assets: float,
        retained_earnings: float,
        ebit: float,
        market_cap: float,
        total_liabilities: float,
        revenue: float,
    ) -> dict[str, Any]:
        """Altman Z''-Score para Mercados Emergentes e Serviços: Z'' = 6.56*X1 + 3.26*X2 + 6.72*X3 + 1.05*X4."""
        if total_assets <= 0 or total_liabilities <= 0:
            return {"z_score": 0.0, "zone": "Indeterminado"}

        x1 = working_capital / total_assets
        x2 = retained_earnings / total_assets
        x3 = ebit / total_assets
        x4 = market_cap / total_liabilities

        z_score = 6.56 * x1 + 3.26 * x2 + 6.72 * x3 + 1.05 * x4

        if z_score > 2.6:
            zone = "Segura"
        elif z_score >= 1.1:
            zone = "Cinza"
        else:
            zone = "Perigo (Distress)"

        return {"z_score": round(z_score, 2), "zone": zone}
