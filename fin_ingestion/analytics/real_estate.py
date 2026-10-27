"""Métricas analíticas especializadas para Fundos de Investimento Imobiliário (FIIs)."""

from typing import Any


class RealEstateAnalytics:
    """Implementa cálculos de cap rate, vacância ponderada, WALT e yields imobiliários."""

    @staticmethod
    def cap_rate(annual_net_operating_income: float, property_value: float) -> float | None:
        """Cap Rate = NOI Anual / Valor do Imóvel."""
        if property_value <= 0:
            return None
        return (annual_net_operating_income / property_value) * 100.0

    @staticmethod
    def weighted_average_lease_term(leases: list[dict[str, Any]]) -> float:
        """WALT (Weighted Average Lease Term) ponderado pela receita ou ABL locada."""
        total_weight = 0.0
        weighted_years = 0.0

        for lease in leases:
            years_remaining = float(lease.get("years_remaining", 0))
            weight = float(lease.get("weight", lease.get("revenue", lease.get("abl", 1.0))))
            if years_remaining > 0 and weight > 0:
                weighted_years += years_remaining * weight
                total_weight += weight

        if total_weight == 0:
            return 0.0
        return round(weighted_years / total_weight, 2)

    @staticmethod
    def weighted_vacancy(properties: list[dict[str, Any]]) -> dict[str, float]:
        """Calcula a vacância física (%) e financeira (%) ponderada pela ABL de cada imóvel."""
        total_abl = 0.0
        vacant_abl = 0.0
        total_potential_revenue = 0.0
        unrealized_revenue = 0.0

        for prop in properties:
            abl = float(prop.get("abl", 0))
            phys_vac_pct = float(prop.get("vacancia_fisica_pct", 0)) / 100.0
            fin_vac_pct = float(prop.get("vacancia_financeira_pct", 0)) / 100.0
            pot_rev = float(prop.get("receita_potencial", abl * 50.0))

            total_abl += abl
            vacant_abl += abl * phys_vac_pct
            total_potential_revenue += pot_rev
            unrealized_revenue += pot_rev * fin_vac_pct

        phys_res = (vacant_abl / total_abl * 100.0) if total_abl > 0 else 0.0
        fin_res = (unrealized_revenue / total_potential_revenue * 100.0) if total_potential_revenue > 0 else 0.0

        return {
            "vacancia_fisica_ponderada_pct": round(phys_res, 2),
            "vacancia_financeira_ponderada_pct": round(fin_res, 2),
        }
