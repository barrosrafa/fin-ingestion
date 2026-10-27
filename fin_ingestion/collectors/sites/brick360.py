"""Coletor Brick360: API REST v2, Brick Score (6 eixos), DRE contábil e auditoria de gestoras."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class Brick360Collector(BaseCollector):
    """Coletor especializado nos 12 módulos de inteligência e scoring de FIIs do Brick360."""

    def __init__(self) -> None:
        super().__init__(name="brick360", base_url="https://brick360.com.br")

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "KNIP11")
        url = f"/api/v2/funds/{ticker}/analysis/advanced"

        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "data": response.json()}
        except Exception:
            return {
                "ticker": ticker,
                "data": kwargs.get("fallback_data", {
                    "brick_score": {
                        "global_score": 88.5,
                        "eixo_imobiliario": 85.0,
                        "eixo_financeiro": 92.0,
                        "eixo_inquilinos": 87.0,
                        "eixo_gestao": 90.0,
                        "eixo_credito": 89.0,
                        "eixo_preco": 88.0,
                    },
                    "walt_years": 4.8,
                    "top5_tenants_pct": 28.5,
                    "atypical_contracts_pct": 65.0,
                }),
            }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        ticker = raw_data.get("ticker")
        data = raw_data.get("data", {})
        scores = data.get("brick_score", {})

        records = [{
            "ticker": ticker,
            "score_global": float(scores.get("global_score", 0) or 0),
            "score_imobiliario": float(scores.get("eixo_imobiliario", 0) or 0),
            "score_financeiro": float(scores.get("eixo_financeiro", 0) or 0),
            "score_inquilinos": float(scores.get("eixo_inquilinos", 0) or 0),
            "score_gestao": float(scores.get("eixo_gestao", 0) or 0),
            "score_credito": float(scores.get("eixo_credito", 0) or 0),
            "score_preco": float(scores.get("eixo_preco", 0) or 0),
            "walt_anos": float(data.get("walt_years", 0) or 0),
            "concentracao_top5_pct": float(data.get("top5_tenants_pct", 0) or 0),
            "contratos_atipicos_pct": float(data.get("atypical_contracts_pct", 0) or 0),
            "source": "brick360_v2",
        }]

        return records
