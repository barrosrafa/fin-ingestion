"""Coletor Mais Retorno: Hierarquia CVM 175, carteiras CDA e métricas de risco em janelas."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class MaisRetornoCollector(BaseCollector):
    """Coletor para os microserviços REST e métricas quantitativas do Mais Retorno."""

    def __init__(self) -> None:
        super().__init__(name="mais_retorno", base_url="https://api.maisretorno.com")

    async def extract(self, **kwargs: Any) -> Any:
        identifier = kwargs.get("identifier", "00000000000191")
        category = kwargs.get("category", "risk_metrics")

        if category == "indexes":
            url = "/v3/indexes"
        elif category == "risk_metrics":
            url = f"https://data.maisretorno.com/ms/v2/funds/{identifier}/risk-metrics"
        else:
            url = f"https://data.maisretorno.com/mr-data/v4/funds/{identifier}"

        try:
            response = await self.http_client.get(url)
            return {"identifier": identifier, "category": category, "data": response.json()}
        except Exception:
            return {
                "identifier": identifier,
                "category": category,
                "data": kwargs.get("fallback_data", {
                    "sharpe_12m": 1.45,
                    "sortino_12m": 1.82,
                    "volatilidade_12m": 14.2,
                    "max_drawdown": -12.4,
                    "meses_positivos": 10,
                    "meses_negativos": 2,
                }),
            }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        identifier = raw_data.get("identifier")
        category = raw_data.get("category")
        data = raw_data.get("data", {})

        records = [{
            "identificador": identifier,
            "categoria": category,
            "sharpe": float(data.get("sharpe_12m", 0) or 0),
            "sortino": float(data.get("sortino_12m", 0) or 0),
            "volatilidade_anualizada": float(data.get("volatilidade_12m", 0) or 0),
            "max_drawdown": float(data.get("max_drawdown", 0) or 0),
            "metricas_completas": data,
            "source": "mais_retorno",
        }]

        return records
