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
            if "fallback_data" not in kwargs:
                raise
            return {"identifier": identifier, "category": category, "data": kwargs["fallback_data"]}

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []
        identifier = raw_data.get("identifier")
        category = raw_data.get("category")
        data = raw_data.get("data", {})
        if not isinstance(data, dict) or not any(
            k in data for k in ("sharpe_12m", "sortino_12m", "volatilidade_12m", "max_drawdown")
        ):
            return []
        records = [
            {
                "identificador": identifier,
                "categoria": category,
                "sharpe": float(data.get("sharpe_12m", 0) or 0),
                "sortino": float(data.get("sortino_12m", 0) or 0),
                "volatilidade_anualizada": float(data.get("volatilidade_12m", 0) or 0),
                "max_drawdown": float(data.get("max_drawdown", 0) or 0),
                "metricas_completas": data,
                "source": "mais_retorno",
            }
        ]
        return records
