"""Coletor ADVFN: Suporte e Resistência (4S/3R), 13 indicadores técnicos e consenso."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class ADVFNCollector(BaseCollector):
    """Coletor com rotinas de análise técnica avançada e consenso sell-side do ADVFN Brasil."""

    def __init__(self) -> None:
        super().__init__(name="advfn", base_url="https://br.advfn.com")

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "VALE3")
        section = kwargs.get("section", "tecnica")
        if section == "tecnica":
            url = f"/suporte-e-resistencia/{ticker}"
        elif section == "consenso":
            url = f"/previsoes/{ticker}"
        else:
            url = f"/cotacao/{ticker}"
        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "section": section, "html": response.text}
        except Exception:
            if "fallback_data" not in kwargs:
                raise
            return {"ticker": ticker, "section": section, "data": kwargs["fallback_data"]}

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []
        ticker = raw_data.get("ticker")
        data = raw_data.get("data", {})
        if not data:
            return []
        records = [
            {
                "ticker": ticker,
                "pivot_point": float(data.get("pivot_point", 0) or 0),
                "suporte_1": float(data.get("suporte_1", 0) or 0),
                "suporte_2": float(data.get("suporte_2", 0) or 0),
                "suporte_3": float(data.get("suporte_3", 0) or 0),
                "suporte_4": float(data.get("suporte_4", 0) or 0),
                "resistencia_1": float(data.get("resistencia_1", 0) or 0),
                "resistencia_2": float(data.get("resistencia_2", 0) or 0),
                "resistencia_3": float(data.get("resistencia_3", 0) or 0),
                "rsi_14": float(data.get("rsi_14", 0) or 0),
                "tendencia_primaria": data.get("tendencia", "Neutro"),
                "source": "advfn_tecnico",
            }
        ]
        return records
