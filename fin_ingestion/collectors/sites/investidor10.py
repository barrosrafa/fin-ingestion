"""Coletor Investidor10: Histórico de múltiplos, balanços, mapas de proventos e checklists."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class Investidor10Collector(BaseCollector):
    """Coletor para APIs REST e dados estruturados do Investidor10."""

    def __init__(self) -> None:
        super().__init__(
            name="investidor10",
            base_url="https://investidor10.com.br",
            custom_headers={"X-Requested-With": "XMLHttpRequest"},
        )

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "PETR4")
        endpoint = kwargs.get("endpoint", "indicadores")
        if endpoint == "indicadores":
            url = f"/api/historico-indicadores/{ticker}/anual/"
        elif endpoint == "balancos":
            url = f"/api/balancos/receitas/{ticker}/anual/"
        elif endpoint == "dividendos":
            url = f"/api/acoes/dividends-map/{ticker}/"
        else:
            url = f"/api/{endpoint}/{ticker}/"
        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "endpoint": endpoint, "data": response.json()}
        except Exception:
            if "fallback_data" not in kwargs:
                raise
            return {"ticker": ticker, "endpoint": endpoint, "data": kwargs["fallback_data"]}

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []
        ticker = raw_data.get("ticker")
        endpoint = raw_data.get("endpoint")
        data = raw_data.get("data", {})
        records = []
        if isinstance(data, dict):
            if not any(key in data for key in ("pl", "pvp", "roe", "dy", "ev_ebitda")):
                return []
            pl_val = float(data.get("pl", 0) or 0)
            pvp_val = float(data.get("pvp", 0) or 0)
            roe_val = float(data.get("roe", 0) or 0)
            dy_val = float(data.get("dy", 0) or 0)
            ev_ebitda_val = float(data.get("ev_ebitda", 0) or 0)
            records.append(
                {
                    "ticker": ticker,
                    "endpoint": endpoint,
                    "pl": pl_val,
                    "preco_lucro": pl_val,
                    "pvp": pvp_val,
                    "preco_vpa": pvp_val,
                    "roe": roe_val,
                    "dividend_yield": dy_val,
                    "ev_ebitda": ev_ebitda_val,
                    "dados_completos": data,
                    "source": "investidor10",
                }
            )
        elif isinstance(data, list):
            for entry in data:
                records.append(
                    {
                        "ticker": ticker,
                        "endpoint": endpoint,
                        "ano": entry.get("year") or entry.get("ano"),
                        "dados": entry,
                        "source": "investidor10",
                    }
                )
        return records
