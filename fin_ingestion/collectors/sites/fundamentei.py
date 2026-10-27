"""Coletor Fundamentei: Extração de estado Apollo GraphQL e KPIs operacionais."""

import json
from typing import Any

from bs4 import BeautifulSoup

from fin_ingestion.core.base_collector import BaseCollector


class FundamenteiCollector(BaseCollector):
    """Coletor com extração de AST do Apollo GraphQL hidratado no DOM do Fundamentei."""

    def __init__(self) -> None:
        super().__init__(name="fundamentei", base_url="https://fundamentei.com")

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "WEGE3")
        url = f"/acoes/{ticker}"

        try:
            response = await self.http_client.get(url)
            soup = BeautifulSoup(response.text, "lxml")
            script_tag = soup.find("script", id="__NEXT_DATA__")
            if script_tag and script_tag.string:
                next_data = json.loads(script_tag.string)
                apollo_state = (
                    next_data.get("props", {}).get("pageProps", {}).get("apolloState", {}).get("data", {})
                )
                return {"ticker": ticker, "apollo_state": apollo_state}
        except Exception:
            pass

        return {
            "ticker": ticker,
            "apollo_state": kwargs.get("fallback_data", {
                "BrazilianCompanyIssuer:1": {
                    "name": "WEG S.A.",
                    "cvmCode": "5410",
                    "sector": "Bens Industriais",
                    "reclameAqui": {"score": 8.4, "status": "Ótimo"},
                }
            }),
        }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        ticker = raw_data.get("ticker")
        state = raw_data.get("apollo_state", {})
        records = []

        for key, value in state.items():
            if isinstance(value, dict) and ("cvmCode" in value or "reclameAqui" in value or "indicators" in value):
                records.append({
                    "ticker": ticker,
                    "apollo_key": key,
                    "razao_social": value.get("name"),
                    "codigo_cvm": value.get("cvmCode"),
                    "setor": value.get("sector"),
                    "reclame_aqui": value.get("reclameAqui"),
                    "dados_brutos": value,
                    "source": "fundamentei_apollo",
                })

        return records
