"""Coletor Status Invest: Cotações XHR, Balanços DRE/DFC, BTC e CRIs de FIIs."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class StatusInvestCollector(BaseCollector):
    """Coletor com emulação de sessão para a plataforma Status Invest."""

    def __init__(self) -> None:
        super().__init__(
            name="status_invest",
            base_url="https://statusinvest.com.br",
            custom_headers={
                "Referer": "https://statusinvest.com.br/",
                "X-Requested-With": "XMLHttpRequest",
            },
        )

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "PETR4")
        category = kwargs.get("category", "acao")  # acao, fii, fundoinvestimento

        if category == "acao":
            url = f"/acao/tickerprice?ticker={ticker}&type=4"
        elif category == "fii_cri":
            url = f"/fii/cridetail/{ticker}"
        elif category == "btc":
            url = f"/category/borrowhistory?ticker={ticker}"
        else:
            url = f"/{category}/tickerprice?ticker={ticker}&type=4"

        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "category": category, "data": response.json()}
        except Exception:
            # Fixtures de fallback permitidas estritamente se passadas explicitamente em testes
            if "fallback_data" in kwargs:
                return {
                    "ticker": ticker,
                    "category": category,
                    "data": kwargs["fallback_data"],
                }
            # Em produção, nunca fabricar dados fictícios
            raise

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        ticker = raw_data.get("ticker")
        category = raw_data.get("category")
        items = raw_data.get("data", [])

        if not isinstance(items, list):
            items = [items]

        records = []
        for item in items:
            records.append({
                "ticker": ticker,
                "category": category,
                "data_referencia": item.get("date") or item.get("Date"),
                "valor": item.get("value") or item.get("Value"),
                "detalhes": item,
                "source": "status_invest",
            })
        return records
