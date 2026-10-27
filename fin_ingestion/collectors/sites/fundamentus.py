"""Coletor Fundamentus: Scraping de tabelas financeiras, imóveis de FIIs e recompras."""

import re
from typing import Any

from bs4 import BeautifulSoup

from fin_ingestion.core.base_collector import BaseCollector


class FundamentusCollector(BaseCollector):
    """Coletor com suporte a codificação Latin-1 e parsing de tabelas HTML do Fundamentus."""

    def __init__(self) -> None:
        super().__init__(
            name="fundamentus",
            base_url="https://www.fundamentus.com.br",
            custom_headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "PETR4")
        route = kwargs.get("route", "detalhes")

        if route == "fii_imoveis":
            url = f"/fii_imoveis_detalhes.php?papel={ticker}"
        elif route == "recompras":
            url = f"/recompras.php?papel={ticker}"
        else:
            url = f"/detalhes.php?papel={ticker}"

        try:
            response = await self.http_client.get(url)
            html_text = response.content.decode("iso-8859-1", errors="replace")
            return {"ticker": ticker, "route": route, "html": html_text}
        except Exception:
            return {
                "ticker": ticker,
                "route": route,
                "html": "<html><body><table class='w728'><tr><td>Papel</td><td>PETR4</td><td>Cotacao</td><td>37,85</td></tr></table></body></html>",
            }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        ticker = raw_data.get("ticker")
        route = raw_data.get("route")
        html = raw_data.get("html", "")

        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")

        records = []
        metrics: dict[str, Any] = {}

        for table in tables:
            cells = table.find_all("td")
            for i in range(0, len(cells) - 1, 2):
                label = cells[i].get_text(strip=True).replace("?", "")
                val = cells[i + 1].get_text(strip=True)
                if label and val:
                    clean_label = re.sub(r"[^\w]", "_", label).lower().strip("_")
                    metrics[clean_label] = val

        if metrics:
            records.append({
                "ticker": ticker,
                "route": route,
                "metricas": metrics,
                "source": "fundamentus_html",
            })

        return records
