"""Resolve the official CKAN resource and preserve the original Treasury CSV."""

import csv
import io
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlsplit

from fin_ingestion.core.base_collector import BaseCollector


class TesouroCollector(BaseCollector):
    def __init__(self):
        super().__init__("tesouro", "https://www.tesourotransparente.gov.br", requests_per_second=1)

    async def extract(self, **kwargs):
        response = await self.http_client.get(
            "/ckan/api/3/action/package_show",
            params={"id": "taxas-dos-titulos-ofertados-pelo-tesouro-direto"},
        )
        response.raise_for_status()
        metadata = response.json()
        if not metadata.get("success"):
            raise ValueError("CKAN resource discovery failed")
        resources = [r for r in metadata["result"]["resources"] if r["format"].upper() == "CSV"]
        if len(resources) != 1:
            raise ValueError("CKAN Treasury CSV is ambiguous")
        url = resources[0]["url"]
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.hostname != "www.tesourotransparente.gov.br":
            raise ValueError("Unexpected Treasury resource host")
        response = await self.http_client.get(url)
        response.raise_for_status()
        return response.content

    def parse(self, raw_data):
        try:
            text = raw_data.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw_data.decode("latin1")
        records = []
        for row in csv.DictReader(io.StringIO(text), delimiter=";"):

            def number(field):
                value = row.get(field, "").strip()
                if not value:
                    return None
                parsed = Decimal(value.replace(".", "").replace(",", "."))
                if not parsed.is_finite():
                    raise ValueError("Non-finite Treasury value")
                return parsed

            records.append(
                {
                    "title": row["Tipo Titulo"],
                    "maturity": datetime.strptime(row["Data Vencimento"], "%d/%m/%Y").date(),
                    "reference_date": datetime.strptime(row["Data Base"], "%d/%m/%Y").date(),
                    "buy_rate": number("Taxa Compra Manha"),
                    "sell_rate": number("Taxa Venda Manha"),
                    "buy_price": number("PU Compra Manha"),
                    "sell_price": number("PU Venda Manha"),
                    "base_price": number("PU Base Manha"),
                    "source": "tesouro",
                }
            )
        return records
