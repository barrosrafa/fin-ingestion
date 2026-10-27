"""Coletor oficial BCB: Séries Temporais SGS (Selic, IPCA, CDI), PTAX e Focus."""

from datetime import datetime
from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class BCBCollector(BaseCollector):
    """Coleta indicadores macroeconômicos oficiais do Banco Central do Brasil."""

    SERIES_MAP = {
        "selic_over": 11,
        "meta_selic": 432,
        "ipca": 433,
        "igpm": 189,
        "cdi": 12,
        "tr": 226,
    }

    def __init__(self) -> None:
        super().__init__(name="bcb", base_url="https://api.bcb.gov.br")

    async def extract(self, **kwargs: Any) -> Any:
        serie_name = kwargs.get("serie", "selic_over")
        serie_id = self.SERIES_MAP.get(serie_name, kwargs.get("serie_id", 11))
        ultimos = kwargs.get("ultimos", 30)

        url = f"/dados/serie/bcdata.sgs.{serie_id}/dados/ultimos/{ultimos}?formato=json"
        response = await self.http_client.get(url)
        return {"serie_name": serie_name, "serie_id": serie_id, "data": response.json()}

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict) or "data" not in raw_data:
            return []

        serie_name = raw_data.get("serie_name")
        serie_id = raw_data.get("serie_id")
        items = raw_data.get("data", [])

        if not isinstance(items, list):
            return []

        records = []
        for item in items:
            if not isinstance(item, dict):
                continue
            data_str = item.get("data")
            valor_str = item.get("valor")

            if data_str and valor_str is not None:
                try:
                    dt = datetime.strptime(data_str, "%d/%m/%Y").date().isoformat()
                    records.append({
                        "data": dt,
                        "serie_nome": serie_name,
                        "serie_id": serie_id,
                        "valor": float(str(valor_str).replace(",", ".")),
                        "source": "bcb_sgs",
                    })
                except (ValueError, TypeError):
                    continue

        return records
