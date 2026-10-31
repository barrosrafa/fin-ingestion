"""Coletor oficial BCB: Séries Temporais SGS (Selic, IPCA, CDI), PTAX e Focus."""

import math
from datetime import date, datetime, timedelta
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
        # Oceans14 Macro, Credit & Government Accounts Series
        "poupanca_saldo": 1832,
        "poupanca_captacao": 2006,
        "inadimplencia_pf": 21082,
        "inadimplencia_pj": 21085,
        "inadimplencia_total": 21084,
        "credito_pj": 20539,
        "credito_pf": 20540,
        "credito_imobiliario": 20541,
        "tcr_efetiva": 11752,
        "resultado_primario_governo": 4447,
        "receita_liquida_governo": 4448,
        "despesa_total_governo": 4449,
        "divida_bruta_pib": 4505,
    }

    def __init__(self) -> None:
        super().__init__(name="bcb", base_url="https://api.bcb.gov.br")

    async def extract(self, **kwargs: Any) -> Any:
        serie_name = kwargs.get("serie", "selic_over")
        serie_id = kwargs.get("serie_id") or self.SERIES_MAP.get(serie_name)
        if not isinstance(serie_id, int) or isinstance(serie_id, bool) or serie_id <= 0:
            raise ValueError("Informe uma série SGS conhecida ou serie_id inteiro positivo")
        start, end = kwargs.get("start_date"), kwargs.get("end_date")
        if start is not None or end is not None:
            if start is None or end is None:
                raise ValueError("start_date e end_date devem ser informadas juntas")
            start = date.fromisoformat(start) if isinstance(start, str) else start
            end = date.fromisoformat(end) if isinstance(end, str) else end
            if type(start) is not date or type(end) is not date or start > end:
                raise ValueError("Intervalo SGS inválido; use datas ISO sem horário")
            items = {}
            for first, last in self.date_windows(start, end):
                response = await self.http_client.get(
                    f"/dados/serie/bcdata.sgs.{serie_id}/dados",
                    params={"formato": "json", "dataInicial": first.strftime("%d/%m/%Y"),
                            "dataFinal": last.strftime("%d/%m/%Y")},
                )
                response.raise_for_status()
                await self.raw_sink.save_payload(
                    source=self.name, endpoint=f"sgs/{serie_id}", payload=response.content,
                    metadata={"serie_id": serie_id, "start_date": first, "end_date": last},
                )
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("Resposta SGS não é uma lista")
                for item in payload:
                    day = datetime.strptime(item["data"], "%d/%m/%Y").date()
                    if first <= day <= last:
                        items[day] = item
            return {"serie_name": serie_name, "serie_id": serie_id,
                    "data": [items[day] for day in sorted(items)]}
        ultimos = kwargs.get("ultimos", 30)
        if not isinstance(ultimos, int) or isinstance(ultimos, bool) or ultimos <= 0:
            raise ValueError("ultimos deve ser inteiro positivo")

        url = f"/dados/serie/bcdata.sgs.{serie_id}/dados/ultimos/{ultimos}?formato=json"
        response = await self.http_client.get(url)
        response.raise_for_status()
        return {"serie_name": serie_name, "serie_id": serie_id, "data": response.json()}

    @staticmethod
    def date_windows(start: date, end: date):
        """Inclusive non-overlapping windows below the SGS ten-year limit, including leap days."""
        current = start
        while current <= end:
            last = min(end, current + timedelta(days=3 * 365 - 1))
            yield current, last
            if last == end:
                break
            current = last + timedelta(days=1)

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
                    value = float(str(valor_str).replace(",", "."))
                    if not math.isfinite(value):
                        raise ValueError("Valor SGS não finito")
                    records.append({
                        "data": dt,
                        "serie_nome": serie_name,
                        "serie_id": serie_id,
                        "valor": value,
                        "source": "bcb_sgs",
                    })
                except (ValueError, TypeError):
                    continue

        return records
