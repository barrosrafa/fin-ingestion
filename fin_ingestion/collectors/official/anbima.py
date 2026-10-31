"""Coletor oficial ANBIMA: Títulos Públicos (TPF), Curva de Juros e Índices IMA."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class ANBIMACollector(BaseCollector):
    """Coleta dados de mercado secundário de renda fixa e taxas indicativas da ANBIMA."""

    def __init__(self) -> None:
        super().__init__(name="anbima", base_url="https://www.anbima.com.br")

    async def extract(self, **kwargs: Any) -> Any:
        # Explicit offline payloads are supported; never fabricate a live market result.
        if "data" not in kwargs:
            raise NotImplementedError("ANBIMA Feed requer integração autenticada; forneça data para parsing offline")
        return kwargs["data"]

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, list):
            return []

        records = []
        for item in raw_data:
            records.append({
                "codigo_selic": item.get("codigo_selic"),
                "tipo_titulo": item.get("titulo"),
                "data_vencimento": item.get("data_vencimento"),
                "taxa_indicativa": float(item.get("taxa_indicativa", 0)),
                "pu_indicativo": float(item.get("pu_indicativo", 0)),
                "taxa_minima": float(item.get("intervalo_min", 0)),
                "taxa_maxima": float(item.get("intervalo_max", 0)),
                "source": "anbima_mercado_secundario",
            })
        return records
