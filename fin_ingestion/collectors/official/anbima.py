"""Coletor oficial ANBIMA: Títulos Públicos (TPF), Curva de Juros e Índices IMA."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class ANBIMACollector(BaseCollector):
    """Coleta dados de mercado secundário de renda fixa e taxas indicativas da ANBIMA."""

    def __init__(self) -> None:
        super().__init__(name="anbima", base_url="https://www.anbima.com.br")

    async def extract(self, **kwargs: Any) -> Any:
        # Suporta parsing de payload ou mock estruturado de arquivo msYYMMDD.txt
        return kwargs.get("data", [
            {
                "codigo_selic": "100000",
                "titulo": "LTN",
                "data_vencimento": "2027-01-01",
                "taxa_indicativa": 11.45,
                "pu_indicativo": 820.50,
                "intervalo_min": 11.40,
                "intervalo_max": 11.50,
            },
            {
                "codigo_selic": "216198",
                "titulo": "NTN-B",
                "data_vencimento": "2035-05-15",
                "taxa_indicativa": 6.35,
                "pu_indicativo": 4120.30,
                "intervalo_min": 6.30,
                "intervalo_max": 6.40,
            },
        ])

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
