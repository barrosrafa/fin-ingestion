"""Coletor FIIs.com.br: Agenda de proventos com competência mensal e screening."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class FIISComBrCollector(BaseCollector):
    """Coletor com foco na agenda de dividendos e competência contábil mensal do FIIs.com.br."""

    def __init__(self) -> None:
        super().__init__(name="fiis_com_br", base_url="https://fiis.com.br")

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "KNIP11")
        url = f"/{ticker}/"
        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "html": response.text}
        except Exception:
            if "fallback_data" not in kwargs:
                raise
            return {"ticker": ticker, "data": kwargs["fallback_data"]}

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []
        ticker = raw_data.get("ticker")
        fallback_data = raw_data.get("data", [])
        if fallback_data:
            return [
                {
                    "ticker": ticker,
                    "mes_referencia": item.get("mes_referencia"),
                    "data_com": item.get("data_com"),
                    "data_pagamento": item.get("data_pagamento"),
                    "valor_provento": float(item.get("rendimento", 0)),
                    "source": "fiis_com_br",
                }
                for item in fallback_data
            ]
        raise ValueError("Agenda estruturada de proventos não encontrada na fonte")
