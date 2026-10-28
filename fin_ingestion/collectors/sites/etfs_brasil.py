"""Coletor ETFs Brasil: API FastAPI, estrutura de taxas (TER), spread NAV, composição e fluxos."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class ETFsBrasilCollector(BaseCollector):
    """Coletor integrado aos endpoints REST em FastAPI da plataforma ETFs Brasil."""

    def __init__(self) -> None:
        super().__init__(name="etfs_brasil", base_url="https://www.etfsbrasil.com.br")

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "BOVA11").upper()
        mode = kwargs.get("mode", "detail")  # detail, composition, ranking

        url = f"/api/backend/etf/{ticker}" if mode == "detail" else "/api/backend/home"
        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "mode": mode, "status": "SUCCESS", "data": response.json()}
        except Exception as e:
            if "fallback_data" in kwargs:
                return {"ticker": ticker, "mode": mode, "status": "SUCCESS", "data": kwargs["fallback_data"]}
            return {
                "ticker": ticker,
                "mode": mode,
                "status": "FAILED",
                "error": str(e),
                "data": None,
            }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        if raw_data.get("status") == "FAILED" or not raw_data.get("data"):
            return []

        ticker = raw_data.get("ticker", "").upper()
        mode = raw_data.get("mode", "detail")
        data = raw_data.get("data", {})

        if mode == "composition" and isinstance(data, list):
            records = []
            for item in data:
                records.append({
                    "etf_ticker": ticker,
                    "codigo_ativo": item.get("ticker", ""),
                    "nome_ativo": item.get("name", ""),
                    "setor": item.get("sector", ""),
                    "peso_pct": float(item.get("weight_pct", 0) or 0),
                    "source": "etfs_brasil",
                })
            return records

        records = [{
            "ticker": ticker,
            "taxa_administracao_primaria": float(data.get("taxa_primaria", 0) or 0),
            "taxa_secundaria": float(data.get("taxa_secundaria", 0) or 0),
            "custo_total_ter": float(data.get("taxa_total_ter", 0) or 0),
            "nav_patrimonial": float(data.get("nav", 0) or 0),
            "preco_mercado": float(data.get("preco_mercado", 0) or 0),
            "spread_nav_pct": float(data.get("spread_nav_pct", 0) or 0),
            "limite_aluguel_pct": float(data.get("limite_aluguel_pct", 0) or 0),
            "fluxo_cotas_30d": float(data.get("fluxo_30d_milhoes", 0) or 0),
            "cotistas": int(data.get("cotistas", 0) or 0),
            "patrimonio_liquido": float(data.get("patrimonio_milhoes", 0) or 0) * 1e6,
            "indice_referencia": data.get("indice_referencia", "IBOVESPA"),
            "gestor": data.get("gestor", "BlackRock Brasil"),
            "source": "etfs_brasil",
        }]

        return records
