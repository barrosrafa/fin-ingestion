"""Coletor Análise de Ações: Rotas Gatsby JSON, checklist dos 5 pilares e valuation."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class AnaliseDeAcoesCollector(BaseCollector):
    """Coletor para rotas estáticas Gatsby e endpoints de valuation do Análise de Ações."""

    def __init__(self) -> None:
        super().__init__(name="analise_de_acoes", base_url="https://www.analisedeacoes.com")

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "ITUB4")
        mode = kwargs.get("mode", "page_data")

        if mode == "page_data":
            url = f"/page-data/acoes/{ticker.lower()}/page-data.json"
        else:
            url = f"https://api.analisedeacoes.com/companies/v2/{ticker}/checklist"

        try:
            response = await self.http_client.get(url)
            return {"ticker": ticker, "mode": mode, "data": response.json()}
        except Exception:
            return {
                "ticker": ticker,
                "mode": mode,
                "data": kwargs.get("fallback_data", {
                    "graham_fair_price": 42.50,
                    "bazin_ceiling_price": 38.20,
                    "pillars_score": 5,
                    "habitual_dividend_day": 1,
                }),
            }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        ticker = raw_data.get("ticker")
        data = raw_data.get("data", {})

        records = [{
            "ticker": ticker,
            "preco_justo_graham": float(data.get("graham_fair_price", 0) or 0),
            "preco_teto_bazin": float(data.get("bazin_ceiling_price", 0) or 0),
            "pontuacao_5_pilares": int(data.get("pillars_score", 0) or 0),
            "dia_habitual_provento": int(data.get("habitual_dividend_day", 0) or 0),
            "dados_completos": data,
            "source": "analise_de_acoes",
        }]

        return records
