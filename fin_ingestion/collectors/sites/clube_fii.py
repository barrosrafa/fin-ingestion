"""Coletor Clube FII: CR Data GraphQL API, transações SiiLA e imóveis físicos."""

from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class ClubeFIICollector(BaseCollector):
    """Coletor para a API GraphQL CR Data e serviços de inteligência imobiliária do Clube FII."""

    def __init__(self) -> None:
        super().__init__(
            name="clube_fii",
            base_url="https://api.clubefii.dev.br",
            custom_headers={"Origin": "https://www.clubefii.com.br"},
        )

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "HGLG11")
        query = "\n        query GetFiiPortfolio($ticker: String!) {\n          fundo(ticker: $ticker) {\n            cnpj\n            razaoSocial\n            gestora\n            tipoGestao\n            patrimonioLiquido\n            vacanciaFisica\n            vacanciaFinanceira\n            imoveis {\n              nome\n              cidade\n              uf\n              abl\n            }\n          }\n        }\n        "
        payload = {"query": query, "variables": {"ticker": ticker}}
        try:
            response = await self.http_client.post("/graphql", json=payload)
            return {"ticker": ticker, "data": response.json()}
        except Exception:
            if "fallback_data" not in kwargs:
                raise
            return {"ticker": ticker, "data": kwargs["fallback_data"]}

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []
        ticker = raw_data.get("ticker")
        data = raw_data.get("data", {}).get("data", {}).get("fundo", {})
        if not data:
            return []
        imoveis = data.get("imoveis", [])
        records = [
            {
                "ticker": ticker,
                "cnpj": data.get("cnpj"),
                "razao_social": data.get("razaoSocial"),
                "gestora": data.get("gestora"),
                "patrimonio_liquido": data.get("patrimonioLiquido"),
                "vacancia_fisica_pct": data.get("vacanciaFisica"),
                "vacancia_financeira_pct": data.get("vacanciaFinanceira"),
                "total_imoveis": len(imoveis),
                "imoveis": imoveis,
                "source": "clube_fii_graphql",
            }
        ]
        return records
