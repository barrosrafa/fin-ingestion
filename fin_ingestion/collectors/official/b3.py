"""Coletor oficial B3: Cotações Históricas (COTAHIST), Instrumentos e Índices."""

import base64
import json
from datetime import datetime
from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class B3Collector(BaseCollector):
    """Coleta e processa dados públicos da B3 (Bovespa)."""

    def __init__(self) -> None:
        super().__init__(name="b3", base_url="https://sistemaswebb3-listados.b3.com.br")

    async def extract(self, **kwargs: Any) -> Any:
        endpoint = kwargs.get("endpoint", "index_portfolio")
        index_name = kwargs.get("index", "IBOV")

        if endpoint == "index_portfolio":
            encoded = base64.b64encode(json.dumps({
                "language": "pt-br", "pageNumber": 1, "pageSize": 1000,
                "index": index_name, "segment": "1",
            }).encode()).decode()
            response = await self.http_client.get(f"/indexProxy/indexCall/GetPortfolioDay/{encoded}")
            response.raise_for_status()
            payload = response.json()
            if not payload.get("results"):
                raise ValueError(f"Carteira vazia para o índice {index_name}")
            return payload
        elif endpoint == "cothist_line":
            # Permite parsing direto de linhas posicionais COTAHIST
            return kwargs.get("lines", [])

        raise ValueError(f"Endpoint não suportado para B3: {endpoint}")

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if isinstance(raw_data, dict) and "results" in raw_data:
            records = []
            for item in raw_data["results"]:
                records.append({
                    "ticker": item.get("codAtivo", item.get("cod")),
                    "nome_empresa": item.get("acao", item.get("asset")),
                    "tipo_acao": item.get("tipo", item.get("type")),
                    "quantidade_teorica": float(item.get("qtdeTeorica", item.get("theoricalQty", "0")).replace(".", "").replace(",", ".")),
                    "participacao_pct": float(item.get("partPct", item.get("part", "0")).replace(",", ".")),
                    "source": "b3_index_proxy",
                })
            return records

        if isinstance(raw_data, list):
            # Parse de lista de linhas brutas posicionais do arquivo COTAHIST
            return [self.parse_cothist_line(line) for line in raw_data if line.startswith("01")]

        return []

    @staticmethod
    def parse_cothist_line(line: str) -> dict[str, Any]:
        """Faz o parsing exato do padrão posicional da B3 (COTAHIST Registro tipo 01)."""
        data_pregao_str = line[2:10].strip()
        data_pregao = datetime.strptime(data_pregao_str, "%Y%m%d").date().isoformat()

        return {
            "tipo_registro": line[0:2],
            "data_pregao": data_pregao,
            "cod_bdi": line[10:12].strip(),
            "ticker": line[12:24].strip(),
            "tipo_mercado": int(line[24:27].strip() or 0),
            "nome_resumido": line[27:39].strip(),
            "especificacao": line[39:49].strip(),
            "prazo_dias_termo": line[49:52].strip(),
            "moeda_referencia": line[52:56].strip(),
            "preco_abertura": float(line[56:69]) / 100.0,
            "preco_maximo": float(line[69:82]) / 100.0,
            "preco_minimo": float(line[82:95]) / 100.0,
            "preco_medio": float(line[95:108]) / 100.0,
            "preco_fechamento": float(line[108:121]) / 100.0,
            "melhor_oferta_compra": float(line[121:134]) / 100.0,
            "melhor_oferta_venda": float(line[134:147]) / 100.0,
            "numero_negocios": int(line[147:152].strip() or 0),
            "quantidade_titulos": int(line[152:170].strip() or 0),
            "volume_financeiro": float(line[170:188]) / 100.0,
            "preco_exercicio_opcoes": float(line[188:201]) / 100.0,
            "indicador_correcao_exercicio": line[201:202],
            "data_vencimento_opcoes": line[202:210].strip(),
            "fator_cotacao": int(line[210:217].strip() or 1),
            "codigo_isin": line[230:242].strip(),
            "source": "b3_cothist",
        }
