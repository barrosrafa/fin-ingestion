"""Coletor oficial B3: Cotações Históricas (COTAHIST), Instrumentos e Índices."""

import base64
import io
import json
import zipfile
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from fin_ingestion.core.base_collector import BaseCollector


class B3Collector(BaseCollector):
    """Coleta e processa dados públicos da B3 (Bovespa)."""

    def __init__(self) -> None:
        super().__init__(name="b3", base_url="https://sistemaswebb3-listados.b3.com.br")
        self.recent_daily_quotes = []

    async def get_daily_quotes(self, reference_date):
        url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D" + reference_date.strftime("%d%m%Y") + ".ZIP"
        response = await self.http_client.get(url)
        response.raise_for_status()
        await self.raw_sink.save_payload(self.name, url, response.content, mime_type="application/zip")
        records = [r for r in self.parse(response.content) if r["tipo_mercado"] == 10]
        if any(r["data_pregao"] != reference_date.isoformat() for r in records):
            raise ValueError("COTAHIST returned a different session date")
        return records

    async def get_recent_traded_tickers(self) -> list[str]:
        """Universo spot negociado no último arquivo diário publicado pela B3."""
        today = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
        for offset in range(0, 11):
            day = today - timedelta(days=offset)
            if day.weekday() >= 5:
                continue
            url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D" + day.strftime("%d%m%Y") + ".ZIP"
            response = await self.http_client.get(url)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            if not response.content.startswith(b"PK"):
                continue
            await self.raw_sink.save_payload(
                source=self.name, endpoint=url, payload=response.content,
                mime_type="application/zip", metadata={"data_pregao": day.isoformat()},
            )
            self.recent_daily_quotes = [q for q in self.parse(response.content) if q["tipo_mercado"] == 10]
            for quote in self.recent_daily_quotes:
                factor = quote["fator_cotacao"]
                if factor <= 0:
                    raise ValueError("COTAHIST quotation factor must be positive")
                if factor != 1:
                    for key in ("preco_abertura", "preco_maximo", "preco_minimo", "preco_medio", "preco_fechamento"):
                        quote[key] /= factor
            tickers = {q["ticker"] for q in self.recent_daily_quotes}
            if tickers:
                return sorted(tickers)
        raise RuntimeError("Nenhum arquivo COTAHIST diário disponível nos últimos dez dias")

    async def extract(self, **kwargs: Any) -> Any:
        endpoint = kwargs.get("endpoint", "index_portfolio")
        index_name = kwargs.get("index", "IBOV")

        if endpoint == "cotahist":
            year = int(kwargs["year"])
            if not 1986 <= year <= datetime.now(ZoneInfo("America/Sao_Paulo")).year:
                raise ValueError("Invalid annual COTAHIST year")
            url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{year}.ZIP"
            response = await self.http_client.get(url)
            response.raise_for_status()
            return response.content

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
        if isinstance(raw_data, bytes):
            if raw_data.startswith(b"PK"):
                with zipfile.ZipFile(io.BytesIO(raw_data)) as archive:
                    if sum(info.file_size for info in archive.infolist()) > 512 * 1024 * 1024:
                        raise ValueError("COTAHIST uncompressed size exceeds 512 MiB")
                    records = []
                    for info in archive.infolist():
                        if info.filename.lower().endswith(".txt"):
                            with archive.open(info) as stream:
                                for line in stream:
                                    if line.startswith(b"01"):
                                        records.append(self.parse_cothist_line(line.decode("latin1").rstrip("\r\n")))
                    return records
            return self.parse(raw_data.decode("latin1").splitlines())
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
        if len(line) != 245 or not line.startswith("01"):
            raise ValueError("COTAHIST requires a 245-character type-01 record")
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
