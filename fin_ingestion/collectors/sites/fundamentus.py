"""Coletor Fundamentus Avançado: Engenharia Reversa Completa de Ações e FIIs.

Cobre 13 rotas do ecossistema Fundamentus:
1. Screener Geral de Ações (/resultado.php) — O(1) para ~1.000 papéis B3
2. Screener Geral de FIIs (/fii_resultado.php) — O(1) para ~600 fundos
3. Portfólio de Imóveis FIIs (/fii_imoveis.php) — ~1.750 ativos físicos
4. Fatos Relevantes & Comunicados CVM (/fr.php)
5. Monitor de Entrega de Balanços ITR/DFP (/ultimos-resultados.php)
6. Séries Históricas Diárias JSON (/amline/cot_hist.php)
7. Detalhes de Ações com Balanço e DRE 12m vs 3m (/detalhes.php)
8. Detalhes de FIIs (/detalhes.php?h=1)
9. Proventos de FIIs (/fii_proventos.php)
10. Administradores de FIIs (/fii_administrador.php)
11. Negociação de Insiders Art. 11 CVM 44 (/insiders.php)
12. Programas de Recompra de Ações (/recompras.php)
13. Histórico de Proventos de Ações (/proventos.php)
"""

import re
import unicodedata
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

from fin_ingestion.core.base_collector import BaseCollector


class FundamentusCollector(BaseCollector):
    """Coletor universal para todo o ecossistema Fundamentus com decodificação ISO-8859-1."""

    def __init__(self) -> None:
        super().__init__(
            name="fundamentus",
            base_url="https://www.fundamentus.com.br",
            custom_headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )

    @staticmethod
    def parse_br_number(val: Any) -> float | None:
        """Converte números formatados em português brasileiro (ex: '1.586.020,00', '22,4%') para float."""
        if val is None:
            return None
        text = str(val).strip()
        if not text or text in ("-", "--", "N/A", "null"):
            return None
        clean = text.replace(".", "").replace(",", ".")
        is_pct = "%" in clean
        clean = clean.replace("%", "").strip()
        try:
            num = float(clean)
            return round(num / 100.0, 6) if is_pct else num
        except ValueError:
            return None

    @staticmethod
    def parse_br_int(val: Any) -> int | None:
        """Converte strings inteiras com pontos para int."""
        if val is None:
            return None
        text = str(val).strip()
        if not text or text in ("-", "--", "N/A", "null"):
            return None
        clean = text.replace(".", "").strip()
        try:
            return int(clean)
        except ValueError:
            return None

    @staticmethod
    def clean_key(text: str) -> str:
        """Normaliza rótulos de tabelas para formato snake_case limpo e sem acentos."""
        norm = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")
        clean = re.sub(r"[^\w\s]", "", norm).strip().lower()
        return re.sub(r"\s+", "_", clean)

    async def extract(self, **kwargs: Any) -> Any:
        ticker = kwargs.get("ticker", "ITUB4").upper()
        route = kwargs.get("route", "detalhes")

        headers: dict[str, str] = {}
        if route == "screener_acoes":
            url = "/resultado.php"
        elif route == "screener_fiis":
            url = "/fii_resultado.php"
        elif route == "fii_imoveis":
            url = "/fii_imoveis.php"
        elif route == "fatos_relevantes":
            url = "/fr.php"
        elif route == "ultimos_resultados":
            url = "/ultimos-resultados.php"
        elif route == "cotacoes_historicas":
            url = f"/amline/cot_hist.php?papel={ticker}"
            headers["Referer"] = f"https://www.fundamentus.com.br/cotacoes.php?papel={ticker}&t=f"
        elif route == "detalhes_fii":
            url = f"/detalhes.php?papel={ticker}&h=1"
        elif route == "fii_proventos":
            url = f"/fii_proventos.php?papel={ticker}"
        elif route == "fii_administrador":
            url = f"/fii_administrador.php?papel={ticker}"
        elif route == "insiders":
            url = f"/insiders.php?papel={ticker}&tipo=1"
        elif route == "recompras":
            url = f"/recompras.php?papel={ticker}&tipo=1"
        elif route == "proventos":
            url = f"/proventos.php?papel={ticker}&tipo=2"
        else:  # detalhes de ações padrão
            url = f"/detalhes.php?papel={ticker}"

        try:
            client = await self.http_client.get_client()
            response = await client.get(url, headers=headers)
            response.raise_for_status()

            if route == "cotacoes_historicas":
                # Resposta JSON nativa
                return {
                    "ticker": ticker,
                    "route": route,
                    "is_json": True,
                    "content": response.json(),
                }

            html_text = response.content.decode("iso-8859-1", errors="replace")
            return {
                "ticker": ticker,
                "route": route,
                "is_json": False,
                "html": html_text,
            }
        except Exception as e:
            return {
                "ticker": ticker,
                "route": route,
                "is_json": False,
                "error": str(e),
                "html": kwargs.get("fallback_html", ""),
            }

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_data, dict):
            return []

        ticker = raw_data.get("ticker", "")
        route = raw_data.get("route", "")

        # 1. Rota de Cotações Históricas (JSON)
        if raw_data.get("is_json"):
            series = raw_data.get("content", [])
            records = []
            for item in series:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    ts_ms = item[0]
                    price = float(item[1])
                    dt_str = datetime.fromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d")
                    records.append({
                        "ticker": ticker,
                        "data_pregao": dt_str,
                        "timestamp_ms": ts_ms,
                        "preco_fechamento_ajustado": price,
                        "source": "fundamentus_amline_json",
                    })
            return records

        html = raw_data.get("html", "")
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")

        if route == "screener_acoes":
            return self._parse_screener_acoes(tables)
        elif route == "screener_fiis":
            return self._parse_screener_fiis(tables)
        elif route == "fii_imoveis":
            return self._parse_fii_imoveis(tables)
        elif route == "fatos_relevantes":
            return self._parse_fatos_relevantes(tables)
        elif route == "ultimos_resultados":
            return self._parse_ultimos_resultados(tables)
        elif route == "detalhes_fii":
            return self._parse_detalhes_fii(ticker, tables)
        elif route == "fii_proventos":
            return self._parse_proventos_fii(ticker, tables)
        elif route == "fii_administrador":
            return self._parse_fii_administrador(ticker, tables)
        elif route == "insiders":
            return self._parse_insiders(ticker, tables)
        elif route == "recompras":
            return self._parse_recompras(ticker, tables)
        elif route == "proventos":
            return self._parse_proventos_acoes(ticker, tables)
        else:
            return self._parse_detalhes_acao(ticker, tables)

    # =========================================================================
    # PARSERS ESPECÍFICOS POR ROTA
    # =========================================================================

    def _parse_screener_acoes(self, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/resultado.php (Universo B3 em 1 call)."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 20:
                t = cols[0]
                records.append({
                    "ticker": t,
                    "cotacao": self.parse_br_number(cols[1]),
                    "pl": self.parse_br_number(cols[2]),
                    "pvp": self.parse_br_number(cols[3]),
                    "psr": self.parse_br_number(cols[4]),
                    "dividend_yield": self.parse_br_number(cols[5]),
                    "p_ativo": self.parse_br_number(cols[6]),
                    "p_cap_giro": self.parse_br_number(cols[7]),
                    "p_ebit": self.parse_br_number(cols[8]),
                    "p_ativo_circ_liq": self.parse_br_number(cols[9]),
                    "ev_ebit": self.parse_br_number(cols[10]),
                    "ev_ebitda": self.parse_br_number(cols[11]),
                    "margem_bruta": self.parse_br_number(cols[12]),
                    "margem_ebit": self.parse_br_number(cols[13]),
                    "margem_liquida": self.parse_br_number(cols[14]),
                    "liquidez_corrente": self.parse_br_number(cols[15]),
                    "roic": self.parse_br_number(cols[16]),
                    "roe": self.parse_br_number(cols[17]),
                    "liquidez_2m": self.parse_br_number(cols[18]),
                    "patrimonio_liquido": self.parse_br_number(cols[19]),
                    "divida_liquida_patrimonio": self.parse_br_number(cols[20]) if len(cols) > 20 else None,
                    "cagr_receita_5a": self.parse_br_number(cols[21]) if len(cols) > 21 else None,
                    "source": "fundamentus_screener_acoes",
                })
        return records

    def _parse_screener_fiis(self, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/fii_resultado.php."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 13:
                records.append({
                    "ticker": cols[0],
                    "segmento": cols[1],
                    "cotacao": self.parse_br_number(cols[2]),
                    "ffo_yield": self.parse_br_number(cols[3]),
                    "dividend_yield": self.parse_br_number(cols[4]),
                    "pvp": self.parse_br_number(cols[5]),
                    "valor_mercado": self.parse_br_number(cols[6]),
                    "liquidez": self.parse_br_number(cols[7]),
                    "qtd_imoveis": self.parse_br_int(cols[8]),
                    "preco_m2": self.parse_br_number(cols[9]),
                    "aluguel_m2": self.parse_br_number(cols[10]),
                    "cap_rate": self.parse_br_number(cols[11]),
                    "vacancia_media": self.parse_br_number(cols[12]),
                    "source": "fundamentus_screener_fiis",
                })
        return records

    def _parse_fii_imoveis(self, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/fii_imoveis.php (~1.750 imóveis físicos)."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 3:
                records.append({
                    "ticker": cols[0],
                    "nome_imovel": cols[1],
                    "endereco": cols[2],
                    "caracteristicas": cols[3] if len(cols) > 3 else "",
                    "source": "fundamentus_fii_imoveis",
                })
        return records

    def _parse_fatos_relevantes(self, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/fr.php (Feed de Comunicados e Fatos CVM)."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            links = [a.get("href") for a in row.find_all("a") if a.get("href")]
            if len(cols) >= 4:
                download_url = links[0] if links else None
                ticker_analisar = cols[-1] if len(cols) >= 7 else None
                records.append({
                    "data_divulgacao": cols[0],
                    "empresa": cols[1],
                    "tipo_documento": cols[2],  # 'FR' ou 'CO'
                    "descricao": cols[3],
                    "download_cvm_url": download_url,
                    "ticker": ticker_analisar,
                    "source": "fundamentus_fr_cvm",
                })
        return records

    def _parse_ultimos_resultados(self, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/ultimos-resultados.php."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 6:
                records.append({
                    "empresa": cols[0],
                    "data_referencia": cols[1],
                    "data_entrega_cvm": cols[4],
                    "ja_processado": cols[5].lower() == "sim",
                    "ticker": cols[6] if len(cols) > 6 else cols[0],
                    "source": "fundamentus_ultimos_resultados",
                })
        return records

    def _parse_detalhes_acao(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse da página completa de detalhes de ações (/detalhes.php?papel=X)."""
        if len(tables) < 5:
            return []

        t1 = self._table_to_dict(tables[0])
        t2 = self._table_to_dict(tables[1])
        t3 = self._table_to_dict(tables[2])
        t4 = self._table_to_dict(tables[3])

        setor = t1.get("setor", "")
        subsetor = t1.get("subsetor", "")
        is_bank = "intermediarios financeiros" in setor.lower() or "bancos" in subsetor.lower()

        header = {
            "ticker": ticker,
            "classe_acao": t1.get("tipo"),
            "empresa": t1.get("empresa"),
            "setor": setor,
            "subsetor": subsetor,
            "cotacao": self.parse_br_number(t1.get("cotacao")),
            "data_ultima_cotacao": t1.get("data_ult_cot"),
            "min_52_sem": self.parse_br_number(t1.get("min_52_sem")),
            "max_52_sem": self.parse_br_number(t1.get("max_52_sem")),
            "volume_medio_2m": self.parse_br_number(t1.get("vol_med_2m")),
            "valor_mercado": self.parse_br_number(t2.get("valor_de_mercado")),
            "valor_firma": self.parse_br_number(t2.get("valor_da_firma")),
            "ultimo_balanco_processado": t2.get("ult_balanco_processado"),
            "numero_acoes": self.parse_br_int(t2.get("nro_acoes")),
            "is_instituicao_financeira": is_bank,
        }

        multiplos = {
            "pl": self.parse_br_number(t3.get("pl")),
            "lpa": self.parse_br_number(t3.get("lpa")),
            "pvp": self.parse_br_number(t3.get("pvp")),
            "vpa": self.parse_br_number(t3.get("vpa")),
            "p_ebit": self.parse_br_number(t3.get("pebit")),
            "psr": self.parse_br_number(t3.get("psr")),
            "p_ativos": self.parse_br_number(t3.get("pativos")),
            "p_cap_giro": self.parse_br_number(t3.get("pcap_giro")),
            "p_ativo_circ_liq": self.parse_br_number(t3.get("pativ_circ_liq")),
            "dividend_yield": self.parse_br_number(t3.get("div_yield")),
            "ev_ebitda": self.parse_br_number(t3.get("ev_ebitda")),
            "ev_ebit": self.parse_br_number(t3.get("ev_ebit")),
            "margem_bruta": self.parse_br_number(t3.get("marg_bruta")),
            "margem_ebit": self.parse_br_number(t3.get("marg_ebit")),
            "margem_liquida": self.parse_br_number(t3.get("marg_liquida")),
            "ebit_ativo": self.parse_br_number(t3.get("ebit_ativo")),
            "roic": self.parse_br_number(t3.get("roic")),
            "roe": self.parse_br_number(t3.get("roe")),
            "liquidez_corrente": self.parse_br_number(t3.get("liquidez_corr")),
            "divida_liquida_patrimonio": self.parse_br_number(t3.get("div_liq_patrim")),
            "cagr_receita_5a": self.parse_br_number(t3.get("cres_rec_5a")),
            "giro_ativos": self.parse_br_number(t3.get("giro_ativos")),
        }

        balanco = {
            "ativo_total": self.parse_br_number(t4.get("ativo")),
            "patrimonio_liquido": self.parse_br_number(t4.get("patrim_liq")),
        }
        if is_bank:
            balanco["carteira_credito"] = self.parse_br_number(t4.get("cart_de_credito"))
            balanco["depositos"] = self.parse_br_number(t4.get("depositos"))
        else:
            balanco["disponibilidades"] = self.parse_br_number(t4.get("disponibilidades"))
            balanco["ativo_circulante"] = self.parse_br_number(t4.get("ativo_circulante"))
            balanco["divida_bruta"] = self.parse_br_number(t4.get("div_bruta"))
            balanco["divida_liquida"] = self.parse_br_number(t4.get("div_liquida"))

        dre_12m, dre_3m = self._parse_dre_table(tables[4], is_bank)

        return [{
            "ticker": ticker,
            "header": header,
            "multiplos": multiplos,
            "balanco": balanco,
            "dre_12m": dre_12m,
            "dre_3m": dre_3m,
            "source": "fundamentus_detalhes_acao",
        }]

    def _parse_detalhes_fii(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/detalhes.php?papel=X&h=1."""
        if len(tables) < 5:
            return []
        t1 = self._table_to_dict(tables[0])
        t2 = self._table_to_dict(tables[1])
        t3 = self._table_to_dict(tables[2])
        t5 = self._table_to_dict(tables[4])  # Imóveis

        return [{
            "ticker": ticker,
            "nome": t1.get("nome"),
            "cotacao": self.parse_br_number(t1.get("cotacao")),
            "min_52_sem": self.parse_br_number(t1.get("min_52_sem")),
            "max_52_sem": self.parse_br_number(t1.get("max_52_sem")),
            "segmento": t1.get("segmento"),
            "mandato": t1.get("mandato"),
            "gestao": t1.get("gestao"),
            "valor_mercado": self.parse_br_number(t2.get("valor_de_mercado")),
            "numero_cotas": self.parse_br_int(t2.get("nro_cotas")),
            "ffo_yield": self.parse_br_number(t3.get("ffo_yield")),
            "ffo_cota": self.parse_br_number(t3.get("ffocota")),
            "dividend_yield": self.parse_br_number(t3.get("div_yield")),
            "dividendo_cota": self.parse_br_number(t3.get("dividendocota")),
            "pvp": self.parse_br_number(t3.get("pvp")),
            "vpa": self.parse_br_number(t3.get("vpcota")),
            "qtd_imoveis": self.parse_br_int(t5.get("qtd_imoveis")),
            "area_m2": self.parse_br_number(t5.get("area_m2")),
            "cap_rate": self.parse_br_number(t5.get("cap_rate")),
            "vacancia_media": self.parse_br_number(t5.get("vacancia_media")),
            "aluguel_m2": self.parse_br_number(t5.get("aluguelm2")),
            "source": "fundamentus_detalhes_fii",
        }]

    def _parse_proventos_fii(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/fii_proventos.php?papel=X."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 4:
                records.append({
                    "ticker": ticker,
                    "data_com": cols[0],
                    "tipo": cols[1],
                    "data_pagamento": cols[2],
                    "valor_por_cota": self.parse_br_number(cols[3]),
                    "source": "fundamentus_fii_proventos",
                })
        return records

    def _parse_fii_administrador(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/fii_administrador.php?papel=X."""
        if not tables:
            return []
        d = self._table_to_dict(tables[0])
        return [{
            "ticker": ticker,
            "administrador": d.get("nome"),
            "cnpj_fundo": d.get("cnpj_do_fundo"),
            "site": d.get("site"),
            "telefone": d.get("telefone"),
            "email": d.get("email"),
            "endereco": d.get("endereco"),
            "source": "fundamentus_fii_admin",
        }]

    def _parse_insiders(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/insiders.php?papel=X."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 4:
                records.append({
                    "ticker": ticker,
                    "mes_referencia": cols[0],
                    "quantidade": self.parse_br_int(cols[1]),
                    "valor_total": self.parse_br_number(cols[2]),
                    "preco_medio": self.parse_br_number(cols[3]),
                    "source": "fundamentus_insiders_cvm44",
                })
        return records

    def _parse_recompras(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/recompras.php?papel=X."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 4:
                records.append({
                    "ticker": ticker,
                    "mes_referencia": cols[0],
                    "quantidade_adquirida": self.parse_br_int(cols[1]),
                    "valor_desembolsado": self.parse_br_number(cols[2]),
                    "preco_medio": self.parse_br_number(cols[3]),
                    "capital_pct": self.parse_br_number(cols[4]) if len(cols) > 4 else None,
                    "source": "fundamentus_recompras_acoes",
                })
        return records

    def _parse_proventos_acoes(self, ticker: str, tables: list[Any]) -> list[dict[str, Any]]:
        """Parse de https://www.fundamentus.com.br/proventos.php?papel=X."""
        if not tables:
            return []
        rows = tables[0].find_all("tr")
        records = []
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 4:
                records.append({
                    "ticker": ticker,
                    "data_com": cols[0],
                    "valor_por_acao": self.parse_br_number(cols[1]),
                    "tipo": cols[2],
                    "data_pagamento": cols[3],
                    "por_quantas_acoes": self.parse_br_int(cols[4]) if len(cols) > 4 else 1,
                    "source": "fundamentus_proventos_acao",
                })
        return records

    # =========================================================================
    # UTILITÁRIOS AUXILIARES
    # =========================================================================

    def _table_to_dict(self, table: Any) -> dict[str, str]:
        data = {}
        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            i = 0
            while i < len(cells) - 1:
                key = cells[i].replace("?", "").strip()
                val = cells[i + 1].strip()
                norm_key = self.clean_key(key)
                if norm_key:
                    data[norm_key] = val
                i += 2
        return data

    def _parse_dre_table(self, table: Any, is_bank: bool) -> tuple[dict[str, Any], dict[str, Any]]:
        dre_12m: dict[str, Any] = {}
        dre_3m: dict[str, Any] = {}
        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) >= 4:
                key = self.clean_key(cells[0])
                val_12m = self.parse_br_number(cells[1])
                val_3m = self.parse_br_number(cells[3])

                if is_bank:
                    if "result_int_financ" in key:
                        dre_12m["resultado_intermediacao"] = val_12m
                        dre_3m["resultado_intermediacao"] = val_3m
                    elif "rec_servicos" in key:
                        dre_12m["receita_servicos"] = val_12m
                        dre_3m["receita_servicos"] = val_3m
                    elif "lucro_liquido" in key:
                        dre_12m["lucro_liquido"] = val_12m
                        dre_3m["lucro_liquido"] = val_3m
                else:
                    if "receita_liquida" in key:
                        dre_12m["receita_liquida"] = val_12m
                        dre_3m["receita_liquida"] = val_3m
                    elif "ebit" in key:
                        dre_12m["ebit"] = val_12m
                        dre_3m["ebit"] = val_3m
                    elif "lucro_liquido" in key:
                        dre_12m["lucro_liquido"] = val_12m
                        dre_3m["lucro_liquido"] = val_3m
        return dre_12m, dre_3m
