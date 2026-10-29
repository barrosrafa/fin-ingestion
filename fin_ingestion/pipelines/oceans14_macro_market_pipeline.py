"""Pipeline de ingestão de Indicadores Macroeconômicos, Crédito, Fluxo de Investidores B3 e Valuation IBOV."""

from typing import Any

import structlog

from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.collectors.official.bcb import BCBCollector

logger = structlog.get_logger(__name__)


class Oceans14MacroMarketPipeline:
    """Executa a captura e harmonização dos indicadores macroeconômicos e de mercado B3 (Padrão Oceans14)."""

    def __init__(self) -> None:
        self.bcb = BCBCollector()
        self.b3 = B3Collector()

    async def run(self, tickers: list[str] | None = None) -> dict[str, Any]:
        from fin_ingestion.pipelines.result import summarize_results
        results = {}
        try:
            for key in self.bcb.SERIES_MAP:
                results[key] = await self.bcb.run(serie=key, ultimos=12)
            return summarize_results("macro_market", results)
        finally:
            await self.bcb.close()
            await self.b3.close()

    async def collect_macro_credit_indicators(self, ultimos: int = 12) -> dict[str, list[dict[str, Any]]]:
        """Coleta séries temporais do Bacen SGS para crédito, poupança, contas públicas e TCR."""
        series_keys = [
            "poupanca_captacao",
            "poupanca_saldo",
            "inadimplencia_pf",
            "inadimplencia_pj",
            "credito_pj",
            "credito_imobiliario",
            "tcr_efetiva",
            "receita_liquida_governo",
            "despesa_total_governo",
            "resultado_primario_governo",
            "divida_bruta_pib",
        ]

        logger.info("Iniciando coleta de indicadores macro e crédito Bacen", series_count=len(series_keys))
        collected_data: dict[str, list[dict[str, Any]]] = {}

        for key in series_keys:
            try:
                res = await self.bcb.run(serie=key, ultimos=ultimos)
                collected_data[key] = res.data if res and res.data else []
            except Exception as e:
                logger.warning("Falha ao coletar série Bacen", serie=key, error=str(e))
                collected_data[key] = []

        return collected_data

    def harmonize_credit_system_monthly(
        self, raw_macro_series: dict[str, list[dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """Harmoniza múltiplos indicadores mensais de crédito em registros unificados por data_referencia."""
        monthly_map: dict[str, dict[str, Any]] = {}

        for serie_key, rows in raw_macro_series.items():
            for r in rows:
                dt = r.get("data")
                val = r.get("valor")
                if not dt or val is None:
                    continue
                # Alinhar ao primeiro dia do mês YYYY-MM-01
                try:
                    month_key = dt[:7] + "-01"
                except Exception:
                    continue

                if month_key not in monthly_map:
                    monthly_map[month_key] = {
                        "data_referencia": month_key,
                        "poupanca_captacao_liquida": None,
                        "poupanca_saldo_total": None,
                        "inadimplencia_pf_pct": None,
                        "inadimplencia_pj_pct": None,
                        "inadimplencia_total_pct": None,
                        "estoque_credito_pj": None,
                        "estoque_credito_pf": None,
                        "concessao_imobiliario_pf": None,
                        "tcr_efetiva_indice": None,
                    }

                if serie_key == "poupanca_captacao":
                    monthly_map[month_key]["poupanca_captacao_liquida"] = val
                elif serie_key == "poupanca_saldo":
                    monthly_map[month_key]["poupanca_saldo_total"] = val
                elif serie_key == "inadimplencia_pf":
                    monthly_map[month_key]["inadimplencia_pf_pct"] = val
                elif serie_key == "inadimplencia_pj":
                    monthly_map[month_key]["inadimplencia_pj_pct"] = val
                elif serie_key == "credito_pj":
                    monthly_map[month_key]["estoque_credito_pj"] = val
                elif serie_key == "credito_imobiliario":
                    monthly_map[month_key]["concessao_imobiliario_pf"] = val
                elif serie_key == "tcr_efetiva":
                    monthly_map[month_key]["tcr_efetiva_indice"] = val

        return sorted(monthly_map.values(), key=lambda x: x["data_referencia"])

    def harmonize_government_accounts_monthly(
        self, raw_macro_series: dict[str, list[dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """Harmoniza dados do Tesouro Nacional / Bacen de receitas, despesas e dívida pública."""
        monthly_map: dict[str, dict[str, Any]] = {}

        for serie_key, rows in raw_macro_series.items():
            for r in rows:
                dt = r.get("data")
                val = r.get("valor")
                if not dt or val is None:
                    continue
                month_key = dt[:7] + "-01"

                if month_key not in monthly_map:
                    monthly_map[month_key] = {
                        "data_referencia": month_key,
                        "receita_liquida": 0.0,
                        "despesa_total": 0.0,
                        "resultado_primario": 0.0,
                        "divida_bruta_pib_pct": None,
                    }

                if serie_key == "receita_liquida_governo":
                    monthly_map[month_key]["receita_liquida"] = val
                elif serie_key == "despesa_total_governo":
                    monthly_map[month_key]["despesa_total"] = val
                elif serie_key == "resultado_primario_governo":
                    monthly_map[month_key]["resultado_primario"] = val
                elif serie_key == "divida_bruta_pib":
                    monthly_map[month_key]["divida_bruta_pib_pct"] = val

        return sorted(monthly_map.values(), key=lambda x: x["data_referencia"])

    def calculate_ibov_pe_corridor(
        self, historical_points: list[dict[str, Any]], window_months: int = 60
    ) -> list[dict[str, Any]]:
        """
        Calcula a média móvel causal de 5 anos (60 meses) e as bandas de desvio padrão
        (+-1dp e +-2dp) para a série histórica do P/L Ibov sem lookahead bias.
        """
        sorted_points = sorted(historical_points, key=lambda x: str(x["data_referencia"]))
        results = []

        for idx, p in enumerate(sorted_points):
            # Janela causal: somente pontos passados até a data de referência corrente
            window_slice = sorted_points[max(0, idx - window_months + 1):idx + 1]
            pe_window = [
                pt["pl_ponderado"]
                for pt in window_slice
                if "pl_ponderado" in pt and pt["pl_ponderado"] is not None and pt["pl_ponderado"] > 0
            ]

            if len(pe_window) >= 6:
                mean_pe = sum(pe_window) / len(pe_window)
                variance = sum((x - mean_pe) ** 2 for x in pe_window) / (len(pe_window) - 1)
                sd_pe = variance ** 0.5
            elif pe_window:
                mean_pe = sum(pe_window) / len(pe_window)
                sd_pe = 1.5
            else:
                mean_pe = float(p.get("pl_ponderado") or 10.0)
                sd_pe = 1.5

            results.append({
                "data_referencia": p["data_referencia"],
                "codigo_indice": p.get("codigo_indice", "IBOV"),
                "fechamento_indice": p["fechamento_indice"],
                "pl_ponderado": p["pl_ponderado"],
                "pl_mediano": p.get("pl_mediano", p["pl_ponderado"]),
                "pl_medio_aritmetico": p.get("pl_medio_aritmetico", p["pl_ponderado"]),
                "lucro_projetado_fwd": p.get("lucro_projetado_fwd", p["pl_ponderado"]),
                "media_historica_5y": round(mean_pe, 2),
                "desvio_padrao_5y": round(sd_pe, 2),
                "banda_superior_1dp": round(mean_pe + sd_pe, 2),
                "banda_inferior_1dp": round(max(0.1, mean_pe - sd_pe), 2),
                "banda_superior_2dp": round(mean_pe + 2 * sd_pe, 2),
                "banda_inferior_2dp": round(max(0.1, mean_pe - 2 * sd_pe), 2),
            })

        return results
