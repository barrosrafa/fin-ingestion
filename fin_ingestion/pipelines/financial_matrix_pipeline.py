"""Pipeline de normalização de demonstrações financeiras em matriz histórica decenal (DRE, BP, DFC)."""

from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class FinancialMatrixPipeline:
    """Consolida demonstrações contábeis DFP/ITR em matrizes anuais e trimestrais padronizadas de 10+ anos."""

    ACCOUNT_MAPPING = {
        "3.01": "receita_liquida",
        "3.02": "custos_bens_servicos",
        "3.03": "lucro_bruto",
        "3.04": "despesas_operacionais",
        "3.05": "ebitda",
        "3.06": "depreciacao_amortizacao",
        "3.07": "ebit",
        "3.08": "resultado_financeiro",
        "3.09": "receitas_financeiras",
        "3.10": "despesas_financeiras",
        "3.11": "lucro_antes_ir",
        "3.12": "ir_csll",
        "3.13": "lucro_liquido",
        "1": "ativo_total",
        "1.01": "ativo_circulante",
        "1.01.01": "disponibilidades",
        "1.01.02": "contas_a_receber",
        "1.01.04": "estoques",
        "2.01": "passivo_circulante",
        "2.03": "patrimonio_liquido",
        "6.01": "fco_operacional",
        "6.02": "fci_investimentos",
        "6.03": "fcf_financiamentos",
    }

    @staticmethod
    def calculate_cagr(start_value: float, end_value: float, years: int) -> float | None:
        """Calcula a taxa de crescimento anual composta (CAGR) em percentual."""
        if years <= 0 or start_value <= 0 or end_value <= 0:
            return None
        try:
            cagr = ((end_value / start_value) ** (1.0 / years)) - 1.0
            return round(cagr * 100.0, 2)
        except Exception:
            return None

    def build_matrix_record(
        self,
        id_emissor: int,
        ano: int,
        raw_accounts: dict[str, Any],
        shares_count: int | None = None,
        trimestre: int = 0,
        tipo_demonstracao: str = "ANUAL_CONSOL",
    ) -> dict[str, Any]:
        """Gera uma linha normalizada para a tabela analytics.demonstrativo_padronizado_serie preservando None vs 0."""
        def _clean_val(key: str) -> float | None:
            val = raw_accounts.get(key)
            if val is None or val == "" or val == "-":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        lucro_liq = _clean_val("lucro_liquido")
        pl_val = _clean_val("patrimonio_liquido")
        fco = _clean_val("fco_operacional")
        fci = _clean_val("fci_investimentos")
        fcf = (fco + fci) if (fco is not None and fci is not None) else None

        rec = {
            "id_emissor": id_emissor,
            "ano_exercicio": ano,
            "trimestre": trimestre,
            "tipo_demonstracao": tipo_demonstracao,
            "data_fim_periodo": f"{ano}-12-31" if trimestre == 0 else f"{ano}-{trimestre * 3:02d}-30",
            "moeda": "BRL",
            "fator_escala": 1.0,
            "receita_liquida": _clean_val("receita_liquida"),
            "custos_bens_servicos": _clean_val("custos_bens_servicos"),
            "lucro_bruto": _clean_val("lucro_bruto"),
            "despesas_operacionais": _clean_val("despesas_operacionais"),
            "ebitda": _clean_val("ebitda"),
            "depreciacao_amortizacao": _clean_val("depreciacao_amortizacao"),
            "ebit": _clean_val("ebit"),
            "resultado_financeiro": _clean_val("resultado_financeiro"),
            "receitas_financeiras": _clean_val("receitas_financeiras"),
            "despesas_financeiras": _clean_val("despesas_financeiras"),
            "lucro_antes_ir": _clean_val("lucro_antes_ir"),
            "ir_csll": _clean_val("ir_csll"),
            "lucro_liquido": lucro_liq,
            "ativo_total": _clean_val("ativo_total"),
            "ativo_circulante": _clean_val("ativo_circulante"),
            "disponibilidades": _clean_val("disponibilidades"),
            "contas_a_receber": _clean_val("contas_a_receber"),
            "estoques": _clean_val("estoques"),
            "passivo_circulante": _clean_val("passivo_circulante"),
            "divida_bruta_total": _clean_val("divida_bruta_total"),
            "divida_liquida": _clean_val("divida_liquida"),
            "patrimonio_liquido": pl_val,
            "fco_operacional": fco,
            "fci_investimentos": fci,
            "fcf_financiamentos": _clean_val("fcf_financiamentos"),
            "fluxo_caixa_livre": fcf,
            "numero_acoes_ajustado": shares_count,
            "lpa_ajustado": round(lucro_liq / shares_count, 4) if (lucro_liq is not None and shares_count) else None,
            "vpa_ajustado": round(pl_val / shares_count, 4) if (pl_val is not None and shares_count) else None,
            "payout_exercicio_pct": _clean_val("payout_exercicio_pct"),
            "proventos_declarados_ano": _clean_val("proventos_declarados_ano"),
        }
        return rec
