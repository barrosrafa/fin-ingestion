"""Pipeline de normalização de demonstrações financeiras em matriz histórica decenal (DRE, BP, DFC)."""

from typing import Any, Dict, List, Optional
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
    def calculate_cagr(start_value: float, end_value: float, years: int) -> Optional[float]:
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
        raw_accounts: Dict[str, float],
        shares_count: Optional[int] = None,
        trimestre: int = 0,
        tipo_demonstracao: str = "ANUAL_CONSOL",
    ) -> Dict[str, Any]:
        """Gera uma linha normalizada para a tabela analytics.demonstrativo_padronizado_serie."""
        rec = {
            "id_emissor": id_emissor,
            "ano_exercicio": ano,
            "trimestre": trimestre,
            "tipo_demonstracao": tipo_demonstracao,
            "data_fim_periodo": f"{ano}-12-31" if trimestre == 0 else f"{ano}-{trimestre * 3:02d}-30",
            "moeda": "BRL",
            "fator_escala": 1.0,
            "receita_liquida": raw_accounts.get("receita_liquida", 0.0),
            "custos_bens_servicos": raw_accounts.get("custos_bens_servicos", 0.0),
            "lucro_bruto": raw_accounts.get("lucro_bruto", 0.0),
            "despesas_operacionais": raw_accounts.get("despesas_operacionais", 0.0),
            "ebitda": raw_accounts.get("ebitda", 0.0),
            "depreciacao_amortizacao": raw_accounts.get("depreciacao_amortizacao", 0.0),
            "ebit": raw_accounts.get("ebit", 0.0),
            "resultado_financeiro": raw_accounts.get("resultado_financeiro", 0.0),
            "receitas_financeiras": raw_accounts.get("receitas_financeiras", 0.0),
            "despesas_financeiras": raw_accounts.get("despesas_financeiras", 0.0),
            "lucro_antes_ir": raw_accounts.get("lucro_antes_ir", 0.0),
            "ir_csll": raw_accounts.get("ir_csll", 0.0),
            "lucro_liquido": raw_accounts.get("lucro_liquido", 0.0),
            "ativo_total": raw_accounts.get("ativo_total", 0.0),
            "ativo_circulante": raw_accounts.get("ativo_circulante", 0.0),
            "disponibilidades": raw_accounts.get("disponibilidades", 0.0),
            "contas_a_receber": raw_accounts.get("contas_a_receber", 0.0),
            "estoques": raw_accounts.get("estoques", 0.0),
            "passivo_circulante": raw_accounts.get("passivo_circulante", 0.0),
            "divida_bruta_total": raw_accounts.get("divida_bruta_total", 0.0),
            "divida_liquida": raw_accounts.get("divida_liquida", 0.0),
            "patrimonio_liquido": raw_accounts.get("patrimonio_liquido", 0.0),
            "fco_operacional": raw_accounts.get("fco_operacional", 0.0),
            "fci_investimentos": raw_accounts.get("fci_investimentos", 0.0),
            "fcf_financiamentos": raw_accounts.get("fcf_financiamentos", 0.0),
            "fluxo_caixa_livre": raw_accounts.get("fco_operacional", 0.0) + raw_accounts.get("fci_investimentos", 0.0),
            "numero_acoes_ajustado": shares_count,
            "lpa_ajustado": round(raw_accounts.get("lucro_liquido", 0.0) / shares_count, 4) if shares_count else None,
            "vpa_ajustado": round(raw_accounts.get("patrimonio_liquido", 0.0) / shares_count, 4) if shares_count else None,
            "payout_exercicio_pct": raw_accounts.get("payout_exercicio_pct", 50.0),
            "proventos_declarados_ano": raw_accounts.get("proventos_declarados_ano", 0.0),
        }
        return rec
