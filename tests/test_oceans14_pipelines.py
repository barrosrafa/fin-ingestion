"""Testes unitários dos pipelines do Oceans14 (Macro, Crédito, Mercado e Demonstrações Financeiras)."""

import pytest
from fin_ingestion.pipelines.oceans14_macro_market_pipeline import Oceans14MacroMarketPipeline
from fin_ingestion.pipelines.financial_matrix_pipeline import FinancialMatrixPipeline


def test_calculate_cagr():
    pipeline = FinancialMatrixPipeline()
    # R$ 100M crescendo para R$ 161.05M em 5 anos = ~10% CAGR
    cagr = pipeline.calculate_cagr(start_value=100.0, end_value=161.051, years=5)
    assert cagr is not None
    assert abs(cagr - 10.0) < 0.1

    # Casos limites
    assert pipeline.calculate_cagr(0.0, 100.0, 5) is None
    assert pipeline.calculate_cagr(100.0, -50.0, 5) is None
    assert pipeline.calculate_cagr(100.0, 150.0, 0) is None


def test_build_matrix_record():
    pipeline = FinancialMatrixPipeline()
    raw = {
        "receita_liquida": 50000.0,
        "custos_bens_servicos": 25000.0,
        "lucro_bruto": 25000.0,
        "ebitda": 15000.0,
        "lucro_liquido": 8000.0,
        "patrimonio_liquido": 40000.0,
        "fco_operacional": 12000.0,
        "fci_investimentos": -4000.0,
    }
    rec = pipeline.build_matrix_record(
        id_emissor=73,
        ano=2025,
        raw_accounts=raw,
        shares_count=1000,
        trimestre=0,
    )
    assert rec["id_emissor"] == 73
    assert rec["ano_exercicio"] == 2025
    assert rec["receita_liquida"] == 50000.0
    assert rec["lucro_liquido"] == 8000.0
    assert rec["lpa_ajustado"] == 8.0 # 8000 / 1000
    assert rec["vpa_ajustado"] == 40.0 # 40000 / 1000
    assert rec["fluxo_caixa_livre"] == 8000.0 # 12000 + (-4000)


def test_harmonize_credit_system_monthly():
    pipeline = Oceans14MacroMarketPipeline()
    raw_series = {
        "poupanca_captacao": [{"data": "2026-09-15", "valor": -800000000.0}],
        "poupanca_saldo": [{"data": "2026-09-15", "valor": 986000000000.0}],
        "inadimplencia_pf": [{"data": "2026-09-15", "valor": 5.1}],
        "inadimplencia_pj": [{"data": "2026-09-15", "valor": 2.8}],
        "credito_pj": [{"data": "2026-09-15", "valor": 2410000000000.0}],
        "tcr_efetiva": [{"data": "2026-09-15", "valor": 96.90}],
    }
    harmonized = pipeline.harmonize_credit_system_monthly(raw_series)
    assert len(harmonized) == 1
    item = harmonized[0]
    assert item["data_referencia"] == "2026-09-01"
    assert item["poupanca_captacao_liquida"] == -800000000.0
    assert item["inadimplencia_pf_pct"] == 5.1
    assert item["inadimplencia_pj_pct"] == 2.8
    assert item["tcr_efetiva_indice"] == 96.90


def test_calculate_ibov_pe_corridor():
    pipeline = Oceans14MacroMarketPipeline()
    pts = [
        {"data_referencia": "2024-01-02", "fechamento_indice": 130000.0, "pl_ponderado": 8.0},
        {"data_referencia": "2024-06-02", "fechamento_indice": 125000.0, "pl_ponderado": 7.5},
        {"data_referencia": "2025-01-02", "fechamento_indice": 135000.0, "pl_ponderado": 8.5},
        {"data_referencia": "2025-06-02", "fechamento_indice": 140000.0, "pl_ponderado": 9.0},
        {"data_referencia": "2026-01-02", "fechamento_indice": 145000.0, "pl_ponderado": 9.5},
    ]
    corridor = pipeline.calculate_ibov_pe_corridor(pts)
    assert len(corridor) == 5
    for c in corridor:
        assert c["media_historica_5y"] > 0
        assert c["banda_superior_1dp"] > c["media_historica_5y"]
        assert c["banda_inferior_1dp"] < c["media_historica_5y"]
        assert c["banda_superior_2dp"] > c["banda_superior_1dp"]
        assert c["banda_inferior_2dp"] < c["banda_inferior_1dp"]
