from fin_ingestion.pipelines.financial_matrix_pipeline import FinancialMatrixPipeline
from fin_ingestion.pipelines.oceans14_macro_market_pipeline import Oceans14MacroMarketPipeline
from fin_ingestion.pipelines.orchestrator import PipelineOrchestrator


def test_ibov_pe_corridor_no_lookahead_bias():
    """Verify F10 remediation: adding future points does NOT alter past corridor bands."""
    pipeline = Oceans14MacroMarketPipeline()

    base_points = [
        {"data_referencia": f"2023-{m:02d}-01", "fechamento_indice": 115000 + m * 500, "pl_ponderado": 7.5 + m * 0.1}
        for m in range(1, 13)
    ]

    result_without_future = pipeline.calculate_ibov_pe_corridor(base_points)
    december_2023_initial = result_without_future[-1]

    # Now append an extreme future bull run in 2024
    future_points = [
        {"data_referencia": f"2024-{m:02d}-01", "fechamento_indice": 140000 + m * 1000, "pl_ponderado": 25.0}
        for m in range(1, 13)
    ]
    extended_points = base_points + future_points

    result_with_future = pipeline.calculate_ibov_pe_corridor(extended_points)
    december_2023_after_future = result_with_future[11]

    # In a causal model without lookahead bias, December 2023 statistics must remain identical
    assert december_2023_initial["media_historica_5y"] == december_2023_after_future["media_historica_5y"]
    assert december_2023_initial["desvio_padrao_5y"] == december_2023_after_future["desvio_padrao_5y"]
    assert december_2023_initial["banda_superior_1dp"] == december_2023_after_future["banda_superior_1dp"]


def test_financial_matrix_preserves_none():
    """Verify F11 remediation: unrecorded or non-applicable accounts remain None instead of 0.0."""
    matrix_pipe = FinancialMatrixPipeline()
    raw_accounts = {
        "receita_liquida": 500000.0,
        "lucro_liquido": 50000.0,
        # estoques, divida_bruta are not present in raw
    }

    rec = matrix_pipe.build_matrix_record(
        id_emissor=1,
        ano=2025,
        raw_accounts=raw_accounts,
        shares_count=100000,
    )

    assert rec["receita_liquida"] == 500000.0
    assert rec["lucro_liquido"] == 50000.0
    assert rec["estoques"] is None  # NOT 0.0!
    assert rec["divida_bruta_total"] is None  # NOT 0.0!
    assert rec["lpa_ajustado"] == 0.50


def test_orchestrator_knows_all_pipelines():
    """Verify F22 remediation: orchestrator recognizes all system pipelines."""
    orchestrator = PipelineOrchestrator()
    assert "daily_market" in orchestrator.pipelines
    assert "etfs_brasil" in orchestrator.pipelines
    assert "financial_matrix" in orchestrator.pipelines
    assert "macro_market" in orchestrator.pipelines
    assert "dividend_seasonality" in orchestrator.pipelines
