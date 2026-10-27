import pytest
from fin_ingestion.pipelines.reconciliation_pipeline import ReconciliationEngine, MetricInput, DivergenceResult


def test_reconciliation_detects_dividend_window_divergence():
    engine = ReconciliationEngine()
    metrics = [
        MetricInput(
            source_id="STATUS_INVEST",
            ticker="KNIP11",
            metric_name="DY_12M",
            value=8.20,
            methodology="12_MONTHS_TRAILING",
        ),
        MetricInput(
            source_id="FUNDS_EXPLORER",
            ticker="KNIP11",
            metric_name="DY_12M",
            value=7.90,
            methodology="LAST_DIVIDEND_ANNUALIZED",
        ),
    ]

    result = engine.reconcile(ticker="KNIP11", metric_name="DY_12M", metrics=metrics)

    assert result.has_divergence is True
    assert result.delta_pct == pytest.approx(3.66, 0.05) # |8.2 - 7.9| / 8.2 * 100
    assert result.divergence_taxonomy == "DIVIDEND_WINDOW_MISMATCH"
    assert result.consensus_min == 7.90
    assert result.consensus_max == 8.20
    assert result.anchor_source == "STATUS_INVEST" # highest freshness / stability score


def test_reconciliation_below_threshold_produces_no_divergence():
    engine = ReconciliationEngine()
    metrics = [
        MetricInput(
            source_id="STATUS_INVEST",
            ticker="KNIP11",
            metric_name="P_VP",
            value=1.002,
        ),
        MetricInput(
            source_id="FUNDS_EXPLORER",
            ticker="KNIP11",
            metric_name="P_VP",
            value=1.000,
        ),
    ]

    result = engine.reconcile(ticker="KNIP11", metric_name="P_VP", metrics=metrics)

    assert result.has_divergence is False
    assert result.divergence_taxonomy is None
    assert result.delta_pct < 1.5
