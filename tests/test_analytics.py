"""Testes unitários dos motores de Valuation, Health Scores, Risco e Real Estate."""


from fin_ingestion.analytics.health_scores import HealthScoreEngine
from fin_ingestion.analytics.real_estate import RealEstateAnalytics
from fin_ingestion.analytics.risk_metrics import RiskAnalyticsEngine
from fin_ingestion.analytics.technical import TechnicalAnalysisEngine
from fin_ingestion.analytics.valuation import ValuationEngine


def test_graham_fair_price():
    # V = sqrt(22.5 * LPA * VPA)
    # LPA = 4.0, VPA = 25.0 -> 22.5 * 100 = 2250 -> sqrt(2250) = 47.434
    price = ValuationEngine.graham_fair_price(lpa=4.0, vpa=25.0)
    assert price is not None
    assert round(price, 2) == 47.43


def test_bazin_ceiling_price():
    # Preço teto = DPA / 0.06
    # DPA = 3.00 -> 3 / 0.06 = 50.00
    price = ValuationEngine.bazin_ceiling_price(dpa_medio=3.0, target_yield=0.06)
    assert price is not None
    assert round(price, 2) == 50.00


def test_discounted_cash_flow():
    dcf = ValuationEngine.discounted_cash_flow(
        free_cash_flow_0=100.0,
        growth_rate_5y=0.05,
        terminal_growth=0.02,
        wacc=0.10,
        shares_outstanding=10,
        net_debt=20.0,
    )
    assert dcf is not None
    assert dcf["fair_price_per_share"] > 0
    assert dcf["equity_value"] < dcf["enterprise_value"]


def test_piotroski_f_score():
    res = HealthScoreEngine.piotroski_f_score(
        net_income=50.0,
        operating_cash_flow=70.0,
        roa_current=0.12,
        roa_previous=0.10,
        cf_vs_net_income=True,
        leverage_current=0.8,
        leverage_previous=1.0,
        current_ratio_current=1.8,
        current_ratio_previous=1.5,
        shares_current=100,
        shares_previous=100,
        gross_margin_current=0.40,
        gross_margin_previous=0.35,
        asset_turnover_current=1.1,
        asset_turnover_previous=1.0,
    )
    assert res["f_score"] == 9
    assert res["health_level"] == "Forte"


def test_altman_z_score_emerging():
    res = HealthScoreEngine.altman_z_score_emerging(
        working_capital=40.0,
        total_assets=100.0,
        retained_earnings=30.0,
        ebit=20.0,
        market_cap=150.0,
        total_liabilities=50.0,
        revenue=120.0,
    )
    assert res["z_score"] > 2.6
    assert res["zone"] == "Segura"


def test_risk_metrics():
    prices = [10.0, 10.5, 11.0, 10.8, 11.2, 11.5, 12.0, 11.8, 12.2]
    returns = RiskAnalyticsEngine.calculate_returns(prices)
    sharpe = RiskAnalyticsEngine.sharpe_ratio(returns, risk_free_rate_annual=0.10)
    dd = RiskAnalyticsEngine.max_drawdown(prices)

    assert sharpe is not None
    assert dd["max_drawdown_pct"] <= 0.0


def test_real_estate_cap_rate_and_walt():
    cap = RealEstateAnalytics.cap_rate(annual_net_operating_income=8.0, property_value=100.0)
    assert cap == 8.0

    leases = [
        {"years_remaining": 5.0, "revenue": 60.0},
        {"years_remaining": 2.0, "revenue": 40.0},
    ]
    walt = RealEstateAnalytics.weighted_average_lease_term(leases)
    # (5*60 + 2*40) / 100 = (300 + 80) / 100 = 3.8 anos
    assert walt == 3.8


def test_technical_analysis_pivots_and_rsi():
    pivots = TechnicalAnalysisEngine.pivot_points_4s_3r(high=20.0, low=18.0, close=19.0)
    assert pivots["pivot_point"] == 19.0
    assert pivots["resistencia_1"] == 20.0
    assert pivots["suporte_1"] == 18.0

    prices = [10.0 + i * 0.5 for i in range(20)]
    rsi = TechnicalAnalysisEngine.relative_strength_index(prices, period=14)
    assert rsi is not None
    assert rsi > 70.0
