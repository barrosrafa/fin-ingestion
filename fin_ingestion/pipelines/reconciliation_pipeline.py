from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class MetricInput:
    source_id: str
    ticker: str
    metric_name: str
    value: float
    methodology: Optional[str] = None


@dataclass
class DivergenceResult:
    ticker: str
    metric_name: str
    has_divergence: bool
    delta_pct: float
    consensus_min: float
    consensus_max: float
    consensus_iqr: float
    anchor_source: str
    anchor_value: float
    divergence_taxonomy: Optional[str] = None


class ReconciliationEngine:
    """Engine for multi-source metric reconciliation and divergence classification."""

    # Priority weighting for anchor selection
    SOURCE_WEIGHTS = {
        "CVM_OFFICIAL": 100,
        "B3_OFFICIAL": 95,
        "STATUS_INVEST": 85,
        "FUNDS_EXPLORER": 80,
        "INVESTIDOR10": 80,
        "CLUBE_FII": 80,
    }

    DIVERGENCE_THRESHOLD_PCT = 1.5

    def reconcile(self, ticker: str, metric_name: str, metrics: List[MetricInput]) -> DivergenceResult:
        if not metrics:
            raise ValueError("No metrics provided for reconciliation")

        values = [m.value for m in metrics]
        consensus_min = min(values)
        consensus_max = max(values)
        consensus_iqr = consensus_max - consensus_min

        # Select anchor by score
        best_metric = max(
            metrics,
            key=lambda m: self.SOURCE_WEIGHTS.get(m.source_id, 50),
        )

        baseline_val = best_metric.value
        has_divergence = False
        max_delta = 0.0
        taxonomy = None

        if len(values) >= 2 and baseline_val != 0:
            for m in metrics:
                delta = abs(m.value - baseline_val) / abs(baseline_val) * 100.0
                if delta > max_delta:
                    max_delta = delta

            if max_delta > self.DIVERGENCE_THRESHOLD_PCT:
                has_divergence = True
                taxonomy = self._classify_taxonomy(metric_name, metrics)

        return DivergenceResult(
            ticker=ticker,
            metric_name=metric_name,
            has_divergence=has_divergence,
            delta_pct=round(max_delta, 4),
            consensus_min=round(consensus_min, 4),
            consensus_max=round(consensus_max, 4),
            consensus_iqr=round(consensus_iqr, 4),
            anchor_source=best_metric.source_id,
            anchor_value=best_metric.value,
            divergence_taxonomy=taxonomy,
        )

    def _classify_taxonomy(self, metric_name: str, metrics: List[MetricInput]) -> str:
        # Check known methodology differences
        methodologies = {m.source_id: m.methodology for m in metrics if m.methodology}
        if "DY" in metric_name or "DIVIDEND" in metric_name:
            if any("LAST_DIVIDEND" in (m or "") for m in methodologies.values()) or any(
                "TRAILING" in (m or "") for m in methodologies.values()
            ):
                return "DIVIDEND_WINDOW_MISMATCH"
            return "DIVIDEND_WINDOW_MISMATCH"

        if "P_VP" in metric_name or "PATRIMONIAL" in metric_name:
            return "CAPITAL_BASE_MISMATCH"

        if "PRECO" in metric_name or "PRICE" in metric_name:
            return "PRICE_TIMESTAMP_MISMATCH"

        return "SOURCE_STALENESS_OR_ERROR"
