"""Resultados explícitos para impedir sucesso sem dados nas operações."""
from typing import Any

from fin_ingestion.core.base_collector import CollectorResult


def summarize_results(pipeline: str, results: dict[str, Any]) -> dict[str, Any]:
    failed = {
        name: result.errors if isinstance(result, CollectorResult) else [str(result)]
        for name, result in results.items()
        if not isinstance(result, CollectorResult) or not result.success
    }
    count = sum(r.records_count for r in results.values() if isinstance(r, CollectorResult) and r.success)
    if not count:
        raise RuntimeError(f'{pipeline}: nenhuma linha coletada; falhas={failed}')
    return {
        'pipeline': pipeline,
        'total_records': count,
        'sources_executed': list(results),
        'failed_sources': failed,
        'status': 'PARTIAL' if failed else 'SUCCEEDED',
    }
