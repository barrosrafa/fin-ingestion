"""Resultados explícitos para impedir sucesso sem dados nas operações."""
from typing import Any

from fin_ingestion.core.base_collector import CollectorResult


class PipelineExecutionError(RuntimeError):
    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def require_complete_result(result: dict[str, Any]) -> None:
    """Never advance a queue checkpoint after partial or empty extraction."""
    if result.get("status", "SUCCEEDED") != "SUCCEEDED" or result.get("total_records", 0) <= 0:
        raise PipelineExecutionError(
            f"Incomplete pipeline result: {result}", result.get("retry_after_seconds")
        )


def summarize_results(pipeline: str, results: dict[str, Any]) -> dict[str, Any]:
    failed = {
        name: result.errors if isinstance(result, CollectorResult) else [str(result)]
        for name, result in results.items()
        if not isinstance(result, CollectorResult) or not result.success
    }
    count = sum(r.records_count for r in results.values() if isinstance(r, CollectorResult) and r.success)
    retry_after = max((r.retry_after_seconds or 0 for r in results.values() if isinstance(r, CollectorResult)), default=0)
    if not count:
        raise PipelineExecutionError(f'{pipeline}: nenhuma linha coletada; falhas={failed}', retry_after or None)
    return {
        'pipeline': pipeline,
        'total_records': count,
        'sources_executed': list(results),
        'failed_sources': failed,
        'status': 'PARTIAL' if failed else 'SUCCEEDED',
        'retry_after_seconds': retry_after or None,
    }
