"""Classe abstrata base para todos os coletores e scrapers do ecossistema."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

from fin_ingestion.core.http_client import ResilientHttpClient
from fin_ingestion.core.raw_sink import RawSink

logger = structlog.get_logger(__name__)


@dataclass
class CollectorResult:
    """Resultado encapsulado de uma execução de coletor."""

    source: str
    success: bool
    records_count: int
    raw_payload_id: str | None = None
    data: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    execution_time_seconds: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class BaseCollector(ABC):
    """Contrato e ciclo de vida base para coletores oficiais e plataformas de análise."""

    def __init__(
        self,
        name: str,
        base_url: str | None = None,
        requests_per_second: float = 2.0,
        custom_headers: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.base_url = base_url
        self.http_client = ResilientHttpClient(
            name=name,
            base_url=base_url,
            rate_limiter=None,  # default usa o limite padrão
            custom_headers=custom_headers,
        )
        self.raw_sink = RawSink()

    async def close(self) -> None:
        await self.http_client.close()
        await self.raw_sink.close()

    @abstractmethod
    async def extract(self, **kwargs: Any) -> Any:
        """Executa a requisição externa bruta e retorna o payload original."""
        raise NotImplementedError

    @abstractmethod
    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        """Converte os dados brutos (JSON, HTML, CSV) em registros normalizados."""
        raise NotImplementedError

    async def run(self, **kwargs: Any) -> CollectorResult:
        """Ciclo completo: Coleta -> Raw Landing -> Normalização -> Estatísticas."""
        start_time = datetime.now(timezone.utc)
        logger.info("Iniciando coleta", source=self.name, params=kwargs)

        try:
            raw_data = await self.extract(**kwargs)
            raw_id = await self.raw_sink.save_payload(
                source=self.name,
                endpoint=str(kwargs.get("endpoint", "default")),
                payload=raw_data if isinstance(raw_data, (dict, list)) else {"raw": str(raw_data)},
                metadata=kwargs,
            )

            records = self.parse(raw_data)
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            logger.info(
                "Coleta concluída com sucesso",
                source=self.name,
                records=len(records),
                duration=duration,
            )

            return CollectorResult(
                source=self.name,
                success=True,
                records_count=len(records),
                raw_payload_id=raw_id,
                data=records,
                execution_time_seconds=duration,
            )
        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.error("Erro durante coleta", source=self.name, error=str(e), duration=duration)
            return CollectorResult(
                source=self.name,
                success=False,
                records_count=0,
                errors=[str(e)],
                execution_time_seconds=duration,
            )
