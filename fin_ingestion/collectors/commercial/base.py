"""Shared safe transport, quota and Bronze lifecycle for commercial APIs."""

import re
from typing import Any

import httpx
from redis.asyncio import Redis

from fin_ingestion.config import settings
from fin_ingestion.core.base_collector import BaseCollector
from fin_ingestion.core.provider_budget import ProviderBudget
from fin_ingestion.core.raw_sink import RawSink


def symbol(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9.^/-]{1,32}", value):
        raise ValueError("Invalid instrument symbol")
    return value


class ProviderUnavailableError(RuntimeError):
    pass


class CommercialCollector(BaseCollector):
    def __init__(self, name: str, base_url: str, key: str | None):
        super().__init__(name, base_url, requests_per_second=1)
        self.key = key
        if settings.ingestion_env == "production":
            # Commercial payloads stay in the configured database, never public
            # workflow artifacts. Official bulk archives use the local backend.
            self.raw_sink = RawSink(backend="postgres")
        self.budget_redis = None

    def authorize(self) -> None:
        if not self.key:
            raise ValueError(f"Missing credentials for {self.name}")
        if self.http_client.budget is None:
            self.budget_redis = Redis.from_url(
                settings.redis_url, socket_connect_timeout=3, socket_timeout=3
            )
            self.http_client.budget = ProviderBudget(
                self.budget_redis, self.name, settings.provider_monthly_credits.get(self.name, 0)
            )

    async def request_json(
        self,
        endpoint: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        cost: float = 1,
    ) -> Any:
        self.authorize()
        try:
            response = await self.http_client.get(
                endpoint, params=params, headers=headers, cost=cost
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            status = (
                exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else "transport"
            )
            # Query-string credentials must never reach errors, queue audit or logs.
            error = (
                ProviderUnavailableError
                if status == "transport" or isinstance(status, int) and status >= 500
                else RuntimeError
            )
            raise error(f"{self.name}: upstream request failed ({status})") from None
        await self.raw_sink.save_payload(
            self.name,
            endpoint.split("?")[0],
            response.content,
            metadata={"dataset": endpoint.split("?")[0]},
        )
        return response.json()

    async def close(self) -> None:
        await super().close()
        if self.budget_redis is not None:
            await self.budget_redis.aclose()
