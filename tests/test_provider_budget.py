import asyncio
import os
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from redis.asyncio import Redis

from fin_ingestion.collectors.commercial.anbima_feed import AnbimaFeedCollector
from fin_ingestion.collectors.commercial.base import ProviderUnavailableError
from fin_ingestion.collectors.commercial.brapi import BrapiCollector
from fin_ingestion.config import settings
from fin_ingestion.core.provider_budget import ProviderBudget


@pytest.mark.asyncio
@pytest.mark.skipif(not os.environ.get("TEST_REDIS_URL"), reason="Requires isolated Redis")
async def test_monthly_credit_ceiling_is_atomic_across_clients():
    a, b = (
        Redis.from_url(os.environ["TEST_REDIS_URL"]),
        Redis.from_url(os.environ["TEST_REDIS_URL"]),
    )
    provider = "test-" + uuid4().hex
    try:
        x, y = ProviderBudget(a, provider, 4), ProviderBudget(b, provider, 4)
        results = await asyncio.gather(
            x.reserve(2), y.reserve(2), x.reserve(2), return_exceptions=True
        )
        assert len([r for r in results if r is None]) == 2
        assert len([r for r in results if isinstance(r, RuntimeError)]) == 1
    finally:
        keys = [key async for key in a.scan_iter(match=f"fin:budget:{provider}:*")]
        if keys:
            await a.delete(*keys)
        await a.aclose()
        await b.aclose()


@pytest.mark.asyncio
async def test_commercial_errors_never_expose_query_credentials(monkeypatch):
    monkeypatch.setattr(settings, "distributed_source_controls", False)
    collector = BrapiCollector()
    collector.authorize = lambda: None
    request = httpx.Request("GET", "https://example.test?apikey=sensitive-value")
    collector.http_client.get = AsyncMock(
        side_effect=httpx.HTTPStatusError(
            "secret URL", request=request, response=httpx.Response(503, request=request)
        )
    )
    with pytest.raises(ProviderUnavailableError) as error:
        await collector.request_json("/test", params={"apikey": "sensitive-value"})
    assert "sensitive-value" not in str(error.value)


@pytest.mark.asyncio
async def test_anbima_token_is_cached_then_renewed_on_expiry(monkeypatch):
    monkeypatch.setattr(settings, "distributed_source_controls", False)
    monkeypatch.setattr(settings, "anbima_client_id", SecretStr("id"))
    monkeypatch.setattr(settings, "anbima_client_secret", SecretStr("secret"))
    collector = AnbimaFeedCollector()
    collector.authorize = lambda: None

    def response(token):
        return httpx.Response(
            200,
            request=httpx.Request("POST", "https://example.test"),
            json={"access_token": token, "expires_in": 120},
        )

    collector.auth.post = AsyncMock(side_effect=[response("one"), response("two")])
    assert await collector.access_token() == "one"
    assert await collector.access_token() == "one"
    collector.expires_at = 0
    assert await collector.access_token() == "two"
    assert collector.auth.post.await_count == 2
