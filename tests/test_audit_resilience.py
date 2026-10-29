import asyncio
import os
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from fin_ingestion.core.circuit_breaker import CircuitBreakerOpenError, CircuitState
from fin_ingestion.core.distributed_controls import DistributedSourceControls
from fin_ingestion.core.http_client import (
    AuthenticationBlockedError,
    RateLimitExceededError,
    ResilientHttpClient,
)
from fin_ingestion.core.rate_limiter import AsyncRateLimiter


@pytest.mark.parametrize("rate,burst", [(0, 1), (-1, 5), (float("inf"), 5), (1, 0)])
def test_invalid_rate_rejected(rate, burst):
    with pytest.raises(ValueError):
        AsyncRateLimiter(rate, burst)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize(
    "status,error",
    [
        (429, RateLimitExceededError),
        (503, httpx.HTTPStatusError),
        (403, AuthenticationBlockedError),
    ],
)
async def test_errors_count_once_and_stop_on_block(method, status, error):
    client = ResilientHttpClient("test")
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status, request=request))
    )
    try:
        with pytest.raises(error):
            await getattr(client, method)("https://example.test")
        if status == 403:
            assert client.circuit_breaker.state == CircuitState.OPEN
            with pytest.raises(CircuitBreakerOpenError):
                await getattr(client, method)("https://example.test")
        else:
            assert client.circuit_breaker.failure_count == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_redis_outage_prevents_external_request():
    client = ResilientHttpClient("test")
    client.distributed = AsyncMock()
    client.distributed.before_request.side_effect = ConnectionError()
    client.get_client = AsyncMock()
    with pytest.raises(ConnectionError):
        await client.get("https://example.test")
    client.get_client.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="Requires isolated test Redis")
async def test_shared_bucket_breaker_and_single_probe():
    redis_a, redis_b = (
        Redis.from_url(os.environ["TEST_REDIS_URL"]),
        Redis.from_url(os.environ["TEST_REDIS_URL"]),
    )
    source = "test:" + uuid4().hex
    a = DistributedSourceControls(redis_a, source, 100, 100, threshold=2, cooldown=60)
    b = DistributedSourceControls(redis_b, source, 100, 100, threshold=2, cooldown=60)
    try:
        old_permit = await b.before_request()
        await a.after_request(await a.before_request(), False)
        await b.after_request(await b.before_request(), False)
        # A late success from before the OPEN transition must not close the circuit.
        await b.after_request(old_permit, True)
        with pytest.raises(CircuitBreakerOpenError):
            await a.before_request()
        # Expire cooldown without sleeping, then race independent replicas for a probe.
        await redis_a.hset(a.breaker_key, "until", 1)
        permits = await asyncio.gather(
            a.before_request(), b.before_request(), return_exceptions=True
        )
        valid = [p for p in permits if isinstance(p, int)]
        assert len(valid) == 1
        await a.after_request(valid[0], True)
        assert isinstance(await b.before_request(), int)
        assert await redis_a.ttl(a.bucket_key) > 0
    finally:
        await redis_a.delete(a.bucket_key, a.breaker_key)
        await redis_a.aclose()
        await redis_b.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("TEST_REDIS_URL"), reason="Requires isolated test Redis")
async def test_global_burst_does_not_multiply_with_replicas():
    from fin_ingestion.core.distributed_controls import BUCKET_LUA

    a, b = (
        Redis.from_url(os.environ["TEST_REDIS_URL"]),
        Redis.from_url(os.environ["TEST_REDIS_URL"]),
    )
    key = "test:bucket:" + uuid4().hex
    try:
        results = await asyncio.gather(
            *[(a if i % 2 else b).eval(BUCKET_LUA, 1, key, 5, 0.01) for i in range(40)]
        )
        assert results.count(0) == 5
    finally:
        await a.delete(key)
        await a.aclose()
        await b.aclose()
