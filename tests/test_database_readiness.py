from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from fin_ingestion.core.database import check_database_ready, create_pool_with_retry


@pytest.mark.asyncio
async def test_suspended_compute_recovers_without_replaying_writes():
    pool = MagicMock()
    with (
        patch("fin_ingestion.core.database.asyncpg.create_pool", new_callable=AsyncMock) as create,
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        create.side_effect = [OSError("offline"), asyncpg.CannotConnectNowError(), pool]
        assert await create_pool_with_retry("postgresql://unused") is pool
        assert create.await_count == 3
        assert create.call_args.kwargs["timeout"] == 20


@pytest.mark.asyncio
async def test_unavailable_compute_exhausts_bounded_attempts():
    with (
        patch("fin_ingestion.core.database.asyncpg.create_pool", new_callable=AsyncMock) as create,
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        create.side_effect = OSError("offline")
        with pytest.raises(OSError):
            await create_pool_with_retry("postgresql://unused")
        assert create.await_count == 6


@pytest.mark.asyncio
async def test_wrong_credentials_fail_immediately():
    with patch("fin_ingestion.core.database.asyncpg.create_pool", new_callable=AsyncMock) as create:
        create.side_effect = asyncpg.InvalidPasswordError()
        with pytest.raises(asyncpg.InvalidPasswordError):
            await create_pool_with_retry("postgresql://unused")
        assert create.await_count == 1


@pytest.mark.asyncio
async def test_readiness_executes_query_and_closes_pool_on_failure():
    pool = MagicMock()
    conn = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.close = AsyncMock()
    conn.fetchval.side_effect = RuntimeError("query failed")
    with patch("fin_ingestion.core.database.create_pool_with_retry", AsyncMock(return_value=pool)):
        with pytest.raises(RuntimeError):
            await check_database_ready("postgresql://unused")
    conn.fetchval.assert_awaited_once_with("SELECT 1", timeout=20)
    pool.close.assert_awaited_once()
