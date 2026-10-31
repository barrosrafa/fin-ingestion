"""Bounded retries for opening connections to a suspended PostgreSQL compute.

Only connection establishment is retried. Replaying an arbitrary transaction
after an uncertain commit would require its own idempotency contract.
"""

import asyncio

import asyncpg
import structlog
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = structlog.get_logger(__name__)

TRANSIENT_CONNECTION_ERRORS = (
    OSError,
    asyncio.TimeoutError,
    asyncpg.CannotConnectNowError,
    asyncpg.TooManyConnectionsError,
    asyncpg.ConnectionDoesNotExistError,
)


async def create_pool_with_retry(dsn: str) -> asyncpg.Pool:
    """Wake the compute with up to six 20s attempts and capped full jitter.

    Authentication and schema errors fail immediately. Logs deliberately omit
    the DSN and exception message, which may contain credentials.
    """
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(6),
        wait=wait_random_exponential(multiplier=2, max=20),
        retry=retry_if_exception_type(TRANSIENT_CONNECTION_ERRORS),
        reraise=True,
    ):
        with attempt:
            try:
                return await asyncpg.create_pool(
                    dsn=dsn, min_size=1, max_size=5, timeout=20,
                    max_inactive_connection_lifetime=240,
                )
            except TRANSIENT_CONNECTION_ERRORS as exc:
                logger.warning(
                    "Database connection unavailable",
                    attempt=attempt.retry_state.attempt_number,
                    error_type=type(exc).__name__,
                )
                raise


async def check_database_ready(dsn: str) -> None:
    """Establish a real database session before collecting data."""
    pool = await create_pool_with_retry(dsn)
    try:
        async with pool.acquire(timeout=20) as connection:
            await connection.fetchval("SELECT 1", timeout=20)
    finally:
        await pool.close()
