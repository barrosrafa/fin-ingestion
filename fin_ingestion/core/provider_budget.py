"""Atomic monthly credit ceilings shared across workers. Failed attempts consume quota."""

from datetime import datetime, timezone
from math import isfinite

from redis.asyncio import Redis

RESERVE_LUA = """
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local cost, ceiling = tonumber(ARGV[1]), tonumber(ARGV[2])
if used + cost > ceiling then return -1 end
redis.call('INCRBYFLOAT', KEYS[1], cost)
redis.call('EXPIRE', KEYS[1], 3888000)
return 1
"""


class ProviderBudget:
    def __init__(self, redis: Redis, provider: str, ceiling: int):
        if ceiling <= 0:
            raise ValueError(f"Configure a positive monthly credit ceiling for {provider}")
        self.redis, self.provider, self.ceiling = redis, provider, ceiling

    async def reserve(self, cost: float = 1) -> None:
        if not isfinite(cost) or cost <= 0:
            raise ValueError("Invalid credit cost")
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        key = f"fin:budget:{self.provider}:{month}"
        if int(await self.redis.eval(RESERVE_LUA, 1, key, cost, self.ceiling)) < 0:
            raise RuntimeError(f"Monthly credit ceiling exhausted for {self.provider}")
