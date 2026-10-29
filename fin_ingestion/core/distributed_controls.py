"""Redis-coordinated source protection. Redis failures stop outbound requests."""

import asyncio
import hashlib
from math import isfinite

from redis.asyncio import Redis

from fin_ingestion.core.circuit_breaker import CircuitBreakerOpenError

BUCKET_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
local capacity, rate = tonumber(ARGV[1]), tonumber(ARGV[2])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
local tokens = math.min(capacity, (tonumber(d[1]) or capacity) + math.max(0, now-(tonumber(d[2]) or now))*rate)
local delay = 0
if tokens >= 1 then tokens = tokens-1 else delay = (1-tokens)/rate end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
redis.call('EXPIRE', KEYS[1], math.ceil(capacity/rate)+60)
return math.ceil(delay*1000)
"""

# Generation numbers prevent old in-flight successes from closing a newly opened circuit.
BREAKER_LUA = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
local d = redis.call('HMGET', KEYS[1], 'epoch', 'failures', 'until')
local epoch, failures, until_at = tonumber(d[1]) or 0, tonumber(d[2]) or 0, tonumber(d[3]) or 0
local action, cooldown = ARGV[1], tonumber(ARGV[4])
if action == 'acquire' then
  if until_at > now then return -1 end
  if until_at > 0 then
    epoch = epoch+1
    redis.call('HSET', KEYS[1], 'epoch', epoch, 'until', now+cooldown)
  end
  return epoch
end
if tonumber(ARGV[2]) ~= epoch then return 0 end
if action == 'success' then
  -- Only a half-open probe (new epoch) can reset an open circuit.
  failures, until_at = 0, 0
else
  failures = failures+1
  if action == 'block' or until_at > 0 or failures >= tonumber(ARGV[3]) then
    epoch, until_at = epoch+1, now+cooldown
  end
end
redis.call('HSET', KEYS[1], 'epoch', epoch, 'failures', failures, 'until', until_at)
return 1
"""


class DistributedSourceControls:
    def __init__(
        self,
        redis: Redis,
        source: str,
        rate: float,
        capacity: int,
        threshold: int = 5,
        cooldown: float = 60,
    ):
        if not isfinite(rate) or rate <= 0 or capacity < 1 or threshold < 1 or cooldown <= 0:
            raise ValueError("Invalid source protection limits")
        self.redis = redis
        prefix = "fin:ingestion:" + hashlib.sha256(source.encode()).hexdigest()
        self.bucket_key = prefix + ":bucket"
        self.breaker_key = prefix + ":breaker"
        self.rate, self.capacity = rate, capacity
        self.threshold, self.cooldown = threshold, cooldown

    async def before_request(self) -> int:
        while True:
            delay = int(
                await self.redis.eval(BUCKET_LUA, 1, self.bucket_key, self.capacity, self.rate)
            )
            if delay == 0:
                break
            await asyncio.sleep(delay / 1000)
        epoch = int(
            await self.redis.eval(
                BREAKER_LUA, 1, self.breaker_key, "acquire", 0, self.threshold, self.cooldown
            )
        )
        if epoch < 0:
            raise CircuitBreakerOpenError("Shared source circuit is open")
        return epoch

    async def after_request(self, epoch: int, success: bool, blocked: bool = False):
        action = "block" if blocked else ("success" if success else "failure")
        await self.redis.eval(
            BREAKER_LUA, 1, self.breaker_key, action, epoch, self.threshold, self.cooldown
        )
