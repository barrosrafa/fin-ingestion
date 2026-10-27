"""Limitador assíncrono de taxa (Rate Limiter) baseado em token bucket."""

import asyncio
import time


class AsyncRateLimiter:
    """Controla a cadência de requisições por segundo para respeitar limites de APIs."""

    def __init__(self, requests_per_second: float = 2.0, burst: int = 5) -> None:
        self.rate = requests_per_second
        self.capacity = burst
        self.tokens = float(burst)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.last_update = now

                # Reabastece tokens proporcionalmente ao tempo decorrido
                self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                # Calcula quanto tempo dormir até ter pelo menos 1 token
                sleep_time = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
