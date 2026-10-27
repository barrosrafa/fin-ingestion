"""Cliente HTTP assíncrono resiliente com emulação de navegador e retries com backoff."""

import random
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from fin_ingestion.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from fin_ingestion.core.rate_limiter import AsyncRateLimiter

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]


class ResilientHttpClient:
    """Cliente HTTP com suporte a pooling, retries, circuit breaker e headers de navegação real."""

    def __init__(
        self,
        name: str,
        base_url: str | None = None,
        rate_limiter: AsyncRateLimiter | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        timeout: float = 30.0,
        cookies: dict[str, str] | None = None,
        custom_headers: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.base_url = base_url
        self.rate_limiter = rate_limiter or AsyncRateLimiter(requests_per_second=3.0)
        self.circuit_breaker = circuit_breaker or CircuitBreaker(name=name)
        self.timeout = timeout
        self.cookies = cookies or {}
        self.custom_headers = custom_headers or {}
        self._client: httpx.AsyncClient | None = None

    async def get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            headers = {
                "User-Agent": random.choice(DEFAULT_USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8,application/json",
                "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"macOS"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                **self.custom_headers,
            }
            # Verifica se suporte a HTTP/2 está disponível
            use_http2 = False
            try:
                import h2  # noqa: F401
                use_http2 = True
            except ImportError:
                use_http2 = False

            self._client = httpx.AsyncClient(
                base_url=self.base_url or "",
                headers=headers,
                cookies=self.cookies,
                timeout=httpx.Timeout(self.timeout),
                follow_redirects=True,
                http2=use_http2,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @retry(
        retry=retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException)),
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        reraise=True,
    )
    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        if not self.circuit_breaker.can_execute():
            raise CircuitBreakerOpenException(
                f"Circuito aberto para {self.name}. Tentativa cancelada."
            )

        await self.rate_limiter.acquire()
        client = await self.get_client()

        try:
            response = await client.get(endpoint, params=params, headers=headers)
            if response.status_code >= 500:
                self.circuit_breaker.record_failure()
            else:
                self.circuit_breaker.record_success()
            return response
        except Exception:
            self.circuit_breaker.record_failure()
            raise

    async def post(
        self,
        endpoint: str,
        data: Any = None,
        json: Any = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        if not self.circuit_breaker.can_execute():
            raise CircuitBreakerOpenException(
                f"Circuito aberto para {self.name}. Tentativa cancelada."
            )

        await self.rate_limiter.acquire()
        client = await self.get_client()

        try:
            response = await client.post(endpoint, data=data, json=json, headers=headers)
            if response.status_code >= 500:
                self.circuit_breaker.record_failure()
            else:
                self.circuit_breaker.record_success()
            return response
        except Exception:
            self.circuit_breaker.record_failure()
            raise
