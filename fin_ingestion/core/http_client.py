"""Cliente HTTP assíncrono resiliente com emulação de navegador e retries com backoff."""

import random
from typing import Any
from urllib.parse import urlsplit

import httpx
from redis.asyncio import Redis
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from fin_ingestion.config import settings
from fin_ingestion.core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenException,
)
from fin_ingestion.core.distributed_controls import DistributedSourceControls
from fin_ingestion.core.rate_limiter import AsyncRateLimiter


class RateLimitExceededError(Exception):
    """Lançada quando a fonte externa retorna HTTP 429 Too Many Requests."""

    def __init__(self, message: str, retry_after: int = 60) -> None:
        super().__init__(message)
        self.retry_after = retry_after


RateLimitExceededException = RateLimitExceededError


class AuthenticationBlockedError(Exception):
    """Lançada quando a fonte externa rejeita credenciais ou bloqueia acesso (HTTP 401/403)."""
    pass


AuthenticationBlockedException = AuthenticationBlockedError

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
        self._redis = None
        self.distributed = None
        if settings.distributed_source_controls:
            self._redis = Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=2)
            # All collectors for one upstream host share protection across replicas.
            source = urlsplit(base_url or '').netloc.lower() or name
            self.distributed = DistributedSourceControls(
                self._redis, source, self.rate_limiter.rate, self.rate_limiter.capacity,
                self.circuit_breaker.failure_threshold,
                max(self.circuit_breaker.recovery_timeout_seconds, timeout + 10),
            )

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
        if self._redis is not None:
            await self._redis.aclose()

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
        epoch = None
        if self.distributed is not None:
            epoch = await self.distributed.before_request()
        elif not self.circuit_breaker.can_execute():
            raise CircuitBreakerOpenException(
                f"Circuito aberto para {self.name}. Tentativa cancelada."
            )

        if self.distributed is None:
            await self.rate_limiter.acquire()
        client = await self.get_client()

        try:
            response = await client.get(endpoint, params=params, headers=headers)
        except (httpx.NetworkError, httpx.TimeoutException):
            if self.distributed is not None:
                await self.distributed.after_request(epoch, success=False)
            else:
                self.circuit_breaker.record_failure()
            raise
        blocked = response.status_code in (401, 403)
        failed = blocked or response.status_code == 429 or response.status_code >= 500
        if self.distributed is not None:
            await self.distributed.after_request(epoch, success=not failed, blocked=blocked)
        elif blocked:
            self.circuit_breaker.open()
        elif failed:
            self.circuit_breaker.record_failure()
        else:
            self.circuit_breaker.record_success()
        self._handle_response_status(response)
        return response

    @retry(
        retry=retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException)),
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        reraise=True,
    )
    async def post(
        self,
        endpoint: str,
        data: Any = None,
        json: Any = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        epoch = None
        if self.distributed is not None:
            epoch = await self.distributed.before_request()
        elif not self.circuit_breaker.can_execute():
            raise CircuitBreakerOpenException(
                f"Circuito aberto para {self.name}. Tentativa cancelada."
            )

        if self.distributed is None:
            await self.rate_limiter.acquire()
        client = await self.get_client()

        try:
            response = await client.post(endpoint, data=data, json=json, headers=headers)
        except (httpx.NetworkError, httpx.TimeoutException):
            if self.distributed is not None:
                await self.distributed.after_request(epoch, success=False)
            else:
                self.circuit_breaker.record_failure()
            raise
        blocked = response.status_code in (401, 403)
        failed = blocked or response.status_code == 429 or response.status_code >= 500
        if self.distributed is not None:
            await self.distributed.after_request(epoch, success=not failed, blocked=blocked)
        elif blocked:
            self.circuit_breaker.open()
        elif failed:
            self.circuit_breaker.record_failure()
        else:
            self.circuit_breaker.record_success()
        self._handle_response_status(response)
        return response

    def _handle_response_status(self, response: httpx.Response) -> None:
        """Avalia códigos HTTP de erro e aciona circuit breaker / cooldown."""
        if response.status_code == 429:
            retry_after_hdr = response.headers.get("Retry-After")
            wait_sec = int(retry_after_hdr) if retry_after_hdr and retry_after_hdr.isdigit() else 60
            raise RateLimitExceededException(
                f"Rate limit (429) excedido para {self.name}. Retry-After: {wait_sec}s.",
                retry_after=wait_sec,
            )
        elif response.status_code in (401, 403):
            # Bloqueio cadastral ou de IP: abre o circuito imediatamente
            raise AuthenticationBlockedException(
                f"Acesso bloqueado ({response.status_code}) para {self.name}."
            )
        elif response.status_code >= 500:
            response.raise_for_status()
