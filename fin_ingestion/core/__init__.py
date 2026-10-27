"""Core utilities: HTTP Client resiliente, Rate Limiting, Circuit Breaker e Raw Sink."""

from fin_ingestion.core.base_collector import BaseCollector, CollectorResult
from fin_ingestion.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from fin_ingestion.core.http_client import ResilientHttpClient
from fin_ingestion.core.rate_limiter import AsyncRateLimiter
from fin_ingestion.core.raw_sink import RawSink

__all__ = [
    "BaseCollector",
    "CollectorResult",
    "CircuitBreaker",
    "CircuitBreakerOpenException",
    "ResilientHttpClient",
    "AsyncRateLimiter",
    "RawSink",
]
