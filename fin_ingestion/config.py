"""Configurações centrais do fin-ingestion via variáveis de ambiente."""

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database URLs
    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/fin",
        description="URL síncrona do PostgreSQL (usada pelo dbt e scripts)",
    )
    database_url_async: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/fin",
        description="URL assíncrona do PostgreSQL (usada pelo motor de ingestão)",
    )

    # Redis URL
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="URL de conexão ao Redis",
    )

    distributed_source_controls: bool = Field(default=False)

    # Ingestion Engine Parameters
    ingestion_env: str = Field(default="development")
    log_level: str = Field(default="INFO")
    max_concurrent_requests: int = Field(default=10)
    default_timeout_seconds: float = Field(default=30.0)
    enable_raw_landing: bool = Field(default=True)
    raw_landing_backend: Literal["postgres", "local"] = "postgres"
    raw_landing_dir: str = "./data/bronze"

    # dbt Configuration
    dbt_profiles_dir: str = Field(default="./dbt")
    dbt_project_dir: str = Field(default="./dbt")
    dbt_target: str = Field(default="dev")

    # Tokens & Sessions opcionais
    clube_fii_jwt_token: str | None = None
    status_invest_cookie: str | None = None
    brick360_api_key: str | None = None
    brapi_api_key: SecretStr | None = None
    massive_api_key: SecretStr | None = None
    fmp_api_key: SecretStr | None = None
    twelve_data_api_key: SecretStr | None = None
    anbima_client_id: SecretStr | None = None
    anbima_client_secret: SecretStr | None = None
    provider_monthly_credits: dict[str, int] = Field(default_factory=dict)
    brapi_batch_size: int = Field(default=1, ge=1, le=20)
    brapi_raw_history: bool = False
    intraday_enabled: bool = False
    intraday_tickers: list[str] = Field(default_factory=list)
    worker_datasets: list[str] = Field(default_factory=list)
    intraday_max_age_seconds: int = Field(default=1200, ge=1)


settings = Settings()
