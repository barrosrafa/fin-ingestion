"""Configurações centrais do fin-ingestion via variáveis de ambiente."""

from pydantic import Field
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

    # dbt Configuration
    dbt_profiles_dir: str = Field(default="./dbt")
    dbt_project_dir: str = Field(default="./dbt")
    dbt_target: str = Field(default="dev")

    # Tokens & Sessions opcionais
    clube_fii_jwt_token: str | None = None
    status_invest_cookie: str | None = None
    brick360_api_key: str | None = None


settings = Settings()
