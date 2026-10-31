"""Read-only workflow preflight; prints no database credentials."""

import asyncio

from fin_ingestion.config import settings
from fin_ingestion.core.database import check_database_ready


def main() -> int:
    try:
        asyncio.run(check_database_ready(
            settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
        ))
    except Exception as exc:
        print(f"Database readiness failed: {type(exc).__name__}")
        return 1
    print("Database session verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
