"""Apply the additive runtime ledger with a checksum and transaction lock."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from fin_ingestion.config import settings
from fin_ingestion.core.database import create_pool_with_retry

ROOT = Path(__file__).resolve().parents[1]


async def migrate(pool, apply=False):
    report = []
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=not apply):
            if apply:
                await conn.execute("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='120s'")
                await conn.execute("SELECT pg_advisory_xact_lock(742198274)")
            for path in sorted((ROOT / "migrations").glob("*/*.sql")):
                version = str(path.relative_to(ROOT / "migrations"))
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                stored = await conn.fetchval(
                    "SELECT checksum FROM system.schema_migrations WHERE version=$1", version
                )
                if stored and stored != digest:
                    raise RuntimeError("Runtime migration checksum drift")
                state = "already_applied" if stored else "pending"
                if apply and not stored:
                    await conn.execute(path.read_text())
                    if version == "22_cash_dividend_integrity/01_cash_dividends.sql":
                        # Operational refresh belongs to this runner, not schema-only DDL.
                        if await conn.fetchval("SELECT to_regclass('analytics.cagg_provento_mensal') IS NOT NULL"):
                            await conn.execute("REFRESH MATERIALIZED VIEW analytics.cagg_provento_mensal")
                    await conn.execute(
                        "INSERT INTO system.schema_migrations(version,checksum) VALUES($1,$2)",
                        version, digest,
                    )
                    state = "applied"
                report.append({"migration": version, "checksum": digest, "state": state})
    return report


async def run(apply):
    pool = await create_pool_with_retry(
        settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        return await migrate(pool, apply)
    finally:
        await pool.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args.apply)), indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
