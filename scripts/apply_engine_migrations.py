"""Checksum-ledger migration of the additive scheduled-engine stage only.

Default is read-only preflight. --apply atomically applies the reviewed stage, with
short lock timeout and no deletion, reclassification or activation of ingestion.
"""

import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class SchemaCompatibilityError(RuntimeError):
    pass


async def migrate(dsn: str, db_repo: Path, apply=False):
    files = sorted((db_repo / "sql/17_scheduled_engine_v4.5").glob("*.sql"))
    if not files:
        raise ValueError("Scheduled-engine migrations not found")
    conn = await asyncpg.connect(dsn, timeout=15)
    try:
        async with conn.transaction(readonly=not apply):
            from fin_ingestion.dw.silver_writer import SPECS

            for table, columns, _ in SPECS.values():
                schema, name = table.split(".")
                present = await conn.fetch(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2",
                    schema,
                    name,
                )
                if present:
                    missing = set(columns.split()) - {r["column_name"] for r in present}
                    if missing:
                        raise SchemaCompatibilityError(
                            f"Incompatible existing {table}: missing {sorted(missing)}"
                        )
            required = [
                "core.ativo",
                "market.cotacao_intraday",
                "ingestion.ingestion_policy",
                "ingestion.source_policy",
                "system.schema_migrations",
            ]
            if apply:
                # Operational application metadata is owned by fin-ingestion, not the
                # schema-only fin-db repository. Bootstrap is part of this transaction.
                await conn.execute("CREATE SCHEMA IF NOT EXISTS system")
                await conn.execute("CREATE TABLE IF NOT EXISTS system.schema_migrations(version varchar(255) PRIMARY KEY,checksum varchar(64) NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())")
            for table in required:
                if not await conn.fetchval("SELECT to_regclass($1)::text", table):
                    raise RuntimeError(f"Prerequisite missing: {table}")
            if apply:
                await conn.execute(
                    "SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='120s'"
                )
                await conn.execute("SELECT pg_advisory_xact_lock(742198273)")
            report = []
            for path in files:
                version = f"17_scheduled_engine_v4.5/{path.name}"
                checksum = hashlib.sha256(path.read_bytes()).hexdigest()
                stored = await conn.fetchval(
                    "SELECT checksum FROM system.schema_migrations WHERE version=$1", version
                )
                if stored and stored != checksum:
                    raise RuntimeError(f"Checksum drift: {version}")
                state = "already_applied" if stored else "pending"
                if apply and not stored:
                    await conn.execute(path.read_text())
                    await conn.execute(
                        "INSERT INTO system.schema_migrations(version,checksum) VALUES($1,$2)",
                        version,
                        checksum,
                    )
                    state = "applied"
                report.append({"migration": version, "sha256": checksum, "state": state})
            return {"mode": "apply" if apply else "read_only_preflight", "migrations": report}
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-repo", type=Path, default=ROOT / "tests/schema_contract")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    from fin_ingestion.config import settings

    dsn = settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
    try:
        print(json.dumps(asyncio.run(migrate(dsn, args.db_repo, args.apply)), indent=2))
    except Exception as exc:
        # Do not echo credentials from connection errors.
        detail = (
            str(exc) if isinstance(exc, (asyncpg.PostgresError, SchemaCompatibilityError)) else None
        )
        print(
            json.dumps({"status": "FAILED", "error_type": type(exc).__name__, "sql_detail": detail})
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
