import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest

from fin_ingestion.config import settings
from fin_ingestion.operations.reliable_runtime import (
    claim,
    due_slots,
    execute_job,
    finish,
    materialize,
    parameters,
)
from scripts.apply_runtime_migrations import migrate


@pytest.fixture
def runtime_db(tmp_path):
    pgserver = pytest.importorskip("pgserver")
    db = pgserver.get_server(tmp_path / "postgres", cleanup_mode="stop")

    async def prepare():
        pool = await asyncpg.create_pool(db.get_uri(), min_size=1, max_size=2)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "CREATE SCHEMA ingestion; CREATE SCHEMA system; "
                    "CREATE TABLE system.schema_migrations(version text PRIMARY KEY,checksum text)"
                )
            await migrate(pool, apply=True)
        finally:
            await pool.close()

    asyncio.run(prepare())
    try:
        yield db.get_uri()
    finally:
        db.cleanup()


def test_due_slots_preserve_all_missed_occurrences_and_timezone():
    slots = list(due_slots(
        "0 9 * * 1-5", datetime(2026, 10, 28, 12, tzinfo=timezone.utc),
        datetime(2026, 10, 30, 13, tzinfo=timezone.utc),
    ))
    assert slots == [datetime(2026, 10, day, 12, tzinfo=timezone.utc) for day in (29, 30)]


@pytest.mark.asyncio
async def test_real_migration_replay_and_atomic_catchup(runtime_db, monkeypatch):
    monkeypatch.setattr(settings, "intraday_enabled", False)
    pool = await asyncpg.create_pool(runtime_db, min_size=1, max_size=4)
    try:
        assert all(r["state"] == "already_applied" for r in await migrate(pool, apply=True))
        first = datetime(2026, 10, 28, 22, tzinfo=timezone.utc)
        await materialize(pool, first)
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT count(*) FROM ingestion.runtime_job")
        assert await materialize(pool, first) == 0
        assert count > 0
        await asyncio.gather(*(materialize(pool, datetime(2026, 10, 30, 23, tzinfo=timezone.utc)) for _ in range(2)))
        async with pool.acquire() as conn:
            slots = await conn.fetch(
                "SELECT scheduled_at FROM ingestion.runtime_job WHERE profile='daily' ORDER BY scheduled_at"
            )
        assert [r["scheduled_at"].day for r in slots] == [28, 29, 30]
    finally:
        await pool.close()


async def enqueue(pool, profile="morning", scheduled=None):
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO ingestion.runtime_job(profile,scheduled_at,reference_date,partition_key,parameters) "
            "VALUES($1,$2,CURRENT_DATE,'test',$3::jsonb) RETURNING job_id",
            profile, scheduled or datetime(2026, 10, 30, 12, tzinfo=timezone.utc),
            json.dumps(parameters()),
        )


@pytest.mark.asyncio
async def test_claim_is_exclusive_and_crashed_worker_is_recovered(runtime_db):
    pool = await asyncpg.create_pool(runtime_db, min_size=1, max_size=4)
    try:
        job_id = await enqueue(pool)
        claimed = await asyncio.gather(claim(pool), claim(pool))
        assert len([r for r in claimed if r]) == 1
        old = next(r for r in claimed if r)
        async with pool.acquire() as conn:
            await conn.execute("UPDATE ingestion.runtime_job SET locked_until=now()-interval '1 second'")
        new = await claim(pool)
        assert new["job_id"] == job_id and new["lock_token"] != old["lock_token"]
        with pytest.raises(RuntimeError, match="lease lost"):
            await finish(pool, old, "SUCCEEDED")
        await finish(pool, new, "SUCCEEDED", {"status": "SUCCEEDED"})
    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "SKIPPED_OUTSIDE_WINDOW"])
async def test_success_requires_confirmed_receipt_and_failures_stay_durable(runtime_db, tmp_path, status):
    pool = await asyncpg.create_pool(runtime_db, min_size=1, max_size=2)

    async def fake_execute(profile, reference_date, output, scheduled_at):
        assert reference_date is not None and scheduled_at is not None
        output.write_text(json.dumps({"status": status}))
        return status != "FAILED"

    try:
        await enqueue(pool)
        job = await claim(pool)
        with patch("fin_ingestion.operations.reliable_runtime.execute", side_effect=fake_execute):
            actual = await execute_job(pool, job, tmp_path)
        assert actual == ("SUCCEEDED" if status == "SUCCEEDED" else "PENDING")
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ingestion.runtime_job")
            assert row["lock_token"] is None
            if status != "SUCCEEDED":
                assert row["next_attempt_at"] > await conn.fetchval("SELECT now()")
                assert await claim(pool) is None
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_stale_intraday_expires_without_provider_request(runtime_db, tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "intraday_enabled", True)
    pool = await asyncpg.create_pool(runtime_db, min_size=1, max_size=2)
    try:
        await enqueue(pool, "intraday", datetime.now(timezone.utc) - timedelta(days=3))
        job = await claim(pool)
        with patch("fin_ingestion.operations.reliable_runtime.execute", new_callable=AsyncMock) as provider:
            assert await execute_job(pool, job, tmp_path) == "EXPIRED"
            provider.assert_not_called()
    finally:
        await pool.close()


def test_runtime_schema_matches_fin_db_authority():
    root = Path(__file__).resolve().parents[1]
    schema = "sql/21_ingestion_schedule_reliability/01_runtime_ledger.sql"
    authority = root.parent / "fin" / schema
    if authority.exists():
        assert (root / "migrations" / Path(schema).relative_to("sql")).read_bytes() == authority.read_bytes()


@pytest.mark.asyncio
async def test_retry_after_is_preserved_and_exhausted_crashes_stop(runtime_db, tmp_path):
    pool = await asyncpg.create_pool(runtime_db, min_size=1, max_size=2)
    try:
        job_id = await enqueue(pool)
        job = await claim(pool)
        await finish(pool, job, "PENDING", retry_after=7200)
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT extract(epoch from(next_attempt_at-now()))>=7199 FROM ingestion.runtime_job"
            )
            await conn.execute(
                "UPDATE ingestion.runtime_job SET attempts=12,next_attempt_at=now() WHERE job_id=$1", job_id
            )
        exhausted = await claim(pool)
        with patch("fin_ingestion.operations.reliable_runtime.execute", new_callable=AsyncMock) as provider:
            assert await execute_job(pool, exhausted, tmp_path) == "FAILED"
            provider.assert_not_called()
    finally:
        await pool.close()
