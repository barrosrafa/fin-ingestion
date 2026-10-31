"""Real isolated PostgreSQL tests. pgserver is available on the Python 3.12 test runtime."""

import asyncio
from datetime import date
from pathlib import Path

import asyncpg
import pytest

from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.dw.silver_writer import SilverWriter
from fin_ingestion.queue.task_queue import PostgresTaskQueue

DB_REPO = Path(__file__).parent / "schema_contract"


@pytest.fixture
def isolated_database(tmp_path):
    pgserver = pytest.importorskip(
        "pgserver", reason="Real PostgreSQL tests require Python 3.12 pgserver runtime"
    )
    db = pgserver.get_server(tmp_path / "postgres", cleanup_mode="stop")
    try:

        async def prepare():
            conn = await asyncpg.connect(db.get_uri())
            try:
                await conn.execute("""
                    CREATE SCHEMA core; CREATE SCHEMA market; CREATE SCHEMA canonical;
                    CREATE TABLE canonical.financial_statement(statement_id int PRIMARY KEY,legacy_payload text);
                    INSERT INTO canonical.financial_statement VALUES(1,'legacy-untouched');
                    CREATE SCHEMA system;
                    CREATE TABLE system.schema_migrations(version text PRIMARY KEY,checksum text NOT NULL);
                    CREATE TABLE core.ativo(id_ativo int PRIMARY KEY,ticker text UNIQUE);
                    CREATE TABLE core.emissor_companhia(cnpj text NOT NULL,nome_empresarial text);
                    INSERT INTO core.ativo VALUES(1,'PETR4');
                """)
                source = (DB_REPO / "sql/01_core_schema/02_market.sql").read_text()
                start = source.index("CREATE TABLE market.cotacao_intraday")
                await conn.execute(source[start : source.index(");", start) + 2])
                await conn.execute(
                    (
                        DB_REPO
                        / "sql/14_ingestion_control_and_data_quality_v4.2/01_ingestion_control.sql"
                    ).read_text()
                )
                from scripts.apply_engine_migrations import migrate

                report = await migrate(db.get_uri(), DB_REPO, apply=True)
                assert all(m["state"] == "applied" for m in report["migrations"])
            finally:
                await conn.close()

        asyncio.run(prepare())
        yield db.get_uri()
    finally:
        db.cleanup()


@pytest.mark.asyncio
async def test_migration_replay_intraday_and_silver_are_idempotent(isolated_database):
    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=4)
    writer, silver = DWWriter(), SilverWriter(pool)
    writer._pool = pool
    try:
        for _ in range(2):
            assert await writer.write_price_observation(
                "PETR4", 37, "2026-10-30T15:00:00Z", source_name="brapi"
            )
            await silver.write_records(
                "ptax",
                [
                    {
                        "currency": "USD",
                        "quoted_at": "2026-10-30T16:00:00Z",
                        "bulletin": "Fechamento",
                        "buy": 5,
                        "sell": 5.1,
                    }
                ],
            )
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM market.cotacao_intraday") == 1
            assert await conn.fetchval("SELECT count(*) FROM market.ptax_observation") == 1
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM ingestion.ingestion_policy WHERE policy_id LIKE 'scheduled_%' AND is_active"
                )
                == 0
            )
            assert (
                await conn.fetchval(
                    "SELECT legacy_payload FROM canonical.financial_statement WHERE statement_id=1"
                )
                == "legacy-untouched"
            )
            for path in sorted((DB_REPO / "sql/17_scheduled_engine_v4.5").glob("*.sql")):
                await conn.execute(path.read_text())
        from scripts.apply_engine_migrations import migrate

        report = await migrate(isolated_database, DB_REPO, apply=True)
        assert all(m["state"] == "already_applied" for m in report["migrations"])
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_real_queue_claim_is_exclusive_and_enqueue_is_idempotent(isolated_database):
    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=4)
    try:
        a, b = PostgresTaskQueue(pool, "a"), PostgresTaskQueue(pool, "b")
        options = dict(
            policy_id="b3_daily_quotes",
            task_key="test:unique",
            dataset_name="cotacao_diaria",
            reference_date=date(2026, 10, 30),
        )
        assert await a.enqueue_task(**options)
        assert await b.enqueue_task(**options) is None
        claims = await asyncio.gather(a.claim_task(), b.claim_task())
        assert len([task for task in claims if task]) == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_batch_rollback_and_event_corrections_preserve_identity(isolated_database):
    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=2)
    writer = SilverWriter(pool)
    try:
        good = {
            "title": "Tesouro Selic",
            "maturity": "2030-01-01",
            "reference_date": "2026-10-30",
            "buy_price": 100,
        }
        bad = {**good, "title": "Invalid", "buy_price": -1}
        with pytest.raises(asyncpg.CheckViolationError):
            await writer.write_records("tesouro", [good, bad])
        event = {
            "source": "fixture",
            "event_id": "stable-1",
            "ticker": "PETR4",
            "event_type": "dividend",
            "payload": {"amount": 1},
        }
        await writer.write_records("events", [event, event])
        await writer.write_records("events", [{**event, "payload": {"amount": 2}}])
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM market.treasury_quote") == 0
            assert (
                await conn.fetchval("SELECT count(*) FROM canonical.corporate_action_revision") == 2
            )
            assert (
                await conn.fetchval("SELECT count(*) FROM canonical.corporate_action_latest") == 1
            )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_checkpoint_commit_requires_live_lease_and_real_watermark(isolated_database):
    from datetime import datetime, timezone

    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=3)
    queue = PostgresTaskQueue(pool, "atomic-test")
    try:
        await queue.enqueue_task("b3_daily_quotes", "receipt:1", "cotacao_diaria")
        task = await queue.claim_task()
        result = {"total_records": 1, "records_loaded": 1, "watermark_date": "2026-10-29"}
        await queue.finish_success(task, result, datetime.now(timezone.utc), 10)
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT watermark_date FROM ingestion.ingestion_checkpoint"
            ) == date(2026, 10, 29)
            assert (
                await conn.fetchval(
                    "SELECT status FROM ingestion.ingestion_task WHERE task_id=$1", task["task_id"]
                )
                == "COMPLETED"
            )
        with pytest.raises(RuntimeError, match="lease"):
            await queue.finish_success(task, result, datetime.now(timezone.utc), 10)
        async with pool.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM ingestion.ingestion_run WHERE status='SUCCEEDED'"
                )
                == 1
            )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_filtered_worker_does_not_steal_other_datasets(isolated_database, monkeypatch):
    from fin_ingestion.config import settings

    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE ingestion.ingestion_policy SET is_active=true WHERE policy_id='scheduled_intraday'"
            )
        queue = PostgresTaskQueue(pool, "intraday-only")
        await queue.enqueue_task("b3_daily_quotes", "batch:1", "cotacao_diaria")
        await queue.enqueue_task(
            "scheduled_intraday", "intraday:1", "cotacao_intraday", target_entity="PETR4"
        )
        monkeypatch.setattr(settings, "worker_datasets", ["cotacao_intraday"])
        assert (await queue.claim_task())["dataset_name"] == "cotacao_intraday"
        assert await queue.claim_task() is None
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_cvm_latest_document_does_not_resurrect_removed_accounts(isolated_database):
    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=2)
    writer = SilverWriter(pool)
    base = {
        "cnpj": "00000000000100",
        "filing": "dfp",
        "statement": "BPA",
        "scope": "con",
        "reference_date": "2025-12-31",
        "period_start": "not_applicable",
        "period_end": "2025-12-31",
        "exercise_order": "ÚLTIMO",
        "currency": "REAL",
        "scale": "MIL",
        "amount": 100,
        "payload": {},
    }
    try:
        await writer.write_records(
            "financials",
            [{**base, "account": "1", "version": 1}, {**base, "account": "old", "version": 1}],
        )
        await writer.write_records("financials", [{**base, "account": "1", "version": 2}])
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM canonical.cvm_statement") == 3
            assert await conn.fetchval("SELECT count(*) FROM canonical.cvm_statement_latest") == 1
    finally:
        await pool.close()


def test_dbt_selected_gold_models_build_on_real_postgres(isolated_database, tmp_path):
    from scripts.homologate_engine import build_gold

    result = build_gold(isolated_database, str(tmp_path), tmp_path)
    assert result["status"] == "PASSED", (tmp_path / "dbt.log").read_text()[-5000:]


@pytest.mark.asyncio
async def test_real_queue_failure_respects_retry_after_and_releases_lease(isolated_database):
    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=2)
    queue = PostgresTaskQueue(pool, "retry-test")
    try:
        await queue.enqueue_task("b3_daily_quotes", "retry:1", "cotacao_diaria")
        task = await queue.claim_task()
        assert await queue.fail_task(
            task["task_id"], task["lock_token"], "429", retry_delay_seconds=300
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status,lock_token,extract(epoch from (next_run_at-now())) AS delay FROM ingestion.ingestion_task WHERE task_id=$1",
                task["task_id"],
            )
            assert row["status"] == "PENDING" and row["lock_token"] is None
            assert 299 <= row["delay"] <= 330
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_official_session_keeps_uncatalogued_prices_and_replay_identity(isolated_database):
    from unittest.mock import AsyncMock

    from fin_ingestion.pipelines.market_gateway import MarketGateway

    pool = await asyncpg.create_pool(isolated_database, min_size=1, max_size=3)
    gateway = MarketGateway(pool)
    day = date(2026, 10, 30)
    rows = [{"ticker": ticker, "data_pregao": day.isoformat(), "fator_cotacao": 1,
             "preco_abertura": 10, "preco_maximo": 12, "preco_minimo": 9,
             "preco_fechamento": 11, "quantidade_titulos": 100,
             "volume_financeiro": 1100, "numero_negocios": 5}
            for ticker in ("PETR4", "A1AP34")]
    gateway.b3.get_daily_quotes = AsyncMock(return_value=rows)
    # The old serving schema is independently guarded by its writer tests. This
    # test proves real nominal persistence even when no serving ID exists.
    gateway.writer.write_daily_quotes = AsyncMock(return_value=1)
    try:
        for _ in range(2):
            result = await gateway.official_session(day)
            assert result["records_loaded"] == 2
            assert result["serving_records_loaded"] == 1
            assert result["universe"]["unresolved"] == ["A1AP34"]
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM market.nominal_bar") == 2
            assert await conn.fetchval("SELECT count(*) FROM core.ativo") == 1
        serving = gateway.writer.write_daily_quotes.call_args.args[0]
        assert [r["ticker"] for r in serving] == ["PETR4"]
    finally:
        await gateway.close()
        await pool.close()
