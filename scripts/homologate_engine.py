"""Run the full suite with an ephemeral Redis and optional one-symbol brapi canary.

Usage on Python 3.12 (pgserver wheel): python scripts/homologate_engine.py
Never reads production database credentials into test subprocesses.
"""

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def build_gold(dsn, temporary, output):
    import yaml

    parts = urlsplit(dsn)
    query = parse_qs(parts.query)
    profile = {
        "fin": {
            "target": "isolated",
            "outputs": {
                "isolated": {
                    "type": "postgres",
                    "host": query.get("host", [parts.hostname or "localhost"])[0],
                    "port": int(query.get("port", [parts.port or 5432])[0]),
                    "user": parts.username or "postgres",
                    "password": parts.password or "",
                    "dbname": parts.path.strip("/"),
                    "schema": "public",
                    "threads": 2,
                    "sslmode": "disable",
                }
            },
        }
    }
    profile_dir = Path(temporary) / "profiles"
    profile_dir.mkdir()
    (profile_dir / "profiles.yml").write_text(yaml.safe_dump(profile))
    dbt = str(Path(sys.executable).with_name("dbt"))
    deps = subprocess.run(
        [dbt, "deps", "--project-dir", str(ROOT / "dbt")], capture_output=True, text=True
    )
    build = subprocess.run(
        [
            dbt,
            "build",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(profile_dir),
            "--target",
            "isolated",
            "--target-path",
            str(Path(temporary) / "dbt-target"),
            "--log-path",
            str(Path(temporary) / "dbt-logs"),
            "--select",
            "+fct_cvm_statement_v2",
            "fct_nominal_bar",
            "fct_ptax_boletim",
            "fct_focus_expectativa",
            "fct_tesouro_taxa",
            "fct_debenture_indicativa",
            "fct_external_fundamental",
            "fct_corporate_action_latest",
            "stg_sgs_observation",
        ],
        capture_output=True,
        text=True,
    )
    (output / "dbt.log").write_text(deps.stdout + deps.stderr + build.stdout + build.stderr)
    return {
        "status": "PASSED" if deps.returncode == build.returncode == 0 else "FAILED",
        "build_exit_code": build.returncode,
    }


async def canary(redis_url, output, live_public=False):
    import asyncpg
    import pgserver

    from fin_ingestion.config import settings
    from fin_ingestion.pipelines.market_gateway import MarketGateway
    from fin_ingestion.pipelines.provider_pipeline import ProviderPipeline

    settings.redis_url = redis_url
    settings.distributed_source_controls = True
    settings.provider_monthly_credits = {"brapi": 10}
    settings.raw_landing_backend = "local"
    settings.raw_landing_dir = str(output / "bronze")
    settings.enable_raw_landing = True
    settings.intraday_enabled = True
    settings.brapi_batch_size = 1
    temporary = tempfile.TemporaryDirectory(prefix="fin-live-canary-pg-")
    db = pgserver.get_server(Path(temporary.name) / "postgres", cleanup_mode="stop")
    pool = await asyncpg.create_pool(db.get_uri(), min_size=1, max_size=3)
    db_repo = ROOT / "tests/schema_contract"
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE SCHEMA core; CREATE SCHEMA market; CREATE TABLE core.ativo(id_ativo int PRIMARY KEY,ticker text UNIQUE); INSERT INTO core.ativo VALUES(1,'PETR4')"
        )
        source = (db_repo / "sql/01_core_schema/02_market.sql").read_text()
        start = source.index("CREATE TABLE market.cotacao_intraday")
        await conn.execute(source[start : source.index(");", start) + 2])
        start = source.index("CREATE TABLE market.cotacao_diaria")
        await conn.execute(source[start : source.index(");", start) + 2])
        await conn.execute(
            (db_repo / "sql/17_scheduled_engine_v4.5/01_expand_silver.sql").read_text()
        )
    gateway = MarketGateway(pool)
    try:
        # Archive a real provider snapshot under its actual timestamp. Outside trading
        # hours it must not be advertised as a fresh current observation.
        payload = await gateway.brapi.extract(tickers=["PETR4"], endpoint="quote")
        records = gateway.brapi.parse(payload)
        from datetime import datetime, timezone

        age = (datetime.now(timezone.utc) - records[0]["timestamp"]).total_seconds()
        freshness = (
            "fresh" if 0 <= age <= settings.intraday_max_age_seconds else "rejected_as_current"
        )
        for record in records:
            await gateway.writer.write_price_observation(
                record["ticker"], record["price"], record["timestamp"], source_name="brapi"
            )
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT timestamp,preco_atual FROM market.cotacao_intraday")
        await gateway.writer.write_price_observation(
            "PETR4", row["preco_atual"], row["timestamp"], source_name="brapi"
        )
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT count(*) FROM market.cotacao_intraday")
        assert count == 1
        report = {
            "status": "PASSED",
            "provider": "brapi",
            "endpoint": "quote",
            "symbols": len(records),
            "timestamp": row["timestamp"].isoformat(),
            "snapshot_rows_after_replay": count,
            "redis_budget": "enforced",
            "raw_history": "not_requested",
            "current_freshness": freshness,
        }
        if live_public:
            report["public_sources"] = {}
            for dataset, day in (
                ("ptax", "2026-10-29"),
                ("focus", "2026-10-23"),
                ("tesouro", "2026-10-29"),
                ("sgs", "2026-10-29"),
            ):
                try:
                    result = await ProviderPipeline(dataset, pool).run(reference_date=day)
                    report["public_sources"][dataset] = {
                        "status": "PASSED",
                        "records_loaded": result["records_loaded"],
                    }
                except Exception as exc:
                    report["public_sources"][dataset] = {
                        "status": "FAILED",
                        "error_type": type(exc).__name__,
                        "detail": str(exc),
                    }
            try:
                from datetime import date
                result=await gateway.eod(["PETR4"],date(2026,10,29))
                report["public_sources"]["b3_eod"]={"status":"PASSED","records_loaded":result["records_loaded"]}
            except Exception as exc:
                report["public_sources"]["b3_eod"]={"status":"FAILED","error_type":type(exc).__name__,"detail":str(exc)}
            try:
                result = await ProviderPipeline("cvm_dfp", pool).run(
                    reference_date="2026-10-30", year=2025, cnpjs=["33000167000101"]
                )
                report["public_sources"]["cvm_dfp"] = {
                    "status": "PASSED",
                    "records_loaded": result["records_loaded"],
                }
            except Exception as exc:
                report["public_sources"]["cvm_dfp"] = {
                    "status": "FAILED",
                    "error_type": type(exc).__name__,
                    "detail": str(exc),
                }
            report["dbt"] = build_gold(db.get_uri(), temporary.name, output)
            if any(item["status"] != "PASSED" for item in report["public_sources"].values()):
                report["status"] = "PARTIAL"
            if report["dbt"]["status"] != "PASSED":
                report["status"] = "PARTIAL"
        return report
    except Exception as exc:
        return {"status": "FAILED", "error_type": type(exc).__name__, "detail": str(exc)}
    finally:
        await gateway.close()
        await pool.close()
        db.cleanup()
        temporary.cleanup()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-server", type=Path, required=True)
    parser.add_argument("--live-brapi", action="store_true")
    parser.add_argument("--live-public", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "data/homologation")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="fin-redis-homologation-") as temporary:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        redis_url = f"redis://127.0.0.1:{port}/0"
        log = Path(temporary) / "redis.log"
        with log.open("w") as stream:
            process = subprocess.Popen(
                [
                    str(args.redis_server),
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--dir",
                    temporary,
                ],
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
        try:
            from redis import Redis

            client = Redis.from_url(redis_url)
            for _ in range(100):
                try:
                    if client.ping():
                        break
                except Exception:
                    time.sleep(0.05)
            else:
                raise RuntimeError("Isolated Redis startup failed")
            client.close()
            env = {
                **os.environ,
                "TEST_REDIS_URL": redis_url,
                "REDIS_URL": redis_url,
                "DATABASE_URL": "postgresql://unused@127.0.0.1:1/unused",
                "DATABASE_URL_ASYNC": "postgresql://unused@127.0.0.1:1/unused",
                "DISTRIBUTED_SOURCE_CONTROLS": "false",
            }
            tests = subprocess.run(
                [sys.executable, "-m", "pytest", "-q"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            (args.output / "pytest.log").write_text(tests.stdout + tests.stderr)
            summary = {
                "tests_exit_code": tests.returncode,
                "test_output": tests.stdout.strip().splitlines()[-1:],
                "redis": "isolated-real-server",
                "postgres": "pgserver ephemeral PostgreSQL 16.2",
            }
            if args.live_brapi and tests.returncode == 0:
                summary["brapi"] = asyncio.run(canary(redis_url, args.output, args.live_public))
            (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
            print(json.dumps(summary, indent=2))
            return tests.returncode or (
                1 if summary.get("brapi", {}).get("status", "PASSED") != "PASSED" else 0
            )
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    raise SystemExit(main())
