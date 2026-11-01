"""Bounded GitHub-hosted runtime: source protection, real writes and release receipts."""

import argparse
import asyncio
import json
import math
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from redis.asyncio import Redis

from fin_ingestion.collectors.commercial.brapi import BrapiCollector
from fin_ingestion.config import settings
from fin_ingestion.core.distributed_controls import DistributedSourceControls
from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.pipelines.market_gateway import MarketGateway, MarketPipeline
from fin_ingestion.pipelines.provider_pipeline import ProviderPipeline
from fin_ingestion.pipelines.result import require_complete_result
from fin_ingestion.scheduler.market_calendar import TZ_SP, MarketCalendar

PROFILES = {"morning", "ptax", "treasury", "cvm", "intraday", "deploy", "daily", "monthly", "discovery", "reconcile"}
GOLD_MODELS = [
    "+fct_cvm_statement_v2",
    "fct_nominal_bar",
    "fct_ptax_boletim",
    "fct_focus_expectativa",
    "fct_tesouro_taxa",
    "fct_debenture_indicativa",
    "fct_external_fundamental",
    "fct_corporate_action_latest",
    "+fct_sgs_observation",
]


def datasets_for(profile: str, now: datetime) -> list[str]:
    if profile not in PROFILES or now.tzinfo is None:
        raise ValueError("Invalid runtime profile or timezone")
    local = now.astimezone(TZ_SP)
    if profile == "deploy":
        return ["sgs", "ptax", "focus", "tesouro", "cvm_dfp", "cvm_itr", "brapi_preflight"]
    if profile == "intraday":
        return (
            ["intraday"]
            if settings.intraday_enabled and MarketCalendar.is_b3_market_open(local)
            else []
        )
    if profile == "cvm":
        return ["cvm_dfp", "cvm_itr"]
    if profile in {"monthly", "discovery", "reconcile"}:
        return [profile]
    if local.weekday() >= 5:
        return []
    if profile == "morning":
        return ["sgs", "focus"] if local.weekday() == 0 else ["sgs"]
    if not MarketCalendar.is_b3_trading_day(local.date()):
        return []
    if profile == "daily":
        return ["market_eod", "cash_dividends"]
    return ["ptax"] if profile == "ptax" else ["tesouro"]


async def preflight():
    if settings.ingestion_env == "production" and not settings.redis_url.startswith("rediss://"):
        raise RuntimeError("Production requires shared TLS Redis")
    redis = Redis.from_url(settings.redis_url, socket_connect_timeout=5, socket_timeout=5)
    controls = DistributedSourceControls(redis, "engine-deploy-check:" + uuid4().hex, 1, 2)
    try:
        await redis.ping()
        epoch = await controls.before_request()
        await controls.after_request(epoch, success=True)
    finally:
        await redis.delete(controls.bucket_key, controls.breaker_key)
        await redis.aclose()


async def brapi_preflight(pool):
    collector = BrapiCollector()
    tickers = settings.intraday_tickers or ["PETR4"]
    try:
        async with pool.acquire() as conn:
            found = await conn.fetch(
                "SELECT ticker FROM core.ativo WHERE ticker=ANY($1::text[])", tickers
            )
        if {r["ticker"] for r in found} != set(tickers):
            raise ValueError("Intraday universe contains unregistered instruments")
        payload = await collector.extract(tickers=tickers, endpoint="quote")
        records = collector.parse(payload)
        if any(not math.isfinite(float(r["price"])) or float(r["price"]) <= 0 for r in records):
            raise ValueError("Invalid authenticated provider quote")
        return {
            "status": "SUCCEEDED",
            "total_records": len(records),
            "records_loaded": 0,
            "quote_timestamps": [r["timestamp"].isoformat() for r in records],
            "note": "Credential probe only; snapshots are not promoted as current outside the session",
        }
    finally:
        await collector.close()


def build_gold(profile=None):
    parts = urlsplit(settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://"))
    env = {
        **os.environ,
        "DBT_HOST": parts.hostname or "",
        "DBT_USER": unquote(parts.username or ""),
        "DBT_PASSWORD": unquote(parts.password or ""),
        "DBT_PORT": str(parts.port or 5432),
        "DBT_DBNAME": unquote(parts.path.lstrip("/")),
        "DBT_SSLMODE": "require",
    }
    models = GOLD_MODELS + {
        "daily": ["+fct_cotacao_diaria", "+fct_provento+"],
        "monthly": ["+fct_fii_snapshot_mensal"],
        "discovery": ["+dim_ativo"],
    }.get(profile, [])
    for command in (["dbt", "deps"], ["dbt", "build", "--select", *models]):
        subprocess.run(
            [*command, "--project-dir", "dbt", "--profiles-dir", "dbt"], env=env, check=True,
            timeout=600,
        )


async def run_dataset(dataset, day, pool, cnpjs):
    if dataset == "cash_dividends":
        from fin_ingestion.pipelines.cash_dividends import run

        report = await run(pool, apply=True)
        if report["errors"]:
            raise RuntimeError("Incomplete B3 dividend collection; inspect source coverage")
        return {**report, "total_records": report["written"], "records_loaded": report["written"]}

    if dataset == "market_eod":
        gateway = MarketGateway(pool=pool)
        try:
            result = await gateway.official_session(day)
        finally:
            await gateway.close()
        await asyncio.to_thread(
            subprocess.run, [sys.executable, "-m", "fin_ingestion.cli", "compute-analytics"],
            check=True, timeout=600,
        )
        return result
    if dataset == "monthly":
        from fin_ingestion.pipelines.monthly_fii_funds import MonthlyFIIFundsPipeline
        pipeline = MonthlyFIIFundsPipeline()
        # These legacy dbt models read raw.payload, so a local-only archive
        # would leave Gold stale despite a successful download.
        pipeline.cvm.raw_sink.backend = "postgres"
        try:
            return await pipeline.run()
        finally:
            await pipeline.close()
    if dataset == "discovery":
        from fin_ingestion.pipelines.quarterly_financials import QuarterlyFinancialsPipeline
        pipeline = QuarterlyFinancialsPipeline()
        pipeline.cvm.raw_sink.backend = "postgres"
        try:
            return await pipeline.run()
        finally:
            await pipeline.close()
    if dataset == "reconcile":
        from fin_ingestion.queue.reconciler import IngestionReconciler
        recovered = await IngestionReconciler(pool).recover_zombie_tasks()
        return {"status": "SUCCEEDED", "total_records": recovered, "records_loaded": 0}
    if dataset == "sgs":
        series = {}
        for name in ("selic_over", "meta_selic", "cdi", "ipca", "igpm"):
            series[name] = await ProviderPipeline("sgs", pool=pool).run(
                reference_date=day,
                serie=name,
                start_date=(day - timedelta(days=400)).isoformat(),
                end_date=day.isoformat(),
            )
        return {
            "status": "SUCCEEDED",
            "total_records": sum(r["total_records"] for r in series.values()),
            "records_loaded": sum(r["records_loaded"] for r in series.values()),
            "series": series,
        }
    if dataset == "brapi_preflight":
        return await brapi_preflight(pool)
    if dataset == "intraday":
        return await MarketPipeline(intraday=True, pool=pool).run(tickers=settings.intraday_tickers)
    options = {}
    if dataset.startswith("cvm_"):
        options = {"year": day.year - 1 if dataset == "cvm_dfp" else day.year, "cnpjs": cnpjs}
    if dataset == "tesouro":
        options["latest_on_or_before"] = True
    # PTAX has no bulletin on non-business days; each fallback keeps its real date.
    for offset in range(7 if dataset == "ptax" else 1):
        target = day - timedelta(days=offset)
        if dataset == "ptax" and target.weekday() >= 5:
            continue
        try:
            return await ProviderPipeline(dataset, pool=pool).run(reference_date=target, **options)
        except RuntimeError as exc:
            if dataset != "ptax" or "nenhuma linha" not in str(exc):
                raise
    raise RuntimeError("No PTAX bulletin available in the bounded reference window")


async def execute(profile, reference_date=None, output=Path("data/deployment.json"), scheduled_at=None):
    now = datetime.now(timezone.utc)
    # Batch replay evaluates the original slot, even after midnight/weekend.
    # Intraday always evaluates the current session, never invents a past quote.
    datasets = datasets_for(profile, now if profile == "intraday" else (scheduled_at or now))
    day = reference_date or now.astimezone(TZ_SP).date()
    receipt = {
        "release": os.environ.get("ENGINE_EXECUTED_SHA", os.environ.get("GITHUB_SHA", "local")),
        "profile": profile,
        "started_at": now.isoformat(),
        "reference_date": day.isoformat(),
        "results": {},
    }
    if not datasets:
        receipt["status"] = "SKIPPED_OUTSIDE_WINDOW"
    else:
        await preflight()
        owner = DWWriter()
        pool = await owner.get_pool()
        runtime_redis = Redis.from_url(
            settings.redis_url, socket_connect_timeout=5, socket_timeout=5
        )
        runtime_lock = runtime_redis.lock(
            "fin:scheduled-engine:runtime", timeout=1800, blocking=False
        )
        held = False
        try:
            held = await runtime_lock.acquire()
            if not held:
                raise RuntimeError("Another production ingestion execution holds the lease")
            cnpjs = json.loads(os.environ.get("CVM_CNPJS", '["33000167000101"]'))
            if (
                not isinstance(cnpjs, list)
                or not cnpjs
                or any(not isinstance(c, str) or len(c) != 14 or not c.isdigit() for c in cnpjs)
            ):
                raise ValueError("CVM_CNPJS must be an explicit list of numeric CNPJs")
            for dataset in datasets:
                started, monotonic = datetime.now(timezone.utc), time.monotonic()
                try:
                    result = await run_dataset(dataset, day, pool, cnpjs)
                    if dataset != "reconcile":
                        require_complete_result(result)
                    status, error = "SUCCEEDED", None
                except Exception as exc:
                    result = {"error_type": type(exc).__name__,
                              "retry_after_seconds": getattr(exc, "retry_after", None)}
                    status, error = "FAILED", type(exc).__name__
                receipt["results"][dataset] = {"status": status, **result}
                async with pool.acquire() as audit:
                    await audit.execute(
                        "INSERT INTO ingestion.ingestion_run(policy_id,worker_id,started_at,finished_at,status,records_extracted,records_loaded,duration_ms,error_stack) "
                        "VALUES($1,$2,$3,now(),$4,$5,$6,$7,$8)",
                        f"github:{dataset}",
                        f"github:{os.environ.get('GITHUB_RUN_ID', 'local')}",
                        started,
                        status,
                        result.get("total_records", 0),
                        result.get("records_loaded", 0),
                        int((time.monotonic() - monotonic) * 1000),
                        error,
                    )
            receipt["status"] = (
                "FAILED"
                if any(r["status"] != "SUCCEEDED" for r in receipt["results"].values())
                else "SUCCEEDED"
            )
            if receipt["status"] == "SUCCEEDED" and profile not in {"intraday", "reconcile"}:
                try:
                    await asyncio.to_thread(build_gold, profile)
                    receipt["gold"] = "SUCCEEDED"
                except Exception as exc:
                    receipt.update(
                        status="FAILED", gold="FAILED", gold_error_type=type(exc).__name__
                    )
        finally:
            if held:
                await runtime_lock.release()
            await runtime_redis.aclose()
            await owner.close()
    receipt["finished_at"] = datetime.now(timezone.utc).isoformat()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, default=str) + "\n")
    print(json.dumps(receipt, indent=2, default=str))
    return receipt["status"] != "FAILED"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", choices=sorted(PROFILES))
    parser.add_argument("--reference-date", type=date.fromisoformat)
    parser.add_argument("--output", type=Path, default=Path("data/deployment.json"))
    args = parser.parse_args()
    try:
        return 0 if asyncio.run(execute(args.profile, args.reference_date, args.output)) else 1
    except Exception as exc:
        failure = {"status": "FAILED", "profile": args.profile, "error_type": type(exc).__name__}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(failure, indent=2) + "\n")
        print(json.dumps(failure))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
