"""Durable scheduled occurrences, bounded catch-up, leased execution and receipts.

Both Actions and an external supervisor can run this entry point concurrently.
Trigger delivery is disposable; schedule cursors and jobs are not.
"""

import argparse
import asyncio
import hashlib
import json
import os
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from croniter import croniter

from fin_ingestion.config import settings
from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.operations.production import datasets_for, execute
from fin_ingestion.scheduler.market_calendar import TZ_SP

SCHEDULES = {
    "morning": "0 9 * * 1-5",
    "ptax": "0 10,11,12,13 * * 1-5",
    "ptax_close": "30 13 * * 1-5",
    "treasury": "30 9 * * 1-5",
    "treasury_close": "0 18 * * 1-5",
    "cvm": "0 8 * * 6",
    "intraday": "*/15 10-17 * * 1-5",
    "discovery": "0 6 * * 1-5",
    "monthly": "0 12 * * 1-5",
    "daily": "0 19 * * 1-5",
    "reconcile": "0 20 * * *",
}
PROFILE_ALIASES = {"ptax_close": "ptax", "treasury_close": "treasury"}
CONTRACT = "v1"


def parameters():
    cnpjs = json.loads(os.environ.get("CVM_CNPJS", '["33000167000101"]'))
    if not isinstance(cnpjs, list) or not cnpjs or any(
        not isinstance(c, str) or len(c) != 14 or not c.isdigit() for c in cnpjs
    ):
        raise ValueError("Invalid CVM universe")
    return {
        "contract": CONTRACT,
        "cnpjs": sorted(set(cnpjs)),
        "tickers": sorted(set(settings.intraday_tickers)),
        "intraday_enabled": settings.intraday_enabled,
    }


def due_slots(expression, since, now, limit=300):
    if since.tzinfo is None or now.tzinfo is None or since > now:
        raise ValueError("Invalid schedule interval")
    iterator = croniter(expression, since.astimezone(TZ_SP))
    for _ in range(limit):
        slot = iterator.get_next(datetime)
        if slot > now:
            break
        yield slot.astimezone(timezone.utc)


async def materialize(pool, now=None):
    now = now or datetime.now(timezone.utc)
    config = parameters()
    encoded = json.dumps(config, sort_keys=True)
    partition = hashlib.sha256(encoded.encode()).hexdigest()[:24]
    # First deployment begins at local midnight; subsequent cycles never discard
    # an old cursor. Catch-up is paged rather than truncated after seven days.
    bootstrap = now.astimezone(TZ_SP).replace(hour=0, minute=0, second=0, microsecond=0)
    created = 0
    for profile, expression in SCHEDULES.items():
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO ingestion.runtime_cursor(profile,through_at) VALUES($1,$2) "
                    "ON CONFLICT DO NOTHING", profile, bootstrap - timedelta(microseconds=1),
                )
                cursor = await conn.fetchval(
                    "SELECT through_at FROM ingestion.runtime_cursor WHERE profile=$1 FOR UPDATE",
                    profile,
                )
                slots = list(due_slots(expression, cursor, now))
                for slot in slots:
                    resolved = PROFILE_ALIASES.get(profile, profile)
                    if not datasets_for(resolved, slot):
                        continue
                    expired = resolved == "intraday" and (
                        not datasets_for(resolved, now)
                        or (now - slot).total_seconds() > settings.intraday_max_age_seconds
                    )
                    result = await conn.execute(
                        "INSERT INTO ingestion.runtime_job(profile,scheduled_at,reference_date,"
                        "partition_key,parameters,status) VALUES($1,$2,$3,$4,$5::jsonb,$6) "
                        "ON CONFLICT DO NOTHING",
                        profile, slot, slot.astimezone(TZ_SP).date(), partition, encoded,
                        "EXPIRED" if expired else "PENDING",
                    )
                    created += int(result == "INSERT 0 1")
                # Only advance beyond enumerated slots when no page was truncated.
                through = slots[-1] if len(slots) == 300 else now
                await conn.execute(
                    "UPDATE ingestion.runtime_cursor SET through_at=$2,updated_at=now() "
                    "WHERE profile=$1", profile, max(cursor, through),
                )
    return created


async def claim(pool):
    async with pool.acquire() as conn:
        # SKIP LOCKED and the state change are one statement/transaction.
        return await conn.fetchrow(
            "WITH candidate AS (SELECT job_id FROM ingestion.runtime_job "
            "WHERE (status='PENDING' AND next_attempt_at<=now()) "
            "OR (status='RUNNING' AND locked_until<now()) "
            "ORDER BY CASE WHEN profile='intraday' THEN 0 ELSE 1 END,scheduled_at "
            "FOR UPDATE SKIP LOCKED LIMIT 1) "
            "UPDATE ingestion.runtime_job j SET status='RUNNING',attempts=attempts+1,"
            "lock_token=$1,locked_until=now()+interval '120 seconds',started_at=now() "
            "FROM candidate c WHERE j.job_id=c.job_id RETURNING j.*", uuid4(),
        )


async def renew(pool, job):
    while True:
        await asyncio.sleep(30)
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE ingestion.runtime_job SET locked_until=now()+interval '120 seconds' "
                "WHERE job_id=$1 AND lock_token=$2 AND status='RUNNING' AND locked_until>now()",
                job["job_id"], job["lock_token"],
            )
            if result != "UPDATE 1":
                raise RuntimeError("Runtime lease lost")
            await conn.execute(
                "UPDATE ingestion.runtime_heartbeat SET last_seen_at=now() WHERE worker_id=$1",
                f"runtime:{os.environ.get('GITHUB_RUN_ID', 'external')}",
            )


async def finish(pool, job, status, receipt=None, error_type=None, retry_after=0):
    delay = min(3600, 30 * 2 ** min(job["attempts"], 7))
    delay = max(delay, retry_after)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE ingestion.runtime_job SET status=$3,receipt=$4::jsonb,last_error_type=$5,"
            "finished_at=now(),locked_until=NULL,lock_token=NULL,"
            "next_attempt_at=now()+make_interval(secs=>$6)+random()*interval '15 seconds' "
            "WHERE job_id=$1 AND lock_token=$2 AND status='RUNNING' AND locked_until>now()",
            job["job_id"], job["lock_token"], status,
            json.dumps(receipt, default=str) if receipt else None, error_type, delay,
        )
        if result != "UPDATE 1":
            raise RuntimeError("Runtime lease lost before confirmation")


async def execute_job(pool, job, directory, timeout=900):
    if job["attempts"] > 12:
        await finish(pool, job, "FAILED", error_type="AttemptsExhaustedAfterLeaseTimeout")
        return "FAILED"
    profile = PROFILE_ALIASES.get(job["profile"], job["profile"])
    now = datetime.now(timezone.utc)
    if profile == "intraday" and (
        (now - job["scheduled_at"]).total_seconds() > settings.intraday_max_age_seconds
        or not datasets_for(profile, now)
    ):
        await finish(pool, job, "EXPIRED")
        return "EXPIRED"
    config = json.loads(job["parameters"])
    if config != parameters():
        # Do not silently change a persisted universe midway through a retry.
        await finish(pool, job, "FAILED", error_type="UniverseConfigurationChanged")
        return "FAILED"
    output = directory / f"{job['job_id']}.json"
    heartbeat = asyncio.create_task(renew(pool, job))
    execution = asyncio.create_task(execute(
        profile, reference_date=job["reference_date"], output=output,
        scheduled_at=job["scheduled_at"],
    ))
    receipt = None
    try:
        done, _ = await asyncio.wait(
            {execution, heartbeat}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if heartbeat in done:
            await heartbeat
            raise RuntimeError("Runtime heartbeat ended")
        if execution not in done:
            raise TimeoutError("Runtime task exceeded its time budget")
        success = await execution
        receipt = json.loads(output.read_text())
        if not success or receipt.get("status") != "SUCCEEDED":
            raise RuntimeError("Incomplete production execution")
        await finish(pool, job, "SUCCEEDED", receipt=receipt)
        return "SUCCEEDED"
    except Exception as exc:
        status = "FAILED" if job["attempts"] >= 12 else "PENDING"
        delays = [r.get("retry_after_seconds") for r in (receipt or {}).get("results", {}).values()]
        retry_after = max((d for d in delays if isinstance(d, int) and d > 0), default=0)
        await finish(pool, job, status, receipt=receipt, error_type=type(exc).__name__,
                     retry_after=retry_after)
        return status
    finally:
        for task in (execution, heartbeat):
            if not task.done():
                task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task


async def run_once(seconds=960, directory=Path("data/runtime")):
    directory.mkdir(parents=True, exist_ok=True)
    owner = DWWriter()
    pool = await owner.get_pool()
    worker = f"runtime:{os.environ.get('GITHUB_RUN_ID', 'external')}"
    release = os.environ.get("ENGINE_EXECUTED_SHA", os.environ.get("GITHUB_SHA", "local"))
    started = time.monotonic()
    results = []
    try:
        created = await materialize(pool)
        while time.monotonic() - started < seconds:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ingestion.runtime_heartbeat(worker_id,release_sha) VALUES($1,$2) "
                    "ON CONFLICT(worker_id) DO UPDATE SET last_seen_at=now(),release_sha=$2",
                    worker, release,
                )
            job = await claim(pool)
            if job is None:
                break
            status = await execute_job(pool, job, directory,
                                       timeout=min(900, seconds - (time.monotonic() - started)))
            results.append({"job_id": str(job["job_id"]), "profile": job["profile"], "status": status})
        async with pool.acquire() as conn:
            health = [dict(row) for row in await conn.fetch("SELECT * FROM ingestion.v_runtime_health")]
        failed = any(row["failed"] for row in health)
        receipt = {"release": release, "created": created, "results": results,
                   "health": health, "status": "FAILED" if failed else "SUCCEEDED",
                   "finished_at": datetime.now(timezone.utc).isoformat()}
        (directory / "summary.json").write_text(json.dumps(receipt, default=str, indent=2) + "\n")
        print(json.dumps(receipt, default=str))
        # Even transient failure must be visible in this run while staying queued.
        return not failed and all(r["status"] in {"SUCCEEDED", "EXPIRED"} for r in results)
    finally:
        await owner.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--time-budget", type=int, default=960)
    args = parser.parse_args()
    if not 1 <= args.time_budget <= 3600:
        parser.error("Time budget must be between 1 and 3600 seconds")
    try:
        return 0 if asyncio.run(run_once(args.time_budget)) else 1
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
