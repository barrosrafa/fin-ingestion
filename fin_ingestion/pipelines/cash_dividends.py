"""Confirmed B3 cash payments, matched by ISIN; Yahoo aggregates are not payments."""

import argparse
import asyncio
import base64
import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from fin_ingestion.config import settings
from fin_ingestion.core.database import create_pool_with_retry

B3_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/"
TYPES = {"DIVIDENDO": "DIVIDENDO", "DIVIDENDOS": "DIVIDENDO", "JCP": "JCP",
         "JRS CAP PROPRIO": "JCP", "RENDIMENTO": "RENDIMENTO",
         "REND. TRIBUTADO": "RENDIMENTO_TRIBUTADO"}
CAPITAL_ACTIONS = {"REST CAP DIN", "AMORTIZACAO RF"}


def br_date(value):
    return datetime.strptime(value, "%d/%m/%Y").date()


def cash_event(kind, com, payment, amount, announcement=None, payload=None):
    kind = TYPES.get(kind.strip().upper())
    if not kind:
        raise ValueError("Unsupported cash event type")
    amount = Decimal(amount.strip().replace(".", "").replace(",", "."))
    if not amount.is_finite() or amount <= 0:
        raise ValueError("Invalid cash event amount")
    com, payment = br_date(com), br_date(payment)
    if payment < com:
        raise ValueError("Payment precedes entitlement date")
    return {"tipo_provento": kind, "data_com": com, "data_pagamento": payment,
            "data_ex": None, "data_anuncio": br_date(announcement) if announcement else None,
            "valor_por_acao": amount, "payload": payload}


def parse_b3(payload, isin):
    if not isinstance(payload, list):
        raise ValueError("Invalid B3 supplement")
    records = []
    for company in payload:
        for row in company.get("cashDividends", []):
            if row.get("isinCode") != isin:
                continue
            if row.get("label", "").strip().upper() in CAPITAL_ACTIONS:
                continue  # Capital returned is not dividend income; retain its raw snapshot.
            if not row.get("paymentDate") or row["paymentDate"] in {"01/01/0001", "31/12/9999"}:
                continue  # Unknown payments remain in the source snapshot.
            records.append(cash_event(row["label"], row["lastDatePrior"], row["paymentDate"],
                                      row["rate"], row.get("approvedOn"), row))
    return records


def parse_statusinvest(html):
    soup = BeautifulSoup(html, "html.parser")
    records = []
    for table in soup.select("table"):
        headers = [h.get_text(" ", strip=True).upper() for h in table.select("thead th")]
        if headers != ["TIPO", "DATA COM", "PAGAMENTO", "VALOR"]:
            continue
        for row in table.select("tbody tr"):
            cols = [c.get_text(" ", strip=True) for c in row.select("td")]
            if len(cols) != 4:
                raise ValueError("Malformed Status Invest row")
            records.append(cash_event(*cols, payload={"html_cells": cols}))
    if not records:
        raise ValueError("No Status Invest payments found")
    return records


async def archive_synthetic(conn):
    """Reversible quarantine of payments without any source evidence.

    Referenced rows abort the transaction rather than erase user history.
    """
    # Never cascade away calendar entries. Preserve user references for review.
    referenced = await conn.fetchval("""SELECT count(*) FROM market.calendario_proventos c
        JOIN market.provento p USING(id_provento)
        WHERE p.fonte_provento IS NULL""")
    if referenced:
        raise ValueError("Synthetic events have calendar references; manual reconciliation required")
    return await conn.fetchval("""
        WITH moved AS (
            DELETE FROM market.provento p
            WHERE fonte_provento IS NULL
            RETURNING p.*
        ), archived AS (
            INSERT INTO ingestion.provento_quarentena(id_provento,registro_original,motivo)
            SELECT id_provento,to_jsonb(moved),CASE
                WHEN data_anuncio IS NULL AND observacao IS NULL
                  AND data_com=data_ex-1 AND data_pagamento=data_ex
                  AND mes_referencia=to_char(data_com,'YYYY-MM')
                  AND tipo_provento::text IN ('DIVIDENDO','RENDIMENTO')
                THEN 'YAHOO_AGGREGATE_WITH_INVENTED_PAYMENT'
                ELSE 'LEGACY_WITHOUT_SOURCE_EVIDENCE' END
            FROM moved RETURNING id_provento
        ) SELECT count(*) FROM archived
    """)


async def persist(conn, asset_id, records, source):
    # Multiple announcements can pay the same type on the same day. Aggregate
    # only that natural payment key and keep every original source row.
    groups = {}
    for row in records:
        key = row["tipo_provento"], row["data_com"], row["data_pagamento"]
        groups.setdefault(key, []).append(row)
    for (kind, com, payment), rows in groups.items():
        if source == "STATUSINVEST_USER_HTML" and kind == "RENDIMENTO_TRIBUTADO":
            await conn.execute("""DELETE FROM market.provento WHERE id_ativo=$1
                AND data_com=$2 AND data_pagamento=$3 AND tipo_provento::text='RENDIMENTO'
                AND fonte_provento='B3'""", asset_id, com, payment)
        if source == "B3" and kind == "RENDIMENTO":
            overridden = await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM market.provento
                WHERE id_ativo=$1 AND data_com=$2 AND data_pagamento=$3
                AND tipo_provento::text='RENDIMENTO_TRIBUTADO'
                AND fonte_provento='STATUSINVEST_USER_HTML')""", asset_id, com, payment)
            if overridden:
                continue
        await conn.execute("""
            INSERT INTO market.provento(id_ativo,tipo_provento,data_com,data_pagamento,
                data_anuncio,valor_por_acao,mes_referencia,fonte_provento,payload_fonte)
            VALUES($1,$2::market.tipo_provento_enum,$3,$4,$5,$6,$7,$8,$9::jsonb)
            ON CONFLICT(id_ativo,tipo_provento,data_com,data_pagamento) DO UPDATE SET
                valor_por_acao=EXCLUDED.valor_por_acao, fonte_provento=EXCLUDED.fonte_provento,
                payload_fonte=EXCLUDED.payload_fonte, data_anuncio=EXCLUDED.data_anuncio
            WHERE market.provento.fonte_provento IS DISTINCT FROM 'STATUSINVEST_USER_HTML'
               OR EXCLUDED.fonte_provento='STATUSINVEST_USER_HTML'
        """, asset_id, kind, com, payment, rows[0]["data_anuncio"],
            sum(r["valor_por_acao"] for r in rows), com.strftime("%Y-%m"), source,
            json.dumps([r["payload"] for r in rows]))
    return len(groups)


async def run(pool, *, apply=False, tickers=None, html=None, html_ticker=None, quarantine=False):
    if quarantine and tickers:
        raise ValueError("Global quarantine requires the complete asset universe")
    async with pool.acquire() as conn:
        assets = await conn.fetch("""
            SELECT id_ativo,ticker,isin FROM core.ativo
            WHERE tipo_ativo::text IN ('ACAO','FII','ETF','BDR')
              AND ($1::text[] IS NULL OR ticker=ANY($1)) ORDER BY ticker
        """, tickers)
    issuers = {}
    for asset in assets:
        if re.fullmatch(r"[A-Z]{4}\d{1,2}B?", asset["ticker"]):
            issuers.setdefault(asset["ticker"][:4], []).append(asset)
    report = {"status": "SUCCEEDED", "assets": len(assets), "written": 0,
              "quarantined": 0, "coverage": [], "errors": []}
    report["coverage"].extend({"ticker": a["ticker"], "status": "MISSING_ISIN"}
                              for a in assets if not a["isin"])
    issuers = {issuer: [a for a in members if a["isin"]]
               for issuer, members in issuers.items() if any(a["isin"] for a in members)}
    # Fetch independently with a small bounded concurrency. The single database
    # transaction below prevents exposing a partially repaired calendar.
    semaphore = asyncio.Semaphore(3)
    completed = 0
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        async def fetch(issuer):
            nonlocal completed
            async with semaphore:
                try:
                    params = {"issuingCompany": issuer, "language": "pt-br"}
                    token = base64.b64encode(json.dumps(params).encode()).decode()
                    response = await client.get(B3_URL + "GetListedSupplementCompany/" + token)
                    response.raise_for_status()
                    if not response.text.strip():
                        return None  # No listed supplement; report unavailable coverage.
                    data = response.json()
                    if data == []:
                        return None
                    if not isinstance(data, list):
                        raise ValueError("Invalid B3 supplement")
                    return data
                finally:
                    completed += 1
                    if completed % 50 == 0:
                        print(json.dumps({"phase": "collecting_b3", "issuers": completed,
                                          "total_issuers": len(issuers)}), flush=True)
        results = await asyncio.gather(*(fetch(i) for i in issuers), return_exceptions=True)
    prepared = []
    for (issuer, members), data in zip(issuers.items(), results):
        if isinstance(data, Exception):
            report["errors"].append({"issuer": issuer, "error_type": type(data).__name__})
            continue
        if data is None:
            report["coverage"].extend({"ticker": a["ticker"], "status": "SOURCE_UNAVAILABLE"} for a in members)
            continue
        for asset in members:
            try:
                rows = parse_b3(data, asset["isin"])
            except (ValueError, KeyError) as exc:
                report["errors"].append({"ticker": asset["ticker"], "error_type": type(exc).__name__, "detail": str(exc)})
                continue
            prepared.append((asset["id_ativo"], rows))
            report["coverage"].append({"ticker": asset["ticker"], "status": "VERIFIED" if rows else "NO_DATED_PAYMENTS",
                                       "payments": len(rows)})
    html_rows = parse_statusinvest(Path(html).read_text()) if html else []
    html_asset = next((a for a in assets if a["ticker"] == html_ticker), None)
    if html and not html_asset:
        raise ValueError("HTML requires a known explicit ticker")
    if report["errors"]:
        report["status"] = "PARTIAL_SOURCE_COVERAGE"
    report["verified_assets"] = sum(r["status"] == "VERIFIED" for r in report["coverage"])
    report["coverage_complete"] = len(report["coverage"]) == len(assets) and all(
        r["status"] in {"VERIFIED", "NO_DATED_PAYMENTS"} for r in report["coverage"]
    )
    if apply:
        if quarantine and report["errors"]:
            return report  # Do not start the initial repair after a failed collection.
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(742198275)")
            if quarantine:
                report["quarantined"] = await archive_synthetic(conn)
            for issuer, data in zip(issuers, results):
                if isinstance(data, Exception) or data is None:
                    continue
                payload = json.dumps(data, sort_keys=True)
                digest = hashlib.sha256(payload.encode()).hexdigest()
                await conn.execute("""INSERT INTO ingestion.provento_snapshot(sha256,emissor,fonte,payload)
                    VALUES($1,$2,'B3', $3::jsonb) ON CONFLICT DO NOTHING""", digest, issuer, payload)
            for asset_id, rows in prepared:
                report["written"] += await persist(conn, asset_id, rows, "B3")
            if html_rows:
                report["written"] += await persist(conn, html_asset["id_ativo"], html_rows, "STATUSINVEST_USER_HTML")
            if await conn.fetchval("SELECT to_regclass('analytics.mapa_sazonalidade_dividendo') IS NOT NULL"):
                from fin_ingestion.pipelines.dividend_seasonality_pipeline import (
                    DividendSeasonalityAggregator,
                )

                cache = await DividendSeasonalityAggregator(pool).aggregate_seasonality(conn=conn)
                report["seasonality_rows"] = cache["total_records"]
            if await conn.fetchval("SELECT to_regclass('analytics.cagg_provento_mensal') IS NOT NULL"):
                await conn.execute("REFRESH MATERIALIZED VIEW analytics.cagg_provento_mensal")
    return report


async def main_async(args):
    pool = await create_pool_with_retry(settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        if args.refresh_identifiers:
            if not args.apply:
                raise ValueError("Identifier refresh requires --apply")
            from fin_ingestion.collectors.official.b3 import B3Collector
            from fin_ingestion.dw.dw_writer import DWWriter

            collector, writer = B3Collector(), DWWriter()
            writer._pool = pool
            try:
                await collector.get_recent_traded_tickers()
                await writer.write_instrument_identifiers(collector.recent_daily_quotes)
            finally:
                await collector.close()
        report = await run(pool, apply=args.apply, tickers=args.tickers.split(",") if args.tickers else None,
                           html=args.html, html_ticker=args.html_ticker, quarantine=args.quarantine)
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2))
        print(json.dumps({k: v for k, v in report.items() if k != "coverage"}, indent=2))
        return 1 if report["errors"] else 0
    finally:
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--quarantine", action="store_true")
    parser.add_argument("--refresh-identifiers", action="store_true")
    parser.add_argument("--tickers")
    parser.add_argument("--html")
    parser.add_argument("--html-ticker")
    parser.add_argument("--output", default="data/cash-dividends.json")
    raise SystemExit(asyncio.run(main_async(parser.parse_args())))
