"""Replay official nominal B3 history for the entire spot universe.

Default is a read-only preview. --apply preserves a public-market preimage and
raw source archive before idempotent writes. No ticker classification is inferred.
Nominal evidence is published before legacy serving so a partial failure cannot
make an unverified legacy value pass the API evidence boundary. Rerun is safe.
"""
import argparse
import asyncio
import calendar
import gzip
import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.dw.silver_writer import SilverWriter
from fin_ingestion.pipelines.market_gateway import MarketGateway


def official_records(rows: list[dict], year: int, today: date):
    spot = [r for r in rows if r['tipo_mercado'] == 10]
    keys = set()
    normalized = []
    for row in spot:
        day = date.fromisoformat(row['data_pregao'])
        if day.year != year or day > today:
            raise ValueError('Official archive contains an unexpected or future session')
        key = (row['ticker'], day)
        if key in keys:
            raise ValueError(f'Duplicate official identity/session: {key}')
        keys.add(key)
        record = MarketGateway.normalize_official(row, day)
        record['mean'] = row['preco_medio'] / row['fator_cotacao']
        normalized.append(record)
    if not normalized:
        raise ValueError('Official archive has no spot records')
    return normalized


async def replay(*, year: int, output: Path, month: int | None = None, apply: bool = False):
    today = datetime.now(ZoneInfo('America/Sao_Paulo')).date()
    collector, writer, silver = B3Collector(), DWWriter(), SilverWriter()
    output.mkdir(parents=True, exist_ok=True)
    if not 1986 <= year <= today.year or (month is not None and not 1 <= month <= 12):
        raise ValueError('Invalid official archive period')
    suffix = f'M{month:02}{year}' if month is not None else f'A{year}'
    url = f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_{suffix}.ZIP'
    try:
        response = await collector.http_client.get(url)
        response.raise_for_status()
        payload = response.content
        records = official_records(collector.parse(payload), year, today)
        if month is not None and any(r['reference_date'].month != month for r in records):
            raise ValueError('Official archive returned the wrong month')
        sha = hashlib.sha256(payload).hexdigest()
        archive = output / f'COTAHIST_{suffix}-{sha}.ZIP'
        archive.write_bytes(payload)
        ticker_map = await writer.get_ticker_id_map()
        serving = [r for r in records if r['ticker'] in ticker_map]
        pool = await writer.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                before = await conn.fetch('''SELECT q.*, a.ticker FROM market.cotacao_diaria q
                    JOIN core.ativo a USING(id_ativo)
                    WHERE data_pregao >= $1 AND data_pregao <= $2''', date(year, month or 1, 1), min(date(year, month or 12, calendar.monthrange(year, month or 12)[1]), today))
        by_key = {(r['ticker'], r['data_pregao']): r for r in before}
        changes = sum((old := by_key.get((r['ticker'], r['reference_date']))) is not None
                      and float(old['preco_fechamento']) != r['close'] for r in serving)
        report = {'mode': 'apply' if apply else 'preview', 'source_url': url, 'sha256': sha,
                  'official_rows': len(records), 'official_tickers': len({r['ticker'] for r in records}),
                  'registered_rows': len(serving), 'different_closes': changes,
                  'unregistered_tickers': sorted({r['ticker'] for r in records} - set(ticker_map)),
                  'first_date': str(min(r['reference_date'] for r in records)),
                  'last_date': str(max(r['reference_date'] for r in records))}
        if apply:
            backup = output / f'preimage-{datetime.now().strftime("%Y%m%dT%H%M%S%f")}.json.gz'
            with gzip.open(backup, 'xt') as stream:
                json.dump([dict(r) for r in before], stream, default=str)
            report['preimage'] = str(backup)
            await collector.raw_sink.save_payload('b3', url, payload, mime_type='application/zip')
            # Independent idempotent batches are restartable. Evidence goes first.
            loaded = written = 0
            for index in range(0, len(records), 1000):
                loaded += await silver.write_records('bars', records[index:index+1000])
            for index in range(0, len(serving), 1000):
                batch = serving[index:index+1000]
                written += await writer.write_daily_quotes([{
                    'ticker': r['ticker'], 'data_pregao': r['reference_date'],
                    'preco_abertura': r['open'], 'preco_maximo': r['high'], 'preco_minimo': r['low'],
                    'preco_fechamento': r['close'], 'preco_medio': r['mean'],
                    'volume_total': r['financial_turnover'], 'quantidade_negocios': r['trade_count'],
                } for r in batch])
            if loaded != len(records) or written != len(serving):
                raise ValueError('Incomplete official replay; retain preimage and retry idempotently')
            report.update(nominal_loaded=loaded, serving_loaded=written, status='SUCCEEDED')
        receipt = output / ('apply.json' if apply else 'preview.json')
        receipt.write_text(json.dumps(report, default=str, indent=2))
        return report
    finally:
        await collector.close()
        await writer.close()
        await silver.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--month', type=int)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    result = asyncio.run(replay(year=args.year, month=args.month, output=args.output, apply=args.apply))
    print(json.dumps(result, indent=2, default=str))
