"""Contract-safe gateway: fresh provider snapshots and same-session official EOD fallback."""

from datetime import datetime, timezone

from fin_ingestion.collectors.commercial.base import ProviderUnavailableError
from fin_ingestion.collectors.commercial.brapi import BrapiCollector
from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.config import settings
from fin_ingestion.core.circuit_breaker import CircuitBreakerOpenError
from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.dw.silver_writer import SilverWriter
from fin_ingestion.quality.temporal import timestamp_utc


class MarketGateway:
    def __init__(self, pool=None):
        self.brapi, self.b3 = BrapiCollector(), B3Collector()
        self.writer = DWWriter()
        self.silver = SilverWriter(pool)
        if pool is not None:
            self.writer._pool = pool
        self.external_pool = pool is not None

    @staticmethod
    def normalize_official(row, reference_date):
        factor = row["fator_cotacao"]
        if factor <= 0 or row["data_pregao"] != reference_date.isoformat():
            raise ValueError("Invalid official session or quotation factor")
        return {
            "ticker": row["ticker"], "reference_date": reference_date,
            "open": row["preco_abertura"] / factor,
            "high": row["preco_maximo"] / factor,
            "low": row["preco_minimo"] / factor,
            "close": row["preco_fechamento"] / factor,
            "volume": row["quantidade_titulos"],
            "financial_turnover": row.get("volume_financeiro"),
            "trade_count": row.get("numero_negocios"),
            "source": "b3", "currency": "BRL",
        }

    async def official_session(self, reference_date):
        """Preserve the whole official session before resolving serving IDs.

        Nominal bars do not require an inferred asset class or a synthetic
        issuer. Registry gaps are explicit; they never discard official prices.
        """
        official = await self.b3.get_daily_quotes(reference_date)
        await self.writer.write_instrument_identifiers(official)
        records = [self.normalize_official(row, reference_date) for row in official]
        tickers = {r["ticker"] for r in records}
        if not records or len(tickers) != len(records):
            raise ValueError("Empty or duplicate official session identities")
        loaded = await self.silver.write_records("bars", records)
        if loaded != len(records):
            raise ValueError("Official nominal coverage is incomplete")
        ticker_map = await self.writer.get_ticker_id_map()
        serving = [r for r in records if r["ticker"] in ticker_map]
        written = await self.writer.write_daily_quotes([{
            "ticker": r["ticker"], "data_pregao": r["reference_date"],
            "preco_abertura": r["open"], "preco_maximo": r["high"],
            "preco_minimo": r["low"], "preco_fechamento": r["close"],
            "volume_total": r["financial_turnover"], "quantidade_negocios": r["trade_count"],
        } for r in serving])
        if written != len(serving):
            raise ValueError("Registered serving coverage is incomplete")
        unresolved = sorted(tickers - set(ticker_map))
        return {
            "pipeline": "market_eod", "status": "SUCCEEDED",
            "total_records": len(records), "records_loaded": loaded,
            "serving_records_loaded": written, "watermark_date": reference_date.isoformat(),
            "sources_executed": ["b3"], "universe": {
                "official": len(records), "nominal_loaded": loaded,
                "registered": len(serving), "unresolved": unresolved,
            },
        }

    async def intraday(self, tickers, now=None):
        if not settings.intraday_enabled:
            raise ValueError(
                "Intraday must be explicitly enabled after schema and credential homologation"
            )
        now = timestamp_utc(now or datetime.now(timezone.utc))
        payload = await self.brapi.extract(tickers=tickers, endpoint="quote")
        records = self.brapi.parse(payload)
        for record in records:
            age = (now - record["timestamp"]).total_seconds()
            if not 0 <= age <= settings.intraday_max_age_seconds:
                raise ValueError(f"Stale or future intraday quote for {record['ticker']}")
        loaded = 0
        for record in records:
            saved = await self.writer.write_price_observation(
                ticker=record["ticker"],
                price=record["price"],
                observed_at=record["timestamp"],
                volume=record["volume"],
                source_name=record["source"],
            )
            if not saved:
                raise ValueError(f"Unregistered intraday instrument: {record['ticker']}")
            loaded += 1
        return {
            "pipeline": "intraday",
            "status": "SUCCEEDED",
            "total_records": len(records),
            "records_loaded": loaded,
            "sources_executed": ["brapi"],
            "watermark_timestamp": min(r["timestamp"] for r in records).isoformat(),
        }

    async def eod(self, tickers, reference_date):
        records, used_fallback = [], False
        if settings.brapi_api_key and settings.brapi_raw_history:
            try:
                payload = await self.brapi.extract(
                    tickers=tickers, endpoint="historical", range="1mo"
                )
                records = [
                    r for r in self.brapi.parse(payload) if r["reference_date"] == reference_date
                ]
            except (ProviderUnavailableError, CircuitBreakerOpenError):
                used_fallback = True
        missing = set(tickers) - {r["ticker"] for r in records}
        if missing:
            official = await self.b3.get_daily_quotes(reference_date)
            for row in official:
                if row["ticker"] in missing:
                    records.append(self.normalize_official(row, reference_date))
            used_fallback = True
        if {r["ticker"] for r in records} != set(tickers):
            raise ValueError("Same-session EOD coverage is incomplete; stale fallback refused")
        loaded = await self.silver.write_records("bars", records)
        # Keep the existing application serving path populated with the same nominal
        # OHLC. Share volume is not mislabeled as financial turnover.
        written_daily = await self.writer.write_daily_quotes([{
            "ticker": r["ticker"], "data_pregao": r["reference_date"],
            "preco_abertura": r["open"], "preco_maximo": r["high"],
            "preco_minimo": r["low"], "preco_fechamento": r["close"],
            "volume_total": r.get("financial_turnover"),
            "quantidade_negocios": r.get("trade_count"),
        } for r in records])
        if written_daily != len(records):
            raise ValueError("EOD serving coverage contains unregistered instruments")
        return {
            "pipeline": "market_eod",
            "status": "SUCCEEDED",
            "total_records": len(records),
            "records_loaded": loaded,
            "used_official_fallback": used_fallback,
            "watermark_date": reference_date.isoformat(),
            "sources_executed": sorted({r["source"] for r in records}),
        }

    async def close(self):
        await self.brapi.close()
        await self.b3.close()
        await self.silver.close()
        if not self.external_pool:
            await self.writer.close()


class MarketPipeline:
    def __init__(self, intraday=False, pool=None):
        self.is_intraday, self.pool = intraday, pool

    async def run(self, tickers=None, reference_date=None, **kwargs):
        from datetime import date

        if not tickers:
            raise ValueError("Market gateway requires an explicit ticker universe")
        gateway = MarketGateway(self.pool)
        try:
            if self.is_intraday:
                return await gateway.intraday(tickers)
            if reference_date is None:
                raise ValueError("EOD requires an explicit reference_date")
            day = (
                date.fromisoformat(reference_date)
                if isinstance(reference_date, str)
                else reference_date
            )
            return await gateway.eod(tickers, day)
        finally:
            await gateway.close()
