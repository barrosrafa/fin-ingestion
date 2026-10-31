"""Massive nominal bars, FMP fundamentals and Twelve Data nominal time series."""

from datetime import date, datetime, timezone
from urllib.parse import urlsplit

from fin_ingestion.collectors.commercial.base import CommercialCollector, symbol
from fin_ingestion.config import settings
from fin_ingestion.core.http_client import RateLimitExceededError
from fin_ingestion.quality.temporal import timestamp_utc


class MassiveCollector(CommercialCollector):
    def __init__(self):
        key = settings.massive_api_key
        super().__init__(
            "massive", "https://api.massive.com", key.get_secret_value() if key else None
        )

    async def extract(self, **kwargs):
        ticker = symbol(kwargs["ticker"])
        start, end = (
            date.fromisoformat(str(kwargs["start_date"])),
            date.fromisoformat(str(kwargs["end_date"])),
        )
        if start > end:
            raise ValueError("Invalid Massive date range")
        interval = kwargs.get("interval", "day")
        if interval not in {"day", "minute"}:
            raise ValueError("Unsupported Massive interval")
        endpoint = f"/v2/aggs/ticker/{ticker}/range/1/{interval}/{start}/{end}"
        rows, seen = [], set()
        params = {"adjusted": "false", "sort": "asc", "limit": 50000}
        for _ in range(100):
            payload = await self.request_json(
                endpoint, params=params, headers={"Authorization": f"Bearer {self.key}"}
            )
            if payload.get("adjusted") is not False:
                raise ValueError("Massive response must explicitly confirm adjusted=false")
            rows.extend(payload.get("results", []))
            next_url = payload.get("next_url")
            if not next_url:
                return {"ticker": ticker, "interval": interval, "results": rows}
            parts = urlsplit(next_url)
            if parts.scheme != "https" or parts.netloc != "api.massive.com" or next_url in seen:
                raise ValueError("Invalid Massive pagination URL")
            seen.add(next_url)
            endpoint, params = parts.path + "?" + parts.query, None
        raise ValueError("Massive pagination exceeded 100 pages")

    def parse(self, raw_data):
        return [
            {
                "ticker": raw_data["ticker"],
                "timestamp": datetime.fromtimestamp(row["t"] / 1000, timezone.utc),
                "reference_date": datetime.fromtimestamp(row["t"] / 1000, timezone.utc)
                .astimezone(__import__("zoneinfo").ZoneInfo("America/New_York"))
                .date(),
                "interval": raw_data["interval"],
                "open": row["o"],
                "high": row["h"],
                "low": row["l"],
                "close": row["c"],
                "volume": row.get("v"),
                "source": "massive",
                "currency": "USD",
            }
            for row in raw_data["results"]
        ]


class FMPCollector(CommercialCollector):
    def __init__(self):
        key = settings.fmp_api_key
        super().__init__(
            "fmp", "https://financialmodelingprep.com", key.get_secret_value() if key else None
        )

    async def extract(self, **kwargs):
        endpoint = kwargs.get("endpoint", "dividends")
        if endpoint not in {
            "dividends",
            "company-screener",
            "income-statement",
            "balance-sheet-statement",
            "cash-flow-statement",
        }:
            raise ValueError("Unsupported FMP endpoint")
        params = {"apikey": self.key, "limit": 100}
        if endpoint != "company-screener":
            params["symbol"] = symbol(kwargs["ticker"])
        else:
            params["country"] = "US"
            params["limit"] = 10000
        payload = await self.request_json(f"/stable/{endpoint}", params=params)
        if not isinstance(payload, list):
            raise ValueError("FMP error or invalid response")
        if len(payload)>=params["limit"]:
            raise ValueError("FMP response may be truncated; partition the requested history/universe")
        return {"endpoint": endpoint, "data": payload}

    def parse(self, raw_data):
        return [
            {
                "source": "fmp",
                "dataset": raw_data["endpoint"],
                "ticker": symbol(row["symbol"]),
                "payload": row,
            }
            for row in raw_data["data"]
        ]


class TwelveDataCollector(CommercialCollector):
    def __init__(self):
        key = settings.twelve_data_api_key
        super().__init__(
            "twelve_data", "https://api.twelvedata.com", key.get_secret_value() if key else None
        )

    async def extract(self, **kwargs):
        ticker, interval = symbol(kwargs["ticker"]), kwargs.get("interval", "1day")
        if interval not in {"1day", "1min", "5min", "15min", "1h"}:
            raise ValueError("Unsupported Twelve Data interval")
        params = {
            "symbol": ticker,
            "interval": interval,
            "outputsize": 5000,
            "timezone": "UTC",
            "adjust": "none",
            "order": "asc",
            "apikey": self.key,
        }
        for name in ("start_date", "end_date"):
            if kwargs.get(name):
                params[name] = date.fromisoformat(str(kwargs[name])).isoformat()
        payload = await self.request_json("/time_series", params=params)
        if payload.get("status") == "error":
            if payload.get("code") == 429:
                raise RateLimitExceededError("Twelve Data credits exhausted", 60)
            raise ValueError("Twelve Data returned an error payload")
        if len(payload.get("values", [])) >= 5000:
            raise ValueError("Twelve Data response may be truncated; request a smaller date window")
        return {"ticker": ticker, "interval": interval, **payload}

    def parse(self, raw_data):
        records = []
        for row in raw_data.get("values", []):
            record = {
                "ticker": raw_data["ticker"],
                "interval": raw_data["interval"],
                **{field: row[field] for field in ("open", "high", "low", "close")},
                "volume": row.get("volume"),
                "currency": raw_data["meta"]["currency"],
                "source": "twelve_data",
            }
            if raw_data["interval"] == "1day":
                record["reference_date"] = date.fromisoformat(row["datetime"])
            else:
                record["timestamp"] = timestamp_utc(row["datetime"], "UTC")
                record["reference_date"] = (
                    record["timestamp"]
                    .astimezone(
                        __import__("zoneinfo").ZoneInfo(raw_data["meta"]["exchange_timezone"])
                    )
                    .date()
                )
            records.append(record)
        return records
