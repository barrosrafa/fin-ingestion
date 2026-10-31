"""brapi v2: dedicated snapshots and explicitly raw daily history."""

from datetime import datetime, timezone

from fin_ingestion.collectors.commercial.base import CommercialCollector, symbol
from fin_ingestion.config import settings
from fin_ingestion.quality.temporal import timestamp_utc


class BrapiCollector(CommercialCollector):
    def __init__(self):
        key = settings.brapi_api_key
        super().__init__("brapi", "https://api.brapi.dev", key.get_secret_value() if key else None)

    async def extract(self, **kwargs):
        tickers = list(dict.fromkeys(symbol(t) for t in kwargs.get("tickers", [])))
        if not tickers:
            raise ValueError("brapi requires explicit tickers")
        endpoint = kwargs.get("endpoint", "quote")
        if endpoint not in {"quote", "historical", "dividends"}:
            raise ValueError("Unsupported brapi endpoint")
        if endpoint == "historical" and not settings.brapi_raw_history:
            raise ValueError("Raw historical access must be enabled for the contracted plan")
        results = []
        for start in range(0, len(tickers), settings.brapi_batch_size):
            params = {"symbols": ",".join(tickers[start : start + settings.brapi_batch_size])}
            if endpoint == "historical":
                params.update(
                    interval="1d",
                    range=kwargs.get("range", "1mo"),
                    sortOrder="asc",
                    includeRaw="true",
                )
            if endpoint == "dividends":
                params.update(sortOrder="asc", includeRaw="true")
            payload = await self.request_json(
                f"/api/v2/stocks/{endpoint}",
                params=params,
                headers={"Authorization": f"Bearer {self.key}"},
            )
            results.extend(payload["results"])
        returned = {row["requestedSymbol"] for row in results}
        if returned != set(tickers):
            raise ValueError("brapi returned incomplete symbol coverage")
        return {"endpoint": endpoint, "results": results}

    def parse(self, raw_data):
        records = []
        endpoint = raw_data["endpoint"]
        for result in raw_data["results"]:
            ticker, data = symbol(result["symbol"]), result["data"]
            if endpoint == "quote":
                records.append(
                    {
                        "ticker": ticker,
                        "timestamp": timestamp_utc(data["regularMarketTime"]),
                        "price": data["regularMarketPrice"],
                        "volume": data.get("regularMarketVolume"),
                        "currency": data["currency"],
                        "source": "brapi",
                        "price_kind": "intraday_snapshot",
                    }
                )
            elif endpoint == "historical":
                for bar in data["historicalDataPrice"]:
                    if any(
                        bar.get("raw" + name) is None for name in ("Open", "High", "Low", "Close")
                    ):
                        raise ValueError(
                            "brapi history has no verified raw OHLC; adjusted prices refused"
                        )
                    records.append(
                        {
                            "ticker": ticker,
                            "reference_date": datetime.fromtimestamp(
                                bar["date"], timezone.utc
                            ).date(),
                            **{
                                name.lower(): bar["raw" + name]
                                for name in ("Open", "High", "Low", "Close")
                            },
                            "volume": bar.get("volume"),
                            "source": "brapi",
                            "currency": "BRL",
                        }
                    )
            else:
                for kind in ("cashDividends", "stockDividends", "subscriptions"):
                    for event in data.get(kind, []):
                        records.append(
                            {
                                "ticker": ticker,
                                "event_type": kind,
                                "source": "brapi",
                                "payload": event,
                            }
                        )
        return records
