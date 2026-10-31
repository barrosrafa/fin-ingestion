"""Explicit temporal normalization and causal filling for analytics only."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd


def timestamp_utc(value: datetime | str, source_timezone: str | None = None) -> datetime:
    if isinstance(value, str):
        if "T" not in value and " " not in value:
            raise ValueError("A date alone is not an observed timestamp")
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("An explicit observation timestamp is required")
    if value.tzinfo is None or value.utcoffset() is None:
        if source_timezone is None:
            raise ValueError("Naive timestamps require source_timezone")
        # pandas rejects both ambiguous and nonexistent DST wall times.
        value = (
            pd.Timestamp(value)
            .tz_localize(ZoneInfo(source_timezone), ambiguous="raise", nonexistent="raise")
            .to_pydatetime()
        )
    return value.astimezone(timezone.utc)


def forward_fill_prices(
    frame: pd.DataFrame, price_columns: list[str], limit: int = 3
) -> pd.DataFrame:
    """Fill existing analytic rows only, never backfill or invent market volume."""
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 3:
        raise ValueError("Forward fill is limited to 1–3 observations")
    result = frame.copy()
    result["timestamp"] = [timestamp_utc(value) for value in result["timestamp"]]
    if result["ticker"].isna().any() or result.duplicated(["ticker", "timestamp"]).any():
        raise ValueError("Ticker and timestamp must identify each analytic observation")
    result = result.sort_values(["ticker", "timestamp"], kind="stable")
    result[price_columns] = result.groupby("ticker", sort=False)[price_columns].ffill(limit=limit)
    return result
