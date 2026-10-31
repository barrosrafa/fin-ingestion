"""Point-in-time total-return adjustments over immutable nominal prices."""

from datetime import date
from decimal import Decimal

from fin_ingestion.quality.temporal import timestamp_utc


def adjusted_prices(prices: list[dict], events: list[dict], as_of) -> list[dict]:
    """Use only event revisions known by as_of; reference_close is the cum-event price.

    Each event requires source/event_id/revision, observed_at, ex_date and a provider
    publication timestamp. Missing publication dates cannot be guessed as historical.
    """
    as_of = timestamp_utc(as_of)
    latest = {}
    for event in events:
        known = timestamp_utc(event["observed_at"])
        published = timestamp_utc(event["published_at"])
        if max(known, published) > as_of:
            continue
        key = (event["source"], event["event_id"])
        if key not in latest or known > timestamp_utc(latest[key]["observed_at"]):
            latest[key] = event
    output = []
    for price in prices:
        day = date.fromisoformat(str(price["reference_date"]))
        if day > as_of.date():
            continue
        factor = Decimal(1)
        for event in latest.values():
            ex_date = date.fromisoformat(str(event["ex_date"]))
            if event["ticker"] != price["ticker"] or not day < ex_date <= as_of.date():
                continue
            if event["event_type"] == "split":
                split = Decimal(str(event["factor"]))
                if not split.is_finite() or split <= 0:
                    raise ValueError("Invalid split factor")
                factor /= split
            elif event["event_type"] == "dividend":
                dividend, reference = (
                    Decimal(str(event["amount"])),
                    Decimal(str(event["reference_close"])),
                )
                if (
                    not dividend.is_finite()
                    or not reference.is_finite()
                    or not 0 <= dividend < reference
                ):
                    raise ValueError("Invalid cum-event reference price or dividend")
                factor *= 1 - dividend / reference
            else:
                raise ValueError("Unsupported corporate adjustment")
        nominal = Decimal(str(price["close"]))
        if not nominal.is_finite() or nominal <= 0:
            raise ValueError("Invalid nominal price")
        output.append({**price, "adjustment_factor": factor, "adjusted_close": nominal * factor})
    return output
