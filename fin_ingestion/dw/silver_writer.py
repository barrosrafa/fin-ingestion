"""Transactional, idempotent persistence for explicitly normalized provider domains."""

import hashlib
import json
from datetime import date
from decimal import Decimal

from fin_ingestion.dw.dw_writer import DWWriter
from fin_ingestion.quality.temporal import timestamp_utc


def json_text(value):
    return json.dumps(value, sort_keys=True, default=str, allow_nan=False)


def stable_key(value):
    return hashlib.sha256(json_text(value).encode()).hexdigest()


SPECS = {
    "sgs":("market.sgs_observation","series_code reference_date series_name value","series_code reference_date"),
    "ptax": (
        "market.ptax_observation",
        "currency quoted_at bulletin buy sell",
        "currency quoted_at bulletin",
    ),
    "tesouro": (
        "market.treasury_quote",
        "title maturity reference_date buy_rate sell_rate buy_price sell_price base_price",
        "title maturity reference_date",
    ),
    "focus": (
        "market.focus_expectation",
        "indicator detail published_on reference base_calculation frequency payload",
        "indicator detail published_on reference base_calculation frequency",
    ),
    "anbima_feed": (
        "market.debenture_quote",
        "code reference_date source payload",
        "code reference_date source",
    ),
    "financials": (
        "canonical.cvm_statement",
        "cnpj filing statement scope reference_date version account period_start period_end exercise_order currency scale amount description payload",
        "cnpj filing statement scope reference_date version account period_start period_end exercise_order currency scale",
    ),
    "bars": (
        "market.nominal_bar",
        "ticker source interval reference_date timestamp_utc open high low close volume currency bar_key",
        "ticker source interval bar_key",
    ),
    "provider": (
        "canonical.provider_record",
        "source dataset record_key payload",
        "source dataset record_key",
    ),
    "events": (
        "canonical.corporate_action_revision",
        "source event_id revision_hash ticker event_type payload",
        "source event_id revision_hash",
    ),
}
NUMERIC = {
    "value",
    "buy",
    "sell",
    "buy_rate",
    "sell_rate",
    "buy_price",
    "sell_price",
    "base_price",
    "amount",
    "open",
    "high",
    "low",
    "close",
    "volume",
}
DATES = {"maturity", "reference_date", "published_on", "period_end"}


class SilverWriter(DWWriter):
    def __init__(self, pool=None):
        super().__init__()
        self._pool = pool
        self._external_pool = pool is not None

    async def close(self):
        if not self._external_pool:
            await super().close()

    async def write_records(self, domain: str, rows: list[dict]) -> int:
        if not rows:
            raise ValueError(f"Empty {domain} batch")
        table, fields_text, key_text = SPECS[domain]
        fields, keys = fields_text.split(), key_text.split()
        records = []
        for original in rows:
            row = dict(original)
            if domain == "bars":
                row["interval"] = (
                    "day" if row.get("interval", "day") == "1day" else row.get("interval", "day")
                )
                row["timestamp_utc"] = row.get("timestamp")
                row["bar_key"] = (
                    str(row["reference_date"])
                    if row["interval"] == "day"
                    else timestamp_utc(row["timestamp"]).isoformat()
                )
            if domain == "provider":
                payload = row["payload"]
                # A corrected metric replaces the same fiscal identity, not a second fact.
                identity = {
                    k: payload.get(k)
                    for k in ("symbol", "date", "calendarYear", "period", "fillingDate")
                }
                row["record_key"] = stable_key(identity)
            if domain == "events":
                if not row.get("event_id"):
                    raise ValueError(
                        "Corporate actions require a stable provider event_id; inferred amount-based keys refused"
                    )
                row["revision_hash"] = stable_key(row["payload"])
            for key in keys:
                if row.get(key) is None or row[key] == "":
                    raise ValueError(f"Missing {domain} identity field: {key}")
            values = []
            for field in fields:
                value = row.get(field)
                if field in DATES and isinstance(value, str):
                    value = date.fromisoformat(value)
                if field in {"quoted_at", "timestamp_utc"} and value is not None:
                    value = timestamp_utc(value)
                if field in NUMERIC and value is not None:
                    value = Decimal(str(value))
                    if not value.is_finite():
                        raise ValueError(f"Non-finite {domain}.{field}")
                if field == "payload":
                    value = json_text(value)
                values.append(value)
            records.append(tuple(values))
        placeholders = ",".join(
            f"${i + 1}" + ("::jsonb" if f == "payload" else "") for i, f in enumerate(fields)
        )
        updates = ",".join(f"{f}=EXCLUDED.{f}" for f in fields if f not in keys)
        conflict = "DO NOTHING" if domain == "events" else f"DO UPDATE SET {updates}"
        query = f"INSERT INTO {table} ({','.join(fields)}) VALUES ({placeholders}) ON CONFLICT ({','.join(keys)}) {conflict}"
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, records)
        return len(records)
