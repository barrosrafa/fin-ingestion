"""Strict Silver schema for complete nominal daily candles; no imputation on ingestion."""

import numpy as np
import pandas as pd
import pandera.pandas as pa

PRICE_COLUMNS = ["preco_abertura", "preco_maximo", "preco_minimo", "preco_fechamento"]

DAILY_CANDLE_SCHEMA = pa.DataFrameSchema(
    {
        "ticker": pa.Column(str, pa.Check.str_length(min_value=1)),
        "data_pregao": pa.Column(pa.DateTime),
        **{
            column: pa.Column(float, [pa.Check.gt(0), pa.Check(np.isfinite)])
            for column in PRICE_COLUMNS
        },
        "volume_total": pa.Column(float, [pa.Check.ge(0), pa.Check(np.isfinite)], nullable=True),
    },
    checks=pa.Check(
        lambda frame: (
            (frame.preco_minimo <= frame.preco_abertura)
            & (frame.preco_abertura <= frame.preco_maximo)
            & (frame.preco_minimo <= frame.preco_fechamento)
            & (frame.preco_fechamento <= frame.preco_maximo)
        ),
        error="OHLC geometry is inconsistent",
    ),
    strict=True,
    coerce=True,
)


def validate_complete_candles(quotes: list[dict]) -> None:
    """Validate canonical fields only; provider metadata stays outside the Silver projection."""
    records = []
    for quote in quotes:
        if all(quote.get(column) is not None for column in PRICE_COLUMNS):
            records.append(
                {
                    "ticker": quote.get("ticker") or str(quote.get("id_ativo") or ""),
                    "data_pregao": quote.get("data_pregao"),
                    **{column: quote[column] for column in PRICE_COLUMNS},
                    "volume_total": quote.get("volume_total", quote.get("volume_financeiro")),
                }
            )
    if records:
        DAILY_CANDLE_SCHEMA.validate(pd.DataFrame(records), lazy=True)
