"""Executable provider datasets: extract -> Bronze -> validated Silver -> explicit result."""

from datetime import date

from fin_ingestion.collectors.commercial.anbima_feed import AnbimaFeedCollector
from fin_ingestion.collectors.commercial.international import (
    FMPCollector,
    MassiveCollector,
    TwelveDataCollector,
)
from fin_ingestion.collectors.official.b3 import B3Collector
from fin_ingestion.collectors.official.bcb import BCBCollector
from fin_ingestion.collectors.official.cvm import CVMCollector
from fin_ingestion.collectors.official.olinda import OlindaCollector
from fin_ingestion.collectors.official.tesouro import TesouroCollector
from fin_ingestion.dw.silver_writer import SilverWriter
from fin_ingestion.pipelines.result import require_complete_result, summarize_results


class ProviderPipeline:
    def __init__(self, dataset: str, pool=None):
        self.dataset = dataset
        self.writer = SilverWriter(pool)

    async def run(self, tickers=None, reference_date=None, **kwargs):
        from datetime import datetime

        from fin_ingestion.scheduler.market_calendar import TZ_SP

        day = reference_date or datetime.now(TZ_SP).date()
        day = date.fromisoformat(day) if isinstance(day, str) else day
        if self.dataset in {"ptax", "focus"}:
            collector, domain = OlindaCollector(), self.dataset
            params = {
                "endpoint": "ptax" if domain == "ptax" else "ExpectativasMercadoAnuais",
                "reference_date": day,
            }
        elif self.dataset == "sgs":
            collector, domain = BCBCollector(), "sgs"
            params = {
                k: v
                for k, v in kwargs.items()
                if k in {"serie", "serie_id", "start_date", "end_date", "ultimos"}
            }
            if not any(k in params for k in ("start_date", "end_date", "ultimos")):
                from datetime import timedelta

                params.update(start_date=day - timedelta(days=35), end_date=day)
        elif self.dataset == "tesouro":
            collector, domain, params = TesouroCollector(), "tesouro", {}
        elif self.dataset == "anbima_feed":
            collector, domain, params = (
                AnbimaFeedCollector(),
                "anbima_feed",
                {"reference_date": day},
            )
        elif self.dataset in {"cvm_dfp", "cvm_itr"}:
            collector, domain = CVMCollector(), "financials"
            params = {
                "endpoint": "financials",
                "filing": self.dataset.split("_")[1],
                "year": kwargs.get("year", day.year),
                "cnpjs": kwargs.get("cnpjs"),
            }
        elif self.dataset == "b3_history":
            if not tickers or kwargs.get("year") is None:
                raise ValueError("Annual COTAHIST requires explicit tickers and year")
            collector, domain, params = (
                B3Collector(),
                "bars",
                {"endpoint": "cotahist", "year": kwargs["year"]},
            )
        elif self.dataset in {"massive", "twelve_data", "fmp"}:
            collector = {
                "massive": MassiveCollector,
                "twelve_data": TwelveDataCollector,
                "fmp": FMPCollector,
            }[self.dataset]()
            domain, params = ("provider" if self.dataset == "fmp" else "bars"), dict(kwargs)
            if self.dataset in {"massive", "twelve_data"}:
                params.setdefault("start_date", day.isoformat())
                params.setdefault("end_date", day.isoformat())
        else:
            raise ValueError("Unknown provider pipeline")
        results, records = {}, []
        try:
            if self.dataset in {"massive", "twelve_data", "fmp"} and not (
                self.dataset == "fmp" and params.get("endpoint") == "company-screener"
            ):
                if not tickers:
                    raise ValueError("Commercial pipelines require an explicit ticker universe")
                for ticker in tickers:
                    result = await collector.run(ticker=ticker, **params)
                    results[ticker] = result
                    records.extend(result.data)
            else:
                result = await collector.run(**params)
                results[self.dataset] = result
                records.extend(result.data)
            summary = summarize_results(self.dataset, results)
            require_complete_result(summary)
            if domain == "sgs":
                records = [
                    {
                        "series_code": r["serie_id"],
                        "series_name": r["serie_nome"],
                        "reference_date": r["data"],
                        "value": r["valor"],
                    }
                    for r in records
                ]
            if self.dataset == "b3_history":
                normalized = []
                for row in records:
                    if row["ticker"] not in tickers or row["tipo_mercado"] != 10:
                        continue
                    factor = row["fator_cotacao"]
                    if factor <= 0:
                        raise ValueError("Invalid COTAHIST quotation factor")
                    normalized.append(
                        {
                            "ticker": row["ticker"],
                            "reference_date": row["data_pregao"],
                            "open": row["preco_abertura"] / factor,
                            "high": row["preco_maximo"] / factor,
                            "low": row["preco_minimo"] / factor,
                            "close": row["preco_fechamento"] / factor,
                            "volume": row["quantidade_titulos"],
                            "currency": "BRL",
                            "source": "b3",
                        }
                    )
                records = normalized
                if {r["ticker"] for r in records} != set(tickers):
                    raise ValueError(
                        "Annual COTAHIST coverage is incomplete for the requested universe"
                    )
            if domain == "tesouro":
                if kwargs.get("latest_on_or_before"):
                    available = [r["reference_date"] for r in records if r["reference_date"] <= day]
                    if not available:
                        raise ValueError("No Treasury reference available as of the requested date")
                    day = max(available)
                records = [r for r in records if r["reference_date"] == day]
            if domain == "financials" and kwargs.get("cnpjs"):
                records = [r for r in records if r["cnpj"] in set(kwargs["cnpjs"])]
            if domain == "anbima_feed" and any(r["reference_date"] != day for r in records):
                raise ValueError("ANBIMA returned a different reference date")
            summary["records_loaded"] = await self.writer.write_records(domain, records)
            summary["watermark_date"] = (
                max(str(r["published_on"]) for r in records)
                if domain == "focus"
                else max(str(r["reference_date"]) for r in records)
                if self.dataset == "b3_history" or domain in {"financials", "sgs"}
                else day.isoformat()
            )
            return summary
        finally:
            await collector.close()
            await self.writer.close()
