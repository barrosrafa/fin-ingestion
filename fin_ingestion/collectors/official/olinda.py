"""Bounded, stable OData pagination for official PTAX bulletins and Focus releases."""

from datetime import date
from urllib.parse import quote, urlencode

from fin_ingestion.core.base_collector import BaseCollector
from fin_ingestion.quality.temporal import timestamp_utc


class OlindaCollector(BaseCollector):
    def __init__(self):
        super().__init__("bcb_olinda", "https://olinda.bcb.gov.br", requests_per_second=1)

    async def extract(self, **kwargs):
        endpoint = kwargs.get("endpoint", "ptax")
        day = date.fromisoformat(str(kwargs["reference_date"]))
        currency = kwargs.get("currency", "USD")
        if endpoint == "ptax":
            if len(currency) != 3 or not currency.isalpha() or not currency.isupper():
                raise ValueError("Invalid currency code")
            path = "/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)"
            query = {
                "@moeda": f"'{currency}'",
                "@dataCotacao": f"'{day:%m-%d-%Y}'",
                "$orderby": "dataHoraCotacao,tipoBoletim",
            }
        elif endpoint in {"ExpectativasMercadoAnuais", "ExpectativasMercadoSelic"}:
            path = f"/olinda/servico/Expectativas/versao/v1/odata/{endpoint}"
            probe = {
                "$filter": f"Data le '{day}'",
                "$orderby": "Data desc",
                "$top": 1,
                "$format": "json",
            }
            response = await self.http_client.get(path + "?" + urlencode(probe, quote_via=quote))
            response.raise_for_status()
            await self.raw_sink.save_payload(
                self.name, path, response.content, metadata={"discovery_as_of": day}
            )
            latest = response.json()["value"]
            if not latest:
                raise ValueError("No Focus release available as of requested date")
            published_on = date.fromisoformat(latest[0]["Data"])
            reference_field = (
                "Reuniao" if endpoint == "ExpectativasMercadoSelic" else "DataReferencia"
            )
            query = {
                "$filter": f"Data eq '{published_on}'",
                "$orderby": f"Indicador,Data,{reference_field},baseCalculo"
                + (",IndicadorDetalhe" if endpoint == "ExpectativasMercadoAnuais" else ""),
            }
        else:
            raise ValueError("Unsupported Olinda dataset")
        rows = []
        for page in range(100):
            params = {**query, "$format": "json", "$top": 1000, "$skip": page * 1000}
            # Olinda treats '+' literally inside OData expressions; encode spaces as %20.
            response = await self.http_client.get(path + "?" + urlencode(params, quote_via=quote))
            response.raise_for_status()
            await self.raw_sink.save_payload(
                self.name, path, response.content, metadata={"page": page, "reference_date": day}
            )
            values = response.json()["value"]
            if not isinstance(values, list):
                raise ValueError("Invalid OData values")
            rows.extend(values)
            if len(values) < 1000:
                return {"endpoint": endpoint, "currency": currency, "data": rows}
        raise ValueError("OData pagination exceeded 100 pages")

    def parse(self, raw_data):
        if raw_data["endpoint"] == "ptax":
            return [
                {
                    "currency": raw_data["currency"],
                    "quoted_at": timestamp_utc(row["dataHoraCotacao"], "America/Sao_Paulo"),
                    "bulletin": row["tipoBoletim"],
                    "buy": row["cotacaoCompra"],
                    "sell": row["cotacaoVenda"],
                    "source": "bcb_olinda",
                }
                for row in raw_data["data"]
            ]
        return [
            {
                "indicator": row["Indicador"],
                "detail": row.get("IndicadorDetalhe") or "not_applicable",
                "published_on": row["Data"],
                "reference": row["Reuniao"]
                if raw_data["endpoint"] == "ExpectativasMercadoSelic"
                else row["DataReferencia"],
                "base_calculation": row["baseCalculo"],
                "frequency": raw_data["endpoint"],
                "source": "bcb_focus",
                "payload": row,
            }
            for row in raw_data["data"]
        ]
