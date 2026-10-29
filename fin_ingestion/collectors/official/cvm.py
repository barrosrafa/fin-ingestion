"""Coletor oficial CVM: Dados Abertos (DFP, ITR, Informes FII, CDA Fundos e Governança)."""

import csv
import io
import zipfile
from datetime import datetime
from typing import Any

from fin_ingestion.core.base_collector import BaseCollector


class CVMCollector(BaseCollector):
    """Coleta dados abertos regulatórios da Comissão de Valores Mobiliários (CVM)."""

    def __init__(self) -> None:
        super().__init__(name="cvm", base_url="https://dados.cvm.gov.br")

    async def extract(self, **kwargs: Any) -> Any:
        endpoint = kwargs.get("endpoint", "cad_cia_aberta")

        if endpoint == "cad_cia_aberta":
            url = "/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
            response = await self.http_client.get(url)
            return response.text

        if endpoint == "fii_informe_mensal":
            year = kwargs.get("year", datetime.now().year)
            response = await self.http_client.get(f"/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_{year}.zip")
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                def read_csv(name):
                    with archive.open(name) as data:
                        return list(csv.DictReader(io.TextIOWrapper(data, encoding="latin1"), delimiter=";"))
                general = read_csv(f"inf_mensal_fii_geral_{year}.csv")
                complement = read_csv(f"inf_mensal_fii_complemento_{year}.csv")
            keys = ("CNPJ_Fundo_Classe", "Data_Referencia", "Versao")
            additional = {tuple(r[k] for k in keys): r for r in complement}
            latest = {}
            for row in general:
                row = {**row, **additional.get(tuple(row[k] for k in keys), {})}
                cnpj = row["CNPJ_Fundo_Classe"]
                order = (row["Data_Referencia"], int(row["Versao"]))
                if cnpj not in latest or order > latest[cnpj][0]:
                    latest[cnpj] = (order, row)
            return {"endpoint": endpoint, "records": [v[1] for v in latest.values()]}

        if endpoint == "fii_informe_mensal_sample":
            # Exemplo de linha de informe mensal de FII para processamento direto
            return kwargs.get("csv_content", "")

        raise ValueError(f"Endpoint não suportado para CVM: {endpoint}")

    def parse(self, raw_data: Any) -> list[dict[str, Any]]:
        if isinstance(raw_data, dict) and raw_data.get("endpoint") == "fii_informe_mensal":
            def number(value):
                return float(value) if value not in (None, "") else None
            return [{
                "cnpj_fundo": r["CNPJ_Fundo_Classe"],
                "data_referencia": r["Data_Referencia"],
                "versao": int(r["Versao"]),
                "nome_fundo": r.get("Nome_Fundo_Classe"),
                "codigo_isin": r.get("Codigo_ISIN"),
                "administrador": r.get("Nome_Administrador"),
                "patrimonio_liquido": number(r.get("Patrimonio_Liquido")),
                "valor_patrimonial_cota": number(r.get("Valor_Patrimonial_Cotas")),
                "cotistas_total": number(r.get("Total_Numero_Cotistas")),
                "source": "cvm_inf_mensal_fii",
            } for r in raw_data["records"]]
        if not isinstance(raw_data, str) or not raw_data.strip():
            return []

        records = []
        reader = csv.DictReader(io.StringIO(raw_data), delimiter=";")

        for row in reader:
            if "CNPJ_CIA" in row:
                records.append({
                    "cnpj": row.get("CNPJ_CIA", "").strip(),
                    "razao_social": row.get("DENOM_SOCIAL", "").strip(),
                    "nome_comercial": row.get("DENOM_COMERC", "").strip(),
                    "codigo_cvm": row.get("CD_CVM", "").strip(),
                    "setor_atividade": row.get("SETOR_ATIV", "").strip(),
                    "situacao_registro": row.get("SIT", "").strip(),
                    "data_registro": row.get("DT_REG", "").strip(),
                    "categoria_registro": row.get("TP_MERC", "").strip(),
                    "source": "cvm_cad_cia",
                })
            elif "Numero_Cotistas" in row or "Valor_Patrimonial_Cotas" in row:
                # Layout de informe mensal de FII
                records.append({
                    "cnpj_fundo": row.get("CNPJ_Fundo", "").strip(),
                    "data_referencia": row.get("Data_Referencia", "").strip(),
                    "patrimonio_liquido": float(row.get("Patrimonio_Liquido", "0").replace(",", ".") or 0),
                    "valor_patrimonial_cota": float(row.get("Valor_Patrimonial_Cotas", "0").replace(",", ".") or 0),
                    "cotistas_total": int(row.get("Numero_Cotistas", "0") or 0),
                    "source": "cvm_inf_mensal_fii",
                })

        return records
