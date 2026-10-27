from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class AnbimaCriRecord:
    isin: str
    codigo_emissao: Optional[str]
    emissor_securitizadora: Optional[str]
    cnpj_devedor: str
    razao_social_devedor: str
    nome_fantasia_devedor: Optional[str]
    data_encerramento: Optional[str]
    valor_encerrado: Optional[float]
    tipo_operacao: Optional[str]
    percentual_garantia_devedor: Optional[float]
    is_post_dec_2025: bool = False


class AnbimaCriCollector:
    """Collector for ANBIMA public CRI dataset (available Oct/2026, covering deals from Dec/2025)."""

    DEC_2025_CUTOFF = datetime(2025, 12, 1)

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or "https://data.anbima.com.br/api/v1/ofertas-encerradas/cri"

    def normalize_records(self, raw_items: List[Dict[str, Any]]) -> List[AnbimaCriRecord]:
        records: List[AnbimaCriRecord] = []
        for item in raw_items:
            isin = item.get("codigo_isin") or item.get("isin")
            if not isin:
                continue

            dt_enc_str = item.get("data_encerramento")
            is_post_dec = False
            if dt_enc_str:
                try:
                    dt = datetime.strptime(dt_enc_str[:10], "%Y-%m-%d")
                    is_post_dec = dt >= self.DEC_2025_CUTOFF
                except ValueError:
                    pass

            record = AnbimaCriRecord(
                isin=isin,
                codigo_emissao=item.get("codigo_emissao"),
                emissor_securitizadora=item.get("emissor_securitizadora"),
                cnpj_devedor=item.get("cnpj_devedor", ""),
                razao_social_devedor=item.get("razao_social_devedor", ""),
                nome_fantasia_devedor=item.get("nome_fantasia_devedor"),
                data_encerramento=dt_enc_str,
                valor_encerrado=float(item["valor_encerrado"]) if "valor_encerrado" in item else None,
                tipo_operacao=item.get("tipo_operacao"),
                percentual_garantia_devedor=float(item["percentual_garantia_devedor"])
                if "percentual_garantia_devedor" in item
                else None,
                is_post_dec_2025=is_post_dec,
            )
            records.append(record)
        return records
