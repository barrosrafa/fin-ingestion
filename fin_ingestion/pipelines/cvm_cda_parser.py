from dataclasses import dataclass, field
from typing import Any


@dataclass
class CdaHoldingItem:
    isin: str
    ativo_tipo: str
    peso_pl_pct: float
    valor_contabil: float
    canonical_group: str | None = None
    cnpj_devedor: str | None = None
    coverage_category: str = "TEMPORARY_PENDING_CURATION" # RESOLVED, TEMPORARY_PENDING_CURATION, STRUCTURE_OPAQUE
    provenance: str = "CVM_CDA_175"


@dataclass
class CdaPortfolioResult:
    ticker: str
    pl_total: float
    pct_resolved: float = 0.0
    pct_temporary_pending: float = 0.0
    pct_structure_opaque: float = 0.0
    items: list[CdaHoldingItem] = field(default_factory=list)


class CvmCdaParser:
    """Parser for CVM 175 CDA FII holdings resolving underlying debtors."""

    def __init__(
        self,
        anbima_lookup: dict[str, dict[str, Any]] | None = None,
        legacy_seed_lookup: dict[str, dict[str, Any]] | None = None,
    ):
        self.anbima_lookup = anbima_lookup or {}
        self.legacy_seed_lookup = legacy_seed_lookup or {}

    def parse_fii_cda(
        self,
        ticker: str,
        pl_total: float,
        raw_holdings: list[dict[str, Any]],
    ) -> CdaPortfolioResult:
        items: list[CdaHoldingItem] = []
        pct_resolved = 0.0
        pct_temp_pending = 0.0
        pct_opaque = 0.0

        for h in raw_holdings:
            isin = h.get("isin", "")
            ativo_tipo = h.get("ativo_tipo", "CRI")
            peso_pl = float(h.get("peso_pl_pct", 0.0))
            valor = float(h.get("valor_contabil", 0.0))
            is_pulverized = bool(h.get("is_pulverized", False))

            if is_pulverized:
                category = "STRUCTURE_OPAQUE"
                group = None
                cnpj = None
                pct_opaque += peso_pl
            elif isin in self.anbima_lookup and self.anbima_lookup[isin].get("is_post_dec_2025"):
                category = "RESOLVED"
                info = self.anbima_lookup[isin]
                group = info.get("canonical_group")
                cnpj = info.get("cnpj_devedor")
                pct_resolved += peso_pl
            elif isin in self.legacy_seed_lookup:
                category = "RESOLVED"
                info = self.legacy_seed_lookup[isin]
                group = info.get("canonical_group")
                cnpj = info.get("cnpj_devedor")
                pct_resolved += peso_pl
            else:
                category = "TEMPORARY_PENDING_CURATION"
                group = None
                cnpj = None
                pct_temp_pending += peso_pl

            items.append(
                CdaHoldingItem(
                    isin=isin,
                    ativo_tipo=ativo_tipo,
                    peso_pl_pct=peso_pl,
                    valor_contabil=valor,
                    canonical_group=group,
                    cnpj_devedor=cnpj,
                    coverage_category=category,
                )
            )

        return CdaPortfolioResult(
            ticker=ticker,
            pl_total=pl_total,
            pct_resolved=pct_resolved,
            pct_temporary_pending=pct_temp_pending,
            pct_structure_opaque=pct_opaque,
            items=items,
        )
