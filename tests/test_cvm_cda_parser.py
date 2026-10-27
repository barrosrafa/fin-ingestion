import pytest
from fin_ingestion.pipelines.cvm_cda_parser import CvmCdaParser, CdaHoldingItem, CdaPortfolioResult


def test_cvm_cda_parser_resolution():
    holdings = [
        # 1. Matches ANBIMA
        {
            "ativo_tipo": "CRI",
            "isin": "BRSTNNCRI099",
            "valor_contabil": 1850000.0,
            "peso_pl_pct": 18.5,
        },
        # 2. Matches Legacy Seed
        {
            "ativo_tipo": "CRI",
            "isin": "BRSTNNCRI001",
            "valor_contabil": 1200000.0,
            "peso_pl_pct": 12.0,
        },
        # 3. Pre-Dec 2025 without seed -> TEMPORARY_PENDING_CURATION
        {
            "ativo_tipo": "CRI",
            "isin": "BRPENDING001",
            "valor_contabil": 400000.0,
            "peso_pl_pct": 4.0,
            "data_emissao": "2024-03-01",
        },
        # 4. Pulverized CRI without concentration -> STRUCTURE_OPAQUE
        {
            "ativo_tipo": "CRI",
            "isin": "BROPAQUE001",
            "valor_contabil": 250000.0,
            "peso_pl_pct": 2.5,
            "is_pulverized": True,
        },
    ]

    anbima_lookup = {
        "BRSTNNCRI099": {
            "cnpj_devedor": "61.274.529/0001-00",
            "canonical_group": "Grupo JHSF",
            "is_post_dec_2025": True,
        }
    }

    legacy_seed_lookup = {
        "BRSTNNCRI001": {
            "cnpj_devedor": "61.274.529/0001-00",
            "canonical_group": "Grupo JHSF",
        }
    }

    parser = CvmCdaParser(anbima_lookup=anbima_lookup, legacy_seed_lookup=legacy_seed_lookup)
    result = parser.parse_fii_cda(ticker="KNIP11", pl_total=10000000.0, raw_holdings=holdings)

    assert isinstance(result, CdaPortfolioResult)
    assert result.ticker == "KNIP11"
    assert result.pct_resolved == pytest.approx(30.5, 0.01) # 18.5 + 12.0
    assert result.pct_temporary_pending == pytest.approx(4.0, 0.01)
    assert result.pct_structure_opaque == pytest.approx(2.5, 0.01)
    assert len(result.items) == 4

    # Check JHSF aggregated exposure in KNIP11
    jhsf_items = [i for i in result.items if i.canonical_group == "Grupo JHSF"]
    assert len(jhsf_items) == 2
    assert sum(i.peso_pl_pct for i in jhsf_items) == pytest.approx(30.5, 0.01)
