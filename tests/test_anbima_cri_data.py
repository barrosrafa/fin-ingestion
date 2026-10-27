import pytest
from fin_ingestion.collectors.anbima_cri_data import AnbimaCriCollector, AnbimaCriRecord


def test_normalize_anbima_records():
    raw_sample = [
        {
            "codigo_isin": "BRSTNNCRI099",
            "codigo_emissao": "CRI-JHSF-2026",
            "emissor_securitizadora": "VIRGO COMPANHIA DE SECURITIZACAO",
            "cnpj_devedor": "61.274.529/0001-00",
            "razao_social_devedor": "JHSF PARTICIPACOES S.A.",
            "nome_fantasia_devedor": "JHSF",
            "data_encerramento": "2026-01-15",
            "valor_encerrado": 150000000.00,
            "tipo_operacao": "Concentrado",
            "percentual_garantia_devedor": 100.0,
        },
        {
            "codigo_isin": "BROPEACRI088",
            "codigo_emissao": "CRI-MRV-2026",
            "emissor_securitizadora": "OPEA SECURITIZADORA S.A.",
            "cnpj_devedor": "08.343.492/0001-20",
            "razao_social_devedor": "MRV ENGENHARIA E PARTICIPACOES S.A.",
            "nome_fantasia_devedor": "MRV",
            "data_encerramento": "2026-02-20",
            "valor_encerrado": 80000000.00,
            "tipo_operacao": "Concentrado",
            "percentual_garantia_devedor": 100.0,
        },
    ]

    collector = AnbimaCriCollector()
    normalized = collector.normalize_records(raw_sample)

    assert len(normalized) == 2
    rec1 = normalized[0]
    assert isinstance(rec1, AnbimaCriRecord)
    assert rec1.isin == "BRSTNNCRI099"
    assert rec1.cnpj_devedor == "61.274.529/0001-00"
    assert rec1.razao_social_devedor == "JHSF PARTICIPACOES S.A."
    assert rec1.is_post_dec_2025 is True


def test_anbima_pre_dec_2025_flag():
    raw_sample = [
        {
            "codigo_isin": "BRLEGACY001",
            "codigo_emissao": "CRI-OLD-2024",
            "cnpj_devedor": "11.222.333/0001-44",
            "razao_social_devedor": "OLD CORP S.A.",
            "data_encerramento": "2024-05-10",
            "tipo_operacao": "Concentrado",
        }
    ]
    collector = AnbimaCriCollector()
    normalized = collector.normalize_records(raw_sample)
    assert normalized[0].is_post_dec_2025 is False
