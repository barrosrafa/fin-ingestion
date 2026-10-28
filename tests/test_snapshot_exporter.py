"""Testes unitários para o SnapshotExporter e os publicadores Local e R2."""

import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from fin_ingestion.publishers.snapshot_exporter import (
    CloudflareR2Publisher,
    LocalFilePublisher,
    SnapshotExporter,
    SnapshotJSONEncoder,
)


def test_snapshot_json_encoder():
    data = {
        "val_decimal": Decimal("12.34"),
        "val_int": 42,
        "val_str": "PETR4",
    }
    encoded = json.dumps(data, cls=SnapshotJSONEncoder)
    decoded = json.loads(encoded)
    assert decoded["val_decimal"] == 12.34
    assert decoded["val_int"] == 42
    assert decoded["val_str"] == "PETR4"


def test_local_file_publisher(tmp_path: Path):
    publisher = LocalFilePublisher(base_dir=tmp_path)
    sample_data = {"ticker": "PETR4", "price": 36.50}

    saved_path = publisher.publish("acoes/PETR4.json", sample_data)
    assert Path(saved_path).exists()

    content = json.loads(Path(saved_path).read_text(encoding="utf-8"))
    assert content["ticker"] == "PETR4"
    assert content["price"] == 36.50


def test_cloudflare_r2_publisher_unconfigured():
    publisher = CloudflareR2Publisher(
        account_id="",
        access_key_id="",
        secret_access_key="",
        bucket_name="",
    )
    assert not publisher.is_configured()
    assert not publisher.publish("test.json", {"key": "value"})


@pytest.mark.asyncio
async def test_snapshot_exporter_wrap_envelope():
    data = {"ticker": "VALE3", "price": 62.50}
    envelope = SnapshotExporter.wrap_envelope(data, schema_name="stock_detail_v1")

    assert envelope["meta"]["schema"] == "stock_detail_v1"
    assert envelope["meta"]["schema_version"] == "1.0.0"
    assert envelope["meta"]["content_hash"].startswith("sha256:")
    assert envelope["data"] == data


@pytest.mark.asyncio
async def test_snapshot_exporter_stock(tmp_path: Path):
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_conn
    mock_ctx.__aexit__.return_value = None
    mock_pool.acquire.return_value = mock_ctx

    mock_conn.fetchrow.side_effect = [
        # 1. Ativo cadastral
        {
            "id_ativo": 1,
            "ticker": "PETR4",
            "tipo_ativo": "ACAO",
            "nome_empresarial": "Petroleo Brasileiro S.A.",
            "cnpj": "33.000.167/0001-01",
            "setor": "Petróleo e Gás",
            "subsetor": "Exploração e Refino",
            "cnae": "0600-0/01",
        },
        # 2. Cotação
        {
            "data_pregao": "2026-10-27",
            "preco_fechamento": Decimal("36.50"),
            "preco_abertura": Decimal("36.00"),
            "preco_maximo": Decimal("36.80"),
            "preco_minimo": Decimal("35.90"),
            "preco_medio": Decimal("36.40"),
            "volume_total": Decimal("1500000000.00"),
            "quantidade_negocios": 45000,
            "variacao_percentual": Decimal("1.25"),
        },
        # 3. Indicador
        {
            "data_referencia": "2026-10-27",
            "pl": Decimal("4.12"),
            "pvp": Decimal("0.95"),
            "psr": Decimal("0.85"),
            "dividend_yield": Decimal("14.50"),
            "payout": Decimal("45.00"),
            "roe": Decimal("23.50"),
            "roic": Decimal("18.20"),
            "ebitda_margem": Decimal("42.00"),
            "margem_liquida": Decimal("22.30"),
            "divida_liquida_ebitda": Decimal("0.85"),
            "lpa": Decimal("8.85"),
            "vpa": Decimal("38.40"),
            "ev_ebitda": Decimal("2.80"),
            "ev_ebit": Decimal("3.10"),
            "liquidez_corrente": Decimal("1.25"),
            "giro_ativo": Decimal("0.45"),
        },
    ]

    mock_conn.fetch.side_effect = [
        # Valuation
        [
            {
                "metodologia": "BENJAMIN_GRAHAM_V1",
                "preco_justo": Decimal("45.00"),
                "preco_mercado": Decimal("36.50"),
                "margem_seguranca": Decimal("0.2328"),
                "parametros": {},
            }
        ],
        # Proventos
        [
            {
                "tipo_provento": "DIVIDENDO",
                "valor_por_acao": Decimal("1.50"),
                "data_com": "2026-08-15",
                "data_pagamento": "2026-09-01",
            }
        ],
    ]

    exporter = SnapshotExporter(pool=mock_pool, local_dir=tmp_path)
    snapshot = await exporter.export_stock_snapshot("PETR4")

    assert snapshot is not None
    assert snapshot["meta"]["schema"] == "stock_detail_v1"
    assert snapshot["data"]["ticker"] == "PETR4"
    assert snapshot["data"]["cotacao"]["preco_fechamento"] == 36.50
    assert snapshot["data"]["indicadores"]["pl"] == 4.12
    assert snapshot["data"]["valuation"]["benjamin_graham_v1"]["preco_justo"] == 45.00
    assert len(snapshot["data"]["proventos"]) == 1

    saved_file = tmp_path / "acoes" / "PETR4.json"
    assert saved_file.exists()


@pytest.mark.asyncio
async def test_snapshot_exporter_fii(tmp_path: Path):
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_conn
    mock_ctx.__aexit__.return_value = None
    mock_pool.acquire.return_value = mock_ctx

    mock_conn.fetchrow.side_effect = [
        # 1. FII row
        {
            "id_ativo": 2,
            "ticker": "HGLG11",
            "administrador": "Credit Suisse",
            "gestor": "Credit Suisse Hedging-Griffo",
            "numero_cotistas": 350000,
            "prazo_duracao": "Indeterminado",
        },
        # 2. Cotacao
        {
            "data_pregao": "2026-10-27",
            "preco_fechamento": Decimal("162.50"),
            "variacao_percentual": Decimal("0.50"),
            "volume_total": Decimal("8500000.00"),
        },
    ]

    mock_conn.fetch.side_effect = [
        # Imoveis
        [
            {
                "nome_imovel": "Centro Logístico Itupeva",
                "endereco": "Rod. dos Bandeirantes, km 72",
                "cidade": "Itupeva",
                "estado": "SP",
                "area_bruta_locavel_m2": Decimal("124500.00"),
                "participacao_pct": Decimal("100.00"),
            }
        ],
        # Vacancia
        [
            {
                "data_referencia": "2026-10-01",
                "vacancia_fisica_pct": Decimal("5.20"),
                "vacancia_financeira_pct": Decimal("3.80"),
            }
        ],
        # Proventos / Rendimentos
        [
            {
                "tipo_provento": "RENDIMENTO",
                "valor_bruto": Decimal("1.10"),
                "data_com": "2026-10-15",
                "data_pagamento": "2026-10-25",
            }
        ],
    ]

    exporter = SnapshotExporter(pool=mock_pool, local_dir=tmp_path)
    snapshot = await exporter.export_fii_snapshot("HGLG11")

    assert snapshot is not None
    assert snapshot["meta"]["schema"] == "fii_detail_v1"
    assert snapshot["data"]["ticker"] == "HGLG11"
    assert snapshot["data"]["cotacao"]["preco_fechamento"] == 162.50
    assert len(snapshot["data"]["imoveis"]) == 1
    assert snapshot["data"]["imoveis"][0]["cidade"] == "Itupeva"

    saved_file = tmp_path / "fiis" / "HGLG11.json"
    assert saved_file.exists()
