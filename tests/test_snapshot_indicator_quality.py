from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from fin_ingestion.publishers.snapshot_exporter import SnapshotExporter


@pytest.mark.parametrize('value', [41200, -1, float('nan'), float('inf'), Decimal('NaN'), 'bad', True])
def test_outlier_or_nonfinite_yield_blocks_snapshot(value):
    with pytest.raises(ValueError, match='Snapshot blocked'):
        SnapshotExporter.wrap_envelope({'rankings': [{'dividend_yield_pct': value}]}, 'rankings_summary_v1')


@pytest.mark.parametrize('value', [0, 6.5, 100, None])
def test_real_zero_valid_yield_and_absence_are_preserved(value):
    payload = {'indicadores': {'dividend_yield': value}}
    assert SnapshotExporter.wrap_envelope(payload, 'stock_detail_v1')['data'] == payload


@pytest.mark.asyncio
async def test_bad_ranking_never_replaces_local_snapshot_or_calls_remote(tmp_path):
    old_file = tmp_path / 'rankings' / 'summary.json'
    old_file.parent.mkdir()
    old_file.write_text('last good snapshot')
    connection = AsyncMock()
    connection.fetch.side_effect = [
        [dict(ticker='BAD3', dividend_yield=41200, preco_fechamento=1, nome_empresarial='bad')], []]
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=connection)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    exporter = SnapshotExporter(pool=pool, local_dir=tmp_path)
    exporter.r2_publisher = MagicMock()
    with pytest.raises(ValueError, match='Snapshot blocked'):
        await exporter.export_rankings()
    assert old_file.read_text() == 'last good snapshot'
    exporter.r2_publisher.publish.assert_not_called()
