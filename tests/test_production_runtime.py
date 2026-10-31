import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from fin_ingestion.config import settings
from fin_ingestion.operations.production import datasets_for, execute, preflight


def test_production_plan_never_polls_closed_intraday_session(monkeypatch):
    monkeypatch.setattr(settings, "intraday_enabled", True)
    assert datasets_for("intraday", datetime(2026, 10, 31, 15, tzinfo=timezone.utc)) == []
    assert datasets_for("intraday", datetime(2026, 10, 30, 12, tzinfo=timezone.utc)) == []
    assert datasets_for("intraday", datetime(2026, 10, 30, 15, tzinfo=timezone.utc)) == ["intraday"]
    assert datasets_for("intraday", datetime(2026, 11, 2, 15, tzinfo=timezone.utc)) == []


def test_profiles_preserve_release_and_public_calendars():
    monday = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)
    tuesday = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
    assert datasets_for("morning", monday) == ["sgs", "focus"]
    assert datasets_for("morning", tuesday) == ["sgs"]
    assert datasets_for("ptax", datetime(2026, 11, 2, 15, tzinfo=timezone.utc)) == []
    assert "brapi_preflight" in datasets_for("deploy", monday)


@pytest.mark.asyncio
async def test_closed_session_writes_skip_receipt_without_connections(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "intraday_enabled", False)
    output = tmp_path / "receipt.json"
    with patch("fin_ingestion.operations.production.preflight", new_callable=AsyncMock) as network:
        assert await execute("intraday", output=output)
        network.assert_not_called()
    assert json.loads(output.read_text())["status"] == "SKIPPED_OUTSIDE_WINDOW"


@pytest.mark.asyncio
async def test_production_rejects_plaintext_shared_redis_before_connect(monkeypatch):
    monkeypatch.setattr(settings, "ingestion_env", "production")
    monkeypatch.setattr(settings, "redis_url", "redis://localhost:6379")
    with pytest.raises(RuntimeError, match="TLS Redis"):
        await preflight()
