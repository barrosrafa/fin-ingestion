"""ANBIMA Feed OAuth2 client-credentials with proactive token renewal."""

import asyncio
import base64
import time
from datetime import date

from fin_ingestion.collectors.commercial.base import CommercialCollector
from fin_ingestion.config import settings
from fin_ingestion.core.http_client import ResilientHttpClient


class AnbimaFeedCollector(CommercialCollector):
    def __init__(self):
        client_id = settings.anbima_client_id
        secret = settings.anbima_client_secret
        super().__init__(
            "anbima_feed",
            "https://api.anbima.com.br",
            client_id.get_secret_value() if client_id else None,
        )
        self.secret = secret.get_secret_value() if secret else None
        self.auth = ResilientHttpClient("anbima_oauth", "https://api.anbima.com.br")
        self.token, self.expires_at = None, 0
        self.token_lock = asyncio.Lock()

    async def access_token(self):
        self.authorize()
        if not self.secret:
            raise ValueError("Missing ANBIMA client secret")
        async with self.token_lock:
            if self.token and time.monotonic() < self.expires_at:
                return self.token
            encoded = base64.b64encode(f"{self.key}:{self.secret}".encode()).decode()
            response = await self.auth.post(
                "/oauth/access-token",
                json={"grant_type": "client_credentials"},
                headers={"Authorization": f"Basic {encoded}"},
            )
            response.raise_for_status()
            payload = response.json()
            self.token = payload["access_token"]
            self.expires_at = time.monotonic() + max(1, float(payload["expires_in"]) - 30)
            return self.token

    async def extract(self, **kwargs):
        day = date.fromisoformat(str(kwargs["reference_date"]))
        token = await self.access_token()
        return await self.request_json(
            "/feed/precos-indices/v1/debentures/mercado-secundario",
            params={"data": day.isoformat()},
            headers={"client_id": self.key, "access_token": token},
        )

    def parse(self, raw_data):
        if not isinstance(raw_data, list):
            raise ValueError("ANBIMA debentures response must be a list")
        return [
            {
                "code": row["codigo_ativo"],
                "reference_date": date.fromisoformat(row["data_referencia"]),
                "source": "anbima_feed",
                "payload": row,
            }
            for row in raw_data
        ]

    async def close(self):
        await super().close()
        await self.auth.close()
