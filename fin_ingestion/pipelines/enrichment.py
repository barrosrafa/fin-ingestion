"""Worker assíncrono para enriquecimento e complementação de metadados de ativos.

Busca dados complementares (nome da empresa, setor, subsetor, CNPJ) via APIs
externas (Brapi, Yahoo Finance, CVM) sem travar a carga inicial dos tickers.
"""

import asyncio
from typing import Any

import asyncpg
import httpx
import structlog

logger = structlog.get_logger(__name__)


class AssetMetadataEnricher:
    """Worker assíncrono para buscar e complementar metadados cadastrais de emissores."""

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        max_concurrency: int = 5,
        brapi_token: str | None = None,
    ) -> None:
        self.pool = db_pool
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.brapi_token = brapi_token
        self.official_metadata = {}

    async def fetch_ticker_metadata(
        self,
        ticker: str,
        client: httpx.AsyncClient,
    ) -> dict[str, Any] | None:
        """Consulta APIs públicas (Brapi / Yahoo) para recuperar nome, setor e CNPJ."""
        async with self.semaphore:
            # 1. Tentativa via Brapi (aberta para tickers B3)
            try:
                url = f"https://brapi.dev/api/quote/{ticker}?fundamental=true"
                if self.brapi_token:
                    url += f"&token={self.brapi_token}"
                resp = await client.get(url, timeout=10.0)
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get("results", [])
                    if results:
                        item = results[0]
                        return {
                            "ticker": ticker,
                            "nome_empresarial": item.get("longName") or item.get("shortName") or ticker,
                            "setor": item.get("sector") or "Outros",
                            "subsetor": item.get("subsector") or item.get("industry") or "Outros",
                            "cnpj": item.get("cnpj") or None,
                        }
            except Exception as e:
                logger.debug("Falha na consulta Brapi", ticker=ticker, error=str(e))

            # 2. Fallback via Yahoo Finance Chart metadata
            try:
                y_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.SA"
                headers = {"User-Agent": "Mozilla/5.0 (compatible; FinDataBot/1.0)"}
                resp = await client.get(y_url, headers=headers, timeout=10.0)
                if resp.status_code == 200:
                    data = resp.json()
                    meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                    short_name = meta.get("shortName") or meta.get("symbol") or ticker
                    return {
                        "ticker": ticker,
                        "nome_empresarial": short_name,
                        "setor": "Geral",
                        "subsetor": "Geral",
                        "cnpj": None,
                    }
            except Exception as e:
                logger.debug("Falha na consulta Yahoo", ticker=ticker, error=str(e))

            official = self.official_metadata.get(ticker)
            if official:
                return {"ticker": ticker, "nome_empresarial": official["nome_resumido"], "cnpj": None}
            return None

    async def enrich_ticker(
        self,
        ticker: str,
        client: httpx.AsyncClient,
    ) -> bool:
        """Executa o enriquecimento de um único ticker e persiste em core.emissor_companhia.
        Não gera CNPJ sintético; mantém CNPJ nulo com status pendente se não confirmado."""
        meta = await self.fetch_ticker_metadata(ticker, client)
        if not meta:
            return False

        nome = meta.get("nome_empresarial") or ticker
        setor = meta.get("setor")
        subsetor = meta.get("subsetor")
        cnpj = meta.get("cnpj")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if cnpj:
                    emissor_id = await conn.fetchval(
                        """
                        INSERT INTO core.emissor_companhia (cnpj, nome_empresarial, setor, subsetor, status_cnpj)
                        VALUES ($1, $2, $3, $4, 'CONFIRMADO')
                        ON CONFLICT (cnpj) DO UPDATE SET
                            nome_empresarial = EXCLUDED.nome_empresarial,
                            setor = COALESCE(EXCLUDED.setor, core.emissor_companhia.setor),
                            subsetor = COALESCE(EXCLUDED.subsetor, core.emissor_companhia.subsetor)
                        RETURNING id_emissor;
                        """,
                        cnpj,
                        nome,
                        setor,
                        subsetor,
                    )
                else:
                    # Emissor sem CNPJ confirmado: preserva integridade sem dados sintéticos
                    emissor_id = await conn.fetchval(
                        """
                        INSERT INTO core.emissor_companhia (cnpj, nome_empresarial, setor, subsetor, status_cnpj)
                        VALUES (NULL, $1, $2, $3, 'PENDENTE_CONFIRMACAO')
                        ON CONFLICT (nome_empresarial) WHERE cnpj IS NULL DO UPDATE SET
                            setor = COALESCE(EXCLUDED.setor, core.emissor_companhia.setor),
                            subsetor = COALESCE(EXCLUDED.subsetor, core.emissor_companhia.subsetor)
                        RETURNING id_emissor;
                        """,
                        nome,
                        setor,
                        subsetor,
                    )

                if emissor_id:
                    # Vincula ao core.ativo
                    await conn.execute(
                        """
                        UPDATE core.ativo
                        SET id_emissor = $1
                        WHERE ticker = $2;
                        """,
                        emissor_id,
                        ticker,
                    )
        return True

    async def enrich_missing_assets(self, limit: int = 100) -> dict[str, int]:
        """Busca ativos em core.ativo que ainda não possuem id_emissor e enriquece em lote."""
        from fin_ingestion.collectors.official.b3 import B3Collector
        b3 = B3Collector()
        try:
            active_tickers = await b3.get_recent_traded_tickers()
            self.official_metadata = {q["ticker"]: q for q in b3.recent_daily_quotes}
        finally:
            await b3.close()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker FROM core.ativo
                WHERE id_emissor IS NULL
                  AND tipo_ativo IN ('ACAO','FII','ETF','BDR')
                  AND ticker = ANY($1::text[])
                ORDER BY ticker
                LIMIT $2;
                """,
                active_tickers,
                limit,
            )

        tickers = [r["ticker"] for r in rows]
        if not tickers:
            logger.info("Nenhum ativo pendente de enriquecimento cadastral.")
            return {"total": 0, "enriched": 0, "failed": 0}

        logger.info("Iniciando enriquecimento assíncrono de metadados", count=len(tickers))
        enriched_count = 0
        failed_count = 0

        async with httpx.AsyncClient(timeout=15.0) as client:
            tasks = [self.enrich_ticker(t, client) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if res is True:
                    enriched_count += 1
                else:
                    failed_count += 1

        logger.info(
            "Enriquecimento assíncrono concluído",
            total=len(tickers),
            enriched=enriched_count,
            failed=failed_count,
        )
        return {"total": len(tickers), "enriched": enriched_count, "failed": failed_count}
