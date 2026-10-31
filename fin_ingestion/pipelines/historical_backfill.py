"""Motor de Backfill Histórico de 30 Anos (HistoricalBackfillPipeline).

Executa a carga completa e determinística de dados desde o primeiro dia de negociação (IPO)
para cotações diárias (OHLCV), proventos (dividendos, JCP, rendimentos) e desdobramentos,
garantindo zero duplicação através de chaves primárias e constraints únicas no PostgreSQL 18 / TimescaleDB.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import asyncpg
import httpx
import structlog

from fin_ingestion.config import settings

logger = structlog.get_logger(__name__)


class HistoricalBackfillPipeline:
    """Pipeline de alta performance para backfill de 30 anos desde o IPO."""

    def __init__(self, db_url: str | None = None, concurrency: int = 8) -> None:
        self.db_url = db_url or settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
        self.concurrency = concurrency
        self._pool: asyncpg.Pool | None = None
        self._http_client: httpx.AsyncClient | None = None

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=1,
                max_size=max(5, self.concurrency + 2),
            )
        return self._pool

    async def get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(20.0, connect=10.0),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (fin-ingestion/1.0)",
                    "Accept": "application/json",
                },
                follow_redirects=True,
            )
        return self._http_client

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
        if self._http_client:
            await self._http_client.aclose()

    @staticmethod
    def parse_history_data(ticker: str, tipo_ativo: str, raw_data: dict[str, Any]) -> dict[str, Any]:
        """Faz o parsing determinístico de candles, proventos e splits com detecção de IPO."""
        chart = raw_data.get("chart", {})
        results = chart.get("result")
        if not results:
            return {
                "success": False,
                "ticker": ticker,
                "ipo_date": None,
                "candles": [],
                "dividends": [],
                "splits": [],
                "message": "Nenhum resultado retornado pelo feed",
            }

        res = results[0]
        timestamps = res.get("timestamp") or []
        quote_indicators = (res.get("indicators", {}).get("quote") or [{}])[0]

        opens = quote_indicators.get("open") or []
        highs = quote_indicators.get("high") or []
        lows = quote_indicators.get("low") or []
        closes = quote_indicators.get("close") or []
        volumes = quote_indicators.get("volume") or []

        candles = []
        prev_close: Decimal | None = None

        for t, o, h, low, c, v in zip(timestamps, opens, highs, lows, closes, volumes):
            if c is None or o is None or h is None or low is None or t is None:
                continue

            dt = datetime.fromtimestamp(t, tz=timezone.utc).date()
            c_val = Decimal(str(round(c, 2)))
            o_val = Decimal(str(round(o, 2)))
            h_val = Decimal(str(round(h, 2)))
            l_val = Decimal(str(round(low, 2)))
            v_val = Decimal(str(v or 0))

            var_pct: Decimal | None = None
            if prev_close and prev_close > 0:
                var_pct = round(((c_val - prev_close) / prev_close) * Decimal("100"), 4)
                if abs(var_pct) > Decimal("99999"):
                    var_pct = Decimal("99999.9999") if var_pct > 0 else Decimal("-99999.9999")
            prev_close = c_val

            candles.append({
                "data_pregao": dt,
                "preco_abertura": o_val,
                "preco_maximo": h_val,
                "preco_minimo": l_val,
                "preco_fechamento": c_val,
                "preco_medio": round((o_val + h_val + l_val + c_val) / Decimal("4"), 2),
                "volume_total": v_val,
                "variacao_percentual": var_pct,
            })

        ipo_date = min((c["data_pregao"] for c in candles), default=None)

        # Proventos (Dividends / JCP / Rendimentos)
        raw_events = res.get("events", {})
        raw_divs = raw_events.get("dividends", {})
        raw_splits = raw_events.get("splits", {})

        div_records = []
        is_fii = (tipo_ativo or "").upper() == "FII"

        for ts_str, d in raw_divs.items():
            amt = d.get("amount")
            if amt is None or amt <= 0:
                continue
            dt_ex = datetime.fromtimestamp(int(ts_str), tz=timezone.utc).date()
            dt_com = dt_ex - timedelta(days=1)
            dt_pag = dt_ex

            tipo_prov = "RENDIMENTO" if is_fii else "DIVIDENDO"

            div_records.append({
                "data_com": dt_com,
                "data_ex": dt_ex,
                "data_pagamento": dt_pag,
                "valor_por_acao": Decimal(str(round(amt, 6))),
                "tipo_provento": tipo_prov,
                "mes_referencia": dt_com.strftime("%Y-%m"),
            })

        # Desdobramentos / Grupamentos
        split_records = []
        for ts_str, s in raw_splits.items():
            num = s.get("numerator")
            denom = s.get("denominator")
            if not num or not denom or num <= 0 or denom <= 0:
                continue
            dt_ex = datetime.fromtimestamp(int(ts_str), tz=timezone.utc).date()
            if num > denom:
                tipo_split = "SPLIT"
                fator = Decimal(str(round(num / denom, 6)))
            else:
                tipo_split = "INPLIT"
                fator = Decimal(str(round(denom / num, 6)))

            # Previne overflow numérico caso o agrupamento seja extremo
            if fator > Decimal("999999999999"):
                fator = Decimal("999999999999.999999")

            split_records.append({
                "data_ex": dt_ex,
                "tipo": tipo_split,
                "fator": fator,
                "proporcao_de": int(denom),
                "proporcao_para": int(num),
            })

        return {
            "success": True,
            "ticker": ticker,
            "ipo_date": ipo_date,
            "candles": candles,
            "dividends": div_records,
            "splits": split_records,
        }

    async def fetch_complete_history(self, ticker: str, tipo_ativo: str, days: int | None = None) -> dict[str, Any]:
        """Consulta série histórica completa de 30 anos (period1=0 até agora)."""
        symbol = f"{ticker}.SA" if not ticker.startswith("^") and "." not in ticker else ticker
        now = datetime.now(timezone.utc)
        start = int((now - timedelta(days=days)).timestamp()) if days else 0
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={start}&period2={int(now.timestamp())}&interval=1d&events=div%7Csplit"

        client = await self.get_http_client()
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return self.parse_history_data(ticker, tipo_ativo, resp.json())
            logger.warning("Falha na resposta do feed de mercado", ticker=ticker, status=resp.status_code)
            return {"success": False, "ticker": ticker, "candles": [], "dividends": [], "splits": []}
        except Exception as e:
            logger.error("Erro ao consultar histórico completo", ticker=ticker, error=str(e))
            return {"success": False, "ticker": ticker, "candles": [], "dividends": [], "splits": []}

    async def get_stage_universe(
        self,
        stage: int = 1,
        explicit_tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Seleciona o universo de ativos de acordo com a etapa de validação."""
        index_tickers = []
        if stage == 1 and not explicit_tickers:
            from fin_ingestion.collectors.official.b3 import B3Collector
            b3 = B3Collector()
            try:
                for index in ("IBOV", "IFIX"):
                    payload = await b3.extract(index=index)
                    index_tickers.extend(r["ticker"] for r in b3.parse(payload))
            finally:
                await b3.close()
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            if explicit_tickers:
                rows = await conn.fetch(
                    """
                    SELECT id_ativo, ticker, tipo_ativo::text as tipo_ativo
                    FROM core.ativo
                    WHERE ticker = ANY($1::text[])
                    ORDER BY ticker;
                    """,
                    [t.strip().upper() for t in explicit_tickers],
                )
            elif stage == 1:
                # Etapa 1: ~150 ativos mais líquidos (componentes IBOV, IFIX e maiores volumes)
                rows = await conn.fetch(
                    """
                    SELECT id_ativo, ticker, tipo_ativo::text as tipo_ativo
                    FROM core.ativo
                    WHERE ticker IS NOT NULL
                      AND tipo_ativo IN ('ACAO', 'FII')
                      AND ticker = ANY($1::text[])
                    ORDER BY volume_medio DESC NULLS LAST, ticker ASC
                    LIMIT 150;
                    """,
                    index_tickers,
                )
            else:
                # Etapa 2: restante dos ativos (universo completo)
                rows = await conn.fetch(
                    """
                    SELECT id_ativo, ticker, tipo_ativo::text as tipo_ativo
                    FROM core.ativo
                    WHERE ticker IS NOT NULL
                      AND tipo_ativo IN ('ACAO', 'FII', 'ETF', 'BDR')
                    ORDER BY volume_medio DESC NULLS LAST, ticker ASC
                    OFFSET 150;
                    """
                )
            return [dict(r) for r in rows]

    async def persist_asset_history(
        self,
        asset: dict[str, Any],
        parsed: dict[str, Any],
    ) -> dict[str, int]:
        """Persiste cotações, proventos e splits garantindo 100% de idempotência."""
        id_ativo = asset["id_ativo"]
        candles = parsed.get("candles") or []
        divs = parsed.get("dividends") or []
        splits = parsed.get("splits") or []
        ipo_date = parsed.get("ipo_date")

        quotes_written = 0
        divs_written = 0
        splits_written = 0

        pool = await self.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. Atualiza dados do IPO e métricas de 52 semanas em core.ativo
                if ipo_date and candles:
                    # Calcula 52 semanas (últimos 252 pregões)
                    last_year_candles = candles[-252:] if len(candles) >= 252 else candles
                    min_52 = min(c["preco_minimo"] for c in last_year_candles)
                    max_52 = max(c["preco_maximo"] for c in last_year_candles)
                    avg_vol_2m = Decimal(str(round(
                        sum(float(c["volume_total"]) for c in candles[-42:]) / max(1, len(candles[-42:])),
                        2,
                    ))) if len(candles) >= 10 else None

                    await conn.execute(
                        """
                        UPDATE core.ativo
                        SET data_inicio_negociacao = LEAST(COALESCE(data_inicio_negociacao, $2), $2),
                            data_ipo = LEAST(COALESCE(data_ipo, $2), $2),
                            min_52_semanas = $3,
                            max_52_semanas = $4,
                            volume_medio_2m = COALESCE($5, volume_medio_2m)
                        WHERE id_ativo = $1;
                        """,
                        id_ativo,
                        ipo_date,
                        min_52,
                        max_52,
                        avg_vol_2m,
                    )

                # 2. Upsert idempotente de Cotações Diárias
                if candles:
                    quote_params = [
                        (
                            id_ativo,
                            c["data_pregao"],
                            c["preco_abertura"],
                            c["preco_maximo"],
                            c["preco_minimo"],
                            c["preco_fechamento"],
                            c["preco_medio"],
                            c["volume_total"],
                            c["variacao_percentual"],
                        )
                        for c in candles
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO market.cotacao_diaria (
                            id_ativo, data_pregao, preco_abertura, preco_maximo, preco_minimo,
                            preco_fechamento, preco_medio, volume_total, variacao_percentual
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (id_ativo, data_pregao) DO UPDATE SET
                            preco_abertura = EXCLUDED.preco_abertura,
                            preco_maximo = EXCLUDED.preco_maximo,
                            preco_minimo = EXCLUDED.preco_minimo,
                            preco_fechamento = EXCLUDED.preco_fechamento,
                            preco_medio = EXCLUDED.preco_medio,
                            volume_total = EXCLUDED.volume_total,
                            variacao_percentual = EXCLUDED.variacao_percentual;
                        """,
                        quote_params,
                    )
                    quotes_written = len(candles)

                # 3. Upsert idempotente de Proventos com dupla verificação de constraints
                if divs:
                    existing_div_rows = await conn.fetch(
                        """
                        SELECT tipo_provento::text as tipo, data_ex, valor_por_acao, data_com, data_pagamento
                        FROM market.provento
                        WHERE id_ativo = $1;
                        """,
                        id_ativo,
                    )
                    existing_keys_ex_val = {
                        (r["tipo"], r["data_ex"], r["valor_por_acao"]) for r in existing_div_rows
                    }
                    existing_keys_com_pag = {
                        (r["tipo"], r["data_com"], r["data_pagamento"]) for r in existing_div_rows
                    }

                    div_params = []
                    for d in divs:
                        t_tipo = d["tipo_provento"]
                        t_ex = d["data_ex"]
                        t_val = d["valor_por_acao"]
                        t_com = d["data_com"]
                        t_pag = d["data_pagamento"]

                        if (t_tipo, t_ex, t_val) in existing_keys_ex_val:
                            continue
                        if (t_tipo, t_com, t_pag) in existing_keys_com_pag:
                            continue

                        # Adiciona aos conjuntos para evitar duplicação interna no próprio payload
                        existing_keys_ex_val.add((t_tipo, t_ex, t_val))
                        existing_keys_com_pag.add((t_tipo, t_com, t_pag))

                        div_params.append((
                            id_ativo,
                            t_tipo,
                            t_com,
                            t_ex,
                            t_pag,
                            t_val,
                            d["mes_referencia"],
                        ))

                    if div_params:
                        await conn.executemany(
                            """
                            INSERT INTO market.provento (
                                id_ativo, tipo_provento, data_com, data_ex, data_pagamento,
                                valor_por_acao, mes_referencia
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT DO NOTHING;
                            """,
                            div_params,
                        )
                    divs_written = len(divs)

                # 4. Upsert idempotente de Desdobramentos / Splits
                if splits:
                    split_params = [
                        (
                            id_ativo,
                            s["data_ex"],
                            s["tipo"],
                            s["fator"],
                            s["proporcao_de"],
                            s["proporcao_para"],
                        )
                        for s in splits
                    ]
                    await conn.executemany(
                        """
                        INSERT INTO market.desdobramento_split (
                            id_ativo, data_ex, tipo, fator, quantidade_antes, quantidade_depois
                        )
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (id_ativo, data_ex, tipo) DO UPDATE SET
                            fator = EXCLUDED.fator,
                            quantidade_antes = EXCLUDED.quantidade_antes,
                            quantidade_depois = EXCLUDED.quantidade_depois;
                        """,
                        split_params,
                    )
                    splits_written = len(splits)

        return {
            "quotes_written": quotes_written,
            "divs_written": divs_written,
            "splits_written": splits_written,
        }

    async def process_asset(self, asset: dict[str, Any], semaphore: asyncio.Semaphore) -> dict[str, Any]:
        """Processa um ativo individual com controle de concorrência."""
        ticker = asset["ticker"]
        tipo_ativo = asset.get("tipo_ativo", "ACAO")

        async with semaphore:
            parsed = await self.fetch_complete_history(ticker, tipo_ativo)
            if not parsed.get("success") or not parsed.get("candles"):
                return {
                    "ticker": ticker,
                    "success": False,
                    "ipo_date": None,
                    "candles_count": 0,
                    "divs_count": 0,
                    "splits_count": 0,
                }

            counts = await self.persist_asset_history(asset, parsed)
            return {
                "ticker": ticker,
                "success": True,
                "ipo_date": parsed.get("ipo_date"),
                "candles_count": counts["quotes_written"],
                "divs_count": counts["divs_written"],
                "splits_count": counts["splits_written"],
            }

    async def run(
        self,
        stage: int = 1,
        tickers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Executa a orquestração do backfill histórico para a etapa selecionada."""
        universe = await self.get_stage_universe(stage=stage, explicit_tickers=tickers)
        total_assets = len(universe)
        logger.info("Iniciando backfill histórico de 30 anos", stage=stage, total_assets=total_assets)

        semaphore = asyncio.Semaphore(self.concurrency)
        tasks = [self.process_asset(asset, semaphore) for asset in universe]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful = []
        failed = []
        total_candles = 0
        total_divs = 0
        total_splits = 0

        for asset, r in zip(universe, results):
            if isinstance(r, dict) and r.get("success"):
                successful.append(r)
                total_candles += r["candles_count"]
                total_divs += r["divs_count"]
                total_splits += r["splits_count"]
            else:
                ticker_err = r.get("ticker", "UNKNOWN") if isinstance(r, dict) else asset["ticker"]
                logger.error("Falha no backfill do ativo", ticker=ticker_err, error=str(r))
                failed.append(ticker_err)

        logger.info(
            "Backfill histórico finalizado",
            stage=stage,
            total=total_assets,
            successful=len(successful),
            failed=len(failed),
            candles=total_candles,
            dividends=total_divs,
            splits=total_splits,
        )

        return {
            "stage": stage,
            "total_assets": total_assets,
            "successful_count": len(successful),
            "failed_count": len(failed),
            "total_candles": total_candles,
            "total_dividends": total_divs,
            "total_splits": total_splits,
            "details": successful,
            "failed_tickers": failed,
        }
