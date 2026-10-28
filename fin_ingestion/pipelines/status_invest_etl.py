"""Pipeline completo de ingestão e normalização ETL dos CSVs do Status Invest.

Lê os dados de Ações (617) e FIIs (606) da B3 (1.223 ativos distintos), sanitiza valores monetários e
percentuais brasileiros, normaliza tipos para NUMERIC/INTEGER estritos, e executa inserts transacionados
em lotes de 100 a 200 registros no Neon PostgreSQL com garantias de idempotência via ON CONFLICT.
"""

from __future__ import annotations

import csv
import ssl
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import asyncpg
import structlog

from fin_ingestion.config import settings

logger = structlog.get_logger(__name__)


def sanitize_decimal(raw_val: Any) -> Decimal | None:
    """Sanitiza pontuação brasileira para Decimal estrito.

    Trata pontos de milhar, vírgulas decimais, símbolos 'R$', '%' e strings vazias/traços.
    Retorna None explicitamente se nulo ou vazio.
    """
    if raw_val is None:
        return None
    val = str(raw_val).strip()
    if not val or val in ("-", "--", "null", "None", "NaN"):
        return None

    # Remove símbolos monetários e percentuais
    val = val.replace("R$", "").replace("%", "").strip()

    # Remove pontos de milhar e substitui vírgula decimal por ponto
    val = val.replace(".", "").replace(",", ".")

    try:
        return Decimal(val)
    except (InvalidOperation, ValueError):
        return None


def sanitize_int(raw_val: Any) -> int | None:
    """Converte valor numérico sanitizado para inteiro (ex: número de cotas ou cotistas)."""
    d = sanitize_decimal(raw_val)
    if d is None:
        return None
    try:
        return int(round(d))
    except (ValueError, TypeError):
        return None


class StatusInvestETL:
    """Motor de Ingestão e Carga dos CSVs Status Invest para PostgreSQL / Neon."""

    def __init__(
        self,
        db_url: str | None = None,
        batch_size: int = 150,
        reference_date: date | None = None,
    ) -> None:
        self.db_url = db_url or settings.database_url_async.replace("postgresql+asyncpg://", "postgresql://")
        self.batch_size = max(50, min(batch_size, 300))  # entre 100 e 200 sugerido
        self.reference_date = reference_date or date(2026, 10, 26)
        self._pool: asyncpg.Pool | None = None

    async def get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=1,
                max_size=10,
                ssl=ctx,
            )
        return self._pool

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @staticmethod
    def read_csv_rows(file_path: Path | str) -> list[dict[str, Any]]:
        """Lê um arquivo CSV com separador ';' e cabeçalhos normalizados (sem espaços)."""
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {p.resolve()}")

        with open(p, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            clean_rows = []
            for row in reader:
                clean_row = {
                    (k.strip() if k else ""): (v.strip() if v else "")
                    for k, v in row.items()
                }
                clean_rows.append(clean_row)
            return clean_rows

    async def _resolve_fii_gestao_map(self, conn: asyncpg.Connection) -> dict[str, int]:
        """Garante a existência das modalidades de gestão e retorna mapa nome -> id_tipo_gestao."""
        await conn.execute(
            """
            INSERT INTO asset_details.tipo_gestao_fii (nome)
            VALUES ('Ativa'), ('Passiva')
            ON CONFLICT (nome) DO NOTHING;
            """
        )
        rows = await conn.fetch("SELECT id_tipo_gestao, nome FROM asset_details.tipo_gestao_fii;")
        return {r["nome"]: r["id_tipo_gestao"] for r in rows}

    async def ingest_stocks(self, file_path: Path | str) -> int:
        """Processa e insere todas as 617 ações do Status Invest com batch inserts transacionados."""
        rows = self.read_csv_rows(file_path)
        total = len(rows)
        logger.info("Iniciando ingestão de ações", total_records=total, batch_size=self.batch_size)

        pool = await self.get_pool()
        processed = 0

        for i in range(0, total, self.batch_size):
            chunk = rows[i : i + self.batch_size]

            async with pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Upsert em core.ativo
                    ativo_records = []
                    for r in chunk:
                        ticker = r["TICKER"].upper()
                        volume_medio = sanitize_decimal(r.get("LIQUIDEZ MEDIA DIARIA"))
                        ativo_records.append((ticker, "ACAO", volume_medio, volume_medio))

                    await conn.executemany(
                        """
                        INSERT INTO core.ativo (ticker, tipo_ativo, volume_medio, liquidez)
                        VALUES ($1, $2::core.tipo_ativo_enum, $3, $4)
                        ON CONFLICT (ticker) DO UPDATE SET
                            tipo_ativo = EXCLUDED.tipo_ativo,
                            volume_medio = COALESCE(EXCLUDED.volume_medio, core.ativo.volume_medio),
                            liquidez = COALESCE(EXCLUDED.liquidez, core.ativo.liquidez);
                        """,
                        ativo_records,
                    )

                    # 2. Resolução do id_ativo para o lote
                    tickers_chunk = [r["TICKER"].upper() for r in chunk]
                    id_rows = await conn.fetch(
                        "SELECT ticker, id_ativo FROM core.ativo WHERE ticker = ANY($1::text[]);",
                        tickers_chunk,
                    )
                    ticker_to_id = {r["ticker"]: r["id_ativo"] for r in id_rows}

                    # 3. Upsert em market.cotacao_diaria
                    cotacao_records = []
                    # 4. Upsert em analytics.indicador_fundamentalista
                    indicador_records = []

                    for r in chunk:
                        t = r["TICKER"].upper()
                        id_ativo = ticker_to_id.get(t)
                        if not id_ativo:
                            continue

                        preco = sanitize_decimal(r.get("PRECO"))
                        volume = sanitize_decimal(r.get("LIQUIDEZ MEDIA DIARIA"))

                        cotacao_records.append((
                            id_ativo,
                            self.reference_date,
                            preco,
                            preco,  # preco_medio
                            volume,
                            None,   # patrimonio_liquido_fundo
                            None,   # numero_cotistas_fundo
                        ))

                        indicador_records.append((
                            id_ativo,
                            self.reference_date,
                            sanitize_decimal(r.get("P/L")),
                            sanitize_decimal(r.get("P/VP")),
                            sanitize_decimal(r.get("PSR")),
                            sanitize_decimal(r.get("DY")),
                            sanitize_decimal(r.get("ROE")),
                            sanitize_decimal(r.get("ROIC")),
                            sanitize_decimal(r.get("MARG. LIQUIDA")),
                            sanitize_decimal(r.get("LPA")),
                            sanitize_decimal(r.get("VPA")),
                            sanitize_decimal(r.get("EV/EBIT")),
                            sanitize_decimal(r.get("LIQ. CORRENTE")),
                            sanitize_decimal(r.get("GIRO ATIVOS")),
                            sanitize_decimal(r.get("PEG Ratio")),
                            sanitize_decimal(r.get("P/EBIT")),
                            sanitize_decimal(r.get("P/ATIVOS")),
                            sanitize_decimal(r.get("P/CAP. GIRO")),
                            sanitize_decimal(r.get("P. AT CIR. LIQ.")),
                            sanitize_decimal(r.get("DIV. LIQ. / PATRI.")),
                            sanitize_decimal(r.get("DIVIDA LIQUIDA / EBIT")),
                            sanitize_decimal(r.get("PATRIMONIO / ATIVOS")),
                            sanitize_decimal(r.get("PASSIVOS / ATIVOS")),
                            sanitize_decimal(r.get("MARGEM BRUTA")),
                            sanitize_decimal(r.get("MARGEM EBIT")),
                            sanitize_decimal(r.get("ROA")),
                            sanitize_decimal(r.get("CAGR RECEITAS 5 ANOS")),
                            sanitize_decimal(r.get("CAGR LUCROS 5 ANOS")),
                            sanitize_decimal(r.get("VALOR DE MERCADO")),
                        ))

                    # Persistência de cotações em lote
                    await conn.executemany(
                        """
                        INSERT INTO market.cotacao_diaria (
                            id_ativo, data_pregao, preco_fechamento, preco_medio, volume_total,
                            patrimonio_liquido_fundo, numero_cotistas_fundo
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (id_ativo, data_pregao) DO UPDATE SET
                            preco_fechamento = EXCLUDED.preco_fechamento,
                            preco_medio = COALESCE(EXCLUDED.preco_medio, market.cotacao_diaria.preco_medio),
                            volume_total = EXCLUDED.volume_total;
                        """,
                        cotacao_records,
                    )

                    # Persistência de indicadores fundamentalistas em lote
                    await conn.executemany(
                        """
                        INSERT INTO analytics.indicador_fundamentalista (
                            id_ativo, data_referencia, pl, pvp, psr, dividend_yield, roe, roic,
                            margem_liquida, lpa, vpa, ev_ebit, liquidez_corrente, giro_ativo,
                            peg_ratio, p_ebit, p_ativo, p_cap_giro, p_ativo_circ_liq,
                            divida_liquida_pl, divida_liquida_ebit, patrimonio_ativos, passivos_ativos,
                            margem_bruta, margem_ebit, roa, cagr_receita_5a, cagr_lucro_5a, valor_mercado
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25, $26, $27, $28, $29
                        )
                        ON CONFLICT (id_ativo, data_referencia) DO UPDATE SET
                            pl = EXCLUDED.pl,
                            pvp = EXCLUDED.pvp,
                            psr = EXCLUDED.psr,
                            dividend_yield = EXCLUDED.dividend_yield,
                            roe = EXCLUDED.roe,
                            roic = EXCLUDED.roic,
                            margem_liquida = EXCLUDED.margem_liquida,
                            lpa = EXCLUDED.lpa,
                            vpa = EXCLUDED.vpa,
                            ev_ebit = EXCLUDED.ev_ebit,
                            liquidez_corrente = EXCLUDED.liquidez_corrente,
                            giro_ativo = EXCLUDED.giro_ativo,
                            peg_ratio = EXCLUDED.peg_ratio,
                            p_ebit = EXCLUDED.p_ebit,
                            p_ativo = EXCLUDED.p_ativo,
                            p_cap_giro = EXCLUDED.p_cap_giro,
                            p_ativo_circ_liq = EXCLUDED.p_ativo_circ_liq,
                            divida_liquida_pl = EXCLUDED.divida_liquida_pl,
                            divida_liquida_ebit = EXCLUDED.divida_liquida_ebit,
                            patrimonio_ativos = EXCLUDED.patrimonio_ativos,
                            passivos_ativos = EXCLUDED.passivos_ativos,
                            margem_bruta = EXCLUDED.margem_bruta,
                            margem_ebit = EXCLUDED.margem_ebit,
                            roa = EXCLUDED.roa,
                            cagr_receita_5a = EXCLUDED.cagr_receita_5a,
                            cagr_lucro_5a = EXCLUDED.cagr_lucro_5a,
                            valor_mercado = EXCLUDED.valor_mercado;
                        """,
                        indicador_records,
                    )

            processed += len(chunk)
            logger.info("Lote de ações persistido", processados=processed, total=total)

        return processed

    async def ingest_fiis(self, file_path: Path | str) -> int:
        """Processa e insere todos os 606 FIIs do Status Invest com batch inserts transacionados."""
        rows = self.read_csv_rows(file_path)
        total = len(rows)
        logger.info("Iniciando ingestão de FIIs", total_records=total, batch_size=self.batch_size)

        pool = await self.get_pool()
        processed = 0

        async with pool.acquire() as conn:
            gestao_map = await self._resolve_fii_gestao_map(conn)

        for i in range(0, total, self.batch_size):
            chunk = rows[i : i + self.batch_size]

            async with pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Upsert em core.ativo
                    ativo_records = []
                    for r in chunk:
                        ticker = r["TICKER"].upper()
                        volume_medio = sanitize_decimal(r.get("LIQUIDEZ MEDIA DIARIA"))
                        n_cotas = sanitize_int(r.get("N COTAS"))
                        ativo_records.append((ticker, "FII", n_cotas, volume_medio, volume_medio))

                    await conn.executemany(
                        """
                        INSERT INTO core.ativo (ticker, tipo_ativo, quantidade_emitida, volume_medio, liquidez)
                        VALUES ($1, $2::core.tipo_ativo_enum, $3, $4, $5)
                        ON CONFLICT (ticker) DO UPDATE SET
                            tipo_ativo = EXCLUDED.tipo_ativo,
                            quantidade_emitida = COALESCE(EXCLUDED.quantidade_emitida, core.ativo.quantidade_emitida),
                            volume_medio = COALESCE(EXCLUDED.volume_medio, core.ativo.volume_medio),
                            liquidez = COALESCE(EXCLUDED.liquidez, core.ativo.liquidez);
                        """,
                        ativo_records,
                    )

                    # 2. Resolução do id_ativo para o lote
                    tickers_chunk = [r["TICKER"].upper() for r in chunk]
                    id_rows = await conn.fetch(
                        "SELECT ticker, id_ativo FROM core.ativo WHERE ticker = ANY($1::text[]);",
                        tickers_chunk,
                    )
                    ticker_to_id = {r["ticker"]: r["id_ativo"] for r in id_rows}

                    cotacao_records = []
                    detalhe_fii_records = []
                    historico_patr_records = []
                    indicador_records = []

                    for r in chunk:
                        t = r["TICKER"].upper()
                        id_ativo = ticker_to_id.get(t)
                        if not id_ativo:
                            continue

                        preco = sanitize_decimal(r.get("PRECO"))
                        ultimo_div = sanitize_decimal(r.get("ULTIMO DIVIDENDO"))
                        dy = sanitize_decimal(r.get("DY"))
                        vp_cota = sanitize_decimal(r.get("VALOR PATRIMONIAL COTA"))
                        pvp = sanitize_decimal(r.get("P/VP"))
                        liq_diaria = sanitize_decimal(r.get("LIQUIDEZ MEDIA DIARIA"))
                        pct_caixa = sanitize_decimal(r.get("PERCENTUAL EM CAIXA"))
                        cagr_div_3a = sanitize_decimal(r.get("CAGR DIVIDENDOS 3 ANOS"))
                        cagr_cota_3a = sanitize_decimal(r.get("CAGR VALOR CORA 3 ANOS"))
                        patrimonio = sanitize_decimal(r.get("PATRIMONIO"))
                        n_cotistas = sanitize_int(r.get("N COTISTAS"))
                        n_cotas = sanitize_int(r.get("N COTAS"))
                        gestao_str = r.get("GESTAO", "").strip()
                        id_tipo_gestao = gestao_map.get(gestao_str) if gestao_str else None

                        cotacao_records.append((
                            id_ativo,
                            self.reference_date,
                            preco,
                            preco,
                            liq_diaria,
                            patrimonio,
                            n_cotistas,
                        ))

                        detalhe_fii_records.append((
                            id_ativo,
                            id_tipo_gestao,
                            n_cotistas,
                            ultimo_div,
                            vp_cota,
                            pvp,
                            dy,
                            pct_caixa,
                            cagr_div_3a,
                            cagr_cota_3a,
                            patrimonio,
                            n_cotas,
                        ))

                        historico_patr_records.append((
                            id_ativo,
                            self.reference_date,
                            patrimonio,
                            n_cotas,
                            vp_cota,
                            n_cotistas,
                        ))

                        indicador_records.append((
                            id_ativo,
                            self.reference_date,
                            pvp,
                            dy,
                            vp_cota,    # vpa
                            patrimonio, # valor_mercado
                        ))

                    # Persistência de Cotações Diárias
                    await conn.executemany(
                        """
                        INSERT INTO market.cotacao_diaria (
                            id_ativo, data_pregao, preco_fechamento, preco_medio, volume_total,
                            patrimonio_liquido_fundo, numero_cotistas_fundo
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (id_ativo, data_pregao) DO UPDATE SET
                            preco_fechamento = EXCLUDED.preco_fechamento,
                            preco_medio = COALESCE(EXCLUDED.preco_medio, market.cotacao_diaria.preco_medio),
                            volume_total = EXCLUDED.volume_total,
                            patrimonio_liquido_fundo = COALESCE(EXCLUDED.patrimonio_liquido_fundo, market.cotacao_diaria.patrimonio_liquido_fundo),
                            numero_cotistas_fundo = COALESCE(EXCLUDED.numero_cotistas_fundo, market.cotacao_diaria.numero_cotistas_fundo);
                        """,
                        cotacao_records,
                    )

                    # Persistência de asset_details.detalhe_fii
                    await conn.executemany(
                        """
                        INSERT INTO asset_details.detalhe_fii (
                            id_ativo, id_tipo_gestao, numero_cotistas, ultimo_dividendo,
                            valor_patrimonial_cota, pvp, dividend_yield, percentual_em_caixa,
                            cagr_dividendos_3a, cagr_valor_cota_3a, patrimonio_liquido, quantidade_cotas
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (id_ativo) DO UPDATE SET
                            id_tipo_gestao = COALESCE(EXCLUDED.id_tipo_gestao, asset_details.detalhe_fii.id_tipo_gestao),
                            numero_cotistas = EXCLUDED.numero_cotistas,
                            ultimo_dividendo = EXCLUDED.ultimo_dividendo,
                            valor_patrimonial_cota = EXCLUDED.valor_patrimonial_cota,
                            pvp = EXCLUDED.pvp,
                            dividend_yield = EXCLUDED.dividend_yield,
                            percentual_em_caixa = EXCLUDED.percentual_em_caixa,
                            cagr_dividendos_3a = EXCLUDED.cagr_dividendos_3a,
                            cagr_valor_cota_3a = EXCLUDED.cagr_valor_cota_3a,
                            patrimonio_liquido = EXCLUDED.patrimonio_liquido,
                            quantidade_cotas = EXCLUDED.quantidade_cotas;
                        """,
                        detalhe_fii_records,
                    )

                    # Persistência de asset_details.historico_patrimonio_fii
                    await conn.executemany(
                        """
                        INSERT INTO asset_details.historico_patrimonio_fii (
                            id_ativo, data_referencia, patrimonio_liquido, quantidade_cotas,
                            valor_patrimonial_cota, numero_cotistas
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (id_ativo, data_referencia) DO UPDATE SET
                            patrimonio_liquido = EXCLUDED.patrimonio_liquido,
                            quantidade_cotas = EXCLUDED.quantidade_cotas,
                            valor_patrimonial_cota = EXCLUDED.valor_patrimonial_cota,
                            numero_cotistas = EXCLUDED.numero_cotistas;
                        """,
                        historico_patr_records,
                    )

                    # Persistência de indicadores fundamentalistas simplificados do FII
                    await conn.executemany(
                        """
                        INSERT INTO analytics.indicador_fundamentalista (
                            id_ativo, data_referencia, pvp, dividend_yield, vpa, valor_mercado
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (id_ativo, data_referencia) DO UPDATE SET
                            pvp = EXCLUDED.pvp,
                            dividend_yield = EXCLUDED.dividend_yield,
                            vpa = EXCLUDED.vpa,
                            valor_mercado = EXCLUDED.valor_mercado;
                        """,
                        indicador_records,
                    )

            processed += len(chunk)
            logger.info("Lote de FIIs persistido", processados=processed, total=total)

        return processed

    async def refresh_search_views(self) -> None:
        """Atualiza visões materializadas de busca textual."""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            try:
                await conn.execute("REFRESH MATERIALIZED VIEW analytics.busca_ativo;")
                logger.info("Visão materializada analytics.busca_ativo atualizada com sucesso.")
            except Exception as e:
                logger.warning("Falha ao atualizar busca_ativo", error=str(e))

    async def run(
        self,
        stocks_file: Path | str,
        fiis_file: Path | str,
    ) -> dict[str, Any]:
        """Executa a pipeline completa ponta a ponta para ações e FIIs."""
        logger.info("Iniciando pipeline de carga completa Status Invest (1.223 ativos)...")
        stocks_count = await self.ingest_stocks(stocks_file)
        fiis_count = await self.ingest_fiis(fiis_file)
        await self.refresh_search_views()

        total = stocks_count + fiis_count
        logger.info("Pipeline Status Invest concluído com sucesso!", total_ativos=total)
        return {
            "stocks_processed": stocks_count,
            "fiis_processed": fiis_count,
            "total_assets_loaded": total,
            "reference_date": self.reference_date.isoformat(),
            "batch_size": self.batch_size,
        }
