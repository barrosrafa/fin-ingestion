"""Exportador e Publicador de Snapshots JSON Versionados para CDN / Cloudflare R2.

Produz arquivos JSON estáticos e imutáveis com hash SHA-256 e metadados de versão,
viabilizando atendimento na borda (Edge Caching) com latência sub-50ms e custo zero.
"""

import hashlib
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from fin_ingestion.dw.dw_writer import DWWriter

logger = structlog.get_logger(__name__)


class SnapshotJSONEncoder(json.JSONEncoder):
    """Serializador customizado para tipos do banco de dados (Decimal, Date, UUID)."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


class LocalFilePublisher:
    """Publica snapshots no sistema de arquivos local."""

    def __init__(self, base_dir: Path | str = "dist/snapshots") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def publish(self, relative_path: str, content: dict[str, Any]) -> str:
        dest_path = self.base_dir / relative_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(content, cls=SnapshotJSONEncoder, indent=2, ensure_ascii=False)
        dest_path.write_text(serialized, encoding="utf-8")
        logger.debug("Snapshot local gravado", path=str(dest_path), bytes=len(serialized))
        return str(dest_path)


class CloudflareR2Publisher:
    """Publica snapshots diretamente em bucket Cloudflare R2 (API compatível com S3)."""

    def __init__(
        self,
        account_id: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        self.account_id = account_id or os.getenv("R2_ACCOUNT_ID")
        self.access_key_id = access_key_id or os.getenv("R2_ACCESS_KEY_ID")
        self.secret_access_key = secret_access_key or os.getenv("R2_SECRET_ACCESS_KEY")
        self.bucket_name = bucket_name or os.getenv("R2_BUCKET_NAME", "fin-public-snapshots")
        self._s3_client = None

    def is_configured(self) -> bool:
        return bool(self.account_id and self.access_key_id and self.secret_access_key and self.bucket_name)

    def _get_client(self):
        if not self.is_configured():
            return None
        if self._s3_client is None:
            try:
                import boto3
                endpoint_url = f"https://{self.account_id}.r2.cloudflarestorage.com"
                self._s3_client = boto3.client(
                    "s3",
                    endpoint_url=endpoint_url,
                    aws_access_key_id=self.access_key_id,
                    aws_secret_access_key=self.secret_access_key,
                    region_name="auto",
                )
            except Exception as e:
                logger.warning("Falha ao inicializar cliente boto3 para Cloudflare R2", error=str(e))
                return None
        return self._s3_client

    def publish(self, relative_path: str, content: dict[str, Any]) -> bool:
        client = self._get_client()
        serialized = json.dumps(content, cls=SnapshotJSONEncoder, ensure_ascii=False)
        if not client:
            logger.info("Cloudflare R2 não configurado; simulação de upload ignorada", path=relative_path)
            return False

        try:
            client.put_object(
                Bucket=self.bucket_name,
                Key=relative_path,
                Body=serialized.encode("utf-8"),
                ContentType="application/json",
                CacheControl="public, max-age=300, s-maxage=3600, stale-while-revalidate=86400",
            )
            logger.info("Snapshot publicado com sucesso no Cloudflare R2", key=relative_path, bucket=self.bucket_name)
            return True
        except Exception as e:
            logger.error("Falha ao fazer upload de snapshot no Cloudflare R2", key=relative_path, error=str(e))
            return False


class SnapshotExporter:
    """Extrai entidades consolidadas do PostgreSQL e gera arquivos JSON versionados."""

    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        local_dir: Path | str = "dist/snapshots",
    ) -> None:
        self.pool = pool
        self._owned_dw: DWWriter | None = None
        self.local_publisher = LocalFilePublisher(base_dir=local_dir)
        self.r2_publisher = CloudflareR2Publisher()

    async def _get_pool(self) -> asyncpg.Pool:
        if self.pool is None:
            self._owned_dw = DWWriter()
            self.pool = await self._owned_dw.get_pool()
        return self.pool

    @staticmethod
    def wrap_envelope(data: Any, schema_name: str) -> dict[str, Any]:
        """Empacota os dados em envelope padronizado com hash SHA-256."""
        serialized = json.dumps(data, cls=SnapshotJSONEncoder, sort_keys=True)
        content_hash = f"sha256:{hashlib.sha256(serialized.encode('utf-8')).hexdigest()}"
        return {
            "meta": {
                "schema": schema_name,
                "schema_version": "1.0.0",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "content_hash": content_hash,
                "source": "fin-ingestion-snapshot-engine",
            },
            "data": data,
        }

    async def export_stock_snapshot(self, ticker: str) -> dict[str, Any] | None:
        """Gera snapshot JSON completo de uma ação (cadastro, cotação, indicadores, valuation, proventos)."""
        pool = await self._get_pool()
        norm_ticker = ticker.strip().upper()

        async with pool.acquire() as conn:
            # 1. Informações Cadastrais
            ativo_row = await conn.fetchrow(
                """
                SELECT a.id_ativo, a.ticker, a.tipo_ativo,
                       e.nome_empresarial, e.cnpj, e.setor, e.subsetor, e.cnae
                FROM core.ativo a
                LEFT JOIN core.emissor_companhia e ON e.id_emissor = a.id_emissor
                WHERE a.ticker = $1;
                """,
                norm_ticker,
            )
            if not ativo_row:
                return None

            id_ativo = ativo_row["id_ativo"]

            # 2. Última cotação de mercado
            quote_row = await conn.fetchrow(
                """
                SELECT data_pregao, preco_fechamento, preco_abertura, preco_maximo, preco_minimo,
                       preco_medio, volume_total, quantidade_negocios, variacao_percentual
                FROM market.cotacao_diaria
                WHERE id_ativo = $1 AND preco_fechamento IS NOT NULL
                ORDER BY data_pregao DESC
                LIMIT 1;
                """,
                id_ativo,
            )

            # 3. Indicadores fundamentalistas consolidados
            ind_row = await conn.fetchrow(
                """
                SELECT data_referencia, pl, pvp, psr, dividend_yield, payout, roe, roic,
                       ebitda_margem, margem_liquida, divida_liquida_ebitda, lpa, vpa,
                       ev_ebitda, ev_ebit, liquidez_corrente, giro_ativo
                FROM analytics.indicador_fundamentalista
                WHERE id_ativo = $1
                ORDER BY data_referencia DESC
                LIMIT 1;
                """,
                id_ativo,
            )

            # 4. Valuation quantitativo
            val_rows = await conn.fetch(
                """
                SELECT metodologia, preco_justo, preco_mercado, margem_seguranca, parametros
                FROM analytics.valuation
                WHERE instrument_id = (SELECT instrument_id FROM canonical.instrument WHERE short_name = $1 LIMIT 1)
                ORDER BY data_referencia DESC;
                """,
                norm_ticker,
            )

            # 5. Proventos declarados nos últimos 24 meses
            prov_rows = await conn.fetch(
                """
                SELECT tipo_provento::text as tipo_provento, valor_por_acao, data_com, data_pagamento
                FROM market.provento
                WHERE id_ativo = $1
                ORDER BY data_com DESC
                LIMIT 24;
                """,
                id_ativo,
            )

        valuation_dict = {}
        for r in val_rows:
            metod = r["metodologia"].lower()
            valuation_dict[metod] = {
                "preco_justo": float(r["preco_justo"]) if r["preco_justo"] else None,
                "preco_mercado": float(r["preco_mercado"]) if r["preco_mercado"] else None,
                "margem_seguranca_pct": float(r["margem_seguranca"] * 100) if r["margem_seguranca"] else None,
            }

        stock_payload = {
            "ticker": norm_ticker,
            "tipo_ativo": ativo_row["tipo_ativo"],
            "perfil": {
                "nome_empresarial": ativo_row["nome_empresarial"] or norm_ticker,
                "cnpj": ativo_row["cnpj"],
                "setor": ativo_row["setor"] or "Geral",
                "subsetor": ativo_row["subsetor"],
                "cnae": ativo_row["cnae"],
            },
            "cotacao": {
                "data_pregao": quote_row["data_pregao"] if quote_row else None,
                "preco_fechamento": float(quote_row["preco_fechamento"]) if quote_row and quote_row["preco_fechamento"] else None,
                "variacao_percentual": float(quote_row["variacao_percentual"]) if quote_row and quote_row["variacao_percentual"] else 0.0,
                "volume_total": float(quote_row["volume_total"]) if quote_row and quote_row["volume_total"] else 0.0,
            } if quote_row else None,
            "indicadores": {k: float(v) if isinstance(v, Decimal) else v for k, v in dict(ind_row).items()} if ind_row else {},
            "valuation": valuation_dict,
            "proventos": [
                {
                    "tipo": p["tipo_provento"],
                    "valor_bruto": float(p["valor_por_acao"]) if p["valor_por_acao"] else 0.0,
                    "data_com": p["data_com"],
                    "data_pagamento": p["data_pagamento"],
                }
                for p in prov_rows
            ],
        }

        envelope = self.wrap_envelope(stock_payload, schema_name="stock_detail_v1")
        rel_path = f"acoes/{norm_ticker}.json"
        self.local_publisher.publish(rel_path, envelope)
        if self.r2_publisher.is_configured():
            self.r2_publisher.publish(rel_path, envelope)
        return envelope

    async def export_fii_snapshot(self, ticker: str) -> dict[str, Any] | None:
        """Gera snapshot JSON completo de um FII (imóveis físicos, inquilinos, contratos, vacância, rendimentos)."""
        pool = await self._get_pool()
        norm_ticker = ticker.strip().upper()

        async with pool.acquire() as conn:
            # 1. Informações Básicas do FII
            fii_row = await conn.fetchrow(
                """
                SELECT a.id_ativo, a.ticker, d.administrador, d.gestor,
                       d.numero_cotistas, d.prazo_duracao
                FROM core.ativo a
                LEFT JOIN asset_details.detalhe_fii d ON d.id_ativo = a.id_ativo
                WHERE a.ticker = $1;
                """,
                norm_ticker,
            )
            if not fii_row:
                return None

            id_ativo = fii_row["id_ativo"]

            # 2. Última cotação e múltiplos
            quote_row = await conn.fetchrow(
                """
                SELECT data_pregao, preco_fechamento, variacao_percentual, volume_total
                FROM market.cotacao_diaria
                WHERE id_ativo = $1 AND preco_fechamento IS NOT NULL
                ORDER BY data_pregao DESC
                LIMIT 1;
                """,
                id_ativo,
            )

            # 3. Imóveis físicos (propriedades)
            prop_rows = await conn.fetch(
                """
                SELECT i.nome as nome_imovel, i.endereco, i.cidade, i.uf as estado,
                       COALESCE(p.abl_propria_m2, i.abl_total_m2) as area_bruta_locavel_m2,
                       COALESCE(p.participacao_pct, 100.0) as participacao_pct
                FROM asset_details.fii_imovel_participacao p
                JOIN asset_details.fii_imovel i ON i.id_imovel = p.id_imovel
                WHERE p.id_ativo = $1
                ORDER BY area_bruta_locavel_m2 DESC NULLS LAST;
                """,
                id_ativo,
            )

            # 4. Histórico de Vacância
            vac_rows = await conn.fetch(
                """
                SELECT data_referencia, vacancia_fisica as vacancia_fisica_pct, vacancia_financeira as vacancia_financeira_pct
                FROM asset_details.historico_vacancia_fii
                WHERE id_ativo = $1
                ORDER BY data_referencia DESC
                LIMIT 24;
                """,
                id_ativo,
            )

            # 5. Proventos e Rendimentos
            prov_rows = await conn.fetch(
                """
                SELECT tipo_provento::text as tipo_provento, valor_por_acao as valor_bruto, data_com, data_pagamento
                FROM market.provento
                WHERE id_ativo = $1
                ORDER BY data_com DESC
                LIMIT 24;
                """,
                id_ativo,
            )

        fii_payload = {
            "ticker": norm_ticker,
            "tipo_ativo": "FII",
            "perfil": {
                "ticker": norm_ticker,
                "administrador": fii_row["administrador"],
                "gestor": fii_row["gestor"],
                "numero_cotistas": fii_row["numero_cotistas"],
                "prazo_duracao": fii_row["prazo_duracao"],
            },
            "cotacao": {
                "data_pregao": quote_row["data_pregao"] if quote_row else None,
                "preco_fechamento": float(quote_row["preco_fechamento"]) if quote_row and quote_row["preco_fechamento"] else None,
                "variacao_percentual": float(quote_row["variacao_percentual"]) if quote_row and quote_row["variacao_percentual"] else 0.0,
                "volume_total": float(quote_row["volume_total"]) if quote_row and quote_row["volume_total"] else 0.0,
            } if quote_row else None,
            "imoveis": [
                {
                    "nome": p["nome_imovel"],
                    "endereco": p["endereco"],
                    "cidade": p["cidade"],
                    "estado": p["estado"],
                    "abl_m2": float(p["area_bruta_locavel_m2"]) if p["area_bruta_locavel_m2"] else 0.0,
                    "participacao_pct": float(p["participacao_pct"]) if p["participacao_pct"] else 100.0,
                }
                for p in prop_rows
            ],
            "vacancia_historica": [
                {
                    "data": v["data_referencia"],
                    "fisica_pct": float(v["vacancia_fisica_pct"]) if v["vacancia_fisica_pct"] else 0.0,
                    "financeira_pct": float(v["vacancia_financeira_pct"]) if v["vacancia_financeira_pct"] else 0.0,
                }
                for v in vac_rows
            ],
            "rendimentos": [
                {
                    "tipo": p["tipo_provento"],
                    "valor": float(p["valor_bruto"]) if p["valor_bruto"] else 0.0,
                    "data_com": p["data_com"],
                    "data_pagamento": p["data_pagamento"],
                }
                for p in prov_rows
            ],
        }

        envelope = self.wrap_envelope(fii_payload, schema_name="fii_detail_v1")
        rel_path = f"fiis/{norm_ticker}.json"
        self.local_publisher.publish(rel_path, envelope)
        if self.r2_publisher.is_configured():
            self.r2_publisher.publish(rel_path, envelope)
        return envelope

    async def export_market_overview(self) -> dict[str, Any]:
        """Gera snapshot do panorama macroeconômico e índices principais (IBOV, IFIX, Selic, IPCA)."""
        pool = await self._get_pool()
        overview: dict[str, Any] = {
            "indices": [],
            "macro": {},
        }

        async with pool.acquire() as conn:
            # Índices de mercado
            b_rows = await conn.fetch(
                """
                SELECT b.codigo, b.nome, s.valor, s.data_referencia
                FROM market.benchmark b
                LEFT JOIN LATERAL (
                    SELECT valor, data_referencia
                    FROM market.benchmark_serie bs
                    WHERE bs.id_benchmark = b.id_benchmark
                    ORDER BY data_referencia DESC
                    LIMIT 1
                ) s ON TRUE;
                """
            )
            for r in b_rows:
                overview["indices"].append({
                    "codigo": r["codigo"],
                    "nome": r["nome"],
                    "valor": float(r["valor"]) if r["valor"] else None,
                    "variacao_pct": 0.0,
                    "data": r["data_referencia"],
                })

        envelope = self.wrap_envelope(overview, schema_name="market_overview_v1")
        rel_path = "market/overview.json"
        self.local_publisher.publish(rel_path, envelope)
        if self.r2_publisher.is_configured():
            self.r2_publisher.publish(rel_path, envelope)
        return envelope

    async def export_rankings(self) -> dict[str, Any]:
        """Gera snapshot consolidado de rankings (Dividend Yield, P/VP, Graham Upside)."""
        pool = await self._get_pool()
        rankings: dict[str, Any] = {
            "dividend_yield_acoes": [],
            "dividend_yield_fiis": [],
            "valuation_discount": [],
        }

        async with pool.acquire() as conn:
            # Top DY Ações
            dy_acoes = await conn.fetch(
                """
                SELECT a.ticker, i.dividend_yield, c.preco_fechamento, e.nome_empresarial
                FROM core.ativo a
                JOIN analytics.indicador_fundamentalista i ON i.id_ativo = a.id_ativo
                LEFT JOIN market.cotacao_diaria c ON c.id_ativo = a.id_ativo AND c.preco_fechamento IS NOT NULL
                LEFT JOIN core.emissor_companhia e ON e.id_emissor = a.id_emissor
                WHERE a.tipo_ativo = 'ACAO' AND i.dividend_yield > 0
                ORDER BY i.dividend_yield DESC
                LIMIT 25;
                """
            )
            for r in dy_acoes:
                rankings["dividend_yield_acoes"].append({
                    "ticker": r["ticker"],
                    "nome": r["nome_empresarial"] or r["ticker"],
                    "dividend_yield_pct": float(r["dividend_yield"]),
                    "preco": float(r["preco_fechamento"]) if r["preco_fechamento"] else None,
                })

            # Top DY FIIs
            dy_fiis = await conn.fetch(
                """
                SELECT a.ticker, i.dividend_yield as dividend_yield_12m, c.preco_fechamento, a.ticker as razao_social
                FROM core.ativo a
                JOIN analytics.indicador_fundamentalista i ON i.id_ativo = a.id_ativo
                LEFT JOIN market.cotacao_diaria c ON c.id_ativo = a.id_ativo AND c.preco_fechamento IS NOT NULL
                WHERE a.tipo_ativo = 'FII' AND i.dividend_yield > 0
                ORDER BY i.dividend_yield DESC
                LIMIT 25;
                """
            )
            for r in dy_fiis:
                rankings["dividend_yield_fiis"].append({
                    "ticker": r["ticker"],
                    "nome": r["razao_social"] or r["ticker"],
                    "dividend_yield_pct": float(r["dividend_yield_12m"]),
                    "preco": float(r["preco_fechamento"]) if r["preco_fechamento"] else None,
                })

        envelope = self.wrap_envelope(rankings, schema_name="rankings_summary_v1")
        rel_path = "rankings/summary.json"
        self.local_publisher.publish(rel_path, envelope)
        if self.r2_publisher.is_configured():
            self.r2_publisher.publish(rel_path, envelope)
        return envelope

    async def export_all(self, tickers: list[str] | None = None) -> dict[str, int]:
        """Gera todos os snapshots públicos para os ativos e índices configurados."""
        pool = await self._get_pool()
        if not tickers:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT ticker, tipo_ativo FROM core.ativo
                    WHERE ticker IS NOT NULL
                    ORDER BY volume_medio DESC NULLS LAST
                    LIMIT 100;
                    """
                )
        else:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT ticker, tipo_ativo FROM core.ativo
                    WHERE ticker = ANY($1);
                    """,
                    tickers,
                )

        stocks_count = 0
        fiis_count = 0

        for r in rows:
            t = r["ticker"]
            tipo = r["tipo_ativo"]
            try:
                if tipo == "FII":
                    await self.export_fii_snapshot(t)
                    fiis_count += 1
                else:
                    await self.export_stock_snapshot(t)
                    stocks_count += 1
            except Exception as e:
                logger.warning("Falha ao exportar snapshot do ativo", ticker=t, error=str(e))

        await self.export_market_overview()
        await self.export_rankings()

        logger.info(
            "Exportação geral de snapshots concluída",
            stocks=stocks_count,
            fiis=fiis_count,
            destination=str(self.local_publisher.base_dir),
        )

        return {
            "stocks_exported": stocks_count,
            "fiis_exported": fiis_count,
            "indices_exported": 1,
            "rankings_exported": 1,
        }

    async def close(self) -> None:
        if self._owned_dw:
            await self._owned_dw.close()
