"""CLI unificada para orquestração de ingestão, cálculos analíticos e dbt."""

import asyncio
import os
import subprocess
import urllib.parse
from typing import Annotated

import structlog
import typer
from rich.console import Console
from rich.table import Table

from fin_ingestion.analytics.technical import TechnicalAnalysisEngine
from fin_ingestion.analytics.valuation import ValuationEngine
from fin_ingestion.collectors.official import (
    ANBIMACollector,
    B3Collector,
    BCBCollector,
    CVMCollector,
)
from fin_ingestion.collectors.sites import (
    ADVFNCollector,
    AnaliseDeAcoesCollector,
    Brick360Collector,
    ClubeFIICollector,
    ETFsBrasilCollector,
    FIISComBrCollector,
    FundamenteiCollector,
    FundamentusCollector,
    FundsExplorerCollector,
    Investidor10Collector,
    MaisRetornoCollector,
    StatusInvestCollector,
)
from fin_ingestion.config import settings
from fin_ingestion.pipelines.enrichment import AssetMetadataEnricher
from fin_ingestion.pipelines.orchestrator import PipelineOrchestrator
from fin_ingestion.pipelines.status_invest_etl import StatusInvestETL

logger = structlog.get_logger(__name__)

app = typer.Typer(
    name="fin-ingestion",
    help="Pipeline de Ingestão, Analytics e Transformação dbt para Mercados Financeiros.",
)
console = Console()


@app.command()
def test_sources() -> None:
    """Testa a conectividade e integridade de todos os 16 coletores."""
    console.print("\n[bold cyan]🔍 Testando Conectividade dos 16 Coletores (Oficiais & Plataformas de Análise)...[/bold cyan]\n")

    collectors = [
        ("B3 (Oficial)", B3Collector()),
        ("CVM (Oficial)", CVMCollector()),
        ("BCB (Oficial)", BCBCollector()),
        ("ANBIMA (Oficial)", ANBIMACollector()),
        ("Status Invest", StatusInvestCollector()),
        ("Investidor10", Investidor10Collector()),
        ("Fundamentei", FundamenteiCollector()),
        ("Fundamentus", FundamentusCollector()),
        ("Clube FII", ClubeFIICollector()),
        ("Funds Explorer", FundsExplorerCollector()),
        ("FIIs.com.br", FIISComBrCollector()),
        ("Mais Retorno", MaisRetornoCollector()),
        ("Análise de Ações", AnaliseDeAcoesCollector()),
        ("Brick360", Brick360Collector()),
        ("ADVFN Brasil", ADVFNCollector()),
        ("ETFs Brasil", ETFsBrasilCollector()),
    ]

    async def _test_all():
        results = []
        for name, col in collectors:
            res = await col.run()
            results.append((name, res.success, res.records_count, res.execution_time_seconds, res.errors))
            await col.close()
        return results

    results = asyncio.run(_test_all())

    table = Table(title="Status dos Coletores")
    table.add_column("Fonte", style="bold")
    table.add_column("Status", justify="center")
    table.add_column("Registros", justify="right")
    table.add_column("Latência (s)", justify="right")
    table.add_column("Detalhes", style="dim")

    for name, success, count, latency, errs in results:
        status_str = "[bold green]ONLINE[/bold green]" if success else "[bold red]FALHA[/bold red]"
        err_msg = ", ".join(errs[:1]) if errs else "OK"
        table.add_row(name, status_str, str(count), f"{latency:.3f}", err_msg)

    console.print(table)


@app.command()
def collect(
    pipeline: Annotated[str, typer.Option("--pipeline", "-p", help="Nome do pipeline (daily, quarterly, monthly, all)")] = "daily",
    tickers: Annotated[str | None, typer.Option("--tickers", "-t", help="Lista de tickers separados por vírgula")] = None,
) -> None:
    """Dispara a execução de um pipeline de coleta de dados."""
    ticker_list = [t.strip().upper() for t in tickers.split(",")] if tickers else None
    orchestrator = PipelineOrchestrator()

    console.print(f"[bold green]▶️ Disparando pipeline '{pipeline}'...[/bold green]")
    res = asyncio.run(orchestrator.execute_pipeline(pipeline, tickers=ticker_list))

    console.print(f"[bold cyan]Concluído:[/bold cyan] {res}")


@app.command()
def compute_analytics(
    tickers: Annotated[str, typer.Option("--tickers", "-t", help="Lista de tickers")] = "PETR4,VALE3",
    persist: Annotated[bool, typer.Option("--persist", "-p", help="Persistir resultados no DW")] = True,
) -> None:
    """Calcula e exibe métricas de Valuation, Scores de Saúde e Risco usando dados reais do DW."""
    ticker_list = [t.strip().upper() for t in tickers.split(",")]
    table = Table(title="Cálculo de Valuation & Indicadores Analíticos (Dados Reais do DW)")
    table.add_column("Ticker", style="bold")
    table.add_column("Último Fech.", justify="right")
    table.add_column("Preço Graham", justify="right")
    table.add_column("Preço Teto Bazin", justify="right")
    table.add_column("P/L", justify="right")
    table.add_column("P/VP", justify="right")
    table.add_column("RSI (14)", justify="right")
    table.add_column("Pivot Point", justify="right")

    async def _process_analytics():
        from fin_ingestion.dw.dw_writer import DWWriter
        dw = DWWriter()
        pool = await dw.get_pool()
        rows_to_display = []

        async with pool.acquire() as conn:
            for t in ticker_list:
                # 1. Busca id_ativo
                ativo_row = await conn.fetchrow(
                    "SELECT id_ativo FROM core.ativo WHERE ticker = $1;",
                    t,
                )
                if not ativo_row:
                    rows_to_display.append((t, "-", "-", "-", "-", "-", "-", "-"))
                    continue

                id_ativo = ativo_row["id_ativo"]

                # 2. Busca últimos indicadores fundamentalistas reais
                ind = await conn.fetchrow(
                    """
                    SELECT lpa, vpa, pl, pvp, dividend_yield, data_referencia
                    FROM analytics.indicador_fundamentalista
                    WHERE id_ativo = $1
                    ORDER BY data_referencia DESC
                    LIMIT 1;
                    """,
                    id_ativo,
                )

                # 3. Busca últimas 15 cotações para RSI e Pivots
                quotes = await conn.fetch(
                    """
                    SELECT data_pregao, preco_fechamento, preco_maximo, preco_minimo
                    FROM market.cotacao_diaria
                    WHERE id_ativo = $1 AND preco_fechamento IS NOT NULL
                    ORDER BY data_pregao DESC
                    LIMIT 15;
                    """,
                    id_ativo,
                )

                close_prices = [float(q["preco_fechamento"]) for q in reversed(quotes)] if quotes else []
                latest_q = quotes[0] if quotes else None
                latest_close = float(latest_q["preco_fechamento"]) if latest_q and latest_q["preco_fechamento"] else None
                latest_high = float(latest_q["preco_maximo"]) if latest_q and latest_q["preco_maximo"] else latest_close
                latest_low = float(latest_q["preco_minimo"]) if latest_q and latest_q["preco_minimo"] else latest_close

                # Cálculos reais
                lpa_val = float(ind["lpa"]) if ind and ind["lpa"] else None
                vpa_val = float(ind["vpa"]) if ind and ind["vpa"] else None
                dy_val = float(ind["dividend_yield"]) if ind and ind["dividend_yield"] else None

                graham = ValuationEngine.graham_fair_price(lpa=lpa_val, vpa=vpa_val) if lpa_val and vpa_val else None

                # Bazin: DPA / 0.06
                dpa_estimado = (dy_val / 100.0 * latest_close) if dy_val and latest_close else None
                bazin = ValuationEngine.bazin_ceiling_price(dpa_medio=dpa_estimado) if dpa_estimado else None

                rsi = TechnicalAnalysisEngine.relative_strength_index(close_prices) if len(close_prices) >= 5 else None
                pivots = (
                    TechnicalAnalysisEngine.pivot_points_4s_3r(high=latest_high, low=latest_low, close=latest_close)
                    if latest_high and latest_low and latest_close
                    else None
                )

                # Persistência versionada se solicitado
                if persist and graham and latest_close and ind:
                    try:
                        import json
                        margem = (graham - latest_close) / latest_close if latest_close else None
                        inst_id = await conn.fetchval(
                            """
                            SELECT instrument_id FROM canonical.instrument
                            WHERE short_name = $1 OR attributes->>'ticker' = $1
                            LIMIT 1;
                            """,
                            t,
                        )
                        if inst_id:
                            await conn.execute(
                                """
                                INSERT INTO analytics.valuation (
                                    instrument_id, data_referencia, metodologia, parametros,
                                    preco_justo, preco_mercado, margem_seguranca, source
                                )
                                VALUES ($1, $2, 'BENJAMIN_GRAHAM_V1', $3::jsonb, $4, $5, $6, 'fin-ingestion-analytics')
                                ON CONFLICT (instrument_id, data_referencia, metodologia, source) DO UPDATE SET
                                    preco_justo = EXCLUDED.preco_justo,
                                    preco_mercado = EXCLUDED.preco_mercado,
                                    margem_seguranca = EXCLUDED.margem_seguranca,
                                    parametros = EXCLUDED.parametros,
                                    updated_at = NOW();
                                """,
                                inst_id,
                                ind["data_referencia"],
                                json.dumps({"lpa": lpa_val, "vpa": vpa_val, "formula_version": "1.2.0"}),
                                graham,
                                latest_close,
                                margem,
                            )
                    except Exception as e:
                        logger.warning("Falha na persistência de valuation", ticker=t, error=str(e))

                rows_to_display.append((
                    t,
                    f"R$ {latest_close:.2f}" if latest_close else "-",
                    f"R$ {graham:.2f}" if graham else "-",
                    f"R$ {bazin:.2f}" if bazin else "-",
                    f"{float(ind['pl']):.2f}" if ind and ind['pl'] else "-",
                    f"{float(ind['pvp']):.2f}" if ind and ind['pvp'] else "-",
                    f"{rsi:.1f}" if rsi else "-",
                    f"R$ {pivots['pivot_point']:.2f}" if pivots else "-",
                ))
        await dw.close()
        return rows_to_display

    rows = asyncio.run(_process_analytics())
    for r in rows:
        table.add_row(*r)

    console.print(table)


@app.command()
def dbt_run(
    models: Annotated[str, typer.Option("--models", "-m", help="Seletor de modelos (staging, intermediate, marts)")] = "marts",
) -> None:
    """Invoca o motor de transformação dbt."""
    console.print(f"[bold green]▶️ Executando 'dbt run --select {models}'...[/bold green]")
    env = os.environ.copy()
    if settings.database_url:
        import urllib.parse
        parsed = urllib.parse.urlparse(settings.database_url)
        env.setdefault("DBT_HOST", parsed.hostname or "localhost")
        env.setdefault("DBT_USER", parsed.username or "postgres")
        env.setdefault("DBT_PASSWORD", parsed.password or "")
        env.setdefault("DBT_PORT", str(parsed.port or 5432))
        env.setdefault("DBT_DBNAME", parsed.path.lstrip("/") or "neondb")
        qs = urllib.parse.parse_qs(parsed.query)
        env.setdefault("DBT_SSLMODE", qs.get("sslmode", ["require"])[0])

    cmd = ["dbt", "run", "--select", models, "--project-dir", settings.dbt_project_dir, "--profiles-dir", settings.dbt_profiles_dir]
    try:
        subprocess.run(cmd, env=env, check=True)
    except FileNotFoundError:
        console.print("[bold yellow]dbt-core não instalado no path global. Instale via 'uv sync' ou 'pip install dbt-postgres'.[/bold yellow]")
    except Exception as e:
        console.print(f"[bold red]Erro ao executar dbt:[/bold red] {e}")


@app.command()
def ingest_statusinvest(
    stocks_file: Annotated[str, typer.Option("--stocks-file", "-s", help="Caminho do CSV de Ações")] = "data/statusinvest-busca-avancada.csv",
    fiis_file: Annotated[str, typer.Option("--fiis-file", "-f", help="Caminho do CSV de FIIs")] = "data/statusinvest-busca-avancada-2.csv",
    batch_size: Annotated[int, typer.Option("--batch-size", "-b", help="Tamanho do lote de batch insert (100 a 200)")] = 150,
) -> None:
    """Executa a ingestão completa e determinística dos CSVs do Status Invest no PostgreSQL (Neon)."""
    console.print(f"[bold cyan]▶️ Iniciando Ingestão Status Invest (batch_size={batch_size})...[/bold cyan]")
    etl = StatusInvestETL(batch_size=batch_size)

    async def _run():
        try:
            return await etl.run(stocks_file, fiis_file)
        finally:
            await etl.close()

    result = asyncio.run(_run())
    table = Table(title="Resultado da Ingestão Status Invest")
    table.add_column("Métrica", style="bold")
    table.add_column("Valor", justify="right", style="green")

    table.add_row("Ações Processadas", str(result["stocks_processed"]))
    table.add_row("FIIs Processados", str(result["fiis_processed"]))
    table.add_row("Total de Ativos Ingeridos", str(result["total_assets_loaded"]))
    table.add_row("Data de Referência", str(result["reference_date"]))
    table.add_row("Tamanho do Lote", str(result["batch_size"]))
    console.print(table)


@app.command()
def enrich_metadata(
    limit: Annotated[int, typer.Option("--limit", "-l", help="Quantidade máxima de ativos a enriquecer")] = 50,
) -> None:
    """Dispara worker assíncrono para buscar dados cadastrais (nome, setor, CNPJ) via APIs externas."""
    console.print(f"[bold cyan]▶️ Disparando enriquecimento cadastral para até {limit} ativos...[/bold cyan]")
    etl = StatusInvestETL()

    async def _run():
        pool = await etl.get_pool()
        try:
            enricher = AssetMetadataEnricher(db_pool=pool)
            return await enricher.enrich_missing_assets(limit=limit)
        finally:
            await etl.close()

    res = asyncio.run(_run())
    console.print(f"[bold green]Enriquecimento concluído:[/bold green] {res}")


@app.command()
def worker_start(
    concurrency: Annotated[int, typer.Option("--concurrency", "-c", help="Concorrência do worker")] = 3,
) -> None:
    """Inicia o processo worker consumindo a fila PostgreSQL com SKIP LOCKED."""
    console.print(f"[bold green]▶️ Iniciando Worker Python com concorrência={concurrency}...[/bold green]")
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.workers.base_worker import IngestionWorker

    async def _run():
        dw = DWWriter()
        pool = await dw.get_pool()
        worker = IngestionWorker(pool=pool)
        try:
            await worker.run_forever()
        finally:
            await worker.close()
            await dw.close()

    asyncio.run(_run())


@app.command()
def scheduler_start(
    interval: Annotated[int, typer.Option("--interval", "-i", help="Intervalo de varredura em segundos")] = 60,
) -> None:
    """Inicia o agendador autônomo baseado em políticas e calendários de mercado."""
    console.print(f"[bold cyan]▶️ Iniciando Agendador Autônomo (Intervalo: {interval}s)...[/bold cyan]")
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.scheduler.scheduler_service import IngestionSchedulerService

    async def _run():
        dw = DWWriter()
        pool = await dw.get_pool()
        scheduler = IngestionSchedulerService(pool=pool)
        try:
            await scheduler.run_forever(interval_seconds=interval)
        finally:
            await dw.close()

    asyncio.run(_run())


@app.command()
def queue_status() -> None:
    """Exibe o estado operacional da fila persistente e o resumo de SLAs."""
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.queue.task_queue import PostgresTaskQueue

    async def _fetch():
        dw = DWWriter()
        pool = await dw.get_pool()
        q = PostgresTaskQueue(pool=pool)
        stats = await q.get_queue_stats()

        async with pool.acquire() as conn:
            health_rows = await conn.fetch("SELECT * FROM ingestion.v_pipeline_health_summary;")

        await dw.close()
        return stats, health_rows

    stats, health = asyncio.run(_fetch())

    # Tabela de status da fila
    q_table = Table(title="Estado da Fila Persistente de Tarefas (PostgreSQL)")
    q_table.add_column("Status", style="bold")
    q_table.add_column("Contagem", justify="right", style="cyan")
    for k, v in stats.items():
        q_table.add_row(k, str(v))
    console.print(q_table)

    # Tabela de SLAs por dataset
    h_table = Table(title="Saúde da Ingestão e Cumprimento de SLAs")
    h_table.add_column("Dataset", style="bold")
    h_table.add_column("Fonte")
    h_table.add_column("SLA", justify="center")
    h_table.add_column("Minutos Último Sucesso", justify="right")
    h_table.add_column("Pendentes", justify="right")
    h_table.add_column("Falhas", justify="right")

    for r in health:
        sla_color = "green" if r["sla_status"] == "SAUDAVEL" else "red"
        h_table.add_row(
            r["dataset_name"],
            r["source_name"],
            f"[{sla_color}]{r['sla_status']}[/{sla_color}]",
            str(r["minutes_since_last_success"] or "-"),
            str(r["pending_tasks"]),
            str(r["failed_tasks"]),
        )
    console.print(h_table)


@app.command()
def queue_reconcile() -> None:
    """Dispara reconciliação imediata de tarefas zumbis e lacunas de dados."""
    console.print("[bold yellow]▶️ Executando reconciliação autônoma de fila...[/bold yellow]")
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.queue.reconciler import IngestionReconciler

    async def _run():
        dw = DWWriter()
        pool = await dw.get_pool()
        reconciler = IngestionReconciler(pool=pool)
        recovered = await reconciler.recover_zombie_tasks()
        stale = await reconciler.detect_stale_policies()
        await dw.close()
        return recovered, stale

    recovered, stale = asyncio.run(_run())
    console.print(f"[bold green]Reconciliação concluída:[/bold green] {recovered} tarefas zumbis recuperadas.")
    if stale:
        console.print(f"[bold red]Atenção: {len(stale)} políticas com dados atrasados![/bold red]")


@app.command()
def dispatch_tasks(
    max_tasks: Annotated[int, typer.Option("--max-tasks", "-m", help="Número máximo de tarefas a processar")] = 10,
    lease_duration: Annotated[int, typer.Option("--lease-sec", "-l", help="Tempo de lease em segundos")] = 300,
) -> None:
    """Processa um lote de tarefas pendentes na fila PostgreSQL com SKIP LOCKED."""
    console.print(f"[bold green]▶️ Despachando tarefas da fila (limite: {max_tasks})...[/bold green]")
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.dispatcher.task_dispatcher import TaskDispatcher

    async def _run():
        dw = DWWriter()
        pool = await dw.get_pool()
        dispatcher = TaskDispatcher(pool=pool)
        try:
            return await dispatcher.dispatch_batch(max_tasks=max_tasks, lease_duration_sec=lease_duration)
        finally:
            await dispatcher.close()

    tasks = asyncio.run(_run())
    console.print(f"[bold cyan]Processamento finalizado:[/bold cyan] {len(tasks)} tarefas executadas.")


@app.command()
def enqueue_task(
    policy: Annotated[str, typer.Option("--policy", "-p", help="ID da política de ingestão")] = "b3_daily_quotes",
    dataset: Annotated[str, typer.Option("--dataset", "-d", help="Nome do dataset")] = "cotacao_diaria",
    target: Annotated[str | None, typer.Option("--target", "-t", help="Ticker ou entidade alvo")] = None,
    priority: Annotated[int, typer.Option("--priority", help="Prioridade (1 a 10)")] = 5,
) -> None:
    """Enfileira uma tarefa na fila persistente PostgreSQL (ingestion.ingestion_task)."""
    import time
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.queue.task_queue import PostgresTaskQueue

    async def _run():
        dw = DWWriter()
        pool = await dw.get_pool()
        queue = PostgresTaskQueue(pool=pool)
        task_key = f"{dataset}:{target or 'ALL'}:{int(time.time())}"
        task_id = await queue.enqueue_task(
            policy_id=policy,
            task_key=task_key,
            dataset_name=dataset,
            target_entity=target,
            priority=priority,
        )
        await dw.close()
        return task_id

    tid = asyncio.run(_run())
    console.print(f"[bold green]Tarefa enfileirada com sucesso:[/bold green] {tid}")


@app.command()
def export_snapshots(
    tickers: Annotated[str | None, typer.Option("--tickers", "-t", help="Tickers separados por vírgula")] = None,
    output_dir: Annotated[str, typer.Option("--output-dir", "-o", help="Diretório de saída")] = "dist/snapshots",
    publish_r2: Annotated[bool, typer.Option("--r2", help="Publicar também no Cloudflare R2")] = False,
) -> None:
    """Gera snapshots JSON versionados de ações, FIIs, rankings e panorama macro."""
    console.print(f"[bold cyan]▶️ Exportando snapshots para '{output_dir}' (R2={publish_r2})...[/bold cyan]")
    from fin_ingestion.dw.dw_writer import DWWriter
    from fin_ingestion.publishers.snapshot_exporter import SnapshotExporter

    ticker_list = [t.strip().upper() for t in tickers.split(",")] if tickers else None

    async def _run():
        dw = DWWriter()
        pool = await dw.get_pool()
        exporter = SnapshotExporter(pool=pool, local_dir=output_dir)
        try:
            return await exporter.export_all(tickers=ticker_list)
        finally:
            await exporter.close()
            await dw.close()

    res = asyncio.run(_run())
    console.print(f"[bold green]Exportação concluída:[/bold green] {res}")


@app.command()
def backfill_history(
    stage: Annotated[int, typer.Option("--stage", "-s", help="Etapa de carga: 1 (Validação rápida ~150 ativos) ou 2 (Universo completo)")] = 1,
    tickers: Annotated[str | None, typer.Option("--tickers", "-t", help="Lista de tickers separados por vírgula (opcional)")] = None,
    concurrency: Annotated[int, typer.Option("--concurrency", "-c", help="Concorrência simultânea")] = 8,
) -> None:
    """Executa o backfill histórico de 30 anos (desde o IPO) de cotações, proventos e splits."""
    console.print(f"\n[bold cyan]▶️ Iniciando Backfill Histórico de 30 Anos — Etapa {stage}...[/bold cyan]\n")
    from fin_ingestion.pipelines.historical_backfill import HistoricalBackfillPipeline

    ticker_list = [t.strip().upper() for t in tickers.split(",")] if tickers else None
    pipeline = HistoricalBackfillPipeline(concurrency=concurrency)

    async def _run():
        try:
            return await pipeline.run(stage=stage, tickers=ticker_list)
        finally:
            await pipeline.close()

    result = asyncio.run(_run())

    table = Table(title=f"Resultado do Backfill Histórico (Etapa {stage})")
    table.add_column("Métrica", style="bold")
    table.add_column("Valor", justify="right", style="green")

    table.add_row("Total de Ativos Avaliados", str(result["total_assets"]))
    table.add_row("Ativos Processados com Sucesso", str(result["successful_count"]))
    table.add_row("Ativos sem Dados / Falhas", str(result["failed_count"]))
    table.add_row("Total de Cotações Inseridas", f"{result['total_candles']:,}")
    table.add_row("Total de Proventos Inseridos", f"{result['total_dividends']:,}")
    table.add_row("Total de Splits Inseridos", f"{result['total_splits']:,}")
    console.print(table)

    if result["details"]:
        d_table = Table(title="Amostra de Ativos Enriquecidos com Data de IPO")
        d_table.add_column("Ticker", style="bold")
        d_table.add_column("Data IPO / 1º Pregão", justify="center", style="cyan")
        d_table.add_column("Cotações", justify="right")
        d_table.add_column("Proventos", justify="right")
        d_table.add_column("Splits", justify="right")

        for d in result["details"][:10]:
            d_table.add_row(
                d["ticker"],
                str(d["ipo_date"] or "-"),
                f"{d['candles_count']:,}",
                str(d["divs_count"]),
                str(d["splits_count"]),
            )
        console.print(d_table)

    if result["failed_tickers"]:
        console.print(f"[bold yellow]Ativos sem dados de mercado históricos:[/bold yellow] {', '.join(result['failed_tickers'][:15])}")


@app.command()
def version() -> None:
    """Exibe a versão do pacote."""
    console.print("[bold cyan]fin-ingestion v0.1.0[/bold cyan]")


if __name__ == "__main__":
    app()
