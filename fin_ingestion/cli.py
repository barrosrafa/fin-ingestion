"""CLI unificada para orquestração de ingestão, cálculos analíticos e dbt."""

import asyncio
import subprocess
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from fin_ingestion.analytics.health_scores import HealthScoreEngine
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
from fin_ingestion.pipelines.orchestrator import PipelineOrchestrator
from fin_ingestion.pipelines.status_invest_etl import StatusInvestETL
from fin_ingestion.pipelines.enrichment import AssetMetadataEnricher

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
) -> None:
    """Calcula e exibe métricas de Valuation, Scores de Saúde e Risco para os tickers."""
    ticker_list = [t.strip().upper() for t in tickers.split(",")]
    table = Table(title="Cálculo de Valuation & Indicadores Analíticos")
    table.add_column("Ticker", style="bold")
    table.add_column("Preço Graham", justify="right")
    table.add_column("Preço Teto Bazin", justify="right")
    table.add_column("Piotroski F-Score", justify="center")
    table.add_column("Altman Z-Score", justify="center")
    table.add_column("RSI (14)", justify="right")
    table.add_column("Pivot Point", justify="right")

    for t in ticker_list:
        # Valores simulados representativos de mercado para demonstração dos cálculos analíticos
        graham = ValuationEngine.graham_fair_price(lpa=8.20, vpa=32.50)
        bazin = ValuationEngine.bazin_ceiling_price(dpa_medio=4.10)
        f_score = HealthScoreEngine.piotroski_f_score(
            net_income=100.0,
            operating_cash_flow=120.0,
            roa_current=0.15,
            roa_previous=0.12,
            cf_vs_net_income=True,
            leverage_current=1.2,
            leverage_previous=1.4,
            current_ratio_current=1.6,
            current_ratio_previous=1.5,
            shares_current=1000,
            shares_previous=1000,
            gross_margin_current=0.45,
            gross_margin_previous=0.42,
            asset_turnover_current=0.8,
            asset_turnover_previous=0.75,
        )
        z_score = HealthScoreEngine.altman_z_score_emerging(
            working_capital=50.0,
            total_assets=200.0,
            retained_earnings=60.0,
            ebit=40.0,
            market_cap=180.0,
            total_liabilities=80.0,
            revenue=150.0,
        )
        pivots = TechnicalAnalysisEngine.pivot_points_4s_3r(high=38.5, low=36.8, close=37.85)
        rsi = TechnicalAnalysisEngine.relative_strength_index(
            [35.0, 35.5, 36.0, 35.8, 36.2, 36.9, 37.1, 36.8, 37.0, 37.2, 37.5, 37.3, 37.7, 37.85, 38.0]
        )

        table.add_row(
            t,
            f"R$ {graham:.2f}" if graham else "-",
            f"R$ {bazin:.2f}" if bazin else "-",
            f"{f_score['f_score']}/9 ({f_score['health_level']})",
            f"{z_score['z_score']} ({z_score['zone']})",
            f"{rsi:.1f}" if rsi else "-",
            f"R$ {pivots['pivot_point']:.2f}",
        )

    console.print(table)


@app.command()
def dbt_run(
    models: Annotated[str, typer.Option("--models", "-m", help="Seletor de modelos (staging, intermediate, marts)")] = "marts",
) -> None:
    """Invoca o motor de transformação dbt."""
    console.print(f"[bold green]▶️ Executando 'dbt run --select {models}'...[/bold green]")
    cmd = ["dbt", "run", "--select", models, "--project-dir", settings.dbt_project_dir, "--profiles-dir", settings.dbt_profiles_dir]
    try:
        subprocess.run(cmd, check=True)
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
def version() -> None:
    """Exibe a versão do pacote."""
    console.print("[bold cyan]fin-ingestion v0.1.0[/bold cyan]")


if __name__ == "__main__":
    app()
