"""Coletores das 12 plataformas de análise de investimentos."""

from fin_ingestion.collectors.sites.advfn import ADVFNCollector
from fin_ingestion.collectors.sites.analise_de_acoes import AnaliseDeAcoesCollector
from fin_ingestion.collectors.sites.brick360 import Brick360Collector
from fin_ingestion.collectors.sites.clube_fii import ClubeFIICollector
from fin_ingestion.collectors.sites.etfs_brasil import ETFsBrasilCollector
from fin_ingestion.collectors.sites.fiis_com_br import FIISComBrCollector
from fin_ingestion.collectors.sites.fundamentei import FundamenteiCollector
from fin_ingestion.collectors.sites.fundamentus import FundamentusCollector
from fin_ingestion.collectors.sites.funds_explorer import FundsExplorerCollector
from fin_ingestion.collectors.sites.investidor10 import Investidor10Collector
from fin_ingestion.collectors.sites.mais_retorno import MaisRetornoCollector
from fin_ingestion.collectors.sites.status_invest import StatusInvestCollector

__all__ = [
    "StatusInvestCollector",
    "Investidor10Collector",
    "FundamenteiCollector",
    "FundamentusCollector",
    "ClubeFIICollector",
    "FundsExplorerCollector",
    "FIISComBrCollector",
    "MaisRetornoCollector",
    "AnaliseDeAcoesCollector",
    "Brick360Collector",
    "ADVFNCollector",
    "ETFsBrasilCollector",
]
