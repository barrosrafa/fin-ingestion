-- mart_screener_etfs.sql
-- Mart screener comparativo de ETFs (B3 e internacionais)

with etf_base as (
    select
        ticker,
        taxa_administracao_primaria,
        taxa_secundaria,
        custo_total_ter,
        nav_patrimonial,
        spread_nav_pct,
        limite_aluguel_pct,
        fluxo_cotas_30d,
        indice_referencia
    from {{ ref('stg_etfs_brasil_taxas') }}
),

cotacoes as (
    select distinct on (ticker)
        ticker,
        preco_fechamento,
        data_pregao
    from {{ ref('fct_cotacao_diaria') }}
    order by ticker, data_pregao desc
)

select
    e.ticker,
    c.preco_fechamento,
    e.nav_patrimonial,
    e.spread_nav_pct,
    e.custo_total_ter,
    e.taxa_administracao_primaria,
    e.taxa_secundaria,
    e.limite_aluguel_pct,
    e.fluxo_cotas_30d,
    e.indice_referencia
from etf_base e
left join cotacoes c on c.ticker = e.ticker
