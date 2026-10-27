-- dim_ativo.sql
-- Dimensão de instrumentos negociáveis (Ações, FIIs, ETFs, BDRs)

with tickers_b3 as (
    select distinct
        ticker,
        nome_resumido,
        especificacao,
        codigo_isin
    from {{ ref('stg_b3_cotacoes') }}
),

etfs as (
    select distinct
        ticker,
        custo_total_ter,
        indice_referencia
    from {{ ref('stg_etfs_brasil_taxas') }}
)

select
    b.ticker,
    b.nome_resumido,
    b.especificacao,
    b.codigo_isin,
    case
        when e.ticker is not null then 'ETF'
        when b.ticker like '%11' and b.especificacao like '%FII%' then 'FII'
        when b.ticker like '%34' or b.ticker like '%35' then 'BDR'
        when b.ticker like '%3' then 'ON'
        when b.ticker like '%4' then 'PN'
        when b.ticker like '%11' then 'UNIT'
        else 'OUTRO'
    end as classe_ativo,
    coalesce(e.custo_total_ter, 0) as custo_total_ter,
    e.indice_referencia
from tickers_b3 b
left join etfs e on e.ticker = b.ticker
