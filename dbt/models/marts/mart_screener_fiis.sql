-- mart_screener_fiis.sql
-- Mart screener especializado para seleção e ranking de Fundos Imobiliários

with cotacoes as (
    select distinct on (ticker)
        ticker,
        preco_fechamento,
        data_pregao
    from {{ ref('fct_cotacao_diaria') }}
    order by ticker, data_pregao desc
),

fii_snap as (
    select * from {{ ref('fct_fii_snapshot_mensal') }}
),

proventos_12m as (
    select
        ticker,
        sum(valor_provento) as proventos_acumulados_12m,
        count(*) as quantidade_pagamentos
    from {{ ref('fct_provento') }}
    where data_pagamento >= current_date - interval '12 months'
      and data_pagamento < current_date
    group by ticker
)

select
    s.ticker,
    s.razao_social,
    s.gestora,
    c.preco_fechamento,
    s.patrimonio_liquido,
    s.vacancia_fisica_pct,
    s.vacancia_financeira_pct,
    s.total_imoveis,
    s.brick_score_global,
    s.walt_anos,
    p.proventos_acumulados_12m,
    case
        when c.preco_fechamento > 0 and p.proventos_acumulados_12m is not null
        then round((p.proventos_acumulados_12m / c.preco_fechamento) * 100.0, 2)
    end as dividend_yield_12m_pct
from fii_snap s
left join cotacoes c on c.ticker = s.ticker
left join proventos_12m p on p.ticker = s.ticker
