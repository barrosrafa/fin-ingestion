-- fct_indicador_fundamentalista.sql
-- Fato de múltiplos e indicadores fundamentalistas consolidados

with inv10 as (
    select
        ticker,
        preco_lucro,
        preco_vpa,
        roe_pct,
        dividend_yield_pct,
        ev_ebitda
    from {{ ref('stg_investidor10_multiplos') }}
),

analise as (
    select
        ticker,
        preco_justo_graham,
        preco_teto_bazin,
        pontuacao_5_pilares
    from {{ ref('stg_analise_de_acoes_checklists') }}
)

select
    coalesce(i.ticker, a.ticker) as ticker,
    i.preco_lucro,
    i.preco_vpa,
    i.roe_pct,
    i.dividend_yield_pct,
    i.ev_ebitda,
    a.preco_justo_graham,
    a.preco_teto_bazin,
    a.pontuacao_5_pilares
from inv10 i
full outer join analise a on a.ticker = i.ticker
