-- int_fii_metricas_consolidadas.sql
-- Harmoniza métricas imobiliárias, vacâncias e pontuações do Clube FII e Brick360

with clube as (
    select
        ticker,
        cnpj_fundo,
        razao_social,
        gestora,
        patrimonio_liquido,
        vacancia_fisica_pct,
        vacancia_financeira_pct,
        total_imoveis
    from {{ ref('stg_clube_fii_imoveis') }}
),

brick as (
    select
        ticker,
        score_global,
        score_imobiliario,
        score_financeiro,
        score_inquilinos,
        score_gestao,
        score_credito,
        score_preco,
        walt_anos,
        concentracao_top5_pct,
        contratos_atipicos_pct
    from {{ ref('stg_brick360_scores') }}
)

select
    coalesce(c.ticker, b.ticker) as ticker,
    c.cnpj_fundo,
    c.razao_social,
    c.gestora,
    c.patrimonio_liquido,
    c.vacancia_fisica_pct,
    c.vacancia_financeira_pct,
    c.total_imoveis,
    b.score_global as brick_score_global,
    b.score_imobiliario,
    b.score_financeiro,
    b.score_inquilinos,
    b.score_gestao,
    b.score_credito,
    b.score_preco,
    b.walt_anos,
    b.concentracao_top5_pct,
    b.contratos_atipicos_pct
from clube c
full outer join brick b on b.ticker = c.ticker
