-- stg_brick360_scores.sql
-- Modelo staging para Brick Score (6 eixos) e métricas contratuais do Brick360

with source as (
    select
        trim(ticker)::text as ticker,
        score_global::numeric(6,2) as score_global,
        score_imobiliario::numeric(6,2) as score_imobiliario,
        score_financeiro::numeric(6,2) as score_financeiro,
        score_inquilinos::numeric(6,2) as score_inquilinos,
        score_gestao::numeric(6,2) as score_gestao,
        score_credito::numeric(6,2) as score_credito,
        score_preco::numeric(6,2) as score_preco,
        walt_anos::numeric(6,2) as walt_anos,
        concentracao_top5_pct::numeric(6,2) as concentracao_top5_pct,
        contratos_atipicos_pct::numeric(6,2) as contratos_atipicos_pct,
        captured_at
    from {{ source('raw', 'brick360_scores') }}
    where nullif(trim(ticker), '') is not null and score_global is not null
)

select * from source
