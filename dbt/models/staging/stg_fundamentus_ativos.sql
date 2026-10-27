-- stg_fundamentus_ativos.sql
-- Modelo staging para parsing HTML estruturado do Fundamentus

with source as (
    select
        trim(ticker)::text as ticker,
        route::text as rota_fundamentus,
        metricas->>'cotacao' as cotacao_bruta,
        metricas->>'pl' as pl_bruto,
        metricas->>'pvp' as pvp_bruto,
        metricas as todas_metricas,
        captured_at
    from {{ source('raw', 'fundamentus_html') }}
)

select * from source
