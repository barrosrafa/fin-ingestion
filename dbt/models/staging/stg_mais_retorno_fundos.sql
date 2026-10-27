-- stg_mais_retorno_fundos.sql
-- Modelo staging para métricas quantitativas de risco do Mais Retorno

with source as (
    select
        trim(identificador)::text as identificador,
        categoria::text as categoria,
        sharpe::numeric(10,4) as indice_sharpe,
        sortino::numeric(10,4) as indice_sortino,
        volatilidade_anualizada::numeric(10,4) as volatilidade_anualizada_pct,
        max_drawdown::numeric(10,4) as max_drawdown_pct,
        metricas_completas as payload_json,
        captured_at
    from {{ source('raw', 'mais_retorno_metricas') }}
)

select * from source
