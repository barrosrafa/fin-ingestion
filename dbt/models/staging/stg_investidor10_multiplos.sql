-- stg_investidor10_multiplos.sql
-- Modelo staging para múltiplos fundamentalistas e séries de 10 anos do Investidor10

with source as (
    select
        trim(ticker)::text as ticker,
        endpoint::text as endpoint_origem,
        coalesce(pl::numeric(14,4), 0) as preco_lucro,
        coalesce(pvp::numeric(14,4), 0) as preco_vpa,
        coalesce(roe::numeric(10,4), 0) as roe_pct,
        coalesce(dividend_yield::numeric(10,4), 0) as dividend_yield_pct,
        coalesce(ev_ebitda::numeric(14,4), 0) as ev_ebitda,
        dados_completos as payload_json,
        captured_at
    from {{ source('raw', 'investidor10_payloads') }}
)

select * from source
