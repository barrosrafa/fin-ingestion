-- stg_funds_explorer_ranking.sql
-- Modelo staging para ranking consolidado e proventos do Funds Explorer

with source as (
    select
        trim(ticker)::text as ticker,
        data_com::date as data_com,
        data_pagamento::date as data_pagamento,
        valor_rendimento::numeric(14,6) as rendimento_cota,
        dividend_yield_pct::numeric(8,4) as dividend_yield_pct,
        raw_item as payload_json,
        captured_at
    from {{ source('raw', 'funds_explorer_payloads') }}
)

select * from source
