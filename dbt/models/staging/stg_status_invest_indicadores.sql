-- stg_status_invest_indicadores.sql
-- Modelo staging para dados capturados do Status Invest (Cotações, DRE, Aluguel BTC)

with source as (
    select
        trim(ticker)::text as ticker,
        data_referencia::date as data_referencia,
        category::text as categoria_ativo,
        valor::numeric(18,4) as valor_cotacao,
        detalhes as payload_json,
        captured_at
    from {{ source('raw', 'status_invest_payloads') }}
)

select * from source
