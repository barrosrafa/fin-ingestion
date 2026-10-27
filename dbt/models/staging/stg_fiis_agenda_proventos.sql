-- stg_fiis_agenda_proventos.sql
-- Modelo staging para agenda de dividendos com competência contábil de apuração do FIIs.com.br

with source as (
    select
        trim(ticker)::text as ticker,
        mes_referencia::text as mes_referencia,
        data_com::date as data_com,
        data_pagamento::date as data_pagamento,
        valor_provento::numeric(14,6) as valor_provento,
        captured_at
    from {{ source('raw', 'fiis_com_br_agenda') }}
)

select * from source
