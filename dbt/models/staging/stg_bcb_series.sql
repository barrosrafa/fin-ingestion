-- stg_bcb_series.sql
-- Modelo staging para séries macroeconômicas do SGS do Banco Central

with source as (
    select
        data::date as data_referencia,
        trim(serie_nome)::text as serie_nome,
        serie_id::integer as serie_id,
        valor::numeric(14,6) as valor_taxa,
        captured_at
    from {{ source('raw', 'bcb_sgs') }}
)

select * from source
