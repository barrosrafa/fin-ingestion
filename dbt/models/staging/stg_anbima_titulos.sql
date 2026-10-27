-- stg_anbima_titulos.sql
-- Modelo staging para taxas indicativas e PU do mercado secundário ANBIMA

with source as (
    select
        data_referencia::date as data_referencia,
        trim(codigo_selic)::text as codigo_selic,
        trim(tipo_titulo)::text as tipo_titulo,
        data_vencimento::date as data_vencimento,
        taxa_indicativa::numeric(10,4) as taxa_indicativa,
        pu_indicativo::numeric(18,6) as pu_indicativo,
        taxa_minima::numeric(10,4) as taxa_minima,
        taxa_maxima::numeric(10,4) as taxa_maxima,
        desvio_padrao::numeric(10,4) as desvio_padrao
    from {{ source('raw', 'anbima_mercado_secundario') }}
)

select * from source
