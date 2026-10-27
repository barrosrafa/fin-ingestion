-- dim_imovel_fii.sql
-- Dimensão de Imóveis Físicos de Portfólio dos Fundos Imobiliários

with imoveis_raw as (
    select
        ticker,
        lista_imoveis_json
    from {{ ref('stg_clube_fii_imoveis') }}
)

select
    ticker,
    elem->>'nome' as nome_imovel,
    elem->>'cidade' as cidade,
    elem->>'uf' as uf,
    (elem->>'abl')::numeric(14,2) as area_bruta_locavel
from imoveis_raw,
jsonb_array_elements(lista_imoveis_json) as elem
