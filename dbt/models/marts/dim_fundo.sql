-- dim_fundo.sql
-- Dimensão de Fundos de Investimento (CVM 175) e Fundos Imobiliários

with fiis as (
    select distinct
        ticker,
        cnpj_fundo as cnpj,
        razao_social,
        gestora,
        'FII' as tipo_fundo
    from {{ ref('int_fii_metricas_consolidadas') }}
)

select
    ticker,
    cnpj,
    razao_social,
    gestora,
    tipo_fundo
from fiis
