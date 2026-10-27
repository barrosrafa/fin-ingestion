-- dim_emissor.sql
-- Dimensão de emissores / companhias abertas registradas na CVM

with cvm_cias as (
    select distinct
        cnpj,
        codigo_cvm,
        razao_social,
        nome_comercial,
        setor_atividade,
        situacao_registro,
        data_registro
    from {{ source('raw', 'cvm_cad_cia') }}
)

select
    cnpj,
    codigo_cvm,
    razao_social,
    nome_comercial,
    setor_atividade,
    situacao_registro,
    data_registro
from cvm_cias
