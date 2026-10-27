-- stg_cvm_demonstrativos.sql
-- Modelo staging para demonstrações financeiras DFP/ITR da CVM

with source as (
    select
        trim(cnpj_cia)::text as cnpj,
        trim(cd_cvm)::text as codigo_cvm,
        trim(denom_cia)::text as denominacao_social,
        dt_refer::date as data_referencia,
        versao::integer as versao_documento,
        grupo_dfp::text as demonstrativo_tipo, -- DRE, BPA, BPP, DFC_MD
        trim(cd_conta)::text as codigo_conta,
        trim(ds_conta)::text as descricao_conta,
        vl_conta::numeric(20,4) as valor_conta,
        ordem_exerc::text as ordem_exercicio -- ULTIMO, PENULTIMO
    from {{ source('raw', 'cvm_demonstrativos') }}
)

select * from source
