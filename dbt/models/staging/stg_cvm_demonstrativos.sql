-- stg_cvm_demonstrativos.sql
-- Modelo staging para demonstrações financeiras DFP/ITR da CVM

with source as (
    select
        trim(cnpj_cia)::text as cnpj,
        trim(dados_brutos->>'CD_CVM')::text as codigo_cvm,
        trim(dados_brutos->>'DENOM_CIA')::text as denominacao_social,
        data_referencia::date as data_referencia,
        versao::integer as versao_documento,
        tipo_demonstrativo::text as demonstrativo_tipo, -- DRE, BPA, BPP, DFC_MD
        trim(conta)::text as codigo_conta,
        trim(descricao_conta)::text as descricao_conta,
        valor_conta::numeric(20,4) as valor_conta,
        (dados_brutos->>'ORDEM_EXERC')::text as ordem_exercicio -- ULTIMO, PENULTIMO
    from {{ source('raw', 'cvm_demonstrativos') }}
    where nullif(trim(cnpj_cia), '') is not null and data_referencia is not null
)

select * from source
