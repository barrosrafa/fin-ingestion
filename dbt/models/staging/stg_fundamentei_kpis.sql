-- stg_fundamentei_kpis.sql
-- Modelo staging para entidades desnormalizadas do Apollo GraphQL do Fundamentei

with source as (
    select
        trim(ticker)::text as ticker,
        trim(razao_social)::text as razao_social,
        trim(codigo_cvm)::text as codigo_cvm,
        trim(setor)::text as setor_fundamentei,
        (reclame_aqui->>'score')::numeric(5,2) as score_reclame_aqui,
        reclame_aqui->>'status' as status_reclame_aqui,
        dados_brutos as apollo_payload,
        captured_at
    from {{ source('raw', 'fundamentei_apollo') }}
)

select * from source
