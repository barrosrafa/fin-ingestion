-- stg_clube_fii_imoveis.sql
-- Modelo staging para portfólio físico e métricas do Clube FII

with source as (
    select
        trim(ticker)::text as ticker,
        null::text as cnpj_fundo,
        trim(nome_fundo)::text as razao_social,
        null::text as gestora,
        null::numeric(20,4) as patrimonio_liquido,
        vacancia_fisica_pct::numeric(8,4) as vacancia_fisica_pct,
        vacancia_financeira_pct::numeric(8,4) as vacancia_financeira_pct,
        quantidade_imoveis::integer as total_imoveis,
        lista_imoveis as lista_imoveis_json,
        captured_at
    from {{ source('raw', 'clube_fii_graphql') }}
    where nullif(trim(ticker), '') is not null
)

select * from source
