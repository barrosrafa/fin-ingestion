-- stg_clube_fii_imoveis.sql
-- Modelo staging para portfólio físico e métricas do Clube FII

with source as (
    select
        trim(ticker)::text as ticker,
        trim(cnpj)::text as cnpj_fundo,
        trim(razao_social)::text as razao_social,
        trim(gestora)::text as gestora,
        patrimonio_liquido::numeric(20,4) as patrimonio_liquido,
        vacancia_fisica_pct::numeric(8,4) as vacancia_fisica_pct,
        vacancia_financeira_pct::numeric(8,4) as vacancia_financeira_pct,
        total_imoveis::integer as total_imoveis,
        imoveis as lista_imoveis_json,
        captured_at
    from {{ source('raw', 'clube_fii_graphql') }}
)

select * from source
