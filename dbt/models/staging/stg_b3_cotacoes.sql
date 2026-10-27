-- stg_b3_cotacoes.sql
-- Modelo staging para cotações diárias brutas originadas do arquivo COTAHIST da B3

with source as (
    select
        data_pregao::date as data_pregao,
        trim(ticker)::text as ticker,
        cod_bdi::text as cod_bdi,
        tipo_mercado::integer as tipo_mercado,
        trim(nome_resumido)::text as nome_resumido,
        trim(especificacao)::text as especificacao,
        preco_abertura::numeric(18,4) as preco_abertura,
        preco_maximo::numeric(18,4) as preco_maximo,
        preco_minimo::numeric(18,4) as preco_minimo,
        preco_medio::numeric(18,4) as preco_medio,
        preco_fechamento::numeric(18,4) as preco_fechamento,
        total_negocios::integer as total_negocios,
        quantidade_titulos::bigint as quantidade_titulos,
        volume_financeiro::numeric(20,4) as volume_financeiro,
        trim(codigo_isin)::text as codigo_isin,
        captured_at
    from {{ source('raw', 'b3_cothist') }}
)

select * from source
