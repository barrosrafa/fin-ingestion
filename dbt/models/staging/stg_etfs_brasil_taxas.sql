-- stg_etfs_brasil_taxas.sql
-- Modelo staging para taxas de administração (TER), spread NAV e liquidez de ETFs

with source as (
    select
        trim(ticker)::text as ticker,
        taxa_administracao_pct::numeric(6,4) as taxa_administracao_primaria,
        null::numeric(6,4) as taxa_secundaria,
        taxa_administracao_pct::numeric(6,4) as custo_total_ter,
        nav_patrimonial::numeric(14,4) as nav_patrimonial,
        spread_nav_pct::numeric(8,4) as spread_nav_pct,
        limite_aluguel_pct::numeric(8,4) as limite_aluguel_pct,
        fluxo_cotas_30d::numeric(18,2) as fluxo_cotas_30d,
        trim(indice_referencia)::text as indice_referencia,
        captured_at
    from {{ source('raw', 'etfs_brasil_taxas') }}
)

select * from source
