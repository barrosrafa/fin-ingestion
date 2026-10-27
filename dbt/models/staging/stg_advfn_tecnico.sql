-- stg_advfn_tecnico.sql
-- Modelo staging para análise técnica, pivôs 4S/3R e RSI da ADVFN

with source as (
    select
        trim(ticker)::text as ticker,
        pivot_point::numeric(14,4) as pivot_point,
        suporte_1::numeric(14,4) as suporte_1,
        suporte_2::numeric(14,4) as suporte_2,
        suporte_3::numeric(14,4) as suporte_3,
        suporte_4::numeric(14,4) as suporte_4,
        resistencia_1::numeric(14,4) as resistencia_1,
        resistencia_2::numeric(14,4) as resistencia_2,
        resistencia_3::numeric(14,4) as resistencia_3,
        rsi_14::numeric(8,4) as rsi_14,
        trim(tendencia_primaria)::text as tendencia_primaria,
        captured_at
    from {{ source('raw', 'advfn_tecnico') }}
)

select * from source
