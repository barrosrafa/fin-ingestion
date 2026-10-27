-- stg_analise_de_acoes_checklists.sql
-- Modelo staging para checklists de 5 pilares e valuation Graham/Bazin do Análise de Ações

with source as (
    select
        trim(ticker)::text as ticker,
        preco_justo_graham::numeric(14,4) as preco_justo_graham,
        preco_teto_bazin::numeric(14,4) as preco_teto_bazin,
        pontuacao_5_pilares::integer as pontuacao_5_pilares,
        dia_habitual_provento::integer as dia_habitual_provento,
        dados_completos as payload_json,
        captured_at
    from {{ source('raw', 'analise_de_acoes_checklists') }}
)

select * from source
