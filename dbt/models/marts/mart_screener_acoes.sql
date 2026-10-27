-- mart_screener_acoes.sql
-- Mart screener completo para seleção quantitativa e fundamentalista de ações

with cotacoes as (
    select distinct on (ticker)
        ticker,
        preco_fechamento,
        data_pregao
    from {{ ref('fct_cotacao_diaria') }}
    order by ticker, data_pregao desc
),

indicadores as (
    select * from {{ ref('fct_indicador_fundamentalista') }}
),

tecnico as (
    select distinct on (ticker)
        ticker,
        rsi_14,
        tendencia_primaria,
        suporte_1,
        resistencia_1
    from {{ ref('stg_advfn_tecnico') }}
    order by ticker, captured_at desc
)

select
    c.ticker,
    c.data_pregao,
    c.preco_fechamento,
    i.preco_lucro,
    i.preco_vpa,
    i.roe_pct,
    i.dividend_yield_pct,
    i.ev_ebitda,
    i.preco_justo_graham,
    i.preco_teto_bazin,
    case
        when i.preco_justo_graham is not null and c.preco_fechamento > 0
        then round(((i.preco_justo_graham - c.preco_fechamento) / c.preco_fechamento) * 100.0, 2)
    end as margem_seguranca_graham_pct,
    case
        when i.preco_teto_bazin is not null and c.preco_fechamento > 0
        then round(((i.preco_teto_bazin - c.preco_fechamento) / c.preco_fechamento) * 100.0, 2)
    end as margem_seguranca_bazin_pct,
    i.pontuacao_5_pilares,
    t.rsi_14,
    t.tendencia_primaria,
    t.suporte_1,
    t.resistencia_1
from cotacoes c
left join indicadores i on i.ticker = c.ticker
left join tecnico t on t.ticker = c.ticker
