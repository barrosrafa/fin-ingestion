-- int_cotacoes_consolidadas.sql
-- Consolida e harmoniza cotações diárias prioritárias da B3 com fallbacks

with b3 as (
    select
        data_pregao,
        ticker,
        preco_abertura,
        preco_maximo,
        preco_minimo,
        preco_fechamento,
        preco_medio,
        volume_financeiro,
        quantidade_titulos,
        'B3' as fonte_primaria
    from {{ ref('stg_b3_cotacoes') }}
),

status_invest as (
    select
        data_referencia as data_pregao,
        ticker,
        valor_cotacao as preco_abertura,
        valor_cotacao as preco_maximo,
        valor_cotacao as preco_minimo,
        valor_cotacao as preco_fechamento,
        valor_cotacao as preco_medio,
        0 as volume_financeiro,
        0 as quantidade_titulos,
        'STATUS_INVEST' as fonte_primaria
    from {{ ref('stg_status_invest_indicadores') }}
),

unificado as (
    select * from b3
    union all
    select s.*
    from status_invest s
    left join b3 on b3.ticker = s.ticker and b3.data_pregao = s.data_pregao
    where b3.ticker is null
),

deduplicado as (
    select
        *,
        row_number() over (
            partition by ticker, data_pregao
            order by case when fonte_primaria = 'B3' then 1 else 2 end
        ) as rn
    from unificado
)

select
    data_pregao,
    ticker,
    preco_abertura,
    preco_maximo,
    preco_minimo,
    preco_fechamento,
    preco_medio,
    volume_financeiro,
    quantidade_titulos,
    fonte_primaria
from deduplicado
where rn = 1
