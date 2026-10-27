-- fct_cotacao_diaria.sql
-- Fato de cotações diárias negociadas na B3

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
from {{ ref('int_cotacoes_consolidadas') }}
