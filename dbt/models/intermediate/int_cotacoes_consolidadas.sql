select
    q.data_pregao, a.ticker, q.preco_abertura, q.preco_maximo, q.preco_minimo,
    q.preco_fechamento, q.preco_medio,
    q.volume_total as volume_financeiro,
    null::bigint as quantidade_titulos,
    'MARKET_CANONICAL'::text as fonte_primaria
from {{ source('market', 'cotacao_diaria') }} q
join {{ source('core', 'ativo') }} a using (id_ativo)
