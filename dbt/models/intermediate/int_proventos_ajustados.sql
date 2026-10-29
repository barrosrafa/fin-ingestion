-- Preserva os eventos canônicos, inclusive proventos históricos do backfill.
select
    a.ticker, p.mes_referencia, p.data_com, p.data_pagamento,
    p.valor_por_acao as valor_provento,
    p.tipo_provento::text as tipo_provento,
    'MARKET_CANONICAL'::text as fonte
from {{ source('market', 'provento') }} p
join {{ source('core', 'ativo') }} a using (id_ativo)
