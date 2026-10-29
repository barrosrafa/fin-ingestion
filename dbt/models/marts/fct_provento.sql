-- fct_provento.sql
-- Fato de proventos declarados e pagos (Dividendos, JCP, Rendimentos FII)

select
    ticker,
    data_com,
    data_pagamento,
    mes_referencia,
    valor_provento,
    tipo_provento,
    fonte
from {{ ref('int_proventos_ajustados') }}
