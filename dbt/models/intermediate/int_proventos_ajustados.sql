-- int_proventos_ajustados.sql
-- Desduplica e unifica proventos de FIIs e Ações com data-com e competência de apuração

with fiis_agenda as (
    select
        ticker,
        mes_referencia,
        data_com,
        data_pagamento,
        valor_provento,
        'FIIS_COM_BR' as fonte
    from {{ ref('stg_fiis_agenda_proventos') }}
),

funds_exp as (
    select
        ticker,
        to_char(data_com, 'YYYY-MM') as mes_referencia,
        data_com,
        data_pagamento,
        rendimento_cota as valor_provento,
        'FUNDS_EXPLORER' as fonte
    from {{ ref('stg_funds_explorer_ranking') }}
),

unificado as (
    select * from fiis_agenda
    union all
    select f.*
    from funds_exp f
    left join fiis_agenda a on a.ticker = f.ticker and a.data_com = f.data_com
    where a.ticker is null
),

deduplicado as (
    select
        *,
        row_number() over (
            partition by ticker, data_com
            order by case when fonte = 'FIIS_COM_BR' then 1 else 2 end
        ) as rn
    from unificado
)

select
    ticker,
    mes_referencia,
    data_com,
    data_pagamento,
    valor_provento,
    fonte
from deduplicado
where rn = 1
