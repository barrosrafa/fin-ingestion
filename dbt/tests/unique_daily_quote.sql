select ticker, data_pregao, count(*) as duplicates
from {{ ref('fct_cotacao_diaria') }}
group by ticker, data_pregao
having count(*) > 1
