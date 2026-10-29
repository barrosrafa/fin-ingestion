-- Detecta marts vazios ou incompletos após um backfill bem sucedido.
select 'quotes' as dataset
where (select count(*) from {{ ref('fct_cotacao_diaria') }}) <>
      (select count(*) from {{ source('market', 'cotacao_diaria') }} q join {{ source('core', 'ativo') }} a using(id_ativo))
union all
select 'dividends' as dataset
where (select count(*) from {{ ref('fct_provento') }}) <>
      (select count(*) from {{ source('market', 'provento') }} p join {{ source('core', 'ativo') }} a using(id_ativo))
