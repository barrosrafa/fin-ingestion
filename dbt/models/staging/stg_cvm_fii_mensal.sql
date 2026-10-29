-- Última coleta oficial; campos ausentes permanecem nulos.
with latest as (
    select convert_from(body, 'UTF8')::jsonb as payload
    from {{ source('raw', 'payload') }}
    where source_url = 'cvm://fii_informe_mensal'
    order by captured_at desc
    limit 1
), records as (
    select item
    from latest, lateral jsonb_array_elements(payload->'records') item
)
select
    asset.ticker,
    item->>'CNPJ_Fundo_Classe' as cnpj_fundo,
    (item->>'Data_Referencia')::date as data_referencia,
    (item->>'Versao')::integer as versao,
    item->>'Nome_Fundo_Classe' as razao_social,
    item->>'Nome_Administrador' as administrador,
    nullif(item->>'Patrimonio_Liquido', '')::numeric as patrimonio_liquido,
    nullif(item->>'Valor_Patrimonial_Cotas', '')::numeric as valor_patrimonial_cota,
    nullif(item->>'Total_Numero_Cotistas', '')::numeric as cotistas_total
from records
left join lateral (
    select a.ticker
    from {{ source('core', 'ativo') }} a
    left join {{ source('core', 'emissor_companhia') }} e using (id_emissor)
    where a.tipo_ativo = 'FII'
      and (a.isin = nullif(item->>'Codigo_ISIN', '') or e.cnpj::text = item->>'CNPJ_Fundo_Classe')
    order by (a.isin = nullif(item->>'Codigo_ISIN', '')) desc nulls last, a.ticker
    limit 1
) asset on true
