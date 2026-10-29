-- O cadastro canônico também contém ativos carregados pelas fontes históricas.
select
    a.ticker, e.nome_empresarial as nome_resumido,
    a.classe_acao::text as especificacao,
    a.isin as codigo_isin,
    a.tipo_ativo::text as classe_ativo,
    etf.custo_total_ter, etf.indice_referencia
from {{ source('core', 'ativo') }} a
left join {{ source('core', 'emissor_companhia') }} e using (id_emissor)
left join (
    select distinct on (ticker) ticker, custo_total_ter, indice_referencia
    from {{ ref('stg_etfs_brasil_taxas') }} order by ticker, captured_at desc
) etf using (ticker)
where a.ticker is not null
