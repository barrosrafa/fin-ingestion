select ticker from {{ ref('stg_etfs_brasil_taxas') }}
where taxa_administracao_primaria < 0 or custo_total_ter < 0
