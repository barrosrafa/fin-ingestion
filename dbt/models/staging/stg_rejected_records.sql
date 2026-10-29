-- Registros inválidos preservados para auditoria, sem excluir a landing raw.
select 'b3_cothist'::text as source_table, 'missing_required_fields'::text as reason, to_jsonb(r) as record from {{ source('raw', 'b3_cothist') }} r where not (nullif(trim(ticker), '') is not null and data_pregao is not null and preco_fechamento is not null)
union all
select 'cvm_demonstrativos'::text as source_table, 'missing_required_fields'::text as reason, to_jsonb(r) as record from {{ source('raw', 'cvm_demonstrativos') }} r where not (nullif(trim(cnpj_cia), '') is not null and data_referencia is not null)
union all
select 'brick360_scores'::text as source_table, 'missing_required_fields'::text as reason, to_jsonb(r) as record from {{ source('raw', 'brick360_scores') }} r where not (nullif(trim(ticker), '') is not null and score_global is not null)
union all
select 'etfs_brasil_taxas'::text as source_table, 'missing_required_fields'::text as reason, to_jsonb(r) as record from {{ source('raw', 'etfs_brasil_taxas') }} r where not (nullif(trim(ticker), '') is not null)
union all
select 'clube_fii_graphql'::text as source_table, 'missing_required_fields'::text as reason, to_jsonb(r) as record from {{ source('raw', 'clube_fii_graphql') }} r where not (nullif(trim(ticker), '') is not null)
