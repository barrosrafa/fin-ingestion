-- Full fiscal identity, nominal values and explicitly supported monetary scales.
select cnpj, payload->>'CD_CVM' as codigo_cvm,
    payload->>'DENOM_CIA' as denominacao_social,
    reference_date as data_referencia, version as versao_documento,
    statement as demonstrativo_tipo, account as codigo_conta,
    description as descricao_conta,
    case when upper(scale)='MIL' then amount*1000 else amount end as valor_conta,
    exercise_order as ordem_exercicio, scope as escopo, currency as moeda,
    period_start as inicio_periodo, period_end as fim_periodo, filing as documento, captured_at
from {{ source('canonical','cvm_statement_latest') }}
where upper(scale) in ('MIL','UNIDADE')
