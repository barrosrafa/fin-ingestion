-- Preserve scope, currency and period; missing financial facts remain NULL.
select cnpj, codigo_cvm, data_referencia, escopo, moeda, documento,
    inicio_periodo, fim_periodo, ordem_exercicio,
    max(case when codigo_conta='3.01' then valor_conta end) as receita_liquida,
    max(case when codigo_conta='3.02' then valor_conta end) as custos_produtos_vendidos,
    max(case when codigo_conta='3.03' then valor_conta end) as lucro_bruto,
    max(case when codigo_conta='3.05' then valor_conta end) as ebit,
    max(case when codigo_conta='3.06' then valor_conta end) as resultado_financeiro,
    max(case when codigo_conta='3.11' then valor_conta end) as lucro_liquido,
    max(case when codigo_conta='1' and demonstrativo_tipo='BPA' then valor_conta end) as ativo_total,
    max(case when codigo_conta='2.03' then valor_conta end) as patrimonio_liquido
from {{ ref('stg_cvm_statement_v2') }}
group by cnpj,codigo_cvm,data_referencia,escopo,moeda,documento,inicio_periodo,fim_periodo,ordem_exercicio
