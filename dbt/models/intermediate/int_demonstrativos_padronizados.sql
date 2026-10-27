-- int_demonstrativos_padronizados.sql
-- Harmoniza demonstrações contábeis DFP/ITR da CVM em rubricas financeiras padrão

with cvm_raw as (
    select
        cnpj,
        codigo_cvm,
        data_referencia,
        demonstrativo_tipo,
        codigo_conta,
        descricao_conta,
        valor_conta
    from {{ ref('stg_cvm_demonstrativos') }}
),

pivoted as (
    select
        cnpj,
        codigo_cvm,
        data_referencia,
        max(case when codigo_conta = '3.01' then valor_conta end) as receita_liquida,
        max(case when codigo_conta = '3.02' then valor_conta end) as custos_produtos_vendidos,
        max(case when codigo_conta = '3.03' then valor_conta end) as lucro_bruto,
        max(case when codigo_conta = '3.05' then valor_conta end) as ebit,
        max(case when codigo_conta = '3.07' then valor_conta end) as resultado_financeiro,
        max(case when codigo_conta = '3.11' then valor_conta end) as lucro_liquido,
        max(case when codigo_conta = '1' then valor_conta end) as ativo_total,
        max(case when codigo_conta = '2.03' then valor_conta end) as patrimonio_liquido
    from cvm_raw
    group by cnpj, codigo_cvm, data_referencia
)

select
    cnpj,
    codigo_cvm,
    data_referencia,
    coalesce(receita_liquida, 0) as receita_liquida,
    coalesce(custos_produtos_vendidos, 0) as custos_produtos_vendidos,
    coalesce(lucro_bruto, 0) as lucro_bruto,
    coalesce(ebit, 0) as ebit,
    coalesce(resultado_financeiro, 0) as resultado_financeiro,
    coalesce(lucro_liquido, 0) as lucro_liquido,
    coalesce(ativo_total, 0) as ativo_total,
    coalesce(patrimonio_liquido, 0) as patrimonio_liquido
from pivoted
