-- fct_demonstrativo_financeiro.sql
-- Fato de demonstrações contábeis DFP/ITR padronizadas

select
    cnpj,
    codigo_cvm,
    data_referencia,
    receita_liquida,
    custos_produtos_vendidos,
    lucro_bruto,
    ebit,
    resultado_financeiro,
    lucro_liquido,
    ativo_total,
    patrimonio_liquido
from {{ ref('int_demonstrativos_padronizados') }}
