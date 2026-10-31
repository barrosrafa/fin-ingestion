select cnpj,data_referencia,escopo,moeda,documento,inicio_periodo,fim_periodo,ordem_exercicio
from {{ ref('fct_cvm_statement_v2') }}
group by cnpj,data_referencia,escopo,moeda,documento,inicio_periodo,fim_periodo,ordem_exercicio
having count(*)>1
