select
    cnpj::text as cnpj,
    codigo_cvm::text as codigo_cvm,
    nome_empresarial as razao_social,
    nome_empresarial as nome_comercial,
    setor as setor_atividade,
    status_cnpj::text as situacao_registro,
    data_registro_cvm as data_registro
from {{ source('core', 'emissor_companhia') }}
