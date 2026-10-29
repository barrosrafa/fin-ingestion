-- fct_fii_snapshot_mensal.sql
-- Fato mensal de métricas de Fundos Imobiliários

select
    ticker,
    data_referencia,
    cnpj_fundo,
    razao_social,
    gestora,
    patrimonio_liquido,
    vacancia_fisica_pct,
    vacancia_financeira_pct,
    total_imoveis,
    brick_score_global,
    score_imobiliario,
    score_financeiro,
    score_inquilinos,
    score_gestao,
    score_credito,
    score_preco,
    walt_anos,
    concentracao_top5_pct,
    contratos_atipicos_pct
from {{ ref('int_fii_metricas_consolidadas') }}
