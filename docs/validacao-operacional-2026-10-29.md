# Validação operacional — 29/10/2026

Validação por execuções manuais do GitHub Actions e consultas ao PostgreSQL. Horários em Brasília (UTC−3). Código funcional final: `9f3e97e`.

## Execuções verificadas

| Rotina | Resultado observado | Evidência |
|---|---|---|
| backfill_stage1 + analytics + dbt | 150/150 ativos; zero falhas; 487.390 cotações, 7.331 eventos de proventos e 224 splits processados | [Actions 40952847145](https://github.com/barrosrafa/fin-ingestion/actions/runs/40952847145) |
| daily_market + analytics + dbt | 5.549 cotações processadas; nenhum ticker ou fonte com falha; 34 modelos e 29 testes dbt aprovados | [Actions 40953047146](https://github.com/barrosrafa/fin-ingestion/actions/runs/40953047146) |
| monthly_fii | 1.428 informes oficiais da CVM, última referência/versão por fundo ou classe | [Actions 40952461119](https://github.com/barrosrafa/fin-ingestion/actions/runs/40952461119) |
| quarterly_cvm | 2.678 registros do cadastro oficial de companhias abertas | [Actions 40952468227](https://github.com/barrosrafa/fin-ingestion/actions/runs/40952468227) |
| macro_bcb | 228 registros coletados | [Actions 40950317750](https://github.com/barrosrafa/fin-ingestion/actions/runs/40950317750) |
| queue_reconcile | Concluído sem erros; nenhuma tarefa zumbi a recuperar | [Actions 40950311272](https://github.com/barrosrafa/fin-ingestion/actions/runs/40950311272) |
| enrich_metadata | 100 ativos enriquecidos; zero falhas | [Actions 40951617371](https://github.com/barrosrafa/fin-ingestion/actions/runs/40951617371) |

A execução final de fechamento passou também nos **97 testes Python**; dois testes de integração que exigem Redis isolado foram ignorados. O workflow verifica conectividade TLS e proteção atômica no Redis de produção antes das operações. Os logs e `run_results.json` estão no artefato `dbt-results-40953047146` da execução final.

Os contadores de backfill e fechamento representam registros processados por upsert, não necessariamente registros novos. Foram realizadas várias cargas para conferir a repetição idempotente.

## Conferência no banco após o fechamento

- Universo stage1: 76 ações e 74 FIIs, todos com última cotação em **29/10/2026**.
- `public_dw.fct_cotacao_diaria`: **822.824 linhas**, **1.239 ativos**, datas de **03/01/2000 a 29/10/2026**.
- `public_dw.fct_provento`: **24.178 eventos**.
- `public_dw.dim_ativo`: **1.378 ativos**.
- Landing mensal CVM: **1.428 informes**, dos quais **252 registros** foram associados a **241 tickers** por identificadores oficiais.
- `public_dw.fct_fii_snapshot_mensal`: **241 linhas para 241 tickers**, referência **setembro/2026**. Seleção da última referência/versão por ticker impede multiplicação de classes e versões.
- Os testes dbt conferem as contagens dos fatos contra `market.cotacao_diaria` e `market.provento`, além de integridade das chaves e unicidade do snapshot corrente.

## Correções aplicadas

- Parâmetro incompatível no pipeline mensal e ausência do ponto de execução da rotina macro.
- Erros dbt ocultados; instalação de dependências ausente no runner limpo; modelos com referências/colunas incompatíveis com o banco.
- Seleção stage1 substituída por componentes atuais do IBOV/IFIX; universo diário limitado aos instrumentos efetivamente negociados no último arquivo spot oficial da B3.
- Fonte pública de cotações com alternativa oficial B3; fechamento sem cotações e tickers sem recuperação agora retornam erro.
- Inicialização concorrente dos pools protegida por lock; concorrência de coletores limitada.
- Remoção de valores de exemplo automáticos de oito coletores secundários. Rotinas agendadas usam fontes oficiais e o feed histórico; coletores secundários ficam opcionais.
- Coleta e transformação dos informes oficiais de FIIs da CVM, associação por ISIN e preenchimento apenas dos ISINs ausentes no cadastro.
- Dimensões e fatos de cotações/proventos ligados às tabelas canônicas do backfill.
- Registros brutos sem campos obrigatórios preservados em `public_staging.stg_rejected_records`; a landing original permanece íntegra.
- A taxa de administração não é apresentada como TER total quando o custo total não é fornecido.

## Horários e limites da validação

O workflow está ativo, com cron em UTC equivalente a:

- Segunda a sexta, 06h: cadastro/descoberta CVM.
- Segunda a sexta, 12h: informes mensais CVM de FIIs.
- Segunda a sexta, 18h: fechamento, analytics e dbt.
- Domingo, 20h: reconciliação e enriquecimento.

**Execuções manuais aprovadas não comprovam pontualidade futura do cron.** Houve atrasos de várias horas nas execuções agendadas anteriores; o GitHub Actions não oferece garantia de início no minuto configurado. A configuração dos horários foi conferida, mas essa limitação da plataforma permanece.

**A carga de 30 anos completos ainda não está atendida.** A rotina carrega o máximo disponível no feed atual, cuja primeira cotação observada é de 03/01/2000. Para os FIIs do stage1, o histórico observado começa em 17/02/2009. A data da primeira cotação disponível não comprova IPO. Cobrir 1996–1999 exige integrar a série histórica oficial, com identificação de códigos antigos e tratamento consistente de fatores de cotação e eventos corporativos; isso não foi certificado por esta execução.

A ação `quarterly_cvm` valida descoberta/cadastro de companhias. Ela não certifica uma carga integral de todos os demonstrativos DFP/ITR. Informes CVM sem associação segura ao cadastro permanecem disponíveis no staging, sem criar tickers por aproximação.
