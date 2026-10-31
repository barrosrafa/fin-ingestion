# Evolução segura do fin-ingestion — plano e registro de execução

Data: 30/10/2026. Origem: relatório técnico anexado pelo usuário.

**Continuação executada:** o estado atual dos conectores, migrações aplicadas e canários
está em [EXECUCAO_ENGINE_2026-10-30.md](EXECUCAO_ENGINE_2026-10-30.md).
Os resultados de 146 testes e o escopo local registrados abaixo são da primeira etapa.

## Diagnóstico e decisões

O repositório já contém fila PostgreSQL com `SKIP LOCKED`, controles Redis compartilhados,
Bronze em `raw.payload`, tabelas canônicas e dbt. A implementação preserva esses contratos.
Não é necessário introduzir Airflow, Celery, Redis adicional ou outro banco para corrigir
as lacunas atuais. A suíte inicial passou: **97 testes, 2 ignorados**.

Correções ao relatório que devem preceder qualquer desenvolvimento:

- COTAHIST: posições 96–108 são PREMED; 109–121 são PREULT (último negócio),
  não uma oferta de fechamento separada. O parser existente está correto nesses campos.
  VOLTOT começa na posição 171. Fonte: [layout oficial B3](https://b3.com.br/data/files/C8/F3/08/B4/297BE410F816C9E492D828A8/SeriesHistoricas_Layout.pdf).
- Horários são políticas operacionais configuráveis, não garantias de publicação. O cron
  existente em `ingestion_policy` será interpretado em America/Sao_Paulo; o Actions usa UTC.
- ANBIMA Feed e dados comerciais internacionais exigem credenciais/contratos. Preços,
  créditos e permissões de redistribuição citados no relatório não são premissas verificadas.
- Bronze preserva os bytes originais; Parquet normalizado não substitui JSON/ZIP originais.
- Dados diários mantêm uma data de pregão. Só observações com hora real devem virar UTC;
  uma data isolada não deve ser convertida em um horário de negociação inventado.
- Ajustes por dividendos exigem convenção de retorno total, preço de referência e data ex
  verificados. A fórmula do relatório não será aplicada cegamente à base nominal.
- Chaves CVM precisam incluir escopo consolidado/individual, moeda, período e demonstração;
  CNPJ/conta/data/versão isolados podem colidir. Eventos exigem identidade da fonte/revisões,
  pois corrigir valor nominal não deve criar um segundo pagamento.

## Passo a passo e arquivos

| Etapa | Arquivo/código | Implementação e critério de aceite |
|---|---|---|
| 1 | `scheduler/scheduler_service.py`, `scheduler/market_calendar.py`, `pyproject.toml` | Ler `cron_expression`, calcular ocorrência vencida por política, respeitar dias úteis e usar chave por ocorrência UTC. Uma varredura não dispara antes do horário nem duplica a ocorrência. Interpretar cron em BRT; recuperar apenas a ocorrência mais recente no próprio dia. |
| 2 | `core/rate_limiter.py`, `core/distributed_controls.py`, `core/http_client.py` | Créditos ponderados locais/Redis, rejeitar custo maior que bucket, retry GET em 5xx com jitter, manter POST sem retry de status, interpretar Retry-After numérico/data. 429 retorna à fila sem loop agressivo. |
| 3 | `collectors/official/bcb.py` | Histórico por datas em janelas conservadoras de 3 anos, sem sobreposição, com ordenação e deduplicação. Validar parâmetros e payloads; preservar rota `ultimos` existente. |
| 4 | `quality/validator.py`, `quality/market_schema.py`, `dw/dw_writer.py` | Pandera para lotes OHLCV completos; validar também candles parciais, finitude, positividade, volumes e data no writer. Rejeitar lote inválido antes de conexão/escrita. Preservar ausências e volume zero. |
| 5 | `quality/temporal.py`, `dw/dw_writer.py` | UTC explícito para timestamps; rejeitar data isolada/horário inválido; nenhuma substituição silenciosa por agora. `ffill` apenas em função analítica explícita, agrupada, ordenada e limitada a 3. |
| 6 | `core/raw_sink.py`, `core/base_collector.py`, `config.py` | Bronze local opcional por SHA-256, escrita atômica, verificação de hash e replay offline. Preservar bytes, manter backend PostgreSQL padrão e não modificar tabelas remotas. |
| 7 | `collectors/official/b3.py`, testes | Validar registro de 245 caracteres; manter PREMED/PREULT corretos e provar com valores distintos. Preservar ZIP antes do parser no caminho diário. |
| 8 | `.github/workflows/scheduled-ingestion.yml`, README | Fechamento às 19h BRT, CI com lock congelado, concorrência operacional sem cancelamento e parâmetros manuais passados por ambiente. Testes isolados sem custos ou acesso a produção. |
| 9 | `tests/test_scheduled_pipeline.py`, documentos | Exercitar horários, falhas, créditos, janelas, dados inválidos, replay e UTC; suíte completa, lint dos arquivos alterados e validação do lock. Registrar resultados e limites. |

Durante a execução, a inspeção mostrou dois caminhos adicionais que precisavam de
correção para atender à segurança do fluxo:

- `pipelines/result.py`, `dispatcher/task_dispatcher.py`, `workers/base_worker.py` e
  `core/base_collector.py`: propagar Retry-After, impedir sucesso/checkpoint após resultado
  vazio ou parcial, serializar erros como JSON válido e compartilhar o despacho entre
  worker e CLI. `daily_market.py` fecha os clientes em `finally` mesmo quando falha.
- `collectors/official/anbima.py` e `pipelines/analise_de_acoes_pipeline.py`: remover
  retornos de preços de exemplo e taxas PTAX padrão. ANBIMA aceita somente payload
  explicitamente fornecido para parsing offline até existir integração autenticada;
  o writer PTAX exige taxas reais explícitas. Políticas intraday não são enfileiradas
  e tarefas intraday manuais falham até haver conector dedicado.

## Continuação por domínio, dependências e critérios de promoção

As etapas abaixo cobrem a evolução restante do relatório. Não devem ser confundidas com
integrações implantadas: exigem contratos externos, desenho de identidade ou migrações
em repositórios responsáveis pelo banco/infraestrutura.

1. **Gateway de preços B3 comercial.** Criar `collectors/commercial/brapi.py` e
   `pipelines/market_gateway.py`; configurar token como segredo, tamanho de lote conforme
   contrato e orçamento mensal. Conferir [OpenAPI oficial](https://api.brapi.dev/docs).
   Normalizar resultado bruto e registrar proveniência, atraso e referência. Fallback
   COTAHIST somente para EOD da mesma sessão; nunca anunciar EOD como intraday.
   Aceite: testes de contrato, 401/403 sem fallback de credencial, 429 com reprogramação,
   5xx com circuito, cobertura parcial e nenhuma mistura entre preço bruto e ajustado.
2. **PTAX/Focus e Tesouro.** Evoluir `pipelines/analise_de_acoes_pipeline.py` (PTAX já
   existente), criar `collectors/official/olinda.py` e `tesouro.py`. Paginar OData com
   limites, preservar horário/boletim, moeda e data de publicação. Resolver recurso CKAN
   oficial em vez de fixar URL volátil. Rotas no `dispatcher/task_dispatcher.py` e
   `pipelines/orchestrator.py`; marts separados para taxas/PU e macro.
   Aceite: fixtures oficiais, correções/revisões, CSV decimal brasileiro e lote vazio falho.
3. **ANBIMA Feed.** Evoluir `collectors/official/anbima.py`, manter fonte pública distinta
   do Feed autenticado. Implementar renovação de token, cota, calendário e persistência
   por ISIN/data/origem; políticas após 20h somente quando o produto estiver habilitado.
   Aceite: homologação do contrato, credencial expirada, deduplicação e ausência de dados.
4. **CVM.** Evoluir `collectors/official/cvm.py`, `pipelines/quarterly_financials.py`,
   modelos `dbt/models/staging/stg_cvm_demonstrativos.sql` e intermediate. Preservar ZIP,
   codificação e versão; reconciliar CNPJ com cadastro oficial. Criar migração no fin-db
   para a identidade completa e observar retificações sem perda de versões.
   Aceite: duas demonstrações com mesma conta/data não colidem; revisão é auditável.
5. **Idempotência intraday e eventos.** O DDL atual de `market.cotacao_intraday` possui
   índice não único. No fin-db, auditar duplicatas e criar migração com identidade por
   instrumento/timestamp/origem/tipo (snapshot ou negócio). Somente depois trocar todos
   os escritores, inclusive `pipelines/status_invest_etl.py`, para UPSERT. Não executar
   DELETE de duplicatas automaticamente. Para eventos, definir identificador estável e
   revisão antes de alterar `historical_backfill.py` e `fct_provento.sql`.
6. **Ouro ajustado e causalidade.** Evoluir `historical_backfill.py`, `analytics/technical.py`,
   `dbt/models/intermediate/int_cotacoes_consolidadas.sql` e `fct_cotacao_diaria.sql`.
   Inventariar séries adjusted/raw existentes e política point-in-time. Implementar ajuste
   apenas em consulta/Ouro; não sobrescrever nominal. Aceite: split 1:10, dividendos,
   preços de referência ausentes e replay em data histórica sem informação futura.
7. **Internacional.** Criar conectores autenticados Massive/FMP/Twelve Data sob
   `collectors/commercial/`; validar docs e contratos vigentes, moedas, sessões, DST,
   créditos por endpoint e cobertura. Ligar ao orquestrador após homologação. Cobertura,
   orçamento e licença são pré-condições; nenhum plano pago é contratado automaticamente.
8. **Produção/escala.** Medir p95, atraso por dataset, rejeições, quota e backlog via
   `ingestion_run/issue/checkpoint`; separar workers por criticidade no fin-infra. Migrar
   para Airflow/Dagster/Prefect, S3, UP2DATA mTLS, streaming ou ClickHouse apenas com
   volume/SLO que justifique. Manter retenção/replay e política de credenciais em segredo.
   Validar calendário B3 anual oficial antes de habilitar intraday em produção.

## Execução e reversão

Desenvolver e testar localmente sem usar `.env` para acessar banco/Redis externos. Não
disparar ingestões reais, migrações remotas ou workflows pagos nesta mudança. A promoção
requer homologação do schema, calendários e contratos, seguida de uma execução canário
com poucos ativos e verificação de contagens/rejeições antes de ampliar o universo.

Rollback: reverter o código e o lock juntos. O backend raw padrão continua PostgreSQL;
o local é opt-in. As novas chaves por ocorrência não apagam tarefas antigas. Antes de
ativar o scheduler atualizado, drenar/inspecionar tarefas antigas do dia para evitar que
uma tarefa legada e a nova ocorrência executem a mesma coleta na transição.

Revisar também políticas antigas antes do rollout: por exemplo, o cron CVM `2-6`
(terça a sábado) conflita com `BUSINESS_DAYS`, que permite somente segunda a sexta.
Não alteramos essas políticas no banco remoto. GitHub Actions é uma via de execução
independente: não habilitar os mesmos datasets simultaneamente no Actions e no scheduler
sem um mecanismo comum de exclusão. `concurrency` do Actions serializa os jobs desse
workflow; não é um lock global contra os containers do fin-infra.

## Registro de validação

**Implementação local concluída para as etapas estruturais 1–9**, incluindo as correções
adicionais identificadas na inspeção. As oito evoluções por domínio da seção anterior
continuam planejadas e não são apresentadas como conectores implantados ou homologados.

Validação final desta execução:

- `uv run --frozen fin-ingestion raw-replay --help`: comando disponível e contrato CLI válido.
- `.venv/bin/pytest -q`: **146 passed, 2 skipped**, em 2,78 s; baseline era 97 passed,
  2 skipped. Os 49 testes novos cobrem o caminho local com mocks e arquivos temporários.
- `.venv/bin/ruff check .`: **All checks passed**. Quatro achados preexistentes foram
  resolvidos com limpeza mecânica de import, nome de variável e espaço em branco nos
  arquivos `historical_backfill.py` e `publishers/`.
- `uv lock --check`: dependências resolvidas e lock consistente (croniter e Pandera).
- Workflow carregado como YAML; todos os blocos shell validados com `bash -n`.
- Sintaxe Python verificada com AST; `git diff --check` sem erros.
- Dois testes que exigem `TEST_REDIS_URL` ficaram ignorados. Não há comprovação de
  integração contra Redis ou PostgreSQL reais nesta execução; os controles ponderados
  foram verificados por testes locais/mocks e precisam de homologação distribuída.

Nenhuma migração, ingestão remota, contratação, push ou implantação foi realizada.
Os passos de promoção permanecem: homologar banco e Redis isolados, revisar calendário
e políticas, drenar tarefas legadas, escolher um único mecanismo de agendamento por
dataset e executar canário antes de ampliar a carga. Os arquivos estão alterados no
working tree do `fin-ingestion`, disponíveis para revisão.
