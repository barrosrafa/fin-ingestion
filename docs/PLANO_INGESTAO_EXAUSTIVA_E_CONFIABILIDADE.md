# Plano de desenvolvimento da ingestão exaustiva e da execução confiável

Data: 31/10/2026. Base: anexo “Plano Arquitetural e Engenharia de Ingestão
Exaustiva”, código local de fin-ingestion, fin-db, consulta somente leitura ao
PostgreSQL configurado e histórico público do GitHub Actions.

## 1. Resultado esperado e limites da garantia

Cada ocorrência prevista deve existir como trabalho durável, com referência,
origem, universo e versão do contrato explícitos. Falhas transitórias devem
reagendar a mesma ocorrência; quedas de processos devem permitir recuperação.
Sucesso significa persistência validada, auditoria e publicação do conjunto
previsto, incluindo os testes dbt. A execução não pode ficar verde por ter pulado
uma janela ou executado apenas parte do universo.

GitHub Actions cron não garante início no minuto solicitado: o GitHub documenta
atrasos e possível descarte de disparos. Para horários operacionais previsíveis,
adotar scheduler e workers em host sempre disponível, supervisionados, com fila
PostgreSQL e verificação externa de ausência de execução. Actions fica com CI,
deploy, operação manual e contingência. Mesmo nessa arquitetura, indisponibilidade
do host, banco ou fonte impede prometer execução pontual absoluta. A garantia
implementável é recuperação, ausência de perda silenciosa e SLO mensurável.

Proposta de SLO, a validar antes do rollout: 99,5% das ocorrências batch iniciadas
até 5 minutos após o horário devido quando as dependências estão disponíveis;
100% das ocorrências vencidas contabilizadas como concluídas, pendentes ou falhas
visíveis; alerta externo após 10 minutos sem heartbeat. Frescor do dado depende
da publicação da fonte e tem SLO próprio. Intraday não recupera cotações históricas
fazendo uma consulta atual e atribuindo-lhe um horário antigo.

## 2. Diagnóstico comprovado

1. O banco configurado é Neon com endpoint pooled; respondeu à consulta nesta
   investigação. PostgreSQL **18.6** e extensão `timescaledb` presentes. Nenhuma
   configuração de suspensão foi alterada. Uma conexão bem-sucedida agora não
   comprova o estado do compute durante os disparos antigos.
2. A execução [41149308698](https://github.com/barrosrafa/fin-ingestion/actions/runs/41149308698)
   foi criada em 31/10 às 00:16:57 UTC, **30/10 às 21:16:57 BRT**. Seu log identifica
   o fechamento então previsto para 18h BRT; a coleta terminou às 21:17:56 BRT,
   com **4.780 cotações gravadas**. Há atraso de criação de aproximadamente
   **3h16min**, anterior à conexão da ingestão. O banco suspenso não explica essa
   demora na criação do workflow.
3. Os três workflows constam ativos. O runtime novo foi publicado em 31/10,
   depois do horário de sua agenda de sábado; a execução manual
   [41218211733](https://github.com/barrosrafa/fin-ingestion/actions/runs/41218211733)
   consta bem-sucedida. Isso não comprova pontualidade futura do cron.
4. `scheduled-ingestion.yml` fazia operações dependerem de toda a suíte de testes.
   A CI já pertence à promoção da release, e essa dependência adicionava atraso
   e permitia que falhas da infraestrutura de testes bloqueassem operações.
5. `engine-schedule.yml`, `deploy-engine.yml` e a manutenção usam o grupo
   `fin-ingestion-operations`. O grupo mantém execução corrente e espera limitada;
   vários disparos, inclusive intraday, disputam esse grupo. `cancel-in-progress:
   false` não transforma essa espera numa fila durável de todas as ocorrências.
6. `production.py` executa perfis diretamente. Não cria ocorrência durável antes
   de coletar. O mutex Redis evita sobreposição, mas não recupera disparos ausentes.
   Resultado fora da janela é `SKIPPED_OUTSIDE_WINDOW`; isso não prova coleta.
7. O scheduler PostgreSQL existente usa apenas a última ocorrência do dia atual;
   perde horários anteriores e dias inteiros após parada. Os perfis novos
   `scheduled_*` estão desativados, conforme o desenho de deploy atual.
8. Há nove políticas legadas ativas e uma tarefa `PENDING` na consulta realizada.
   Política ativa não comprova que exista um processo scheduler/worker operando.
   As execuções auditadas mais recentes eram canários `github:*` bem-sucedidos.

Não foi obtido acesso administrativo autenticado às variáveis/secrets de Actions
ou ao painel Neon. O conector GitHub informa leitura, sem push; `gh` não está
instalado no ambiente. O histórico público foi consultado pela API como fallback.

## 3. Ajustes necessários no anexo

| Proposta do anexo | Decisão de engenharia |
|---|---|
| Criar novamente `canonical.instruments`, `raw.payload_landing`, `market.ohlcv_daily` e fila pública | Reutilizar `core.ativo`, `raw.payload`, `market.cotacao_diaria`, `canonical.nominal_bar` e `ingestion.ingestion_task`. Criar estrutura paralela somente com contrato de migração e consumidores definidos. |
| `registry/`, `pipeline/`, `collectors/rea/`, `storage/db.py` existentes | Não existem com esses caminhos no checkout. Os diretórios reais são `pipelines/`, `collectors/sites/`, `dw/`, `queue/` e `core/`. O `registry/` será novo. |
| Todo ticker terminado em 11 vira FII | Resolver via cadastro oficial; sem evidência, manter pendente. Units, ETFs, FIIs e FIAGRO compartilham o padrão. |
| Ticker global único | Identificar instrumento por mercado/bolsa + código e vínculos ISIN/CNPJ/CIK com vigência. |
| Evento único por valor por ação | Usar ID estável da origem e revisões; uma correção do valor não deve criar um segundo evento econômico. |
| Repetir indefinidamente dentro do job | Workers contínuos, tentativas limitadas por execução e retry durável. Erro permanente vai para quarentena visível. Não prender Actions por tempo ilimitado. |
| Mapear tokens WAF para evitar bloqueios | Usar APIs autorizadas, sessão legítima quando permitida, cadência e tratamento de 403/429. Bloqueio persistente exige revisão da fonte, sem contornar controles. |
| dbt incremental em todos os modelos | Primeiro provar grão e correções tardias. Incremental com janela de revisão e chave explícita somente onde houver benefício. |

## 4. Correções imediatas preparadas neste trabalho

**Implementadas localmente, ainda sem publicação/deploy:**

| Arquivo em fin-ingestion | Alteração |
|---|---|
| `fin_ingestion/core/database.py` (novo) | `create_pool_with_retry`: seis tentativas de conexão, timeout de 20s por tentativa, backoff exponencial com jitter até 20s, sem imprimir DSN. Erros de autenticação falham imediatamente. `check_database_ready` abre sessão e executa `SELECT 1`. |
| `fin_ingestion/dw/dw_writer.py` | Usa o helper mantendo o lock de inicialização do pool. |
| `fin_ingestion/core/raw_sink.py` | Mesmo helper para o Bronze PostgreSQL. |
| `fin_ingestion/operations/database_ready.py` (novo) | Entrada de preflight somente leitura, exit code não zero se o banco não responder. |
| `.github/workflows/scheduled-ingestion.yml` | Testes executam em push/PR; operação deixa de esperar a suíte. Adicionado preflight de banco de até cinco minutos. Ação/cron desconhecido falham explicitamente. |
| `.github/workflows/{scheduled-ingestion,engine-schedule,deploy-engine}.yml` | `queue: max` no grupo compartilhado permite até 100 execuções pendentes, evitando a substituição padrão de uma pendente pela seguinte. Além desse limite ainda há descarte; não substitui fila durável. |
| `tests/test_database_readiness.py` (novo) | Recuperação de compute indisponível, esgotamento de seis tentativas, falha imediata de senha e fechamento do pool em erro de consulta. |

O runtime `production.py` já usa `DWWriter`, portanto recebe o retry quando a
release corrigida for promovida. O checkout operacional usa `ENGINE_RELEASE_SHA`:
publicar apenas o YAML não atualiza o código pinado. Não há retry automático de
escrita arbitrária após commit incerto. A recuperação transacional fica na fila.

Essas correções reduzem falhas de conexão e atraso interno. **Não implementam
sozinhas recuperação de cron perdido, scheduler independente ou alerta externo.**

## 5. Sequência de implementação por arquivo

### Etapa 0 — Inventário e contrato de operação (P0)

1. Criar `fin_ingestion/operations/audit_schedule.py`: consultar workflows/runs/jobs,
   comparar `scheduled_at`, criação, início do job, primeiro acesso ao banco e
   conclusão. Obter variáveis `ENGINE_ENABLED`, `ENGINE_RELEASE_SHA` e situação
   das políticas com credencial administrativa, sem registrar valores de secrets.
2. Acrescentar `docs/evidence/schedule-audit-YYYY-MM-DD.json`, sanitizado, com
   ocorrências esperadas e observadas. A confirmação de sucesso deve ler o
   recibo/dados, além da conclusão verde de Actions.
3. Em `docs/DEPLOY_ENGINE.md`, declarar um único dono para cada dataset:
   scheduler persistente ou Actions durante a transição. Mapear as nove políticas
   legadas, seis políticas novas e perfis do runtime para eliminar duplicidade.
4. Consolidar horário em `America/Sao_Paulo`. O checkout atual de manutenção usa
   06h CVM, 12h FII, **19h** fechamento e domingo 20h reconciliação. O anexo fala
   em 18h fechamento e 20h diário; não substituir silenciosamente a agenda atual.
   A proposta inicial mantém 19h para EOD e torna reconciliação diária às 20h,
   após validar disponibilidade do arquivo B3. PTAX/Tesouro mantêm horários
   detalhados em `DEPLOY_ENGINE.md`.

Aceite: tabela única dataset → dono → cron → calendário → SLO → universo;
nenhum dataset produzido simultaneamente por dois agendadores.

### Etapa 1 — Conexão, retomada do compute e release (P0)

1. Promover as correções da seção 4 após CI. Executar o preflight também no deploy
   antes do script de migrações; o script atual tem conexão própria e precisa
   receber o helper separadamente.
2. Alterar `scripts/apply_engine_migrations.py`, quaisquer conexões diretas em
   `pipelines/status_invest_etl.py` e novos pontos encontrados por `rg create_pool`
   para usar política central, preservando as necessidades de cada transação.
3. Em `dbt/profiles.yml`, configurar timeout de conexão/retries compatíveis com
   a versão travada de dbt-postgres e manter TLS. Homologar com indisponibilidade
   transitória real em ambiente isolado; não inferir os nomes de opções do adapter.
4. Adicionar métricas de tempo de conexão e tentativa. Registrar tipo de erro,
   nunca URL/senha. Uma indisponibilidade persistente deve falhar visivelmente.
5. Neon scale-to-zero retoma ao receber conexão. Manter essa função é compatível
   com ingestão batch com retries; para SLO de latência estrito avaliar compute
   sempre ativo no plano disponível. Essa alteração pode ter custo e deve ser
   uma decisão explícita de infraestrutura, sem ping artificial permanente.

Aceite: primeira conexão após suspensão funciona no prazo configurado; senha
incorreta não fica em loop; falha prolongada não aparece como sucesso.

### Etapa 2 — Ocorrências duráveis e outbox (P0)

No repositório fin-db, criar migration aditiva
`sql/21_ingestion_schedule_reliability/01_occurrences_and_outbox.sql`. A raiz
principal local desse repositório é `/Users/rafaelaparecido/fin`; há também outro
checkout em `/Users/rafaelaparecido/fin/fin-db`. Escolher um checkout de trabalho
e publicar uma única migration; não editar os dois como fontes independentes.
Não alterar checksums de estágios já aplicados.

Estrutura proposta, a ajustar ao DDL final:

```sql
CREATE TABLE ingestion.schedule_occurrence (
    occurrence_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id varchar(50) NOT NULL
        REFERENCES ingestion.ingestion_policy(policy_id),
    scheduled_at timestamptz NOT NULL,
    partition_key text NOT NULL,
    contract_version text NOT NULL,
    reference_date date NOT NULL,
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','EXPIRED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (policy_id, scheduled_at, partition_key, contract_version)
);
-- Relacionar ingestion_task à ocorrência por FK e índice, preservando legado.
-- Criar schedule_cursor e dispatch_outbox com chave única por ocorrência/destino.
```

1. `schedule_cursor`: último intervalo completamente materializado por política;
   não confundir esse cursor com watermark do dado publicado.
2. `dispatch_outbox`: solicitação durável de dispatch com `next_attempt_at`,
   tentativas, lease e resposta remota. Necessária quando o executor for Actions.
3. Criar índice de tarefas por ocorrência e estado; parametrizar prazo máximo
   de retry e expiração intraday. Manter tarefas e falhas para investigação.
4. Criar `fin_ingestion/scheduler/occurrences.py`: funções puras de cron,
   calendário, referência e partição; testes sem rede com relógio injetado.
5. Alterar `scheduler/scheduler_service.py`: bloquear cursor da política em
   transação; enumerar todas as ocorrências desde o cursor, em páginas limitadas;
   inserir ocorrência e tarefas com `ON CONFLICT DO NOTHING`; avançar cursor
   **na mesma transação**. Se cair, tudo reverte ou tudo permanece recuperável.
6. Recuperar batch de até sete dias por ciclo em páginas, deixando o restante
   explícito para backfill. A primeira execução recebe `bootstrap_from` explícito,
   evitando tanto perder histórico quanto disparar anos de tarefas sem limite.
7. Intraday vencido vira `EXPIRED` com motivo e métrica; recuperar apenas por
   provedor histórico autorizado, com timestamp real e rota própria.

Contrato de implementação:

```python
async with conn.transaction():
    cursor = await lock_policy_cursor(conn, policy_id)
    for slot in due_slots(cursor, now, max_slots=100):
        occurrence = await upsert_occurrence(conn, policy, slot)
        await enqueue_occurrence_tasks(conn, occurrence, universe)
    await advance_cursor(conn, last_fully_materialized_slot)
```

As funções acima são propostas novas. Não usar `PostgresTaskQueue.enqueue_task`
com outra conexão dentro dessa transação: adicionar parâmetro `connection` ou
repositório transacional para garantir atomicidade real.

Aceite: parar por dois dias, reiniciar e recuperar todas as ocorrências batch
previstas uma única vez; duas instâncias scheduler não duplicam tarefas; queda
antes/depois do commit não cria buracos no cursor.

### Etapa 3 — Worker, lease e confirmação correta (P0)

Arquivos: `queue/task_queue.py`, `queue/reconciler.py`,
`dispatcher/task_dispatcher.py`, `workers/runner.py`,
`pipelines/provider_pipeline.py`, `pipelines/market_gateway.py` e
`operations/production.py`.

1. Roteamento deve carregar referência, série SGS, ano CVM, CNPJ/ticker e versão
   do universo da tarefa. Hoje nem todos os parâmetros do runtime direto estão
   representados na tarefa; ampliar contrato antes de transferir produção à fila.
2. `claim_task`: continuar com `FOR UPDATE SKIP LOCKED`, reserva e alteração de
   estado atômicas; copiar `max_retries` da política para cada nova tarefa.
3. Adicionar `renew_lease(task_id, lock_token)`: heartbeat a cada 30s, com TTL
   inicial de 300s e renovação condicionada a token, estado e lease vigente.
   Interromper processamento/promoção se perder o lease.
4. `complete_task` legado hoje verifica token, mas não prazo/estado. Endurecer a
   condição ou remover chamadas desse caminho em favor de `finish_success`.
5. `finish_success` já confirma auditoria/checkpoint/conclusão na mesma transação;
   os writers fazem seus próprios commits. Garantir efeito idempotente após queda
   entre esses commits e a confirmação da tarefa, com chave estável por fato.
   Onde for viável, aceitar a conexão transacional do worker nos writers.
6. Rejeitar lote incompleto, ativo não resolvido, zero inesperado e rejeições
   obrigatórias. O sucesso da extração não significa sucesso da ingestão.
7. Erros transitórios: backoff com jitter e `Retry-After`; configuração inválida,
   autenticação, contrato e bloqueio permanente: falha acionável/quarentena.
   Teto de crédito esgotado deve respeitar a próxima janela de orçamento.
8. Concluir ocorrência apenas quando todas as tarefas obrigatórias e o build
   Gold estiverem confirmados. Gold vira etapa durável própria, recuperável sem
   refazer downloads de CVM ou consumir novamente créditos comerciais.
9. Criar comando `run-once --time-budget-seconds ...`: consome lote limitado e
   devolve pendências à fila; supervisor contínuo chama o mesmo dispatcher.

Aceite: matar worker após UPSERT e antes de concluir não duplica fatos; lease
perdido não confirma; tarefa lenta renova; falha de Gold fica visível e recuperável.

### Etapa 4 — Execução supervisionada e contingência (P0)

1. `docker-compose.ingestion.yml`: manter scheduler e workers batch/intraday
   separados; acrescentar healthchecks de processo e progresso, shutdown gracioso
   e limites de recursos. `restart: unless-stopped` sozinho não detecta processo
   vivo que deixou de trabalhar. Validar no host Linux de destino.
2. Em fin-infra criar `ingestion/compose.production.yml`,
   `ingestion/systemd/fin-ingestion.service` e `ingestion/README.md`: release por
   SHA/digest homologado, segredo fora do Git, reinício automático, armazenamento
   Bronze persistente, rotação de logs, backup e runbook de recuperação.
3. Redis gerenciado compartilhado continua protegendo fontes/quotas. Não criar
   Redis separado por runner. PostgreSQL é a verdade da fila, Redis não é a única
   cópia das ocorrências ou do resultado do trabalho.
4. `.github/workflows/engine-schedule.yml`: após validar o host, retirar a agenda
   de produção direta e conservar operação manual/contingência. Enquanto houver
   transição, os triggers só materializam ocorrências idempotentes na fila;
   nenhum perfil deve depender de exclusividade do grupo global do GitHub.
5. `.github/workflows/scheduled-ingestion.yml`: migrar rotas legadas para tarefas
   com referência explícita. Manter CI em push/PR e operação manual observável.
6. `.github/workflows/deploy-engine.yml`: validar commit exato, migrações, canário,
   leitura de retorno e dbt; promover `ENGINE_RELEASE_SHA` apenas após sucesso.
   Não remover os gates de CI para simplificar o disparo operacional.
7. Se Actions for mantido como executor, criar `operations/dispatch_outbox.py` e
   workflow `queue-worker.yml`: scheduler externo envia `workflow_dispatch`,
   guarda intenção no outbox e verifica resultado. Timeout HTTP após aceitação
   pode gerar dispatch repetido; claim durável torna a repetição segura.
8. Monitor externo, hospedado fora do host worker e do GitHub scheduler, compara
   heartbeats e ocorrências devidas/concluídas. Canal de alerta é configurado no
   rollout; nenhuma mensagem externa é enviada automaticamente por este plano.

Aceite: demonstrar parada do host, recuperação, perda de trigger Actions,
dispatch duplicado, indisponibilidade de banco e alerta por heartbeat ausente.
Sem host/scheduler externo implantado, o requisito de pontualidade permanece aberto.

### Etapa 5 — Cadastro canônico e taxonomia multiativo (P1)

Criar `fin_ingestion/registry/{__init__,taxonomy,resolver,sync}.py` e
`tests/test_registry.py`. Alterar `pipelines/enrichment.py`,
`pipelines/status_invest_etl.py`, `collectors/official/b3.py` e
`collectors/official/cvm.py` para usar o resolvedor compartilhado.

1. Precedência: cadastro oficial vigente → identificador confirmado → metadado
   confiável → sintaxe como candidato. Regex não decide FII versus ETF versus unit.
2. Resolver resultado com `asset_id`, `asset_type`, `exchange`, `evidence`,
   `confidence` e `resolution_status`; ambiguidade não recebe classificação falsa.
3. Cobrir ações, units, BDRs, FIIs, FIAGRO, ETFs locais/globais, stocks, REITs,
   Tesouro, debêntures, índices e opções. Opções exigem série/vencimento oficial;
   regex usa `[A-X]`, sem incluir `|` como caractere aceito.
4. Não classificar toda sequência de duas letras/números como debênture. Preservar
   tickers globais com ponto/hífen quando a bolsa permitir. Guardar CNPJ/CIK/CUSIP
   como strings, preservando zeros; ISIN deve passar validação de formato/check digit.
5. fin-db: criar `sql/22_instrument_registry/01_identifiers.sql`, com alias,
   mercado, vigência e evidência; adaptar `core.tipo_ativo_enum` somente após
   inventariar leitores. Mudança de ticker não gera novo instrumento por acidente.
6. Sincronização CVM/B3 em tarefas particionadas, com snapshot/versionamento;
   CSV permanece como bootstrap auditável. Ausência num download parcial não
   desativa automaticamente um ativo.

Aceite: PETR4, SANB11, HGLG11, IVVB11, VGIA11, AAPL34, AAPL, O, VOO e NTN-B
resolvem com evidência; sufixo 11 sem cadastro fica pendente; bolsa diferente
não colide no mesmo ticker.

### Etapa 6 — Coleta exaustiva, Bronze e idempotência (P1)

Arquivos existentes: `core/http_client.py`, `core/base_collector.py`,
`core/distributed_controls.py`, `core/provider_budget.py`, `core/raw_sink.py`,
`core/local_archive.py`, `collectors/official/*.py`, `collectors/sites/*.py`,
`collectors/commercial/*.py`, `dw/silver_writer.py`, `dw/dw_writer.py`.

1. Criar `fin_ingestion/contracts/datasets.py`: para cada fonte declarar grão,
   schema, chave natural, calendário, paginação, universo, janela de revisão,
   persistência, freshness e critérios de completude. Criar manifest por partição.
2. Reutilizar retry/timeout/rate limiter existentes, evitando empilhar Tenacity
   em três camadas e multiplicar requisições. Contabilizar quota em toda tentativa.
3. 429/5xx/timeouts são transitórios; 403 persistente abre incidente. Preservar
   resposta bruta e diagnóstico sanitizado. Uma falha não autoriza fabricar preço,
   indicador, CNPJ ou fallback temporal.
4. B3: COTAHIST diário/anual com data real e metadado de versão; CVM: DFP/ITR,
   FII e fundos particionados por ano/competência, incluindo reapresentações;
   BCB: séries, PTAX e Focus com todas as páginas e identidades próprias;
   Tesouro/ANBIMA: instrumento, vencimento, referência e tipo de taxa preservados.
5. SEC: criar `collectors/official/sec.py` e rota correspondente em
   `pipelines/provider_pipeline.py`, com identificação do cliente, limites oficiais,
   accession/CIK, unidade, conceito e período. Stocks/REITs/ETFs globais dependem
   de cadastro; não inferir essas classes a partir do tamanho do ticker.
6. Para os 12 sites existentes, criar matriz de cobertura em
   `docs/REA_SPECIFICATION.md`: acesso autorizado, dados disponíveis, frequência,
   parser, contrato, limitações e fonte oficial equivalente. Fixture real sanitizada
   para cada layout; mudança de contrato vai para quarentena.
7. Bronze: separar captura de requisição de conteúdo. `raw.payload` hoje deduplica
   por batch/hash, portanto duas capturas em batches distintos não são dedupe global.
   Projetar blob por hash + manifesto de capturas, preservando a observabilidade
   de todas as tentativas; Bronze não deve perder histórico para parecer deduplicado.
8. Mover Bronze oficial do disco efêmero do runner para volume/object storage
   durável; guardar URI, hash e versão. Artefato Actions de sete dias não é retenção
   de auditoria suficiente para replay de longo prazo.
9. Cada writer declara chave `ON CONFLICT`: preço por ativo/sessão/origem/tipo;
   CVM por identidade fiscal completa e versão; evento por ID externo e revisão;
   FII por fundo/competência/versão; macro por série/data/unidade quando aplicável.
   Revisar valores nulos nas chaves e partições Timescale antes de criar índices.
10. Lote com ativos não resolvidos não pode simplesmente ignorá-los e reportar
    cobertura total. `DWWriter.write_daily_quotes` atualmente pula alguns registros
    sem ID; registrar rejeitados e exigir contabilidade `esperado = promovido +
    rejeitado explícito`, com falha quando a rejeição viola o contrato.

Aceite: reprocessar captura duas vezes preserva a mesma quantidade de fatos;
revisão atualiza a visão corrente e guarda histórico; todas as páginas e partições
têm manifest; fonte indisponível fica pendente/falha, nunca inventa sucesso.

### Etapa 7 — Qualidade, dbt, reconciliação e analytics (P1/P2)

1. `quality/market_schema.py`, `quality/validator.py`, `quality/temporal.py`:
   validar OHLC, preços positivos, volumes/unidades, UTC, calendário, referência
   e frescor. Regras variam por dataset; ausência não equivale a zero.
2. `dbt/models/staging/schema.yml`, `intermediate/schema.yml`, `marts/schema.yml`:
   testes de nulidade, identidade composta, relacionamentos e valores aceitos.
   Adicionar `dbt/tests/unique_daily_quote.sql` para o grão ticker/data:

   ```sql
   select ticker, data_pregao, count(*) as n
   from {{ ref('fct_cotacao_diaria') }}
   group by ticker, data_pregao
   having count(*) > 1
   ```

3. Estender os testes já existentes `scheduled_nominal_geometry.sql` e
   `scheduled_cvm_identity.sql`; comparar totais por fonte/partição com o manifest,
   não apenas unicidade de linhas que chegaram.
4. Executar `dbt build` na etapa durável Gold. O caminho legado `dbt-run` executa
   transformação, mas o contrato de sucesso deve incluir os testes obrigatórios.
5. `pipelines/reconciliation_pipeline.py` e `queue/reconciler.py`: distinguir
   lease vencido, fonte atrasada, registro rejeitado e partição ausente. Consultar
   calendário/publicação da fonte antes de tratar dado ausente como defeito.
6. `analytics/adjustments.py`, `valuation.py`, `health_scores.py`,
   `risk_metrics.py` e `real_estate.py`: documentar entradas/unidades, janelas,
   conhecimento disponível na data e tipos de ativos elegíveis. Não calcular
   Graham/Bazin indiscriminadamente para todo instrumento.
7. Revisar `ingestion.v_pipeline_health_summary`: joins simultâneos com tarefas,
   checkpoints e issues podem multiplicar contagens; pré-agregar antes dos joins.
   Health deve distinguir execução recente de dado realmente atualizado.

Aceite: duplicata e lote incompleto bloqueiam publicação; retorno causal usa só
eventos conhecidos na data; falha de teste mantém a ocorrência aberta/fracassada.

### Etapa 8 — Rollout e comprovação operacional (P0 final)

1. Criar release com CI do SHA exato, testes PostgreSQL/Redis reais e fixtures
   sem credenciais de produção. Aplicar migrations aditivas com ledger/checksum,
   advisory lock, timeouts e conferência posterior.
2. Executar canário por fonte e ler o resultado de volta do banco, validando
   quantidade, referências, identidade, ausência de duplicatas e dbt build.
3. Implantar host supervisionado, armazenamento e monitor externo. Homologar
   em shadow mode, sem segundo produtor gravando o mesmo dataset.
4. Definir instante de transferência por dataset; desativar seu cron antigo e
   ativar o novo dono, com cursor inicial explícito e fila preservada.
5. Durante cinco dias úteis, medir atraso por ocorrência, tempo até publicação,
   backlog, leases, retries, quotas, rejeições e espaço Bronze. Observar pelo
   menos uma execução agendada de cada frequência e simular recuperação.
6. Rollback: parar novos dispatches do dono novo, manter tarefas e payloads,
   restaurar release homologada e retomar ocorrências pendentes. Não reativar
   dois donos nem apagar ledger para contornar falha.
7. Registrar `docs/evidence/reliability-acceptance.json` com referências reais
   dos testes/execuções e exceções abertas. Somente então declarar o requisito
   operacional atendido dentro dos SLOs acordados.

## 6. Ordem dos PRs e estimativa inicial

| PR | Escopo | Dependências | Esforço indicativo |
|---|---|---|---|
| 1 | Correções imediatas, diagnóstico e plano | nenhuma | preparado localmente neste trabalho |
| 2 | Migration de ocorrências, cursor, outbox e parâmetros de tarefas | PR 1 | 2–4 dias |
| 3 | Catch-up, worker, heartbeat, retry e etapa Gold durável | PR 2 | 3–5 dias |
| 4 | Host supervisionado, monitor externo e transferência Actions | PR 3 + infraestrutura | 2–4 dias + observação |
| 5 | Registry oficial e taxonomia multiativo | contrato de schema | 3–5 dias |
| 6 | Manifest de cobertura, Bronze durável e auditoria de UPSERT | PRs 2/5 | 3–6 dias |
| 7 | SEC, ampliação de fontes e 12 adaptadores REA por contrato | credenciais/licenças/fixtures | dimensionar por fonte |
| 8 | dbt, reconciliação, analytics e expansão de universo | PRs 3/5/6 | 3–5 dias |

São estimativas de implementação, sem incluir contratação/credenciamento de
fontes. Encerrar P0 antes de expandir o volume. Não prometer ingestão de 100% de
todos os provedores sem universo enumerado, acesso disponível e critérios de
completude por partição.

## 7. Validação das correções locais

Executada em 31/10/2026:

- Suíte focada em conexão, runtime e contratos operacionais: **27 passaram**.
- Suíte completa: **169 passaram, 11 ignorados**. Os ignorados exigem pgserver
  ou `TEST_REDIS_URL` isolado, indisponíveis nesta sessão. Isso não é aprovação
  dos cenários de integração ignorados.
- `ruff check .`: aprovado; `git diff --check`: aprovado.
- Os três YAMLs foram parseados; blocos shell literais passaram em `bash -n`.
  Isso não substitui validação do workflow pelo serviço GitHub na publicação.
- `python -m fin_ingestion.operations.database_ready`: **Database session verified**
  no banco configurado, somente leitura. Não foi provocada suspensão do compute
  de produção para testar cold start; essa recuperação foi exercitada com mocks.

Não houve push, deploy, troca de `ENGINE_RELEASE_SHA`, alteração de plano Neon ou
implantação de scheduler/monitor externo neste trabalho. A garantia operacional
continua condicionada à conclusão e comprovação das etapas P0 acima.

## 8. Fontes e referências

- [GitHub: eventos schedule e limitações](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).
- [GitHub: concorrência de workflows](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency).
- [Neon: ciclo de vida do compute](https://neon.com/docs/introduction/compute-lifecycle).
- [Neon: erros de conexão](https://neon.com/docs/connect/connection-errors).
- Código e runbooks existentes: `docs/DEPLOY_ENGINE.md`,
  `docs/PLANO_EVOLUCAO_SCHEDULED_PIPELINE.md` e
  `docs/EXECUCAO_ENGINE_2026-10-30.md`.
