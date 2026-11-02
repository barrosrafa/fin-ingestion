# Deploy da engine no GitHub Actions

Destino: `barrosrafa/fin-ingestion`, ambiente `production-ingestion`. O runtime usa
PostgreSQL/Redis gerenciados existentes; não depende de Docker Desktop neste computador.
O Compose continua sendo uma alternativa para um host Docker administrável.

## Release e ativação

1. Publicar o DDL em fin-db e o código em fin-ingestion. O DDL atual fica no estágio
   `20_scheduled_engine_v4.5` do fin-db, compatível com sua política schema-only.
   O histórico aplicado da engine mantém os identificadores/checksums originais do
   estágio 17; `tests/schema_contract` é o contrato imutável usado pelo deploy.
   A confiabilidade do runtime usa a migration aditiva
   `21_ingestion_schedule_reliability/01_runtime_ledger.sql`, empacotada em
   `migrations/` com checksum e grants explícitos ao papel de ingestão.
   O runner exige `system.schema_migrations` e falha antes de abrir transação se
   esse ledger estiver ausente; reconcilie o schema-base antes de reaplicar runtime.
2. Exigir aprovação da CI do commit exato: testes reais PostgreSQL/Redis, Ruff e dbt.
3. Executar `Deploy scheduled engine` em `main`. A referência opcional identifica o
   canário histórico; a carga preserva a data real de cada fonte e não a transforma
   em cotação corrente. Migração, fontes oficiais, brapi e Ouro precisam passar.
4. Somente depois do recibo `SUCCEEDED`, atualizar as variáveis de repositório
   `ENGINE_RELEASE_SHA` com o SHA validado e `ENGINE_ENABLED=true`.
5. Habilitar `ENGINE_INTRADAY_ENABLED=true` com universo explícito em
   `ENGINE_INTRADAY_TICKERS`. A janela de mercado ainda é obrigatória; fora dela não
   há chamadas de cotação. Uma cotação defasada é rejeitada, sem substituição por EOD.

Os secrets são `DATABASE_URL`, `DATABASE_URL_ASYNC`, `REDIS_URL` (TLS) e `BRAPI_API_KEY`.
Nenhum deles é versionado. O teto compartilhado de brapi é configurado por
`PROVIDER_MONTHLY_CREDITS`; a configuração inicial é `{"brapi":1000}`.
Os provedores sem credenciais não são habilitados por estes workflows.

## Agenda BRT

| Perfil | Agenda solicitada | Escopo inicial |
|---|---|---|
| morning | seg–sex 09:00 | Selic, meta Selic, CDI, IPCA e IGP-M; Focus às segundas |
| ptax | seg–sex 10:00, 11:00, 12:00, 13:00 e 13:30 | Boletins oficiais com data/hora próprias |
| treasury | seg–sex 09:30 e 18:00 | Última referência publicada até a data solicitada |
| cvm | sábado 08:00 | DFP do ano anterior e ITR corrente; universo `ENGINE_CVM_CNPJS` |
| intraday | seg–sex, a cada 15 minutos na janela B3 | PETR4 inicialmente; frescor máximo 20 minutos |
| discovery | seg–sex 06:00 | Cadastro CVM oficial |
| monthly | seg–sex 12:00 | Informes mensais FII; Bronze PostgreSQL e teste Gold |
| daily | seg–sex 19:00 | EOD oficial da mesma sessão, analytics e teste Gold |
| reconcile | todos os dias 20:00 | Recuperação de leases da fila legada |

O universo inicial CVM é Petrobras (`33000167000101`), explicitamente configurado em
`ENGINE_CVM_CNPJS`. A ampliação é feita nessa variável após conferir cobertura e recursos.
O polling de 15 minutos é compatível com o teto inicial e o feed disponível; não é
streaming de mercado. O GitHub pode atrasar o início das execuções.

Os três workflows operacionais usam `queue: max` no grupo compartilhado, com até
100 execuções pendentes. Isso evita a substituição padrão de uma execução pendente
por outra, mas não recupera disparos ausentes nem garante pontualidade. O plano de
transferência para scheduler supervisionado e fila persistente está em
[`PLANO_INGESTAO_EXAUSTIVA_E_CONFIABILIDADE.md`](PLANO_INGESTAO_EXAUSTIVA_E_CONFIABILIDADE.md).
Pools Bronze/DW usam tentativas limitadas de conexão para retomar compute suspenso;
a manutenção também executa um preflight com sessão SQL real. Promover o novo
`ENGINE_RELEASE_SHA` é necessário para que o runtime pinado receba essas correções.

`engine-schedule.yml` é o único dono das agendas. Acorda nos minutos UTC 7, 22,
37 e 52 de todas as horas; cada ciclo reconstrói os horários BRT devidos desde o
cursor persistido, em páginas limitadas, e consome `ingestion.runtime_job` com
`SKIP LOCKED`. Um trigger perdido é recuperável no ciclo seguinte. O primeiro
bootstrap começa à meia-noite BRT da implantação, sem iniciar anos de backfill.

Jobs possuem token de lease, renovação a cada 30s, timeout, até 12 tentativas com
backoff e recibo durável. Sucesso exige fontes completas e dbt build; falha fica
pendente para retry ou FAILED visível. Cotações intraday vencidas ficam EXPIRED e
não são substituídas por preço atual rotulado como histórico. O prazo é de frescor,
não uma garantia de entrega pontual do GitHub.

O fechamento preserva todos os tickers do arquivo oficial da sessão em
`market.nominal_bar` e no modelo Gold correspondente. Somente instrumentos com
cadastro confirmado são promovidos também ao caminho legado `market.cotacao_diaria`.
O recibo registra cobertura nominal, cobertura de serving e tickers sem cadastro;
esses preços não são descartados nem classificados como FII/ação por suposição.

O workflow de manutenção conserva CI e ferramentas manuais, sem cron paralelo.
As políticas legadas/`scheduled_*` não devem ser consumidas por outro scheduler
contínuo enquanto o runtime for o dono desses datasets. O mutex Redis continua
protegendo a execução direta e a quota é compartilhada a cada tentativa HTTP.

Operação manual da recuperação:

```bash
gh workflow run engine-schedule.yml -f profile=recover
```

Inspecionar `ingestion.v_runtime_health`, `runtime_cursor`, `runtime_heartbeat` e
o artefato `data/runtime/summary.json`. Um job FAILED exige diagnóstico antes de
reabertura deliberada; os retries transitórios mantêm o mesmo job/ocorrência.
O disparador externo gratuito Cloudflare está em `fin-infra/ingestion/`; sua
implantação depende de conta autenticada e token limitado a Actions neste repo.

## Evidência e retenção

Cada execução gera `data/deployment.json`, com SHA, referências reais, contagens e status.
Cada ocorrência recuperada gera `data/runtime/<job_id>.json` e recibo PostgreSQL;
o resumo de cada ciclo também é publicado como artefato Actions.
O deploy publica apenas os modelos dbt novos, sem substituir os modelos contábeis legados.
Arquivos brutos oficiais ficam em Bronze com SHA-256, em artefatos de retenção de sete
dias. Payloads comerciais permanecem no PostgreSQL, fora dos artefatos públicos.

## Rollback

Definir `ENGINE_ENABLED=false` interrompe novos disparos do runtime. Para voltar à release
anterior, restaurar `ENGINE_RELEASE_SHA` para um SHA homologado e habilitar novamente.
Nenhum rollback apaga preços, versões, payloads ou tabelas. Uma execução já em andamento
deve terminar ou ser cancelada pelo operador antes de trocar os produtores.
