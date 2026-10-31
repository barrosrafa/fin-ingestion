# Deploy da engine no GitHub Actions

Destino: `barrosrafa/fin-ingestion`, ambiente `production-ingestion`. O runtime usa
PostgreSQL/Redis gerenciados existentes; não depende de Docker Desktop neste computador.
O Compose continua sendo uma alternativa para um host Docker administrável.

## Release e ativação

1. Publicar o DDL em fin-db e o código em fin-ingestion. O DDL atual fica no estágio
   `20_scheduled_engine_v4.5` do fin-db, compatível com sua política schema-only.
   O histórico aplicado da engine mantém os identificadores/checksums originais do
   estágio 17; `tests/schema_contract` é o contrato imutável usado pelo deploy.
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

O universo inicial CVM é Petrobras (`33000167000101`), explicitamente configurado em
`ENGINE_CVM_CNPJS`. A ampliação é feita nessa variável após conferir cobertura e recursos.
O polling de 15 minutos é compatível com o teto inicial e o feed disponível; não é
streaming de mercado. O GitHub pode atrasar o início das execuções.

O fechamento e os fluxos legados continuam no workflow existente. Os workflows usam
o mesmo grupo de concorrência; o runtime adicional tem mutex Redis com expiração,
auditoria em `ingestion.ingestion_run` e recebimento de quota a cada tentativa HTTP.
As políticas `scheduled_*` da fila PostgreSQL permanecem desativadas enquanto a agenda
for gerenciada pelo GitHub, para não manter dois schedulers para o mesmo dataset.

## Evidência e retenção

Cada execução gera `data/deployment.json`, com SHA, referências reais, contagens e status.
O deploy publica apenas os modelos dbt novos, sem substituir os modelos contábeis legados.
Arquivos brutos oficiais ficam em Bronze com SHA-256, em artefatos de retenção de sete
dias. Payloads comerciais permanecem no PostgreSQL, fora dos artefatos públicos.

## Rollback

Definir `ENGINE_ENABLED=false` interrompe novos disparos do runtime. Para voltar à release
anterior, restaurar `ENGINE_RELEASE_SHA` para um SHA homologado e habilitar novamente.
Nenhum rollback apaga preços, versões, payloads ou tabelas. Uma execução já em andamento
deve terminar ou ser cancelada pelo operador antes de trocar os produtores.
