# Execução da evolução da engine — 30/10/2026

Esta execução continua o plano de `PLANO_EVOLUCAO_SCHEDULED_PIPELINE.md`. Implementa
conectores, persistência, intraday, migrações e homologação. Não confunde código pronto,
contrato comercial disponível, dado corrente e serviço continuamente implantado.

## Resultado por domínio

| Domínio | Arquivos principais | Estado comprovado |
|---|---|---|
| brapi v2 | `collectors/commercial/brapi.py`, `pipelines/market_gateway.py` | Cotação autenticada real de PETR4; batching, cobertura completa, timestamp UTC, limite de defasagem e teto compartilhado. Plano da chave informado pela API: **Free**, limite informado 15.000 requisições. |
| Histórico nominal B3 | `collectors/official/b3.py`, `pipelines/provider_pipeline.py` | EOD oficial de 29/10/2026 passou do ZIP até Prata e `market.cotacao_diaria`; histórico anual COTAHIST disponível em `b3_history`. Não usa cotação de outra sessão como fallback. |
| SGS | `collectors/official/bcb.py`, `dw/silver_writer.py`, `pipelines/oceans14_macro_market_pipeline.py` | 25 observações reais em consulta histórica; identidade série/data, janelas de até três anos e persistência normalizada. |
| PTAX e Focus | `collectors/official/olinda.py`, `pipelines/provider_pipeline.py` | 5 boletins PTAX e 320 expectativas reais promovidos no ambiente isolado. Paginação limitada; Focus conserva detalhe do indicador, base de cálculo, reunião/ano e data real de publicação. |
| Tesouro | `collectors/official/tesouro.py` | Descoberta CKAN oficial; arquivo original preservado; 58 registros da referência selecionada promovidos, sem converter ausência em zero. |
| CVM DFP/ITR | `collectors/official/cvm.py`, `dw/silver_writer.py` | ZIP oficial DFP 2025 processado: 1.175.979 linhas extraídas; canário filtrado promoveu 2.238 linhas da Petrobras. Identidade preserva escopo, conta, período, moeda, escala e versão. ITR tem o mesmo contrato de arquivo e testes de parser. |
| ANBIMA Feed | `collectors/commercial/anbima_feed.py` | OAuth2 com cache e renovação antecipada de token; endpoint oficial de debêntures, data de referência e persistência idempotente. Testes de contrato; credenciais não disponíveis para homologação autenticada. |
| Massive | `collectors/commercial/international.py` | Barras explicitamente `adjusted=false`, paginação limitada ao host autorizado e datas/UTC preservados. Testes de contrato; chave não disponível. |
| FMP | `collectors/commercial/international.py` | Endpoints stable para screener, dividendos e demonstrações, erros sem exposição de chave, limite de resposta e persistência por identidade fiscal. Testes de contrato; chave não disponível. |
| Twelve Data | `collectors/commercial/international.py` | Série temporal com `adjust=none`, UTC intraday, datas diárias sem hora inventada e recusa de respostas possivelmente truncadas. Testes de contrato; chave não disponível. |
| Eventos e Ouro | `analytics/adjustments.py`, `dw/silver_writer.py`, modelos dbt novos | Eventos exigem ID estável e guardam revisões. Ajustes usam somente eventos conhecidos na data consultada; nominal permanece intacto. Sem ID estável, um provento não é promovido automaticamente por uma chave baseada no valor. |

## Segurança operacional e compatibilidade

- A chave recebida foi configurada em `.env`, ignorado pelo Git e com permissão 0600.
  O contexto Docker exclui `.env`, dados locais e caches. Não há chave no código ou nestas evidências.
- `ProviderBudget` usa Lua atômico no Redis; cada tentativa consome créditos, inclusive
  retries. O teto local configurado para brapi é 1.000 créditos. A janela operacional
  é o mês UTC; isso não substitui a leitura da janela contratual de faturamento.
- `market.cotacao_intraday` ganhou identidade única parcial por ativo/timestamp/origem/tipo.
  Linhas legadas sem origem ficam preservadas. Os escritores novos e Status Invest usam UPSERT.
- Falha de lease impede confirmar uma tarefa. Auditoria, watermark real e conclusão são
  confirmados juntos; filas usam backoff com jitter e respeitam Retry-After como mínimo.
- Workers podem filtrar datasets. O Compose separa batch do perfil `intraday`; um worker
  intraday não reserva tarefas batch. As políticas intraday exigem universo explícito.
- O calendário B3 2026 inclui 24/12, 31/12 e abertura especial em 18/02. Ano não revisado
  é recusado pelo controle de sessão intraday.
- O fechamento diário usa o gateway oficial da mesma sessão; o caminho agendado não
  depende mais do fallback de chart público. A CLI de backfill antiga continua disponível
  para compatibilidade; para histórico nominal novo, use COTAHIST ou provedor raw homologado.

## Migrações e banco configurado

Foram aplicados os três arquivos do estágio `17_scheduled_engine_v4.5` no PostgreSQL
configurado (**18.6**), com transação, advisory lock, checksum e limites de duração/lock.
A conferência posterior encontrou os três registros como `already_applied`.

A primeira tentativa identificou uma colisão com a tabela legada
`canonical.financial_statement`. A transação foi revertida integralmente. A versão final
usa **`canonical.cvm_statement`**, sem alterar a tabela legada. O teste de coexistência
preserva uma linha legada ao reaplicar a migração.

As seis políticas novas foram criadas **desativadas**. Nenhum agendamento contínuo foi
iniciado nesta execução. O Docker CLI instalado aponta para um Docker Desktop ausente;
portanto a implantação dos containers não foi executada neste computador.

Os modelos novos são publicados em paralelo, incluindo `fct_cvm_statement_v2`,
`fct_nominal_bar`, `fct_ptax_boletim`, `fct_tesouro_taxa`, `fct_focus_expectativa`,
`fct_debenture_indicativa`, `fct_sgs_observation`, `fct_external_fundamental` e
`fct_corporate_action_latest`. Os modelos contábeis legados não foram substituídos.

Evidências sem credenciais:

- `docs/evidence/migration-17-2026-10-30.json`: ledger e checksums após a aplicação.
- `docs/evidence/post-migration-2026-10-30.json`: versão, políticas e colunas legadas.
- `tests/schema_contract/manifest.json`: contrato SQL versionado usado no CI isolado.

## Homologação

O ambiente isolado usa PostgreSQL real 16.2 via `pgserver` e Redis real 7.2, sem reutilizar
as URLs de produção nos testes. Os testes cobrem reaplicação, UPSERT, rollback de lote,
leases, reserva exclusiva, workers filtrados, teto de créditos, erros sem segredo,
revisões CVM e cálculo causal de ajustes. Os modelos e testes dbt novos foram executados
com perfil isolado explícito.

Última conferência da suíte: **172 testes passaram, sem ignorados**, incluindo o retry
real da fila com Retry-After e liberação de lease.
Ruff, lock e sintaxe YAML/shell passaram. Logs e payloads locais estão em
`data/homologation/`, fora do versionamento.

O snapshot real brapi foi reprocessado com **uma única linha** após duas gravações.
Fora do pregão, sua defasagem foi recusada para uso como cotação corrente. Esse resultado
comprova o guard de frescor, não uma homologação de negociação em tempo real durante pregão.

## Operação e ativação

Antes de habilitar uma fonte, definir sua chave e teto em `PROVIDER_MONTHLY_CREDITS`.
As configurações estão descritas em `.env.example`. O histórico brapi raw permanece
desabilitado: a documentação v2 exige acesso Pro, que não foi comprovado nesta conta.

Exemplos de execução explícita:

```bash
uv run fin-ingestion collect --pipeline ptax --reference-date 2026-10-29
uv run fin-ingestion collect --pipeline focus --reference-date 2026-10-29
uv run fin-ingestion collect --pipeline tesouro --reference-date 2026-10-29
uv run fin-ingestion collect --pipeline sgs --serie ipca --start-date 2000-01-01 --end-date 2026-10-29
uv run fin-ingestion collect --pipeline cvm_dfp --year 2025 --cnpjs 33000167000101
uv run fin-ingestion collect --pipeline b3_history --year 2025 --tickers PETR4
uv run fin-ingestion collect --pipeline market_eod --reference-date 2026-10-29 --tickers PETR4
```

Para intraday, habilitar `INTRADAY_ENABLED`, preencher `INTRADAY_TICKERS`, homologar
uma cotação fresca durante a sessão e ativar somente a política correspondente. O perfil
Compose é `intraday`; batch e intraday compartilham Redis e o mesmo controle mensal.
Escolher um único scheduler por dataset para não duplicar Actions e containers.

Homologação isolada reproduzível em Python 3.12:

```bash
uv run --python 3.12 python scripts/homologate_engine.py --redis-server /caminho/redis-server
```

`--live-brapi --live-public` habilita os canários reais explicitamente. O script não chama
APIs se a suíte falhar. `scripts/apply_engine_migrations.py` faz preflight somente leitura
por padrão; `--apply` aplica exclusivamente o estágio novo com ledger.

## Dependências externas restantes

Homologação autenticada de Massive, FMP, Twelve Data e ANBIMA requer as respectivas
credenciais/assinaturas. Histórico nominal comercial exige o entitlement correspondente.
UP2DATA mTLS, streaming de plano enterprise, Airflow/Kubernetes e migração para outro
banco continuam condicionados a contrato, infraestrutura e SLO; não foram contratados
nem apresentados como serviços implantados. O runtime contínuo e a troca dos leitores
legados por modelos novos são etapas de rollout, após backfill e comparação de cobertura.
