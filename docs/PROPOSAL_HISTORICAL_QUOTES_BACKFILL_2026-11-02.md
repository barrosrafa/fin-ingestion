# Proposta: backfill histórico de cotações B3/COTAHIST

**Status: proposta documental para revisão. Não executar.** Este documento não adiciona comandos de carga, não altera código operacional e não autoriza conexão ou escrita em produção. Neon e Aiven permanecem somente leitura; nenhuma migration, replay, seed, ingestão ou deploy foi executado nesta investigação.

## Sintoma observado e diagnóstico atual

Na página pública do PETR4, os períodos `1M`, `3M`, `6M`, `1Y`, `2Y`, `5Y` e `MAX` aparecem. O frontend versionado carrega todas as páginas até atingir `total_days` (`fin-web/src/lib/api.ts`, `getFullQuoteHistory`) e chama essa função ao trocar o período. A consulta GET pública do endpoint de histórico retornou HTTP 200 e `total_days=21` para cada um de `1Y`, `2Y`, `5Y` e `MAX`; o gráfico MAX mostrou 21 pregões entre 2026-10-01 e 2026-10-30. Assim, a paginação não é a limitação observada: o endpoint que atende a página informa somente 21 registros disponíveis para PETR4.

Isso ainda não identifica a causa. A auditoria somente leitura do Neon disponibilizado para a investigação encontrou zero linhas em `market.cotacao_diaria`, enquanto a API pública retorna 21. O código versionado de `fin-api` usa `DATABASE_URL` em runtime, referenciado por secret no manifesto de `fin-infra`; os arquivos não comprovam o valor ativo nem que aquele Neon seja o banco servido pela API. **Não presumir que o banco auditado e o banco de produção são o mesmo.**

## Fluxo histórico existente

`fin_ingestion/collectors/official/b3.py` contém coleta diária incremental por data e leitura de arquivos COTAHIST anuais. `fin_ingestion/operations/replay_official_quotes.py` aceita um ano e, opcionalmente, um mês; valida período, ano, sessão futura, duplicidade e mercado spot, baixa o arquivo oficial, calcula hash e prepara recibo de preview. Os arquivos são publicados pela B3 desde 1986, mas isso não significa que cada ticker tenha cotações desde 1986 ou que códigos históricos sejam intercambiáveis sem mapeamento.

O modo padrão do replay é `preview`. Ele já abre uma conexão de leitura para obter o mapa de tickers cadastrados e comparar valores existentes no período. Somente `--apply` habilita gravações: salva preimage comprimido das cotações existentes no intervalo, preserva o payload bruto oficial, grava evidência nominal via `SilverWriter` e depois faz UPSERT em `market.cotacao_diaria` em lotes de 1.000. O writer usa a chave `(id_ativo, data_pregao)`, permitindo repetir o lote sem duplicar a identidade da sessão. O preimage consulta todos os ativos cadastrados no intervalo, não apenas PETR4; capacidade e retenção do diretório de saída precisam ser dimensionadas.

O replay preserva evidência nominal. Ele não prova, por si só, que a série ajustada por proventos/splits esteja completa. A API aplica `COALESCE(preco_ajustado, preco_fechamento)`, portanto a cobertura e a procedência de `preco_ajustado` e dos eventos corporativos devem ser verificadas separadamente antes de afirmar que a série histórica ajustada está correta.

## Bloqueios antes de qualquer preview conectado ou carga

1. Identificar, no painel de runtime ou por evidência administrativa redigida, qual serviço/build recebe o tráfego de produção e qual projeto/branch de banco está ligado ao secret `DATABASE_URL`. Não copiar nem registrar a connection string. Confirmar separadamente que o writer de `fin-ingestion` apontaria para o mesmo destino pretendido da API.
2. Reconciliar o schema e o ledger do **destino efetivo**. No Neon auditado, `system.schema_migrations` estava ausente. Enquanto o destino e o estado do ledger não forem reconciliados por evidência, não iniciar replay conectado nem aplicar DDL. Os PRs de fail-closed [fin-db #9](https://github.com/barrosrafa/fin-db/pull/9) e [fin-ingestion #11](https://github.com/barrosrafa/fin-ingestion/pull/11) estão separados e não foram mesclados.
3. Definir o universo e a data inicial: proposta inicial é buscar COTAHIST oficial por partições anuais/mensais e servir somente tickers com cadastro oficial correspondente. `MAX` significaria a primeira observação confiável disponível para aquele ticker, não um início universal em 1986. Tickers antigos ou renomeados sem vínculo seguro devem permanecer sem promoção ao serving até revisão; não inferir equivalências.
4. Definir retenção, criptografia e permissões do diretório de recibos, arquivos de origem e preimages. O arquivo de preimage inclui todas as cotações já existentes naquele intervalo e pode ser grande.

## Plano proposto, sujeito a aprovação separada

**Fase A — baseline somente leitura.** Após identificar o destino efetivo, registrar por ticker e timeframe as contagens e datas mínima/máxima retornadas pela API. Em uma sessão explicitamente read-only no destino correto, comparar contagens, cobertura temporal, tickers cadastrados e a presença de linhas em `market.cotacao_diaria` e no dataset nominal. Distinguir ausência de pregão de lacuna real usando o calendário B3; não tratar `COUNT` igual em todos os timeframes como prova de período completo.

**Fase B — plano de partições.** Determinar anos e meses necessários a partir da baseline e da cobertura oficial pretendida. Preferir partições mensais para reduzir o tamanho de cada lote e facilitar retry; arquivos anuais são aceitos pela CLI, mas não devem ser selecionados sem validar limites de descompressão, memória, tempo e espaço de preimage. O replay atual não oferece um intervalo arbitrário de datas nem checkpoint de subfaixa; o plano deve enumerar as partições e permitir retomada somente por partição idempotente.

**Fase C — preview controlado, após nova autorização.** Executar o modo padrão sem `--apply` somente depois de confirmar o destino; ele ainda baixa um arquivo oficial e faz consultas de leitura. Preservar fora do repositório os recibos contendo hash SHA-256, intervalo, registros oficiais, tickers registrados/não registrados, primeiras/últimas datas e divergências contra os fechamentos existentes. Revisar amostras e limites de recursos antes de solicitar uma autorização específica de escrita.

**Fase D — eventual carga, não autorizada nesta etapa.** Só após aprovação explícita do alvo, universo, períodos e recibos de preview, aplicar uma partição por vez com preimage verificado, `--apply` explícito, supervisão e interrupção segura. Validar os totais gravados contra as linhas oficiais e guardar o recibo. Em qualquer divergência ou falha, parar; não corrigir ledger manualmente nem executar migrations para contornar o erro. Uma repetição só é permitida após revisão do recibo da partição e permanece sujeita à idempotência documentada.

**Fase E — reconciliação.** Após uma carga futuramente autorizada, comparar datas/contagens da origem oficial, da evidência nominal, de `market.cotacao_diaria` e da resposta API para uma amostra aprovada. Confirmar que, para um ticker existente e com dados suficientes, as contagens de `1Y`, `2Y`, `5Y` e `MAX` crescem conforme as janelas e que as primeiras/últimas datas batem com o escopo. Verificar ajustes corporativos separadamente. Só então concluir que o problema de histórico foi corrigido.

## Decisões ainda necessárias

Antes de avançar além desta proposta, o responsável pelo sistema precisa confirmar: o alvo efetivo de produção sem revelar secrets; universo desejado (PETR4 como canário ou todas as ações cadastradas); data inicial/universo de `MAX`; aprovação de um preview conectado read-only no destino já reconciliado; e, em etapa posterior independente, autorização de escrita por partição. Este PR documental não concede nenhuma dessas autorizações.