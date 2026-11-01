# Proventos com pagamento verificável

O calendário usa eventos de caixa da B3 (`GetListedSupplementCompany`), vinculados
ao ISIN do instrumento. Dividendos/JCP agregados do gráfico Yahoo não informam a
data de pagamento nem a natureza de cada parcela e não podem preencher esse
calendário. O backfill Yahoo continua disponível para preços e splits.

A migration `22_cash_dividend_integrity` mantém onze casas decimais e remove o
índice que descartava parcelas iguais com a mesma data ex. A chave de pagamento
continua sendo ativo, tipo, data com e pagamento. Anúncios distintos desse mesmo
pagamento são somados, com as linhas originais preservadas em `payload_fonte`.
A data ex fica desconhecida quando a fonte não a fornece.

O perfil diário atualiza os pagamentos depois do fechamento. Cada suplemento
original fica em `ingestion.provento_snapshot`, identificado por SHA-256.
`data/cash-dividends.json` discrimina cobertura, ISIN ausente, ausência de
pagamentos datados e erros de fonte. Ausência de eventos nesse suplemento não
significa que o ativo nunca pagou proventos; a janela da fonte é limitada.

## Recuperação inicial

Depois de passar na CI, aplicar as migrations e executar:

```sh
python -m fin_ingestion.pipelines.cash_dividends --apply --quarantine \
  --html data/reference/statusinvest-petr4-2026-10-31.html --html-ticker PETR4
```

Sem `--apply`, a operação só consulta fontes e produz o relatório. `--quarantine`
é reservado à recuperação do universo completo. Registros sem proveniência que
correspondam à assinatura dos dois importadores Yahoo antigos (pagamento igual
à data ex, data com igual à data ex menos um dia corrido, sem anúncio/observação,
competência ISO) são movidos, na mesma transação, para
`ingestion.provento_quarentena`, preservando integralmente cada registro original.
Referências de usuários impedem a remoção; o processo falha e reverte a transação.
Não se convertem esses agregados em pagamentos históricos inventados quando a
fonte atual não permite repor as parcelas.

O HTML Status Invest fornecido pelo usuário é uma referência datada para PETR4,
com prioridade explícita sobre o suplemento B3 nos pagamentos correspondentes.
Correção monetária/rendimento tributado pode divergir do valor originalmente
declarado na B3. O HTML Investing agrupa parcelas pela data ex e não pode ser
comparado linha a linha à agenda de pagamentos. Os valores de cada fonte devem
permanecer identificados; não há promessa de igualdade entre fontes divergentes.

Para reverter uma quarentena, selecionar os IDs desejados e restaurar as colunas
do JSON original em uma transação revisada, após conciliar pagamentos novos e
referências. Não restaurar automaticamente os agregados sobre a agenda corrigida.

As restituições de capital (`REST CAP DIN`) e amortizações (`AMORTIZACAO RF`)
permanecem no snapshot original e não entram na soma de renda de dividendos.
O fechamento oficial também completa os ISINs ausentes com o COTAHIST, sem
sobrescrever identificadores existentes.

A migração de precisão destaca temporariamente as views dependentes e as
recria em ordem, preservando definição, proprietário, opções, índices e grants.
O runner operacional atualiza a visão materializada mensal dentro da mesma
transação; a recuperação dos pagamentos volta a atualizá-la após a conciliação.
