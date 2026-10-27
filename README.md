# 🚀 fin-ingestion — Coleta, Analytics e Modelagem dbt para Mercados Financeiros

Pipeline de dados resiliente e modular para coleta, enriquecimento, cálculos analíticos e transformação dbt para o ecossistema **`fin`**, cobrindo órgãos oficiais e regulatórios (B3, CVM, BCB, ANBIMA) e 12 plataformas líderes de análise de investimentos no Brasil.

- **Repositório:** `https://github.com/barrosrafa/fin-ingestion`
- **Banco de Dados Relacionado:** `https://github.com/barrosrafa/fin-db` (PostgreSQL 18 / TimescaleDB no Neon)
- **Especificação REA:** [`docs/REA_SPECIFICATION.md`](docs/REA_SPECIFICATION.md)

---

## 🏛️ Fontes Suportadas

### 1. Órgãos Reguladores e Oficiais
- **B3:** Cotações diárias/históricas (COTAHIST), cadastro de instrumentos (BVBG.028/086), empréstimo de ações (BTC), composição de índices.
- **CVM:** Demonstrações financeiras (DFP/ITR), relatórios de governança (FRE/FCA), Informes Mensais/Trimestrais de FIIs, Composição de Carteira (CDA) CVM 175.
- **BCB:** Sistema Gerenciador de Séries Temporais (SGS), taxas Selic, CDI, IPCA, IGP-M, câmbio PTAX, expectativas Focus.
- **ANBIMA:** Preços e taxas indicativas do mercado secundário de Títulos Públicos (TPF), Debêntures, Curva ETTJ e Família IMA.

### 2. Plataformas de Análise (Engenharia Reversa - REA)
1. **Status Invest:** Cotações, balanços DRE/DFC, histórico de aluguel BTC, detalhamento de CRIs, gregas e opções.
2. **Investidor10:** Indicadores fundamentalistas de 10 anos, balanços, mapas de proventos, checklist Buy & Hold, médias setoriais.
3. **Fundamentei:** Estado Apollo GraphQL: 102 indicadores, segmentos geográficos e de negócio, KPIs operacionais por setor, diretoria CVM.
4. **Fundamentus:** Scraping resiliente de balanços, múltiplos, imóveis físicos de FIIs, programas de recompra e insider trading.
5. **Clube FII:** CR Data GraphQL API (`api.clubefii.dev.br`), transações imobiliárias SiiLA de venda/locação, cronograma de contratos e emissões.
6. **Funds Explorer:** Ranking de 30+ colunas, histórico patrimonial, vacância física e financeira, proventos.
7. **FIIs.com.br:** Lupa de screening de FIIs, agenda de dividendos com competência mensal de apuração (`mes_referencia`).
8. **Mais Retorno:** Arquitetura CVM 175 (Fundo/Classe/Subclasse), carteiras CDA, 10 janelas temporais de risco (Sharpe, Sortino, VaR, Alpha), 170 índices/benchmarks.
9. **Análise de Ações:** Rotas Gatsby JSON + API v2: checklist dos 5 pilares, valuation Graham / Bazin / DCF, dias habituais de proventos.
10. **Brick360:** API REST v2: Brick Score (6 eixos), DRE contábil CVM trimestral, auditoria de gestoras, análise de crédito de CRIs.
11. **ADVFN Brasil:** Suportes/resistências (4S/3R), 13 indicadores técnicos em 9 timeframes, padrões candlestick, previsões de consenso de analistas.
12. **ETFs Brasil:** API FastAPI: universo completo de ETFs locais e globais, taxas primárias/secundárias/totais, spread NAV, limite de aluguel % PL, fluxos 30d.

---

## ⚙️ Arquitetura do Sistema

```
                      ┌────────────────────────────────────────────────────────┐
                      │                 COLLECTORS & SCRAPERS                  │
                      │  [B3] [CVM] [BCB] [ANBIMA] [12 Investment Platforms]   │
                      └───────────────────────────┬────────────────────────────┘
                                                  │ Async Ingestion / Rate-limited
                                                  ▼
                      ┌────────────────────────────────────────────────────────┐
                      │              RAW LANDING (Bronze Layer)                │
                      │           raw.payload & raw.lote_captura               │
                      └───────────────────────────┬────────────────────────────┘
                                                  │
                      ┌───────────────────────────┴────────────────────────────┐
                      ▼                                                        ▼
       ┌─────────────────────────────┐                          ┌─────────────────────────────┐
       │     ANALYTICS ENGINE        │                          │        dbt PROJECT          │
       │  • Valuation (Graham/Bazin) │                          │  • Staging (stg_*)          │
       │  • Health (Piotroski/Altman)│                          │  • Intermediate (int_*)     │
       │  • Risk (Sharpe/Sortino/VaR)│                          │  • Marts (dim_*, fct_*)     │
       │  • FII Metrics (Cap/WALT)   │                          │  • Screener Views & DW      │
       └──────────────┬──────────────┘                          └──────────────┬──────────────┘
                      │                                                        │
                      └───────────────────────────┬────────────────────────────┘
                                                  ▼
                      ┌────────────────────────────────────────────────────────┐
                      │            FIN-DB (PostgreSQL 18 / TimescaleDB)        │
                      │  [canonical] [core] [market] [asset_details] [dw]      │
                      └────────────────────────────────────────────────────────┘
```

---

## 🛠️ Instalação e Execução

### Pré-requisitos
- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (gerenciador ultrarrápido de pacotes)
- PostgreSQL 18 / TimescaleDB

### Configuração do Ambiente
```bash
# Clone o repositório
git clone https://github.com/barrosrafa/fin-ingestion.git
cd fin-ingestion

# Crie e sincronize o ambiente virtual com uv
uv sync

# Configure as variáveis de ambiente
cp .env.example .env
# Edite .env com as credenciais do seu banco Neon
```

### CLI de Coleta e Operações
```bash
# Testar conectividade de coletores
uv run fin-ingestion test-sources

# Coletar dados diários de mercado (B3, BCB, Status Invest)
uv run fin-ingestion collect --pipeline daily

# Coletar informes e balanços trimestrais (CVM, Investidor10, Fundamentei)
uv run fin-ingestion collect --pipeline quarterly

# Coletar dados de FIIs e Fundos (CVM CDA, Clube FII, Mais Retorno)
uv run fin-ingestion collect --pipeline monthly

# Executar cálculo de métricas analíticas (Valuation, Scores, Risco)
uv run fin-ingestion compute-analytics --tickers PETR4,VALE3,HGLG11

# Executar pipelines de transformação dbt
uv run fin-ingestion dbt-run --models staging
uv run fin-ingestion dbt-run --models marts
```

---

## 🧪 Testes de Qualidade
```bash
# Executar suíte de testes unitários e de integração
uv run pytest -v

# Validação e formatação de código com Ruff
uv run ruff check .
uv run ruff format .
```
