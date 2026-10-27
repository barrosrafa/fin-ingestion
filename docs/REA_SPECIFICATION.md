# REA — Reverse Engineering Analysis: 12 Financial Platforms & 4 Regulatory Sources

**Target Project:** `fin-ingestion` (`https://github.com/barrosrafa/fin-ingestion`)  
**Target Database:** `fin-db` (`https://github.com/barrosrafa/fin-db` - PostgreSQL 18 / TimescaleDB)  
**Version:** 1.0  
**Date:** 2026-10-26  

---

## 1. Executive Summary & Purpose

This document provides the definitive reverse engineering analysis (REA) and technical specification for ingesting financial data from the 12 primary Brazilian investment platforms and the 4 official regulatory entities:

- **Official Entities:** B3, CVM, Banco Central do Brasil (BCB), ANBIMA.
- **Analysis Platforms:** Status Invest, Investidor10, Fundamentei, Fundamentus, Clube FII, Funds Explorer, FIIs.com.br, Mais Retorno, Análise de Ações, Brick360, ADVFN Brasil, ETFs Brasil.

This specification drives the collector implementations, parsers, and pipeline jobs in `fin_ingestion.collectors`, orchestrates the raw landing into `raw.payload`, and informs the transformation modeling in `dbt`.

---

## 2. Source Catalog & Architecture Reference

### 2.1 Regulatory & Official Sources

| Entity | Protocol / Architecture | Base URL / Data Feeds | Data Domains |
|---|---|---|---|
| **B3** | Positional TXT (COTAHIST), XML/ZIP (BVBG), JSON (IndexProxy) | `https://www.b3.com.br`, `bvmf.bmfbovespa.com.br` | Quotes, instruments, volumes, BDI, BTC stock lending, indices (IBOV, IFIX) |
| **CVM** | HTTP Open Data CSV (ISO-8859-1 ZIP archives) | `https://dados.cvm.gov.br/dados/` | DFP, ITR, FRE (Governance/Salaries), FCA, FII monthly/quarterly reports, Fund holdings (CDA) |
| **BCB** | REST JSON (SGS API) + OData (Olinda Portal) | `https://api.bcb.gov.br`, `https://olinda.bcb.gov.br` | Selic, IPCA, IGP-M, CDI, PTAX FX rates, Focus market consensus |
| **ANBIMA** | Delimited TXT / Fixed-width feeds | `https://www.anbima.com.br` | Secondary market TPF bonds, debentures, yield curve (ETTJ), IMA benchmarks |

### 2.2 The 12 Analysis Platforms

| # | Platform | Architecture | Scraping & Extraction Strategy | Key Tables in `fin-db` |
|---|---|---|---|---|
| 1 | **Status Invest** | ASP.NET MVC + XHR | Emulated browser headers + JSON XHR endpoints (`/acao/tickerprice`, `/acao/getdre`, `/category/borrowhistory`, `/fii/cridetail`) | `analytics.demonstrativo_financeiro`, `market.aluguel_acoes`, `asset_details.fii_cri_carteira` |
| 2 | **Investidor10** | Laravel 10 + REST + JS state | REST API with session cookies (`/api/historico-indicadores/`, `/api/balancos/`) + inline JS arrays | `analytics.historico_indicador_fundamentalista`, `analytics.receita_segmento_negocio`, `analytics.checklist_avaliacao_ativo` |
| 3 | **Fundamentei** | Next.js SSR + Apollo GraphQL | JSON extraction from `window.__NEXT_DATA__.props.pageProps.apolloState.data` | `analytics.kpi_operacional_emissor`, `core.administrador_companhia`, `analytics.receita_geografia` |
| 4 | **Fundamentus** | PHP + LiteSpeed server | BeautifulSoup HTML table parsing (`ISO-8859-1`) with session cookies (`detalhes.php`, `fii_imoveis_detalhes.php`, `recompras.php`) | `asset_details.fii_imovel`, `market.programa_recompra`, `canonical.insider_transaction` |
| 5 | **Clube FII** | ASP.NET + GraphQL CR Data | GraphQL API (`api.clubefii.dev.br/graphql`) + AJAX SiiLA transactions | `asset_details.fii_imovel`, `asset_details.fii_transacao_venda_imovel`, `asset_details.mercado_imobiliario_indicador` |
| 6 | **Funds Explorer** | WordPress Headless + AJAX | AJAX endpoints with WP nonce (`funds-get-income`, `funds-get-vacancia`, `funds-get-patrimonials`, `/ranking`) | `asset_details.rentabilidade_fii`, `market.provento`, `asset_details.historico_vacancia_fii` |
| 7 | **FIIs.com.br** | WP Sage + Vue 3 islands | Island hydration parsing (`invest-simulator`, `dataLayer_content`, `/agenda-de-dividendos/`) | `market.provento` (`mes_referencia`), `canonical.disclosure_document` |
| 8 | **Mais Retorno** | Next.js + Microservices REST | REST v4/v3 JSON endpoints (`mr-data/v4/funds`, `ms/v2/funds/risk-metrics`, `api/v3/indexes`) | `asset_details.fundo_investimento`, `asset_details.fundo_carteira`, `analytics.historico_risco`, `market.benchmark` |
| 9 | **Análise de Ações**| Gatsby + REST v2 | Gatsby route JSON (`/page-data/*/page-data.json`) + API v2 checklist and valuation | `analytics.valuation`, `analytics.checklist_avaliacao_ativo`, `analytics.indicador_media_setorial` |
| 10 | **Brick360** | Vue 3 SPA + REST API v2 | REST API v2 (`/api/v2/funds/{TICKER}/analysis/advanced`) with 12 modules | `analytics.score_eixo`, `asset_details.fii_demonstrativo_trimestral`, `asset_details.fii_avaliacao_gestora` |
| 11 | **ADVFN Brasil** | Multi-exchange SSR + AJAX | HTML tables & JSON: Pivot points 4S/3R, 13 technical indicators across 9 timeframes, candlestick patterns, sell-side consensus | `analytics.indicador_tecnico`, `analytics.padrao_candlestick`, `analytics.previsao_consenso_analistas` |
| 12 | **ETFs Brasil** | Next.js + FastAPI backend | FastAPI JSON REST endpoints (`/api/backend/home`, `/api/backend/compare`, `/api/backend/etf/{TICKER}/composition`) | `asset_details.etf_detalhe`, `canonical.fund_holding`, `asset_details.etf_nav`, `asset_details.etf_fluxo_cotas` |

---

## 3. Ingestion Strategy & Data Integrity

1. **Bronze Layer (Raw Landing):**
   Every collector stores the unmodified payload into `raw.payload` (PostgreSQL JSONB) alongside request metadata, timestamp, HTTP status, and payload SHA-256 hash.
2. **Silver Layer (Canonical Harmonization):**
   Python pipelines and dbt staging/intermediate models normalize entities into `canonical`, `core`, `market`, and `asset_details`.
3. **Gold Layer (Analytics & DW):**
   Analytical algorithms compute valuations (Graham, Bazin, DCF), risk metrics (Sharpe, Sortino, Drawdown, VaR), health scores (Piotroski, Altman, Beneish), and populate dimensional facts in `dw.*`.
