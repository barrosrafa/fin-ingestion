-- Publication policy: percentage points, never silently rescale suspicious data.
-- Null means unavailable. Exceptional yields require source review before promotion.
{{ config(severity='error') }}
select ticker, dividend_yield_pct as dividend_yield, 'acoes' as market
from {{ ref('mart_screener_acoes') }}
where dividend_yield_pct is not null
  and not (dividend_yield_pct between 0 and 100)
union all
select ticker, dividend_yield_12m_pct as dividend_yield, 'fiis' as market
from {{ ref('mart_screener_fiis') }}
where dividend_yield_12m_pct is not null
  and not (dividend_yield_12m_pct between 0 and 100)
