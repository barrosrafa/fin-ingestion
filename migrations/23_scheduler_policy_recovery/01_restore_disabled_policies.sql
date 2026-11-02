-- Recover the scheduler defaults removed during repository consolidation.
-- Policies remain disabled until they are explicitly homologated and activated.
-- ON CONFLICT DO NOTHING preserves any existing operator-managed configuration.
INSERT INTO ingestion.source_policy(source_name, requests_per_minute, max_concurrency)
VALUES
    ('brapi', 30, 1),
    ('bcb_olinda', 60, 2),
    ('bcb_focus', 60, 1),
    ('tesouro', 10, 1),
    ('anbima_feed', 30, 1)
ON CONFLICT (source_name) DO NOTHING;

INSERT INTO ingestion.ingestion_policy
    (policy_id, dataset_name, source_name, calendar_type, cron_expression, priority, is_active)
VALUES
    ('scheduled_ptax', 'ptax', 'bcb_olinda', 'BUSINESS_DAYS', '0 10,11,12,13 * * 1-5', 2, false),
    ('scheduled_ptax_close', 'ptax', 'bcb_olinda', 'BUSINESS_DAYS', '30 13 * * 1-5', 2, false),
    ('scheduled_focus', 'focus', 'bcb_focus', 'BUSINESS_DAYS', '0 9 * * 1', 4, false),
    ('scheduled_treasury', 'tesouro', 'tesouro', 'BUSINESS_DAYS', '30 9,18 * * 1-5', 3, false),
    ('scheduled_debentures', 'anbima_feed', 'anbima_feed', 'BUSINESS_DAYS', '15 20 * * 1-5', 3, false),
    ('scheduled_intraday', 'cotacao_intraday', 'brapi', 'B3_TRADING_DAYS', '* 10-17 * * 1-5', 1, false)
ON CONFLICT (policy_id) DO NOTHING;
