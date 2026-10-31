-- =============================================================================
-- 14. INGESTION CONTROL, PERSISTENT QUEUE & DATA QUALITY SCHEMA (v4.2)
-- Autonomia Operacional: Agendador, Fila PostgreSQL (SKIP LOCKED), Checkpoints, Quarentena
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ingestion;

-- 1. Políticas de Ingestão por Dataset
CREATE TABLE IF NOT EXISTS ingestion.ingestion_policy (
    policy_id           VARCHAR(50) PRIMARY KEY, -- ex: 'b3_daily_quotes', 'cvm_quarterly_dfp'
    dataset_name        VARCHAR(50) NOT NULL,
    source_name         VARCHAR(50) NOT NULL,
    calendar_type       VARCHAR(30) NOT NULL DEFAULT 'B3_TRADING_DAYS', -- 'B3_TRADING_DAYS', 'CALENDAR_DAYS', 'BUSINESS_DAYS'
    cron_expression     VARCHAR(50) NOT NULL, -- ex: '0 21 * * 1-5'
    priority            SMALLINT NOT NULL DEFAULT 5, -- 1 (Emergência/Intraday) a 10 (Backfill histórico)
    expected_delay_min  INT NOT NULL DEFAULT 120, -- Atraso aceitável em minutos antes de alertar
    max_retries         INT NOT NULL DEFAULT 5,
    retry_delay_seconds INT NOT NULL DEFAULT 60,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Políticas e Estado Operacional das Fontes (Circuit Breaker & Rate Limiting Compartilhados)
CREATE TABLE IF NOT EXISTS ingestion.source_policy (
    source_name         VARCHAR(50) PRIMARY KEY,
    requests_per_minute INT NOT NULL DEFAULT 60,
    max_concurrency     INT NOT NULL DEFAULT 3,
    circuit_state       VARCHAR(20) NOT NULL DEFAULT 'CLOSED' CHECK (circuit_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    failure_count       INT NOT NULL DEFAULT 0,
    consecutive_success INT NOT NULL DEFAULT 0,
    failure_threshold   INT NOT NULL DEFAULT 5,
    cooldown_seconds    INT NOT NULL DEFAULT 300,
    last_failure_at     TIMESTAMPTZ,
    circuit_open_until  TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Fila Persistente de Tarefas de Ingestão
CREATE TABLE IF NOT EXISTS ingestion.ingestion_task (
    task_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id           VARCHAR(50) NOT NULL REFERENCES ingestion.ingestion_policy(policy_id) ON DELETE CASCADE,
    task_key            VARCHAR(255) NOT NULL UNIQUE, -- Chave de idempotência (ex: 'b3_daily:2026-10-27:PETR4')
    dataset_name        VARCHAR(50) NOT NULL,
    target_entity       VARCHAR(50), -- Ex: Ticker ('PETR4'), CNPJ ou Partição
    reference_date      DATE,        -- Data de referência do mercado
    priority            SMALLINT NOT NULL DEFAULT 5,
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    attempts            INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 5,
    next_run_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until        TIMESTAMPTZ,
    lock_token          UUID,
    payload_id          UUID,        -- Vínculo ao raw.payload se concluída
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_task_fetch ON ingestion.ingestion_task (priority ASC, next_run_at ASC)
WHERE status IN ('PENDING', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_ingestion_task_dataset_ref ON ingestion.ingestion_task (dataset_name, reference_date);
CREATE INDEX IF NOT EXISTS idx_ingestion_task_status ON ingestion.ingestion_task (status, locked_until);

-- 4. Histórico de Execuções e Auditoria (Runs)
CREATE TABLE IF NOT EXISTS ingestion.ingestion_run (
    run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID REFERENCES ingestion.ingestion_task(task_id) ON DELETE SET NULL,
    policy_id           VARCHAR(50) NOT NULL,
    worker_id           VARCHAR(100) NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(20) NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED', 'RETRYING')),
    records_extracted   INT NOT NULL DEFAULT 0,
    records_loaded      INT NOT NULL DEFAULT 0,
    duration_ms         INT,
    http_status         INT,
    error_stack         TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_run_task ON ingestion.ingestion_run(task_id, started_at DESC);

-- 5. Checkpoints de Progresso Confirmado
CREATE TABLE IF NOT EXISTS ingestion.ingestion_checkpoint (
    checkpoint_key      VARCHAR(150) PRIMARY KEY, -- ex: 'b3:daily_quotes:PETR4'
    dataset_name        VARCHAR(50) NOT NULL,
    source_name         VARCHAR(50) NOT NULL,
    partition_id        VARCHAR(50) NOT NULL,     -- Ticker, Série ou Competência
    watermark_date      DATE,                     -- Última data de negócio garantida
    watermark_timestamp TIMESTAMPTZ,              -- Último timestamp de dados ingerido
    last_confirmed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_ingestion_checkpoint_lookup ON ingestion.ingestion_checkpoint (dataset_name, partition_id);

-- 6. Quarentena e Registro de Anomalias de Dados
CREATE TABLE IF NOT EXISTS ingestion.ingestion_issue (
    issue_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id             UUID REFERENCES ingestion.ingestion_task(task_id) ON DELETE SET NULL,
    dataset_name        VARCHAR(50) NOT NULL,
    source_name         VARCHAR(50) NOT NULL,
    entity_id           VARCHAR(50),
    issue_type          VARCHAR(50) NOT NULL, -- 'OHLC_INVALID', 'EXTREME_VARIATION', 'SCHEMA_DRIFT', 'RATE_LIMIT_EXHAUSTED', 'EMPTY_RESPONSE'
    severity            VARCHAR(20) NOT NULL DEFAULT 'WARNING' CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    raw_payload_id      UUID,
    details             JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_resolved         BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_note     TEXT,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingestion_issue_active ON ingestion.ingestion_issue (dataset_name, is_resolved)
WHERE is_resolved = FALSE;

-- 7. Função Atômica para Reserva Concorrente de Tarefas (Lease Pattern via SKIP LOCKED)
CREATE OR REPLACE FUNCTION ingestion.claim_next_task(
    p_worker_id VARCHAR(100),
    p_lease_duration_sec INT DEFAULT 300
)
RETURNS TABLE (
    task_id UUID,
    policy_id VARCHAR(50),
    dataset_name VARCHAR(50),
    target_entity VARCHAR(50),
    reference_date DATE,
    attempts INT,
    lock_token UUID
)
LANGUAGE plpgsql AS $$
DECLARE
    v_token UUID := gen_random_uuid();
BEGIN
    RETURN QUERY
    WITH candidate AS (
        SELECT t.task_id
        FROM ingestion.ingestion_task t
        JOIN ingestion.ingestion_policy ip ON ip.policy_id = t.policy_id
        JOIN ingestion.source_policy sp ON sp.source_name = ip.source_name
        WHERE (t.status = 'PENDING' OR (t.status = 'RUNNING' AND t.locked_until < NOW()))
          AND t.next_run_at <= NOW()
          AND (sp.circuit_state != 'OPEN' OR (sp.circuit_open_until IS NOT NULL AND sp.circuit_open_until <= NOW()))
        ORDER BY t.priority ASC, t.next_run_at ASC
        FOR UPDATE OF t SKIP LOCKED
        LIMIT 1
    )
    UPDATE ingestion.ingestion_task t
    SET status = 'RUNNING',
        lock_token = v_token,
        locked_until = NOW() + (p_lease_duration_sec || ' seconds')::INTERVAL,
        attempts = t.attempts + 1,
        updated_at = NOW()
    FROM candidate c
    WHERE t.task_id = c.task_id
    RETURNING t.task_id, t.policy_id, t.dataset_name, t.target_entity, t.reference_date, t.attempts, t.lock_token;
END;
$$;

-- 8. View Operacional: Saúde da Autonomia e Freshness da Ingestão
CREATE OR REPLACE VIEW ingestion.v_pipeline_health_summary AS
SELECT 
    p.dataset_name,
    p.source_name,
    p.expected_delay_min,
    MAX(c.last_confirmed_at) AS last_success_at,
    ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(c.last_confirmed_at))) / 60) AS minutes_since_last_success,
    CASE 
        WHEN MAX(c.last_confirmed_at) IS NULL THEN 'SEM_EXECUCAO'
        WHEN EXTRACT(EPOCH FROM (NOW() - MAX(c.last_confirmed_at))) / 60 > p.expected_delay_min THEN 'ATRASADO'
        ELSE 'SAUDAVEL'
    END AS sla_status,
    COUNT(t.task_id) FILTER (WHERE t.status = 'PENDING') AS pending_tasks,
    COUNT(t.task_id) FILTER (WHERE t.status = 'RUNNING') AS running_tasks,
    COUNT(t.task_id) FILTER (WHERE t.status = 'FAILED') AS failed_tasks,
    COUNT(i.issue_id) FILTER (WHERE i.is_resolved = FALSE) AS active_quarantine_issues
FROM ingestion.ingestion_policy p
LEFT JOIN ingestion.ingestion_checkpoint c ON c.dataset_name = p.dataset_name
LEFT JOIN ingestion.ingestion_task t ON t.policy_id = p.policy_id
LEFT JOIN ingestion.ingestion_issue i ON i.dataset_name = p.dataset_name
GROUP BY p.dataset_name, p.source_name, p.expected_delay_min;

-- 9. Correção de Confiabilidade Cadastral em core.emissor_companhia
-- Torna CNPJ anulável para emissores internacionais ou pendentes de confirmação
ALTER TABLE core.emissor_companhia ALTER COLUMN cnpj DROP NOT NULL;
ALTER TABLE core.emissor_companhia ADD COLUMN IF NOT EXISTS status_cnpj VARCHAR(30) DEFAULT 'CONFIRMADO';
CREATE UNIQUE INDEX IF NOT EXISTS idx_emissor_nome_sem_cnpj 
    ON core.emissor_companhia(nome_empresarial) 
    WHERE cnpj IS NULL;

-- 10. Seeds Iniciais de Políticas e Fontes
INSERT INTO ingestion.source_policy (source_name, requests_per_minute, max_concurrency, failure_threshold, cooldown_seconds)
VALUES 
    ('b3', 30, 2, 5, 300),
    ('cvm', 60, 4, 5, 180),
    ('bcb', 60, 3, 5, 120),
    ('anbima', 30, 2, 5, 300),
    ('status_invest', 20, 2, 4, 600),
    ('investidor10', 20, 2, 4, 600),
    ('fundamentei', 20, 2, 4, 600),
    ('fundamentus', 30, 2, 4, 300),
    ('clube_fii', 25, 2, 4, 300),
    ('funds_explorer', 20, 2, 4, 300),
    ('fiis_com_br', 25, 2, 4, 300),
    ('mais_retorno', 25, 2, 4, 300),
    ('analise_de_acoes', 25, 2, 4, 300),
    ('brick360', 25, 2, 4, 300),
    ('advfn', 20, 2, 4, 300),
    ('etfs_brasil', 30, 2, 4, 300)
ON CONFLICT (source_name) DO UPDATE SET
    requests_per_minute = EXCLUDED.requests_per_minute,
    max_concurrency = EXCLUDED.max_concurrency,
    cooldown_seconds = EXCLUDED.cooldown_seconds;

INSERT INTO ingestion.ingestion_policy (policy_id, dataset_name, source_name, calendar_type, cron_expression, priority, expected_delay_min)
VALUES 
    ('b3_daily_quotes', 'cotacao_diaria', 'b3', 'B3_TRADING_DAYS', '0 19 * * 1-5', 2, 180),
    ('b3_instrument_cad', 'cadastro_instrumento', 'b3', 'CALENDAR_DAYS', '0 22 * * 1-5', 4, 1440),
    ('cvm_quarterly_dfp', 'demonstrativo_financeiro', 'cvm', 'BUSINESS_DAYS', '0 7,19 * * 1-5', 3, 720),
    ('cvm_fii_monthly', 'informe_mensal_fii', 'cvm', 'BUSINESS_DAYS', '0 9 * * 1-5', 3, 720),
    ('cvm_funds_daily', 'cota_fundo_investimento', 'cvm', 'BUSINESS_DAYS', '30 8 * * 2-6', 3, 360),
    ('bcb_macro_series', 'serie_macro', 'bcb', 'BUSINESS_DAYS', '0 14,18,20 * * 1-5', 2, 240),
    ('anbima_indicativos', 'taxa_anbima', 'anbima', 'BUSINESS_DAYS', '0 18 * * 1-5', 3, 360),
    ('etfs_brasil_taxas', 'etf_taxas_nav', 'etfs_brasil', 'B3_TRADING_DAYS', '0 20 * * 1-5', 4, 720),
    ('intraday_quotes_fast', 'cotacao_intraday', 'b3', 'B3_TRADING_DAYS', '*/15 10-18 * * 1-5', 1, 30)
ON CONFLICT (policy_id) DO UPDATE SET
    dataset_name = EXCLUDED.dataset_name,
    source_name = EXCLUDED.source_name,
    cron_expression = EXCLUDED.cron_expression,
    priority = EXCLUDED.priority,
    expected_delay_min = EXCLUDED.expected_delay_min;
