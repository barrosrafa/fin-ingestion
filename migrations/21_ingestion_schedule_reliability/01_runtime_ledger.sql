-- Durable occurrences for the supervised/Actions profile runtime.
-- No policy activation or operational seeds belong in this schema migration.
CREATE TABLE IF NOT EXISTS ingestion.runtime_cursor (
    profile text PRIMARY KEY,
    through_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion.runtime_job (
    job_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    profile text NOT NULL,
    scheduled_at timestamptz NOT NULL,
    reference_date date NOT NULL,
    partition_key text NOT NULL,
    parameters jsonb NOT NULL,
    status text NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','EXPIRED')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lock_token uuid,
    locked_until timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    last_error_type text,
    receipt jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (profile, scheduled_at, partition_key)
);
CREATE INDEX IF NOT EXISTS runtime_job_due ON ingestion.runtime_job(next_attempt_at,scheduled_at)
    WHERE status IN ('PENDING','RUNNING');

CREATE TABLE IF NOT EXISTS ingestion.runtime_heartbeat (
    worker_id text PRIMARY KEY,
    release_sha text NOT NULL,
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE OR REPLACE VIEW ingestion.v_runtime_health AS
SELECT profile, count(*) FILTER (WHERE status='PENDING') AS pending,
    count(*) FILTER (WHERE status='RUNNING') AS running,
    count(*) FILTER (WHERE status='FAILED') AS failed,
    count(*) FILTER (WHERE status='EXPIRED') AS expired,
    max(scheduled_at) FILTER (WHERE status='SUCCEEDED') AS last_succeeded_slot,
    min(scheduled_at) FILTER (WHERE status IN ('PENDING','RUNNING')) AS oldest_open_slot
FROM ingestion.runtime_job GROUP BY profile;

DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='fin_ingestion_rw') THEN
        GRANT USAGE ON SCHEMA ingestion TO fin_ingestion_rw;
        GRANT SELECT,INSERT,UPDATE ON ingestion.runtime_cursor,ingestion.runtime_job,
            ingestion.runtime_heartbeat TO fin_ingestion_rw;
        GRANT SELECT ON ingestion.v_runtime_health TO fin_ingestion_rw;
    END IF;
END $grants$;
