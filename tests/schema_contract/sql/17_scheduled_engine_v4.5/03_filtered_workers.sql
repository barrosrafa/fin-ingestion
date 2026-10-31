-- Dedicated workers claim only their configured datasets, using the same durable queue.
CREATE OR REPLACE FUNCTION ingestion.claim_next_task_filtered(
    p_worker_id text,p_lease_duration_sec integer,p_datasets text[]
) RETURNS SETOF ingestion.ingestion_task LANGUAGE sql AS $$
WITH candidate AS (
    SELECT t.task_id FROM ingestion.ingestion_task t
    JOIN ingestion.ingestion_policy ip USING(policy_id)
    JOIN ingestion.source_policy sp ON sp.source_name=ip.source_name
    WHERE t.dataset_name=ANY(p_datasets) AND ip.is_active
      AND (t.status='PENDING' OR (t.status='RUNNING' AND t.locked_until<now()))
      AND t.next_run_at<=now() AND t.attempts<t.max_attempts
      AND (sp.circuit_state!='OPEN' OR sp.circuit_open_until<=now())
    ORDER BY t.priority,t.next_run_at
    FOR UPDATE OF t SKIP LOCKED LIMIT 1
)
UPDATE ingestion.ingestion_task t SET status='RUNNING',lock_token=gen_random_uuid(),
    locked_until=now()+make_interval(secs=>p_lease_duration_sec),
    attempts=t.attempts+1,updated_at=now()
FROM candidate c WHERE t.task_id=c.task_id RETURNING t.*;
$$;
