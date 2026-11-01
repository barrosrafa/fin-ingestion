-- Preserve the precision published by B3, including small per-share amounts.
-- Temporarily detach dependent views in dependency order. Keep definitions,
-- owners, indexes, comments and explicit grants; fail rather than CASCADE.
-- Materialized data is refreshed by the ingestion operation, never by DDL.
DO $views$
DECLARE
    objects jsonb;
    obj jsonb;
    grant_row jsonb;
    index_sql text;
    qualified_name text;
    object_kind text;
BEGIN
    WITH RECURSIVE dependencies AS (
        SELECT 'market.provento'::regclass::oid AS oid, 0 AS depth
        UNION ALL
        SELECT c.oid, parent.depth + 1
        FROM dependencies parent
        JOIN pg_depend dep ON dep.refobjid=parent.oid
        JOIN pg_rewrite r ON r.oid=dep.objid
        JOIN pg_class c ON c.oid=r.ev_class
        WHERE c.relkind IN ('v','m') AND c.oid<>parent.oid AND parent.depth<32
    ), ordered AS (
        SELECT oid,max(depth) AS depth FROM dependencies WHERE depth>0 GROUP BY oid
    )
    SELECT coalesce(jsonb_agg(jsonb_build_object(
        'schema',n.nspname,'name',c.relname,'kind',c.relkind,
        'definition',pg_get_viewdef(c.oid,true),'owner',pg_get_userbyid(c.relowner),
        'options',c.reloptions,
        'comment',obj_description(c.oid,'pg_class'),
        'indexes',(SELECT coalesce(jsonb_agg(pg_get_indexdef(i.indexrelid)),'[]'::jsonb)
                   FROM pg_index i WHERE i.indrelid=c.oid),
        'grants',(SELECT coalesce(jsonb_agg(jsonb_build_object(
            'grantee',CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END,
            'privilege',a.privilege_type,'grantable',a.is_grantable)),'[]'::jsonb)
            FROM aclexplode(c.relacl) a WHERE a.grantee<>c.relowner)
    ) ORDER BY ordered.depth,c.oid),'[]'::jsonb) INTO objects
    FROM ordered JOIN pg_class c USING(oid) JOIN pg_namespace n ON n.oid=c.relnamespace;

    FOR obj IN SELECT value FROM jsonb_array_elements(objects) WITH ORDINALITY ORDER BY ordinality DESC LOOP
        object_kind := CASE WHEN obj->>'kind'='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END;
        EXECUTE format('DROP %s %I.%I',object_kind,obj->>'schema',obj->>'name');
    END LOOP;

    ALTER TABLE market.provento ALTER COLUMN valor_por_acao TYPE numeric(24,11);

    FOR obj IN SELECT value FROM jsonb_array_elements(objects) LOOP
        qualified_name := format('%I.%I',obj->>'schema',obj->>'name');
        object_kind := CASE WHEN obj->>'kind'='m' THEN 'MATERIALIZED VIEW' ELSE 'VIEW' END;
        EXECUTE format('CREATE %s %s %s AS %s %s',object_kind,qualified_name,
            CASE WHEN obj->'options' IS NULL OR obj->'options'='null'::jsonb THEN ''
                 ELSE 'WITH (' || (SELECT string_agg(value,',') FROM jsonb_array_elements_text(obj->'options')) || ')' END,
            rtrim(obj->>'definition',E';\n '),
            CASE WHEN obj->>'kind'='m' THEN 'WITH NO DATA' ELSE '' END);
        FOR index_sql IN SELECT jsonb_array_elements_text(obj->'indexes') LOOP
            EXECUTE index_sql;
        END LOOP;
        FOR grant_row IN SELECT value FROM jsonb_array_elements(obj->'grants') LOOP
            EXECUTE format('GRANT %s ON %s TO %s %s',grant_row->>'privilege',qualified_name,
                grant_row->>'grantee',CASE WHEN (grant_row->>'grantable')::boolean THEN 'WITH GRANT OPTION' ELSE '' END);
        END LOOP;
        IF obj->>'comment' IS NOT NULL THEN
            EXECUTE format('COMMENT ON %s %s IS %L',object_kind,qualified_name,obj->>'comment');
        END IF;
        EXECUTE format('ALTER %s %s OWNER TO %I',object_kind,qualified_name,obj->>'owner');
    END LOOP;
END $views$;

-- Equal installments with the same ex-date are separate payment events.
DROP INDEX IF EXISTS market.uq_provento_ativo_tipo_ex_valor;
ALTER TABLE market.provento ADD COLUMN IF NOT EXISTS fonte_provento text;
ALTER TABLE market.provento ADD COLUMN IF NOT EXISTS payload_fonte jsonb;

CREATE TABLE IF NOT EXISTS ingestion.provento_quarentena (
    id_provento bigint PRIMARY KEY,
    registro_original jsonb NOT NULL,
    motivo text NOT NULL,
    archived_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ingestion.provento_snapshot (
    sha256 text PRIMARY KEY,
    emissor text NOT NULL,
    fonte text NOT NULL,
    payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now()
);
DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='fin_ingestion_rw') THEN
        GRANT SELECT,INSERT ON ingestion.provento_quarentena,
            ingestion.provento_snapshot TO fin_ingestion_rw;
    END IF;
END $grants$;
