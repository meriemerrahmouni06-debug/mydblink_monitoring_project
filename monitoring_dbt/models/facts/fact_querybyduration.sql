SELECT
    i.instance_id,
    db.database_id,
    qd.query_id,
    qd.date_collecte,
    qd.date_collecte2,
    qd.session_id,
    qd.duration_ms,
    qd.cpu_time_ms,
    qd.wait_time,
    qd.logical_reads,
    qd.statement_text,
    qd.inserted_at
FROM {{ ref('stg_querybyduration') }} qd
INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(qd.instance_name)) = TRIM(LOWER(i.instance_name))
LEFT JOIN {{ ref('dim_databases') }} db
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(qd.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE qd.query_id IS NOT NULL
