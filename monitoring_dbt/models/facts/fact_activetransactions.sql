SELECT
    db.database_id,
    at.date_collecte,
    at.sql_text,
    at.execution_count,
    at.cpu_time,
    at.total_elapsed_time,
    at.creation_time,
    at.inserted_at
FROM {{ ref('stg_activetransactions') }} at
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(at.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(at.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE at.database_name IS NOT NULL
AND at.creation_time IS NOT NULL
