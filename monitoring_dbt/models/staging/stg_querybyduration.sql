SELECT
    query_id,
    TRIM(source_instance) AS source_instance,
    date_collecte,
    date_collecte2,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    session_id,
    duration_ms,
    cpu_time_ms,
    wait_time,
    logical_reads,
    statement_text,
    inserted_at
FROM {{ source('raw_data', 'raw_querybyduration') }}
WHERE query_id IS NOT NULL