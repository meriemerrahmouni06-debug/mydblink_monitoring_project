SELECT
    TRIM(source_instance) AS source_instance,
    date_collecte,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    sql_text,
    execution_count,
    cpu_time,
    total_elapsed_time,
    creation_time,
    inserted_at
FROM {{ source('raw_data', 'raw_activetransactions') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''
AND creation_time IS NOT NULL