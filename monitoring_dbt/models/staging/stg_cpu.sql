SELECT
    cpu_id,
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    cpu_idle,
    cpu_sql,
    date_collecte,
    date_collecte2,
    inserted_at
FROM {{ source('raw_data', 'raw_cpu') }}
WHERE cpu_id IS NOT NULL