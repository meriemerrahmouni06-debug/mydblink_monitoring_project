SELECT
    memory_id,
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    total_osmemory,
    avalaible_memory,
    date_collecte,
    date_collecte2,
    inserted_at
FROM {{ source('raw_data', 'raw_memory') }}
WHERE memory_id IS NOT NULL