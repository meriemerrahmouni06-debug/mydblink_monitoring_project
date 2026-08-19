SELECT
    TRIM(source_instance) AS source_instance,
    instance_id,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_instances') }}
WHERE TRIM(instance_name) <> '' AND instance_id IS NOT NULL AND 
instance_name IS NOT NULL