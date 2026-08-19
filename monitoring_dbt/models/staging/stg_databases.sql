SELECT
    TRIM(source_instance) AS source_instance ,
    database_id,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name ,
    TRIM(database_name) AS database_name,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_databases') }}
WHERE database_id IS NOT NULL 
AND database_name IS NOT NULL AND TRIM(database_name) <> ''