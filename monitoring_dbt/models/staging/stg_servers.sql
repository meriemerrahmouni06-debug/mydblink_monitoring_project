SELECT
    TRIM(source_instance) AS source_instance ,
    server_id,
    TRIM(server_name) AS server_name,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_servers') }}
WHERE server_id IS NOT NULL  AND  server_name IS NOT NULL
AND TRIM(server_name) <> ''