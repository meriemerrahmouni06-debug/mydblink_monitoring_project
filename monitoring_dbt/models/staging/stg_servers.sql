SELECT
    id,
    metric,
    source_instance,
    server_id,
    server_name,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_servers') }}