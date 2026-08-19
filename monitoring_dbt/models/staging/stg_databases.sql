SELECT
    id,
    metric,
    source_instance,
    database_id,
    server_name,
    instance_name,
    database_name,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_databases') }}