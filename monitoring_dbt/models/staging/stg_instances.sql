SELECT
    id,
    metric,
    source_instance,
    instance_id,
    server_name,
    instance_name,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_instances') }}