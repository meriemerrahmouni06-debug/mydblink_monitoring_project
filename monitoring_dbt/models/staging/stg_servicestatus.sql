SELECT
    service_id,
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(service_name) AS service_name,
    TRIM(status) AS status,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_servicestatus') }}
WHERE service_id IS NOT NULL
AND service_name IS NOT NULL
AND TRIM(service_name) <> ''