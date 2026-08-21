SELECT
    backup_id,
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(backup_type) AS backup_type,
    start_date,
    duration,
    size_mb,
    copy_only,
    compressed,
    encrypted,
    TRIM(location) AS location,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_backuphistory') }}
WHERE backup_id IS NOT NULL
AND database_name IS NOT NULL
AND TRIM(database_name) <> ''